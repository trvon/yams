#include <yams/daemon/components/EmbeddingService.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/vector_database.h>

namespace yams {
namespace daemon {

EmbeddingService::EmbeddingService(std::shared_ptr<api::IContentStore> store,
                                                                     std::shared_ptr<metadata::MetadataRepository> meta,
                                                                     WorkCoordinator* coordinator)
        : store_(std::move(store)), meta_(std::move(meta)), coordinator_(coordinator),
            strand_(coordinator_->makeStrand()) {}

EmbeddingService::~EmbeddingService() {
    shutdown();
}

Result<void> EmbeddingService::initialize() {
    embedChannel_ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", 2048);

    if (!embedChannel_) {
        return Error{ErrorCode::InvalidOperation,
                     "Failed to create embedding channel on InternalBus"};
    }

    spdlog::info("EmbeddingService: initialized");
    return Result<void>();
}

void EmbeddingService::start() {
    stop_.store(false);
    boost::asio::co_spawn(strand_, channelPoller(), boost::asio::detached);
    spdlog::info("EmbeddingService: started channel poller");
}

void EmbeddingService::shutdown() {
    if (stop_.exchange(true)) {
        return;
    }
    spdlog::info("EmbeddingService: shutting down (processed={}, failed={})", processed_.load(),
                 failed_.load());
}

void EmbeddingService::setProviders(
    std::function<std::shared_ptr<IModelProvider>()> providerGetter,
    std::function<std::string()> modelNameGetter,
    std::function<std::shared_ptr<yams::vector::VectorDatabase>()> dbGetter) {
    getModelProvider_ = std::move(providerGetter);
    getPreferredModel_ = std::move(modelNameGetter);
    getVectorDatabase_ = std::move(dbGetter);
}

std::size_t EmbeddingService::queuedJobs() const {
    return 0;
}

boost::asio::awaitable<void> EmbeddingService::channelPoller() {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    // Track consecutive failures to implement backoff when deadlocked
    std::size_t consecutiveDeadlocks = 0;
    std::chrono::milliseconds cooldown(0);

    while (!stop_.load()) {
        // Apply cooldown if we've hit repeated deadlocks
        if (cooldown.count() > 0) {
            spdlog::debug("EmbeddingService: cooling down for {} ms after {} consecutive deadlocks",
                          cooldown.count(), consecutiveDeadlocks);
            timer.expires_after(cooldown);
            co_await timer.async_wait(boost::asio::use_awaitable);
            cooldown = std::chrono::milliseconds(0);
        }

        InternalEventBus::EmbedJob job;
        if (embedChannel_ && embedChannel_->try_pop(job)) {
            // Process job inline (not co_spawn) to ensure true serialization.
            // This avoids executor thread pool concurrency issues.
            auto result = co_await processEmbedJobWithStatus(job);
            
            if (result.deadlockDetected) {
                consecutiveDeadlocks++;
                // Exponential backoff: 500ms, 1s, 2s, 4s, max 10s
                cooldown = std::chrono::milliseconds(
                    std::min<int64_t>(500 * (1 << std::min<std::size_t>(consecutiveDeadlocks, 4)), 10000));
                spdlog::warn("EmbeddingService: deadlock detected, will cooldown for {} ms",
                             cooldown.count());
            } else {
                consecutiveDeadlocks = 0;
            }
        } else {
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }
}

boost::asio::awaitable<EmbeddingService::EmbedJobResult>
EmbeddingService::processEmbedJobWithStatus(const InternalEventBus::EmbedJob& job) {
    EmbedJobResult result;
    std::shared_ptr<IModelProvider> provider;
    std::string modelName;
    std::shared_ptr<yams::vector::VectorDatabase> vdb;

    if (getModelProvider_)
        provider = getModelProvider_();
    if (getPreferredModel_)
        modelName = getPreferredModel_();
    if (getVectorDatabase_)
        vdb = getVectorDatabase_();

    if (!job.modelName.empty()) {
        modelName = job.modelName;
    }

    if (!provider || modelName.empty() || !vdb) {
        spdlog::warn("EmbeddingService: providers unavailable for batch of {} documents "
                     "(provider={}, model='{}', vdb={})",
                     job.hashes.size(), provider ? "available" : "null", modelName,
                     vdb ? "available" : "null");
        failed_.fetch_add(job.hashes.size());
        co_return result;
    }

    spdlog::debug("EmbeddingService: processing batch of {} documents with model '{}'",
                  job.hashes.size(), modelName);

    std::size_t succeeded = 0;
    std::size_t skipped = 0;

    for (const auto& hash : job.hashes) {
        try {
            auto docInfoRes = meta_->getDocumentByHash(hash);
            if (!docInfoRes || !docInfoRes.value().has_value()) {
                spdlog::warn("EmbeddingService: document not found: {}", hash);
                failed_.fetch_add(1);
                continue;
            }

            const auto& docInfo = *docInfoRes.value();
            int64_t docId = docInfo.id;

            auto contentOpt = meta_->getContent(docId);
            if (!contentOpt || !contentOpt.value().has_value()) {
                spdlog::debug("EmbeddingService: no content for document {}", hash);
                failed_.fetch_add(1);
                continue;
            }

            const auto& text = contentOpt.value().value().contentText;
            if (text.empty()) {
                spdlog::debug("EmbeddingService: empty content for document {}", hash);
                skipped++;
                continue;
            }

            yams::vector::ChunkingConfig ccfg{};
            spdlog::debug("EmbeddingService: generating embeddings for {} using model '{}'", hash,
                          modelName);

            const int maxAttempts = 5;
            std::chrono::milliseconds backoff(100);
            bool done = false;

            for (int attempt = 1; attempt <= maxAttempts && !done; ++attempt) {
                auto r = yams::ingest::embed_and_insert_document(
                    *provider, modelName, *vdb, *meta_, hash, text, docInfo.fileName,
                    docInfo.filePath, docInfo.mimeType, ccfg);

                if (r) {
                    spdlog::debug("EmbeddingService: successfully generated {} embeddings for {}",
                                  r.value(), hash);
                    succeeded++;
                    done = true;
                    break;
                }

                const auto& msg = r.error().message;
                const bool isDeadlock =
                    msg.find("deadlock") != std::string::npos ||
                    msg.find("resource deadlock") != std::string::npos;
                const bool isBusy =
                    msg.find("busy") != std::string::npos || msg.find("locked") != std::string::npos;

                // On Windows, MSVC can surface EDEADLK as "resource deadlock would occur".
                // That may be wrapped as InternalError by higher layers (e.g., provider failures),
                // so don't gate retries purely on ErrorCode::DatabaseError.
                const bool retryable = (isDeadlock || isBusy);

                if (isDeadlock) {
                    result.deadlockDetected = true;
                }

                if (!retryable || attempt == maxAttempts) {
                    spdlog::warn("EmbeddingService: embed/insert failed for {} (attempt {}/{}): {}",
                                 hash, attempt, maxAttempts, msg);
                    failed_.fetch_add(1);
                    done = true;
                    break;
                }

                spdlog::warn(
                    "EmbeddingService: retrying {} after {} ms (attempt {}/{}): {}",
                    hash, backoff.count(), attempt, maxAttempts, msg);
                boost::asio::steady_timer backoffTimer(co_await boost::asio::this_coro::executor);
                backoffTimer.expires_after(backoff);
                co_await backoffTimer.async_wait(boost::asio::use_awaitable);
                backoff = std::min<std::chrono::milliseconds>(backoff * 2, std::chrono::seconds(2));
            }
        } catch (const std::exception& e) {
            // std::async failures in model loading show as "resource deadlock would occur"
            if (std::string(e.what()).find("deadlock") != std::string::npos ||
                std::string(e.what()).find("resource deadlock") != std::string::npos) {
                result.deadlockDetected = true;
                spdlog::warn("EmbeddingService: deadlock exception processing {}: {}", hash, e.what());
            } else {
                spdlog::error("EmbeddingService: exception processing {}: {}", hash, e.what());
            }
            failed_.fetch_add(1);
        }
    }

    processed_.fetch_add(succeeded);

    spdlog::debug("EmbeddingService: batch complete (succeeded={}, skipped={}, failed={})",
                  succeeded, skipped, job.hashes.size() - succeeded - skipped);
    co_return result;
}

} // namespace daemon
} // namespace yams
