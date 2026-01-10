#include <yams/daemon/components/EmbeddingService.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <thread>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/TuneAdvisor.h>
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
    // Use configurable channel capacity from TuneAdvisor
    const std::size_t capacity = TuneAdvisor::embedChannelCapacity();
    embedChannel_ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", capacity);

    if (!embedChannel_) {
        return Error{ErrorCode::InvalidOperation,
                     "Failed to create embedding channel on InternalBus"};
    }

    spdlog::info("EmbeddingService: initialized with channel capacity {}", capacity);
    return Result<void>();
}

void EmbeddingService::start() {
    stop_.store(false);
    boost::asio::co_spawn(strand_, channelPoller(), boost::asio::detached);
    spdlog::info("EmbeddingService: started parallel channel poller");
}

void EmbeddingService::shutdown() {
    if (stop_.exchange(true)) {
        return;
    }
    spdlog::info("EmbeddingService: shutting down (processed={}, failed={}, inFlight={})",
                 processed_.load(), failed_.load(), inFlight_.load());

    // Wait for in-flight jobs to complete with timeout
    constexpr int kMaxWaitMs = 5000;
    constexpr int kPollIntervalMs = 50;
    int waited = 0;
    while (inFlight_.load() > 0 && waited < kMaxWaitMs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollIntervalMs));
        waited += kPollIntervalMs;
    }
    if (inFlight_.load() > 0) {
        spdlog::warn("EmbeddingService: shutdown timeout, {} jobs still in flight",
                     inFlight_.load());
    } else {
        spdlog::info("EmbeddingService: all in-flight jobs completed");
    }
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
    return embedChannel_ ? embedChannel_->size_approx() : 0;
}

std::size_t EmbeddingService::inFlightJobs() const {
    return inFlight_.load();
}

boost::asio::awaitable<void> EmbeddingService::channelPoller() {
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    auto idleDelay = std::chrono::milliseconds(1);
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(50);

    spdlog::info("[EmbeddingService] Parallel poller started");

    while (!stop_.load()) {
        bool didWork = false;
        InternalEventBus::EmbedJob job;

        // Dynamic concurrency limit from TuneAdvisor (scaled by TuningManager)
        const std::size_t maxConcurrent = TuneAdvisor::postEmbedConcurrent();

        // Dispatch jobs up to concurrency limit
        while (inFlight_.load() < maxConcurrent && embedChannel_ && embedChannel_->try_pop(job)) {
            didWork = true;
            inFlight_.fetch_add(1);
            InternalEventBus::instance().incEmbedConsumed();

            // Dispatch to work executor for parallel processing
            boost::asio::post(coordinator_->getExecutor(), [this, job = std::move(job)]() mutable {
                processEmbedJob(std::move(job));
                inFlight_.fetch_sub(1);
            });
        }

        if (didWork) {
            idleDelay = std::chrono::milliseconds(1);
            continue; // Check for more work immediately
        }

        // Idle - wait before polling again
        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay *= 2;
        }
    }

    spdlog::info("[EmbeddingService] Parallel poller exited");
}

void EmbeddingService::processEmbedJob(InternalEventBus::EmbedJob job) {
    spdlog::debug("[EmbeddingService] processEmbedJob called with {} hashes", job.hashes.size());
    std::shared_ptr<IModelProvider> provider;
    std::string modelName;
    std::shared_ptr<yams::vector::VectorDatabase> vdb;

    if (getModelProvider_)
        provider = getModelProvider_();
    if (getPreferredModel_)
        modelName = getPreferredModel_();
    if (getVectorDatabase_)
        vdb = getVectorDatabase_();

    spdlog::debug("[EmbeddingService] Callbacks: provider={} model='{}' vdb={}",
                  provider ? "yes" : "no", modelName, vdb ? "yes" : "no");

    if (!job.modelName.empty()) {
        modelName = job.modelName;
    }

    if (!provider || modelName.empty() || !vdb) {
        spdlog::warn("EmbeddingService: providers unavailable for batch of {} documents "
                     "(provider={}, model='{}', vdb={})",
                     job.hashes.size(), provider ? "available" : "null", modelName,
                     vdb ? "available" : "null");
        failed_.fetch_add(job.hashes.size());
        return;
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

            if (job.skipExisting && vdb->hasEmbedding(hash)) {
                spdlog::debug("EmbeddingService: skipExisting=true, already embedded: {}", hash);
                skipped++;
                continue;
            }

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
                    // Mark repair as completed
                    (void)meta_->updateDocumentRepairStatus(hash,
                                                            metadata::RepairStatus::Completed);
                    break;
                }

                const auto& msg = r.error().message;
                const bool isDeadlock = msg.find("deadlock") != std::string::npos ||
                                        msg.find("resource deadlock") != std::string::npos;
                const bool isBusy = msg.find("busy") != std::string::npos ||
                                    msg.find("locked") != std::string::npos;

                const bool retryable = (isDeadlock || isBusy);

                if (!retryable || attempt == maxAttempts) {
                    spdlog::warn("EmbeddingService: embed/insert failed for {} (attempt {}/{}): {}",
                                 hash, attempt, maxAttempts, msg);
                    failed_.fetch_add(1);
                    // Mark repair as failed after exhausting retries
                    (void)meta_->updateDocumentRepairStatus(hash, metadata::RepairStatus::Failed);
                    done = true;
                    break;
                }

                spdlog::warn("EmbeddingService: retrying {} after {} ms (attempt {}/{}): {}", hash,
                             backoff.count(), attempt, maxAttempts, msg);
                std::this_thread::sleep_for(backoff);
                backoff = std::min<std::chrono::milliseconds>(backoff * 2, std::chrono::seconds(2));
            }
        } catch (const std::exception& e) {
            spdlog::error("EmbeddingService: exception processing {}: {}", hash, e.what());
            failed_.fetch_add(1);
            // Mark repair as failed on exception
            (void)meta_->updateDocumentRepairStatus(hash, metadata::RepairStatus::Failed);
        }
    }

    processed_.fetch_add(succeeded);

    spdlog::debug("EmbeddingService: batch complete (succeeded={}, skipped={}, failed={})",
                  succeeded, skipped, job.hashes.size() - succeeded - skipped);
}

} // namespace daemon
} // namespace yams
