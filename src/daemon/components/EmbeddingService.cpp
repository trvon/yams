#include <yams/daemon/components/EmbeddingService.h>

#include <spdlog/spdlog.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/api/content_store.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/vector_database.h>

namespace yams::daemon {

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

    while (!stop_.load()) {
        InternalEventBus::EmbedJob job;
        if (embedChannel_ && embedChannel_->try_pop(job)) {
            co_await processEmbedJob(job);
        } else {
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }
}

boost::asio::awaitable<void>
EmbeddingService::processEmbedJob(const InternalEventBus::EmbedJob& job) {
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
        co_return;
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

            auto r = yams::ingest::embed_and_insert_document(
                *provider, modelName, *vdb, *meta_, hash, text, docInfo.fileName, docInfo.filePath,
                docInfo.mimeType, ccfg);

            if (!r) {
                spdlog::warn("EmbeddingService: embed/insert failed for {}: {}", hash,
                             r.error().message);
                failed_.fetch_add(1);
            } else {
                spdlog::debug("EmbeddingService: successfully generated {} embeddings for {}",
                              r.value(), hash);
                succeeded++;
            }
        } catch (const std::exception& e) {
            spdlog::error("EmbeddingService: exception processing {}: {}", hash, e.what());
            failed_.fetch_add(1);
        }
    }

    processed_.fetch_add(succeeded);

    spdlog::debug("EmbeddingService: batch complete (succeeded={}, skipped={}, failed={})",
                  succeeded, skipped, job.hashes.size() - succeeded - skipped);
    co_return;
}

} // namespace yams::daemon
