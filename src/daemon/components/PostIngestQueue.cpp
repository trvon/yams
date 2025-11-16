#include <spdlog/spdlog.h>
#include <regex>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/api/content_store.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/extraction/extraction_util.h>
#include <yams/extraction/text_extractor.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

using yams::extraction::util::extractDocumentText;

namespace yams::daemon {

PostIngestQueue::PostIngestQueue(
    std::shared_ptr<api::IContentStore> store, std::shared_ptr<metadata::MetadataRepository> meta,
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors,
    std::shared_ptr<metadata::KnowledgeGraphStore> kg,
    std::shared_ptr<GraphComponent> graphComponent, WorkCoordinator* coordinator,
    std::size_t capacity)
    : store_(std::move(store)), meta_(std::move(meta)), extractors_(std::move(extractors)),
      kg_(std::move(kg)), graphComponent_(std::move(graphComponent)), coordinator_(coordinator),
      strand_(coordinator_->makeStrand()), capacity_(capacity ? capacity : 1000) {
    spdlog::info("[PostIngestQueue] Created with strand from WorkCoordinator");
}

PostIngestQueue::~PostIngestQueue() {
    stop();
}

void PostIngestQueue::start() {
    if (!stop_.load()) {
        boost::asio::co_spawn(strand_, channelPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Channel poller started");
    }
}

void PostIngestQueue::stop() {
    stop_.store(true);
    spdlog::info("[PostIngestQueue] Stop requested");
}

boost::asio::awaitable<void> PostIngestQueue::channelPoller() {
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", 4096);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    while (!stop_.load()) {
        InternalEventBus::PostIngestTask task;
        if (channel->try_pop(task)) {
            try {
                co_await processMetadataStage(task.hash, task.mime);

                using namespace boost::asio::experimental;
                auto ex = co_await boost::asio::this_coro::executor;

                auto result = co_await make_parallel_group(
                                  co_spawn(ex, processKnowledgeGraphStage(task.hash, task.mime),
                                           boost::asio::deferred),
                                  co_spawn(ex, processEmbeddingStage(task.hash, task.mime),
                                           boost::asio::deferred))
                                  .async_wait(wait_for_all(), boost::asio::use_awaitable);

                processed_++;
                InternalEventBus::instance().incPostConsumed();
            } catch (const std::exception& e) {
                spdlog::error("[PostIngestQueue] Failed to process {}: {}", task.hash, e.what());
                failed_++;
            }
        } else {
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    spdlog::info("[PostIngestQueue] Channel poller exited");
}

void PostIngestQueue::enqueue(Task t) {
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", 4096);

    InternalEventBus::PostIngestTask task;
    task.hash = std::move(t.hash);
    task.mime = std::move(t.mime);

    if (!channel->try_push(task)) {
        spdlog::warn("[PostIngestQueue] Channel full, dropping task");
    }
}

bool PostIngestQueue::tryEnqueue(const Task& t) {
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", 4096);

    InternalEventBus::PostIngestTask task;
    task.hash = t.hash;
    task.mime = t.mime;

    return channel->try_push(task);
}

bool PostIngestQueue::tryEnqueue(Task&& t) {
    return tryEnqueue(t);
}

std::size_t PostIngestQueue::size() const {
    // Channel size not available, return 0
    return 0;
}

boost::asio::awaitable<void> PostIngestQueue::processMetadataStage(const std::string& hash,
                                                                   const std::string& mime) {
    if (!store_ || !meta_) {
        spdlog::warn("[PostIngestQueue] store or metadata unavailable; dropping task {}", hash);
        co_return;
    }

    try {
        auto start = std::chrono::steady_clock::now();

        int64_t docId = -1;
        std::string fileName;
        std::string mimeType = mime;
        std::string extension;

        auto infoRes = meta_->getDocumentByHash(hash);
        if (infoRes && infoRes.value().has_value()) {
            const auto& info = *infoRes.value();
            docId = info.id;
            if (!info.fileName.empty())
                fileName = info.fileName;
            if (!info.mimeType.empty())
                mimeType = info.mimeType;
            if (!info.fileExtension.empty())
                extension = info.fileExtension;
        }

        auto txt = extractDocumentText(store_, hash, mimeType, extension, extractors_);
        if (!txt || txt->empty()) {
            spdlog::debug("[PostIngestQueue] no text extracted for {} (mime={})", hash, mimeType);
            if (docId >= 0) {
                auto d = meta_->getDocument(docId);
                if (d && d.value().has_value()) {
                    auto updated = d.value().value();
                    updated.contentExtracted = false;
                    updated.extractionStatus = metadata::ExtractionStatus::Skipped;
                    (void)meta_->updateDocument(updated);
                }
            }
        } else if (docId >= 0) {
            auto pr = yams::ingest::persist_content_and_index(*meta_, docId, fileName, *txt,
                                                              mimeType, "post_ingest");
            if (!pr) {
                spdlog::warn("[PostIngestQueue] persist/index failed for {}: {}", hash,
                             pr.error().message);
            } else {
                auto duration = std::chrono::steady_clock::now() - start;
                double ms = std::chrono::duration<double, std::milli>(duration).count();
                spdlog::debug("[PostIngestQueue] Metadata stage completed for {} in {:.2f}ms", hash,
                              ms);
            }
        }
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Metadata stage failed for {}: {}", hash, e.what());
    }
}
boost::asio::awaitable<void>
PostIngestQueue::processKnowledgeGraphStage(const std::string& hash, const std::string& /*mime*/) {
    if (!graphComponent_ || !meta_) {
        co_return;
    }

    try {
        auto start = std::chrono::steady_clock::now();

        auto infoRes = meta_->getDocumentByHash(hash);
        if (!infoRes || !infoRes.value().has_value()) {
            co_return;
        }

        const auto& doc = *infoRes.value();
        auto tagsRes = meta_->getDocumentTags(doc.id);
        std::vector<std::string> tags;
        if (tagsRes && !tagsRes.value().empty()) {
            tags = tagsRes.value();
        }

        GraphComponent::DocumentGraphContext ctx{.documentHash = hash,
                                                 .filePath = doc.fileName,
                                                 .tags = std::move(tags),
                                                 .documentDbId = doc.id};

        auto result = graphComponent_->onDocumentIngested(ctx);
        if (!result) {
            spdlog::warn("[PostIngestQueue] Graph ingestion failed for {}: {}", hash,
                         result.error().message);
        } else {
            auto duration = std::chrono::steady_clock::now() - start;
            double ms = std::chrono::duration<double, std::milli>(duration).count();
            spdlog::debug("[PostIngestQueue] KG stage completed for {} in {:.2f}ms", hash, ms);
        }
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] KG stage failed for {}: {}", hash, e.what());
    }
}

boost::asio::awaitable<void> PostIngestQueue::processEmbeddingStage(const std::string& hash,
                                                                    const std::string& mime) {
    try {
        auto embedChannel =
            InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
                "embed_jobs", 2048);

        if (!embedChannel) {
            spdlog::warn("[PostIngestQueue] Embed channel unavailable for {}", hash);
            co_return;
        }

        InternalEventBus::EmbedJob job;
        job.hashes.push_back(hash);
        job.batchSize = 1;
        job.skipExisting = true;
        job.modelName = "";

        if (!embedChannel->try_push(std::move(job))) {
            spdlog::warn("[PostIngestQueue] Embed channel full, dropping job for {}", hash);
            InternalEventBus::instance().incEmbedDropped();
        } else {
            InternalEventBus::instance().incEmbedQueued();
            spdlog::debug("[PostIngestQueue] Dispatched embedding job for {}", hash);
        }
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Embedding dispatch failed for {}: {}", hash, e.what());
    }
    co_return;
}

} // namespace yams::daemon
