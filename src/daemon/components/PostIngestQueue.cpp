#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <thread>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
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
#include <yams/daemon/resource/external_entity_provider_adapter.h>

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
      capacity_(capacity ? capacity : 1000) {
    spdlog::info("[PostIngestQueue] Created (parallel processing via WorkCoordinator)");
}

PostIngestQueue::~PostIngestQueue() {
    stop();
}

void PostIngestQueue::start() {
    spdlog::info("[PostIngestQueue] start() called, stop_={}", stop_.load());
    if (!stop_.load()) {
        spdlog::info("[PostIngestQueue] Spawning channelPoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), channelPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Spawning kgPoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), kgPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Spawning symbolPoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), symbolPoller(), boost::asio::detached);
        spdlog::info("[PostIngestQueue] Spawning entityPoller coroutine...");
        boost::asio::co_spawn(coordinator_->getExecutor(), entityPoller(), boost::asio::detached);

        constexpr int maxWaitMs = 100;
        for (int i = 0; i < maxWaitMs && (!started_.load() || !kgStarted_.load() || !symbolStarted_.load() || !entityStarted_.load()); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        spdlog::info("[PostIngestQueue] Pollers started (extraction={}, kg={}, symbol={}, entity={})", started_.load(),
                     kgStarted_.load(), symbolStarted_.load(), entityStarted_.load());
    } else {
        spdlog::warn("[PostIngestQueue] start() skipped because stop_=true");
    }
}

void PostIngestQueue::stop() {
    stop_.store(true);
    spdlog::info("[PostIngestQueue] Stop requested");
}

boost::asio::awaitable<void> PostIngestQueue::channelPoller() {
    spdlog::info("[PostIngestQueue] channelPoller coroutine STARTED");
    const std::size_t channelCapacity =
        std::max<std::size_t>(65536, TuneAdvisor::postIngestQueueMax());
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", channelCapacity);
    spdlog::info("[PostIngestQueue] channelPoller got channel (cap={})", channelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
    spdlog::info("[PostIngestQueue] channelPoller got timer");

    started_.store(true);

    while (!stop_.load()) {
        InternalEventBus::PostIngestTask task;
        if (inFlight_.load() < kMaxConcurrent_ && channel->try_pop(task)) {
            inFlight_.fetch_add(1);
            boost::asio::post(coordinator_->getExecutor(),
                              [this, hash = std::move(task.hash), mime = std::move(task.mime)]() {
                                  processTask(hash, mime);
                                  inFlight_.fetch_sub(1);
                              });
        } else {
            timer.expires_after(std::chrono::milliseconds(50));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    spdlog::info("[PostIngestQueue] Channel poller exited");
}

void PostIngestQueue::enqueue(Task t) {
    const std::size_t channelCapacity =
        std::max<std::size_t>(65536, TuneAdvisor::postIngestQueueMax());
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", channelCapacity);

    InternalEventBus::PostIngestTask task;
    task.hash = std::move(t.hash);
    task.mime = std::move(t.mime);

    constexpr int maxRetries = 10;
    constexpr auto baseBackoff = std::chrono::milliseconds(50);
    constexpr auto maxBackoff = std::chrono::milliseconds(1000);

    for (int i = 0; i < maxRetries; ++i) {
        if (channel->try_push(task)) {
            return;
        }
        auto delay = std::min(baseBackoff * (1 << i), maxBackoff);
        std::this_thread::sleep_for(delay);
    }

    spdlog::error("[PostIngestQueue] Channel full after {} retries, dropping task for hash: {}",
                  maxRetries, task.hash);
}

bool PostIngestQueue::tryEnqueue(const Task& t) {
    const std::size_t channelCapacity =
        std::max<std::size_t>(65536, TuneAdvisor::postIngestQueueMax());
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", channelCapacity);

    InternalEventBus::PostIngestTask task;
    task.hash = t.hash;
    task.mime = t.mime;

    return channel->try_push(task);
}

bool PostIngestQueue::tryEnqueue(Task&& t) {
    const std::size_t channelCapacity =
        std::max<std::size_t>(65536, TuneAdvisor::postIngestQueueMax());
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", channelCapacity);

    InternalEventBus::PostIngestTask task;
    task.hash = std::move(t.hash);
    task.mime = std::move(t.mime);

    return channel->try_push(std::move(task));
}

std::size_t PostIngestQueue::size() const {
    const std::size_t channelCapacity =
        std::max<std::size_t>(65536, TuneAdvisor::postIngestQueueMax());
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", channelCapacity);
    return channel ? channel->size_approx() : 0;
}

void PostIngestQueue::processTask(const std::string& hash, const std::string& mime) {
    try {
        processMetadataStage(hash, mime);
        processEmbeddingStage(hash, mime);
        processed_++;
        InternalEventBus::instance().incPostConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Failed to process {}: {}", hash, e.what());
        failed_++;
    }
}

void PostIngestQueue::processMetadataStage(const std::string& hash, const std::string& mime) {
    if (!store_ || !meta_) {
        spdlog::warn("[PostIngestQueue] store or metadata unavailable; dropping task {}", hash);
        return;
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
        } else {
            spdlog::warn(
                "[PostIngestQueue] Metadata not found for hash {}; content may be orphaned", hash);
            return;
        }

        auto txt = extractDocumentText(store_, hash, mimeType, extension, extractors_);
        if (!txt || txt->empty()) {
            spdlog::debug("[PostIngestQueue] no text extracted for {} (mime={})", hash, mimeType);
            if (docId >= 0) {
                auto d = meta_->getDocument(docId);
                if (d && d.value().has_value()) {
                    auto updated = d.value().value();
                    updated.contentExtracted = false;
                    updated.extractionStatus = metadata::ExtractionStatus::Failed;
                    auto updateRes = meta_->updateDocument(updated);
                    if (!updateRes) {
                        spdlog::warn("[PostIngestQueue] Failed to mark extraction failed for {}: {}",
                                     hash, updateRes.error().message);
                    }
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

        if (docId >= 0) {
            auto tagsRes = meta_->getDocumentTags(docId);
            std::vector<std::string> tags;
            if (tagsRes && !tagsRes.value().empty()) {
                tags = tagsRes.value();
            }
            dispatchToKgChannel(hash, docId, fileName, std::move(tags));

            // Dispatch symbol extraction for code files (if plugin supports this extension)
            {
                std::lock_guard<std::mutex> lock(extMapMutex_);
                // Extension map keys don't have leading dots, but DB stores with dots
                std::string extKey = extension;
                if (!extKey.empty() && extKey[0] == '.') {
                    extKey = extKey.substr(1);
                }
                auto it = symbolExtensionMap_.find(extKey);
                if (it != symbolExtensionMap_.end()) {
                    dispatchToSymbolChannel(hash, docId, fileName, it->second);
                }
            }

            // Dispatch entity extraction for binary files (if entity provider supports this extension)
            {
                std::lock_guard<std::mutex> lock(entityMutex_);
                spdlog::debug("[PostIngestQueue] Checking {} entity providers for ext={}",
                              entityProviders_.size(), extension);
                for (const auto& provider : entityProviders_) {
                    if (provider && provider->supports(extension)) {
                        dispatchToEntityChannel(hash, docId, fileName, extension);
                        break; // Only dispatch to first matching provider
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Metadata stage failed for {}: {}", hash, e.what());
    }
}

void PostIngestQueue::processKnowledgeGraphStage(const std::string& hash, int64_t docId,
                                                 const std::string& filePath,
                                                 const std::vector<std::string>& tags) {
    if (!graphComponent_) {
        spdlog::warn("[PostIngestQueue] KG stage skipped for {} - no graphComponent", hash);
        return;
    }

    spdlog::info("[PostIngestQueue] KG stage starting for {} ({})", filePath, hash.substr(0, 12));

    try {
        auto start = std::chrono::steady_clock::now();

        GraphComponent::DocumentGraphContext ctx{
            .documentHash = hash, .filePath = filePath, .tags = tags, .documentDbId = docId};

        auto result = graphComponent_->onDocumentIngested(ctx);
        if (!result) {
            spdlog::warn("[PostIngestQueue] Graph ingestion failed for {}: {}", hash,
                         result.error().message);
        } else {
            auto duration = std::chrono::steady_clock::now() - start;
            double ms = std::chrono::duration<double, std::milli>(duration).count();
            spdlog::debug("[PostIngestQueue] KG stage completed for {} in {:.2f}ms", hash, ms);
        }
        InternalEventBus::instance().incKgConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] KG stage failed for {}: {}", hash, e.what());
    }
}

void PostIngestQueue::processEmbeddingStage(const std::string& hash, const std::string& /*mime*/) {
    try {
        auto embedChannel =
            InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
                "embed_jobs", 2048);

        if (!embedChannel) {
            spdlog::warn("[PostIngestQueue] Embed channel unavailable for {}", hash);
            return;
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
}

void PostIngestQueue::dispatchToKgChannel(const std::string& hash, int64_t docId,
                                          const std::string& filePath,
                                          std::vector<std::string> tags) {
    constexpr std::size_t kgChannelCapacity = 16384;
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::KgJob>(
        "kg_jobs", kgChannelCapacity);

    InternalEventBus::KgJob job;
    job.hash = hash;
    job.documentId = docId;
    job.filePath = filePath;
    job.tags = std::move(tags);

    if (!channel->try_push(std::move(job))) {
        spdlog::warn("[PostIngestQueue] KG channel full, dropping job for {}", hash);
        InternalEventBus::instance().incKgDropped();
    } else {
        spdlog::info("[PostIngestQueue] Dispatched KG job for {} ({})", filePath, hash.substr(0, 12));
        InternalEventBus::instance().incKgQueued();
    }
}

boost::asio::awaitable<void> PostIngestQueue::kgPoller() {
    constexpr std::size_t kgChannelCapacity = 16384;
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::KgJob>(
        "kg_jobs", kgChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    kgStarted_.store(true);

    while (!stop_.load()) {
        InternalEventBus::KgJob job;
        if (kgInFlight_.load() < kMaxKgConcurrent_ && channel->try_pop(job)) {
            kgInFlight_.fetch_add(1);
            boost::asio::post(coordinator_->getExecutor(),
                              [this, hash = std::move(job.hash), docId = job.documentId,
                               filePath = std::move(job.filePath), tags = std::move(job.tags)]() {
                                  processKnowledgeGraphStage(hash, docId, filePath, tags);
                                  kgInFlight_.fetch_sub(1);
                              });
        } else {
            timer.expires_after(std::chrono::milliseconds(25));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    spdlog::info("[PostIngestQueue] KG poller exited");
}

boost::asio::awaitable<void> PostIngestQueue::symbolPoller() {
    constexpr std::size_t symbolChannelCapacity = 16384;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::SymbolExtractionJob>(
            "symbol_extraction", symbolChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    symbolStarted_.store(true);
    spdlog::info("[PostIngestQueue] Symbol extraction poller started");

    while (!stop_.load()) {
        InternalEventBus::SymbolExtractionJob job;
        if (symbolInFlight_.load() < kMaxSymbolConcurrent_ && channel->try_pop(job)) {
            symbolInFlight_.fetch_add(1);
            boost::asio::post(
                coordinator_->getExecutor(),
                [this, hash = std::move(job.hash), docId = job.documentId,
                 filePath = std::move(job.filePath), language = std::move(job.language)]() {
                    processSymbolExtractionStage(hash, docId, filePath, language);
                    symbolInFlight_.fetch_sub(1);
                });
        } else {
            timer.expires_after(std::chrono::milliseconds(25));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    spdlog::info("[PostIngestQueue] Symbol extraction poller exited");
}

void PostIngestQueue::dispatchToSymbolChannel(const std::string& hash, int64_t docId,
                                               const std::string& filePath,
                                               const std::string& language) {
    constexpr std::size_t symbolChannelCapacity = 16384;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::SymbolExtractionJob>(
            "symbol_extraction", symbolChannelCapacity);

    InternalEventBus::SymbolExtractionJob job;
    job.hash = hash;
    job.documentId = docId;
    job.filePath = filePath;
    job.language = language;

    if (!channel->try_push(std::move(job))) {
        spdlog::warn("[PostIngestQueue] Symbol channel full, dropping job for {}", hash);
        InternalEventBus::instance().incSymbolDropped();
    } else {
        spdlog::info("[PostIngestQueue] Dispatched symbol extraction job for {} ({}) lang={}",
                     filePath, hash.substr(0, 12), language);
        InternalEventBus::instance().incSymbolQueued();
    }
}

void PostIngestQueue::processSymbolExtractionStage(const std::string& hash, int64_t docId,
                                                    const std::string& filePath,
                                                    const std::string& language) {
    if (!graphComponent_) {
        spdlog::warn("[PostIngestQueue] Symbol extraction skipped for {} - no graphComponent", hash);
        return;
    }

    spdlog::info("[PostIngestQueue] Symbol extraction starting for {} ({}) lang={}", filePath,
                 hash.substr(0, 12), language);

    try {
        auto start = std::chrono::steady_clock::now();

        // Use GraphComponent to submit the extraction job
        GraphComponent::EntityExtractionJob extractJob;
        extractJob.documentHash = hash;
        extractJob.filePath = filePath;
        extractJob.language = language;

        // Load content from store
        if (store_) {
            auto contentResult = store_->retrieveBytes(hash);
            if (contentResult) {
                const auto& bytes = contentResult.value();
                extractJob.contentUtf8 = std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            } else {
                spdlog::warn("[PostIngestQueue] Failed to load content for symbol extraction: {}",
                             hash.substr(0, 12));
                return;
            }
        } else {
            spdlog::warn("[PostIngestQueue] No content store for symbol extraction");
            return;
        }

        auto result = graphComponent_->submitEntityExtraction(std::move(extractJob));
        if (!result) {
            spdlog::warn("[PostIngestQueue] Symbol extraction failed for {}: {}", hash,
                         result.error().message);
        } else {
            auto duration = std::chrono::steady_clock::now() - start;
            double ms = std::chrono::duration<double, std::milli>(duration).count();
            spdlog::debug("[PostIngestQueue] Symbol extraction submitted for {} in {:.2f}ms", hash, ms);
        }
        InternalEventBus::instance().incSymbolConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Symbol extraction failed for {}: {}", hash, e.what());
    }
}

void PostIngestQueue::dispatchToEntityChannel(const std::string& hash, int64_t docId,
                                               const std::string& filePath,
                                               const std::string& extension) {
    constexpr std::size_t entityChannelCapacity = 4096;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityExtractionJob>(
            "entity_extraction", entityChannelCapacity);

    InternalEventBus::EntityExtractionJob job;
    job.hash = hash;
    job.documentId = docId;
    job.filePath = filePath;
    job.extension = extension;

    if (!channel->try_push(std::move(job))) {
        spdlog::warn("[PostIngestQueue] Entity channel full, dropping job for {}", hash);
        InternalEventBus::instance().incEntityDropped();
    } else {
        spdlog::info("[PostIngestQueue] Dispatched entity extraction job for {} ({}) ext={}",
                     filePath, hash.substr(0, 12), extension);
        InternalEventBus::instance().incEntityQueued();
    }
}

boost::asio::awaitable<void> PostIngestQueue::entityPoller() {
    constexpr std::size_t entityChannelCapacity = 4096;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityExtractionJob>(
            "entity_extraction", entityChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    entityStarted_.store(true);
    spdlog::info("[PostIngestQueue] Entity extraction poller started");

    while (!stop_.load()) {
        InternalEventBus::EntityExtractionJob job;
        if (entityInFlight_.load() < kMaxEntityConcurrent_ && channel->try_pop(job)) {
            entityInFlight_.fetch_add(1);
            boost::asio::post(
                coordinator_->getExecutor(),
                [this, hash = std::move(job.hash), docId = job.documentId,
                 filePath = std::move(job.filePath), extension = std::move(job.extension)]() {
                    processEntityExtractionStage(hash, docId, filePath, extension);
                    entityInFlight_.fetch_sub(1);
                });
        } else {
            timer.expires_after(std::chrono::milliseconds(50));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    spdlog::info("[PostIngestQueue] Entity extraction poller exited");
}

void PostIngestQueue::processEntityExtractionStage(const std::string& hash, int64_t /*docId*/,
                                                    const std::string& filePath,
                                                    const std::string& extension) {
    spdlog::info("[PostIngestQueue] Entity extraction starting for {} ({}) ext={}", filePath,
                 hash.substr(0, 12), extension);

    try {
        auto start = std::chrono::steady_clock::now();

        // Find the entity provider that supports this extension
        std::shared_ptr<ExternalEntityProviderAdapter> provider;
        {
            std::lock_guard<std::mutex> lock(entityMutex_);
            for (const auto& p : entityProviders_) {
                if (p && p->supports(extension)) {
                    provider = p;
                    break;
                }
            }
        }

        if (!provider) {
            spdlog::warn("[PostIngestQueue] No entity provider for extension {}", extension);
            return;
        }

        // Load content from store
        std::vector<std::byte> content;
        if (store_) {
            auto contentResult = store_->retrieveBytes(hash);
            if (contentResult) {
                content = contentResult.value();
            } else {
                spdlog::warn("[PostIngestQueue] Failed to load content for entity extraction: {}",
                             hash.substr(0, 12));
                return;
            }
        } else {
            spdlog::warn("[PostIngestQueue] No content store for entity extraction");
            return;
        }

        // Extract entities from binary
        auto result = provider->extractAllEntities(content, filePath);
        if (!result) {
            spdlog::warn("[PostIngestQueue] Entity extraction failed for {}: {}", hash,
                         result.error().message);
            return;
        }

        auto& entities = result.value();
        spdlog::info("[PostIngestQueue] Extracted {} nodes, {} edges, {} aliases from {}",
                     entities.nodes.size(), entities.edges.size(), entities.aliases.size(),
                     filePath);

        // Ingest entities into KG
        if (kg_ && !entities.nodes.empty()) {
            // First insert all nodes and get their IDs
            auto nodeIds = kg_->upsertNodes(entities.nodes);
            if (!nodeIds) {
                spdlog::warn("[PostIngestQueue] Failed to insert entity nodes: {}",
                             nodeIds.error().message);
                return;
            }

            // Build nodeKey -> nodeId map for edge resolution
            std::unordered_map<std::string, std::int64_t> keyToId;
            for (size_t i = 0; i < entities.nodes.size() && i < nodeIds.value().size(); ++i) {
                keyToId[entities.nodes[i].nodeKey] = nodeIds.value()[i];
            }

            // Resolve edge src/dst keys to IDs
            std::vector<metadata::KGEdge> resolvedEdges;
            for (auto& edge : entities.edges) {
                // Parse _src_key and _dst_key from properties
                try {
                    if (!edge.properties) continue;
                    auto props = nlohmann::json::parse(*edge.properties);
                    std::string srcKey = props.value("_src_key", "");
                    std::string dstKey = props.value("_dst_key", "");

                    auto srcIt = keyToId.find(srcKey);
                    auto dstIt = keyToId.find(dstKey);

                    if (srcIt != keyToId.end() && dstIt != keyToId.end()) {
                        edge.srcNodeId = srcIt->second;
                        edge.dstNodeId = dstIt->second;
                        // Remove temporary keys from properties
                        props.erase("_src_key");
                        props.erase("_dst_key");
                        edge.properties = props.dump();
                        resolvedEdges.push_back(std::move(edge));
                    }
                } catch (...) {
                    // Skip edges we can't parse
                }
            }

            if (!resolvedEdges.empty()) {
                kg_->addEdgesUnique(resolvedEdges);
            }

            // Resolve alias nodeIds
            std::vector<metadata::KGAlias> resolvedAliases;
            for (auto& alias : entities.aliases) {
                // Parse node_key from source field
                if (alias.source && alias.source->starts_with("_node_key:")) {
                    std::string nodeKey = alias.source->substr(10);
                    auto it = keyToId.find(nodeKey);
                    if (it != keyToId.end()) {
                        alias.nodeId = it->second;
                        alias.source = "ghidra";
                        resolvedAliases.push_back(std::move(alias));
                    }
                }
            }

            if (!resolvedAliases.empty()) {
                kg_->addAliases(resolvedAliases);
            }

            auto duration = std::chrono::steady_clock::now() - start;
            double ms = std::chrono::duration<double, std::milli>(duration).count();
            spdlog::info("[PostIngestQueue] Entity extraction completed for {} in {:.2f}ms "
                         "(nodes={}, edges={}, aliases={})",
                         hash.substr(0, 12), ms, nodeIds.value().size(), resolvedEdges.size(),
                         resolvedAliases.size());
        }

        InternalEventBus::instance().incEntityConsumed();
    } catch (const std::exception& e) {
        spdlog::error("[PostIngestQueue] Entity extraction failed for {}: {}", hash, e.what());
    }
}

} // namespace yams::daemon
