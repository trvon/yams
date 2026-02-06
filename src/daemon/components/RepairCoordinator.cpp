#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/RepairCoordinator.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/query_helpers.h>

#include <spdlog/spdlog.h>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>
#include <yams/core/repair_fsm.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/ipc/repair_scheduling_adapter.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/content_extractor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

namespace yams::daemon {

namespace {

// Check if vector operations are disabled via environment variables
bool vectorsDisabledByEnv() {
    if (const char* env = std::getenv("YAMS_DISABLE_VECTORS"); env && *env) {
        return true;
    }
    if (const char* env = std::getenv("YAMS_DISABLE_VECTOR_DB"); env && *env) {
        return true;
    }
    return false;
}

// Shared thread pool for all RepairCoordinator instances
// Profile-aware thread count: uses repairTokensIdle + 1 for DB queries
// Static singleton - threads are lazily initialized
struct RepairThreadPool {
    std::unique_ptr<boost::asio::thread_pool> pool_;
    std::once_flag init_flag_;

    static RepairThreadPool& instance() {
        static RepairThreadPool instance;
        return instance;
    }

    boost::asio::any_io_executor get_executor() {
        std::call_once(init_flag_, [this]() {
            uint32_t threads = TuneAdvisor::repairTokensIdle();
            if (threads < 1)
                threads = 1;
            // Add 1 extra thread for DB query coordination
            threads = std::max(threads + 1, 2u);
            pool_ = std::make_unique<boost::asio::thread_pool>(threads);
            spdlog::debug("[RepairThreadPool] Initialized with {} threads", threads);
        });
        return pool_->get_executor();
    }

    // No explicit join in destructor to avoid double-free during static destruction
    // The OS will clean up threads when the process exits
    ~RepairThreadPool() = default;

private:
    RepairThreadPool() = default;
};

/**
 * @brief Build extension-to-language map from loaded symbol extractor plugins
 *
 * Queries all loaded plugins dynamically via get_capabilities_json()
 * No static checks - everything comes from plugin responses
 */
std::unordered_map<std::string, std::string> buildExtensionLanguageMap(
    const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>& extractors) {
    // Defensive: tests may provide placeholder adapters.
    // Only query extractors that have a live vtable.

    std::unordered_map<std::string, std::string> result;

    for (const auto& extractor : extractors) {
        if (!extractor || !extractor->table())
            continue;

        auto supported = extractor->getSupportedExtensions();
        for (const auto& [ext, lang] : supported) {
            result[ext] = lang;
        }
    }

    return result;
}

// Check if a document is extractable based on MIME type, extension, and plugins
// Uses magic_numbers.hpp via FileTypeDetector for robust detection
bool canExtractDocument(
    const std::string& mimeType, const std::string& extension,
    const std::vector<std::shared_ptr<extraction::IContentExtractor>>& customExtractors,
    const std::shared_ptr<yams::api::IContentStore>& contentStore, const std::string& hash) {
    // 1) Check if any custom plugin extractor supports this format
    for (const auto& extractor : customExtractors) {
        if (extractor && extractor->supports(mimeType, extension)) {
            return true;
        }
    }

    // 2) Use FileTypeDetector which leverages magic_numbers.hpp
    auto& detector = yams::detection::FileTypeDetector::instance();

    // Check MIME type directly
    if (!mimeType.empty() && detector.isTextMimeType(mimeType)) {
        return true;
    }

    // 3) For unknown MIME types, try extension-based detection
    if (mimeType.empty() || mimeType == "application/octet-stream" ||
        mimeType == "application/x-octet-stream") {
        // Try extension lookup
        if (!extension.empty()) {
            auto detectedMime =
                yams::detection::FileTypeDetector::getMimeTypeFromExtension(extension);
            if (!detectedMime.empty() && detector.isTextMimeType(detectedMime)) {
                return true;
            }
        }

        // Last resort: sample content to check if binary
        if (contentStore) {
            auto bytesRes = contentStore->retrieveBytes(hash);
            if (bytesRes) {
                const auto& bytes = bytesRes.value();
                size_t checkSize = std::min(bytes.size(), size_t(8192));
                std::span<const std::byte> sample(bytes.data(), checkSize);

                if (!yams::detection::isBinaryData(sample)) {
                    return true;
                }
            }
        }
    }

    return false;
}

// Template helper: queue job with exponential backoff when full
// Returns true if successfully queued, false if interrupted by shutdown
template <typename JobT>
boost::asio::awaitable<bool> queueWithBackoff(std::shared_ptr<SpscQueue<JobT>> queue, JobT&& job,
                                              boost::asio::steady_timer& timer,
                                              std::atomic<bool>& running,
                                              const std::string& jobTypeName, size_t jobSize,
                                              int initialDelayMs = 50, int maxDelayMs = 1000) {
    int retries = 0;
    while (!queue->try_push(std::move(job))) {
        if (!running.load(std::memory_order_relaxed)) {
            co_return false; // Interrupted by shutdown
        }

        // Exponential backoff with configurable limits
        int delayMs = std::min(initialDelayMs * (1 << retries), maxDelayMs);
        timer.expires_after(std::chrono::milliseconds(delayMs));
        co_await timer.async_wait(boost::asio::use_awaitable);
        retries++;

        if (retries == 1) {
            spdlog::debug("RepairCoordinator: {} queue full, waiting (batch size={})", jobTypeName,
                          jobSize);
        }
    }

    if (retries > 0) {
        spdlog::debug("RepairCoordinator: queued {} job after {} retries ({} docs)", jobTypeName,
                      retries, jobSize);
    } else {
        spdlog::debug("RepairCoordinator: queued {} job to InternalEventBus ({} docs)", jobTypeName,
                      jobSize);
    }

    co_return true; // Successfully queued
}
} // namespace

RepairCoordinator::RepairCoordinator(ServiceManager* services, StateComponent* state,
                                     std::function<size_t()> activeConnFn, Config cfg)
    : services_(services), state_(state), activeConnFn_(std::move(activeConnFn)),
      cfg_(std::move(cfg)), shutdownState_(std::make_shared<ShutdownState>()) {}

std::shared_ptr<GraphComponent> RepairCoordinator::getGraphComponentForScheduling() const {
    return services_ ? services_->getGraphComponent() : nullptr;
}

std::shared_ptr<metadata::KnowledgeGraphStore> RepairCoordinator::getKgStoreForScheduling() const {
    return services_ ? services_->getKgStore() : nullptr;
}

const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>&
RepairCoordinator::getSymbolExtractorsForScheduling() const {
    static const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> kEmpty;
    return services_ ? services_->getSymbolExtractors() : kEmpty;
}

RepairCoordinator::~RepairCoordinator() {
    stop();
}

void RepairCoordinator::start() {
    if (!cfg_.enable || running_.exchange(true)) {
        return;
    }
    tokens_.store(cfg_.maintenanceTokens);
    shutdownState_->finished.store(false, std::memory_order_relaxed);
    shutdownState_->running.store(true, std::memory_order_relaxed);
    shutdownState_->config = cfg_;

    auto exec = RepairThreadPool::instance().get_executor();
    auto shutdownState = shutdownState_;
    auto* self = this;

    boost::asio::co_spawn(
        exec,
        [self, shutdownState]() -> boost::asio::awaitable<void> {
            spdlog::debug("RepairCoordinator coroutine starting");
            try {
                if (!shutdownState->running.load(std::memory_order_acquire)) {
                    spdlog::debug(
                        "RepairCoordinator coroutine - already stopped, exiting immediately");
                    co_return;
                }
                co_await self->runAsync(shutdownState);
                spdlog::debug("RepairCoordinator coroutine - runAsync completed");
            } catch (const std::exception& e) {
                spdlog::error("RepairCoordinator coroutine exception: {}", e.what());
            }
            co_return;
        },
        [shutdownState](const std::exception_ptr& eptr) {
            // Completion handler - signal that coroutine has fully exited
            if (eptr) {
                try {
                    std::rethrow_exception(eptr);
                } catch (const std::exception& e) {
                    spdlog::error("RepairCoordinator completion handler caught exception: {}",
                                  e.what());
                }
            }
            spdlog::debug("RepairCoordinator coroutine completion handler - signaling finished");
            shutdownState->finished.store(true, std::memory_order_release);
            shutdownState->cv.notify_all();
        });
    spdlog::debug(
        "RepairCoordinator started (awaitable) (enable={}, batch={}, dedicated_threads=2)",
        cfg_.enable, cfg_.maxBatch);

    // Enqueue initial PathTreeRepair job (run once at startup)
    static auto pathTreeQueue =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PathTreeJob>(
            "path_tree_repair_jobs", 32);
    InternalEventBus::PathTreeJob initialPathTreeJob{1, true}; // requestId=1, runOnce=true
    if (!pathTreeQueue->try_push(initialPathTreeJob)) {
        spdlog::debug("RepairCoordinator: failed to enqueue initial PathTreeRepair job");
    }
}

void RepairCoordinator::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    // Signal the coroutine to stop via shared state
    shutdownState_->running.store(false, std::memory_order_release);
    spdlog::debug("RepairCoordinator::stop() - signaled coroutine to stop");
    queueCv_.notify_all();
    // Wait for coroutine to signal completion
    {
        std::unique_lock<std::mutex> lk(shutdownState_->mutex);
        bool completed = shutdownState_->cv.wait_for(lk, std::chrono::milliseconds(2000), [this] {
            return shutdownState_->finished.load(std::memory_order_relaxed);
        });
        if (!completed) {
            spdlog::debug("RepairCoordinator::stop() - coroutine did not finish within timeout");
        } else {
            spdlog::debug("RepairCoordinator::stop() - coroutine finished");
        }
    }
    // Note: We don't clear services_ here to avoid racing with the coroutine.
    // The coroutine checks running_ before accessing services_, and running_
    // was set to false at the start of stop().
}

bool RepairCoordinator::maintenance_allowed() const {
    if (!activeConnFn_)
        return false;
    return activeConnFn_() == 0;
}

void RepairCoordinator::onDocumentAdded(const DocumentAddedEvent& event) {
    if (!cfg_.enable || !running_) {
        return;
    }

    // If the main post-ingest pipeline is active, it will schedule embeddings
    // immediately after text extraction. Avoid duplicate scheduling here.
    // Use explicit started() check instead of exception-based control flow.
    if (services_) {
        auto piq = services_->getPostIngestQueue();
        if (piq && piq->started()) {
            spdlog::debug(
                "RepairCoordinator: skipping DocumentAdded {} — handled by PostIngestQueue",
                event.hash);
            return;
        }
    }

    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        // Deduplicate: only enqueue if not already in queue
        if (pendingSet_.find(event.hash) == pendingSet_.end()) {
            pendingSet_.insert(event.hash);
            pendingDocuments_.push(event.hash);
        }
        if (state_)
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
    }
    queueCv_.notify_one();
    spdlog::debug("RepairCoordinator: queued document {} for embedding check", event.hash);

    // Opportunistically schedule symbol/entity extraction for newly added documents
    // Query plugins dynamically for supported languages - no static checks
    try {
        if (services_) {
            auto gc = getGraphComponentForScheduling();
            const auto& symbolExtractors = getSymbolExtractorsForScheduling();

            if (gc && !symbolExtractors.empty()) {
                static thread_local std::unordered_map<std::string, std::string> extToLang;
                static thread_local bool mapInitialized = false;

                if (!mapInitialized) {
                    extToLang = buildExtensionLanguageMap(symbolExtractors);
                    mapInitialized = true;
                    spdlog::debug(
                        "RepairCoordinator: initialized symbol extraction for {} extensions",
                        extToLang.size());
                }

                std::string extension;
                auto dotPos = event.path.find_last_of('.');
                if (dotPos != std::string::npos) {
                    extension = event.path.substr(dotPos + 1); // Skip the dot
                }
                // The plugin map stores both "cpp" and ".cpp" forms in some callers.
                // Try the plain form first (current behavior), then the dotted form.
                std::string dottedExtension;
                if (!extension.empty()) {
                    dottedExtension = "." + extension;
                }

                auto it = extToLang.find(extension);
                if (it == extToLang.end() && !dottedExtension.empty()) {
                    it = extToLang.find(dottedExtension);
                }
                if (it != extToLang.end()) {
                    // Avoid re-queuing extraction for documents that already have KG entities.
                    // This is important because RepairCoordinator also runs during daemon startup.
                    auto kg = getKgStoreForScheduling();
                    std::optional<std::int64_t> docId;
                    if (kg) {
                        auto idRes = kg->getDocumentIdByHash(event.hash);
                        if (idRes.has_value()) {
                            docId = idRes.value();
                        }
                    }

                    bool alreadyExtracted = false;
                    if (kg && docId.has_value()) {
                        // Any prior doc-entity record implies extraction already happened.
                        auto entRes = kg->getDocEntitiesForDocument(docId.value(), 1, 0);
                        if (entRes.has_value() && !entRes.value().empty()) {
                            alreadyExtracted = true;
                        }
                    }

                    if (alreadyExtracted) {
                        spdlog::debug(
                            "RepairCoordinator: skip symbol extraction for {} (already extracted)",
                            event.hash.substr(0, 12));
                        return;
                    }

                    GraphComponent::EntityExtractionJob j;
                    j.documentHash = event.hash;
                    j.filePath = event.path;
                    j.language = it->second;

                    (void)gc->submitEntityExtraction(std::move(j));
                    spdlog::debug("RepairCoordinator: queued symbol extraction {} (ext={} lang={})",
                                  event.hash.substr(0, 12), extension, it->second);
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("RepairCoordinator: symbol extraction scheduling failed: {}", e.what());
    } catch (...) {
        spdlog::warn("RepairCoordinator: symbol extraction scheduling failed (unknown error)");
    }
}

void RepairCoordinator::enqueueEmbeddingRepair(const std::vector<std::string>& hashes) {
    if (!cfg_.enable || !running_) {
        return;
    }
    if (hashes.empty()) {
        return;
    }
    size_t enqueuedCount = 0;
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        for (const auto& hash : hashes) {
            // Deduplicate: only enqueue if not already in queue
            if (pendingSet_.find(hash) == pendingSet_.end()) {
                pendingSet_.insert(hash);
                pendingDocuments_.push(hash);
                ++enqueuedCount;
            }
        }
        if (state_) {
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
        }
    }
    queueCv_.notify_one();
    spdlog::debug("RepairCoordinator: queued {} documents for embedding repair ({} deduplicated)",
                  enqueuedCount, hashes.size() - enqueuedCount);
}

void RepairCoordinator::onDocumentRemoved(const DocumentRemovedEvent& event) {
    if (!cfg_.enable || !running_) {
        return;
    }

    // For now, just log - in future could clean up orphaned embeddings
    spdlog::debug("RepairCoordinator: document {} removed", event.hash);
}

// ---------------------------------------------------------------------------
// Extracted helpers for runAsync readability
// ---------------------------------------------------------------------------

boost::asio::awaitable<void>
RepairCoordinator::processPathTreeRepair() {
    using namespace std::chrono_literals;
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    auto repairMgr = services_ ? services_->getRepairManager() : nullptr;
    auto metaRepo = services_ ? services_->getMetadataRepo() : nullptr;

    if (!repairMgr || !metaRepo) {
        spdlog::warn("RepairCoordinator: services unavailable for PathTreeRepair");
        co_return;
    }

    try {
        auto docsResult = metaRepo->queryDocuments(metadata::DocumentQueryOptions{});
        if (!docsResult || docsResult.value().empty()) {
            spdlog::debug("RepairCoordinator: PathTreeRepair - no documents to scan");
            co_return;
        }

        spdlog::debug("RepairCoordinator: PathTreeRepair scanning {} documents",
                       docsResult.value().size());

        uint64_t created = 0;
        uint64_t errors = 0;
        uint64_t scanned = 0;
        const size_t batchSize = TuneAdvisor::repairStartupBatchSize();

        for (size_t i = 0; i < docsResult.value().size(); i += batchSize) {
            if (!running_.load(std::memory_order_relaxed)) {
                spdlog::debug("RepairCoordinator: PathTreeRepair interrupted");
                break;
            }

            size_t batchEnd = std::min(i + batchSize, docsResult.value().size());
            for (size_t j = i; j < batchEnd; ++j) {
                const auto& doc = docsResult.value()[j];
                if (doc.filePath.empty()) {
                    continue;
                }

                auto existingNode = metaRepo->findPathTreeNodeByFullPath(doc.filePath);
                if (existingNode && existingNode.value().has_value()) {
                    continue;
                }

                try {
                    auto treeRes = metaRepo->upsertPathTreeForDocument(doc, doc.id, true,
                                                                       std::span<const float>());
                    if (treeRes) {
                        ++created;
                    } else {
                        ++errors;
                    }
                } catch (const std::exception&) {
                    ++errors;
                }
                ++scanned;

                if (scanned % 500 == 0) {
                    spdlog::debug("RepairCoordinator: PathTreeRepair progress: {}/{}",
                                  scanned, docsResult.value().size());
                }
            }

            // Yield between batches to avoid blocking
            timer.expires_after(10ms);
            co_await timer.async_wait(boost::asio::use_awaitable);
        }

        spdlog::debug("RepairCoordinator: PathTreeRepair complete (scanned={}, created={}, errors={})",
                       scanned, created, errors);
    } catch (const std::exception& e) {
        spdlog::debug("RepairCoordinator: PathTreeRepair exception: {}", e.what());
    }
}

void RepairCoordinator::performVectorCleanup() {
    if (vectorsDisabledByEnv()) {
        spdlog::debug("RepairCoordinator: skipping vector cleanup (vectors disabled)");
        return;
    }

    try {
        auto vectorDb = services_ ? services_->getVectorDatabase() : nullptr;
        if (vectorDb) {
            auto cleanup = vectorDb->cleanupOrphanRows();
            if (cleanup) {
                spdlog::debug(
                    "RepairCoordinator: cleaned vector orphans (metadata_removed={}, "
                    "embeddings_removed={}, metadata_backfilled={})",
                    cleanup.value().metadata_removed, cleanup.value().embeddings_removed,
                    cleanup.value().metadata_backfilled);
            } else {
                spdlog::warn("RepairCoordinator: vector orphan cleanup failed: {}",
                             cleanup.error().message);
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("RepairCoordinator: vector orphan cleanup exception: {}", e.what());
    } catch (...) {
        spdlog::warn("RepairCoordinator: vector orphan cleanup exception (unknown)");
    }
}

boost::asio::awaitable<void> RepairCoordinator::spawnInitialScan() {
    try {
        auto meta = services_ ? services_->getMetadataRepo() : nullptr;
        if (!meta) {
            co_return;
        }

        const bool vectorsDisabled = vectorsDisabledByEnv();
        const size_t batchSize = TuneAdvisor::repairStartupBatchSize();
        size_t offset = 0;
        size_t totalEnqueued = 0;

        while (running_.load(std::memory_order_relaxed)) {
            metadata::DocumentQueryOptions opts;
            opts.limit = static_cast<int>(batchSize);
            opts.offset = static_cast<int>(offset);
            auto batchDocs = meta->queryDocuments(opts);

            if (!batchDocs || batchDocs.value().empty()) {
                break;
            }

            size_t batchEnqueued = 0;
            {
                std::lock_guard<std::mutex> ql(queueMutex_);
                for (const auto& d : batchDocs.value()) {
                    if (!running_.load(std::memory_order_relaxed))
                        break;

                    if (d.repairStatus == yams::metadata::RepairStatus::Completed ||
                        d.repairStatus == yams::metadata::RepairStatus::Processing) {
                        continue;
                    }

                    bool missingEmb = false;
                    if (!vectorsDisabled) {
                        auto hasEmbedRes = meta->hasDocumentEmbeddingByHash(d.sha256Hash);
                        missingEmb = !hasEmbedRes || !hasEmbedRes.value();
                    }
                    const bool missingFts =
                        (!d.contentExtracted) ||
                        (d.extractionStatus != yams::metadata::ExtractionStatus::Success);

                    if (missingEmb || missingFts) {
                        if (pendingSet_.find(d.sha256Hash) == pendingSet_.end()) {
                            pendingSet_.insert(d.sha256Hash);
                            pendingDocuments_.push(d.sha256Hash);
                            ++batchEnqueued;
                        }
                    }
                }
            }

            totalEnqueued += batchEnqueued;
            offset += batchDocs.value().size();

            co_await boost::asio::post(co_await boost::asio::this_coro::executor,
                                       boost::asio::use_awaitable);

            if (batchDocs.value().size() < batchSize) {
                break;
            }
        }

        if (totalEnqueued > 0) {
            totalBacklog_.store(totalEnqueued, std::memory_order_relaxed);
            processed_.store(0, std::memory_order_relaxed);
            spdlog::debug("RepairCoordinator: async scan complete, queued {} documents from {} total",
                          totalEnqueued, offset);
        } else {
            spdlog::debug(
                "RepairCoordinator: async scan complete, no documents need repair (scanned {})",
                offset);
        }
    } catch (const std::exception& e) {
        spdlog::warn("RepairCoordinator: async scan exception: {}", e.what());
    }
    co_return;
}

RepairCoordinator::MissingWorkResult
RepairCoordinator::detectMissingWork(const std::vector<std::string>& batch) {
    MissingWorkResult result;

    auto content = services_ ? services_->getContentStore() : nullptr;
    auto meta_repo = services_ ? services_->getMetadataRepo() : nullptr;
    if (!(content && meta_repo)) {
        return result;
    }

    // Detect documents missing embeddings
    if (!vectorsDisabledByEnv() && meta_repo) {
        for (const auto& hash : batch) {
            auto hasEmbedRes = meta_repo->hasDocumentEmbeddingByHash(hash);
            if (!hasEmbedRes || !hasEmbedRes.value())
                result.missingEmbeddings.push_back(hash);
        }

        if (!result.missingEmbeddings.empty()) {
            auto provider = services_ ? services_->getModelProvider() : nullptr;
            auto vectorDb = services_ ? services_->getVectorDatabase() : nullptr;

            if (provider && provider->isAvailable()) {
                size_t modelDim = 0;
                if (services_) {
                    const auto preferredModel = services_->resolvePreferredModel();
                    if (!preferredModel.empty()) {
                        modelDim = provider->getEmbeddingDim(preferredModel);
                    }
                }
                if (modelDim == 0) {
                    modelDim = provider->getEmbeddingDim("");
                }

                size_t storedDim = 0;
                if (vectorDb) {
                    storedDim = vectorDb->getConfig().embedding_dim;
                }

                if (modelDim > 0 && storedDim > 0 && modelDim != storedDim) {
                    if (cfg_.autoRebuildOnDimMismatch &&
                        !dimMismatchRebuildDone_.exchange(true)) {
                        spdlog::warn(
                            "RepairCoordinator: rebuilding vectors (model dim {} != db dim {})",
                            modelDim, storedDim);
                        if (vectorDb) {
                            try {
                                const auto dbPath = vectorDb->getConfig().database_path;
                                yams::vector::SqliteVecBackend backend;
                                auto initRes = backend.initialize(dbPath);
                                if (!initRes) {
                                    spdlog::warn("RepairCoordinator: open vectors DB failed: {}",
                                                 initRes.error().message);
                                } else {
                                    auto dropRes = backend.dropTables();
                                    if (!dropRes) {
                                        spdlog::warn("RepairCoordinator: dropTables failed: {}",
                                                     dropRes.error().message);
                                    } else {
                                        auto createRes = backend.createTables(modelDim);
                                        if (!createRes) {
                                            spdlog::warn(
                                                "RepairCoordinator: createTables failed: {}",
                                                createRes.error().message);
                                        } else {
                                            ConfigResolver::writeVectorSentinel(cfg_.dataDir,
                                                                                modelDim, "vec0", 1);
                                            spdlog::debug(
                                                "RepairCoordinator: vector schema rebuilt to dim {}",
                                                modelDim);
                                        }
                                    }
                                }
                                backend.close();
                            } catch (const std::exception& e) {
                                spdlog::warn("RepairCoordinator: rebuild failed: {}", e.what());
                            } catch (...) {
                                spdlog::warn("RepairCoordinator: rebuild failed (unknown error)");
                            }
                        }
                    } else {
                        spdlog::debug(
                            "RepairCoordinator: skipping embedding repair - model dimension "
                            "({}) differs from DB dimension ({}). Use 'yams repair "
                            "--rebuild-vectors' to migrate.",
                            modelDim, storedDim);
                        result.missingEmbeddings.clear();
                    }
                } else {
                    spdlog::debug(
                        "RepairCoordinator: {} docs need embeddings, model provider ready (dim={})",
                        result.missingEmbeddings.size(), modelDim);
                }
            }
        }
    }

    // Detect documents missing FTS5 content
    auto meta = services_ ? services_->getMetadataRepo() : nullptr;
    if (meta) {
        auto customExtractors =
            services_ ? services_->getContentExtractors()
                      : std::vector<std::shared_ptr<extraction::IContentExtractor>>{};

        for (const auto& hash : batch) {
            auto docRes = meta->getDocumentByHash(hash);
            if (docRes && docRes.value().has_value()) {
                const auto& d = docRes.value().value();
                if (d.repairStatus == yams::metadata::RepairStatus::Completed ||
                    d.repairStatus == yams::metadata::RepairStatus::Processing) {
                    continue;
                }
                if (!d.contentExtracted ||
                    d.extractionStatus != yams::metadata::ExtractionStatus::Success) {
                    if (canExtractDocument(d.mimeType, d.fileExtension, customExtractors,
                                            content, hash)) {
                        result.missingFts5.push_back(hash);
                    }
                }
            }
        }
    }

    return result;
}

boost::asio::awaitable<void>
RepairCoordinator::runAsync(std::shared_ptr<ShutdownState> shutdownState) {
    using namespace std::chrono_literals;
    auto ex = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(ex);

    core::RepairFsm::Config fsmConfig;
    fsmConfig.enable_online_repair = true;
    fsmConfig.max_repair_concurrency = shutdownState->config.maintenanceTokens;
    fsmConfig.repair_backoff_ms = 250;
    fsmConfig.max_retries = 3;

    core::RepairFsm fsm(fsmConfig);
    fsm.set_on_state_change([](core::RepairFsm::State state) {
        spdlog::debug("RepairFsm state changed to: {}", core::RepairFsm::to_string(state));
    });

    bool initialScanEnqueued = false;
    bool vectorCleanupDone = false;
    // Defer initial scan to avoid blocking during startup
    int deferTicks = 0;
    const int minDeferTicks = 50; // ~5 seconds at 100ms/tick

    // Get or create prune job channel
    static auto pruneQueue =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PruneJob>("prune_jobs",
                                                                                       128);

    // Get or create PathTreeRepair job channel
    static auto pathTreeQueue =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PathTreeJob>(
            "path_tree_repair_jobs", 32);

    // PathTreeRepair completion tracking (run once at startup)
    bool pathTreeRepairDone = false;

    while (running_.load(std::memory_order_relaxed) &&
           shutdownState_->running.load(std::memory_order_acquire)) {
        // Update FSM hints with current running state
        core::RepairFsm::SchedulingHints hints;
        hints.closing = !running_.load(std::memory_order_relaxed) ||
                        !shutdownState_->running.load(std::memory_order_acquire);
        hints.maintenance_allowed = maintenance_allowed();
        fsm.set_scheduling_hints(hints);

        // Check FSM closing signal
        if (fsm.hints().closing) {
            spdlog::debug("RepairCoordinator: FSM signals closing, exiting loop");
            break;
        }

        bool didWork = false;

        // Process prune jobs if available
        InternalEventBus::PruneJob pruneJob;
        if (pruneQueue->try_pop(pruneJob)) {
            didWork = true;
            spdlog::debug("RepairCoordinator: processing prune job {}", pruneJob.requestId);
            InternalEventBus::instance().incPostConsumed();

            // Get metadata repository and content store from services
            auto metaRepo = services_ ? services_->getMetadataRepo() : nullptr;
            auto contentStore = services_ ? services_->getContentStore() : nullptr;

            if (!metaRepo || !contentStore) {
                spdlog::error("RepairCoordinator: services unavailable for prune job {}",
                              pruneJob.requestId);
                continue;
            }

            try {
                // Access storage engine through ContentStoreImpl
                // We need to refactor this - for now, we can't easily get IStorageEngine from
                // IContentStore The proper fix is to make ServiceManager expose getStorageEngine()
                // TODO PBI-062-4: Add getStorageEngine() to ServiceManager
                spdlog::debug("RepairCoordinator: prune job {} - storage engine access needed",
                             pruneJob.requestId);
                spdlog::debug(
                    "RepairCoordinator: prune job {} queued but needs storage engine refactor",
                    pruneJob.requestId);

                // For now, we can execute prune but need proper integration
                // Track this as incomplete work
            } catch (const std::exception& e) {
                spdlog::error("Prune job {} exception: {}", pruneJob.requestId, e.what());
            }
        }

        // Process PathTreeRepair jobs (run once at startup via job queue)
        if (!pathTreeRepairDone) {
            InternalEventBus::PathTreeJob pathTreeJob;
            if (pathTreeQueue->try_pop(pathTreeJob)) {
                didWork = true;
                spdlog::debug("RepairCoordinator: processing PathTreeRepair job");
                InternalEventBus::instance().incPostConsumed();
                co_await processPathTreeRepair();
                pathTreeRepairDone = true;
            }
        }

        // Idle backoff to avoid busy spinning when queues are empty
        if (!didWork) {
            auto snap = TuningSnapshotRegistry::instance().get();
            uint32_t pollMs = snap ? snap->workerPollMs : TuneAdvisor::workerPollMs();
            auto idleMs = std::chrono::milliseconds(std::max<uint32_t>(10, pollMs));
            timer.expires_after(idleMs);
            co_await timer.async_wait(boost::asio::use_awaitable);
        }

        if (!vectorCleanupDone && maintenance_allowed()) {
            performVectorCleanup();
            vectorCleanupDone = true;
        }

        // Defer initial scan to avoid blocking during startup (grace period)
        bool queueEmpty;
        {
            std::lock_guard<std::mutex> lk(queueMutex_);
            queueEmpty = pendingDocuments_.empty();
        }
        if (queueEmpty && !initialScanEnqueued) {
            if (deferTicks < minDeferTicks) {
                ++deferTicks;
            } else if (maintenance_allowed()) {
                spdlog::debug(
                    "RepairCoordinator: starting async initial scan (batched, non-blocking)");
                auto scanExec = RepairThreadPool::instance().get_executor();
                boost::asio::co_spawn(scanExec,
                    [this]() -> boost::asio::awaitable<void> {
                        co_await spawnInitialScan();
                    },
                    boost::asio::detached);
                initialScanEnqueued = true;
            }
        }

        std::vector<std::string> batch;
        {
            std::lock_guard<std::mutex> lk(queueMutex_);
            while (!pendingDocuments_.empty() && batch.size() < shutdownState->config.maxBatch) {
                auto hash = std::move(pendingDocuments_.front());
                pendingDocuments_.pop();
                pendingSet_.erase(hash); // Remove from dedup set when dequeuing
                batch.push_back(std::move(hash));
            }
            if (state_)
                state_->stats.repairQueueDepth.store(
                    static_cast<uint64_t>(pendingDocuments_.size()));
        }
        if (batch.empty()) {
            timer.expires_after(100ms);
            co_await timer.async_wait(boost::asio::use_awaitable);
            // Check FSM closing signal immediately after timer
            hints.closing = !running_.load(std::memory_order_relaxed) ||
                            !shutdownState_->running.load(std::memory_order_acquire);
            fsm.set_scheduling_hints(hints);
            if (fsm.hints().closing) {
                break;
            }
            continue;
        }

        // Check if we're closing
        bool closing = !running_.load(std::memory_order_relaxed);
        if (closing) {
            if (state_)
                state_->stats.repairBusyTicks++;
            continue;
        }

        if (state_)
            state_->stats.repairIdleTicks++;

        auto [missingEmbeddings, missingFts5] = detectMissingWork(batch);
        auto meta_repo = services_ ? services_->getMetadataRepo() : nullptr;

        // Queue embedding repair job with backpressure handling
        if (!missingEmbeddings.empty()) {
            if (state_)
                state_->stats.repairBatchesAttempted++;

            // Mark documents as Processing BEFORE queueing to prevent re-queuing by concurrent
            // scans
            if (meta_repo) {
                auto statusRes = meta_repo->batchUpdateDocumentRepairStatuses(
                    missingEmbeddings, yams::metadata::RepairStatus::Processing);
                if (!statusRes) {
                    spdlog::debug("RepairCoordinator: failed to set Processing status: {}",
                                 statusRes.error().message);
                }
            }

            InternalEventBus::EmbedJob job{missingEmbeddings,
                                           static_cast<uint32_t>(shutdownState->config.maxBatch),
                                           true, std::string{}};

            const uint32_t embedCap = TuneAdvisor::embedChannelCapacity();
            static std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> embedQ =
                InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
                    "embed_jobs", embedCap);

            // Queue with backoff (goal: healthy DB, not speed)
            bool queued = co_await queueWithBackoff(embedQ, std::move(job), timer, running_,
                                                    "embed", missingEmbeddings.size(), 50, 1000);

            if (queued) {
                InternalEventBus::instance().incEmbedQueued();
            } else {
                InternalEventBus::instance().incEmbedDropped();
            }
        }

        bool allowFts5 = maintenance_allowed() ||
                         (shutdownState->config.allowDegraded && activeConnFn_ &&
                          activeConnFn_() <= shutdownState->config.maxActiveDuringDegraded);
        if (!missingFts5.empty() && allowFts5) {
            InternalEventBus::Fts5Job ftsJob{
                .hashes = missingFts5,
                .ids = {},
                .batchSize = static_cast<uint32_t>(shutdownState->config.maxBatch),
                .operation = InternalEventBus::Fts5Operation::ExtractAndIndex};

            static std::shared_ptr<SpscQueue<InternalEventBus::Fts5Job>> fts5Q =
                InternalEventBus::instance().get_or_create_channel<InternalEventBus::Fts5Job>(
                    "fts5_jobs", 512);

            // Queue with backoff (longer delays for DB-heavy FTS5 ops)
            bool queued = co_await queueWithBackoff(fts5Q, std::move(ftsJob), timer, running_,
                                                    "FTS5", missingFts5.size(), 100, 2000);

            if (queued) {
                InternalEventBus::instance().incFts5Queued();
            } else {
                InternalEventBus::instance().incFts5Dropped();
            }
        }

        // Throttle processing to avoid overloading the system (tick-based)
        timer.expires_after(100ms);
        co_await timer.async_wait(boost::asio::use_awaitable);
    }

    spdlog::debug("RepairCoordinator stopped");
    co_return;
}

void RepairCoordinator::update_progress_pct() {
    if (!state_)
        return;
    auto tot = totalBacklog_.load(std::memory_order_relaxed);
    if (tot == 0)
        return;
    auto done = processed_.load(std::memory_order_relaxed);
    int pct = static_cast<int>(std::min<std::uint64_t>(100, (done * 100) / tot));
    state_->readiness.vectorIndexProgress.store(pct, std::memory_order_relaxed);
}

} // namespace yams::daemon
