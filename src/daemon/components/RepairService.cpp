#include <yams/daemon/components/RepairService.h>

#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/content_extractor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include <yams/core/repair_fsm.h>
#include <yams/daemon/components/ConfigResolver.h>

#include <spdlog/spdlog.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>

#include <sqlite3.h>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon {

namespace {

constexpr size_t kMaxTextToPersistInMetadataBytes = 16 * 1024 * 1024; // 16 MiB (best-effort)

// Check if vector operations are disabled via environment variables
bool vectorsDisabledByEnv() {
    if (const char* env = std::getenv("YAMS_DISABLE_VECTORS"); env && *env)
        return true;
    if (const char* env = std::getenv("YAMS_DISABLE_VECTOR_DB"); env && *env)
        return true;
    return false;
}

std::string normalizedRepairExtension(const metadata::DocumentInfo& doc) {
    std::string extension = doc.fileExtension;
    if (extension.empty()) {
        auto pos = doc.fileName.rfind('.');
        if (pos != std::string::npos)
            extension = doc.fileName.substr(pos);
    }
    if (extension.empty())
        return {};
    if (extension.front() != '.')
        extension.insert(extension.begin(), '.');
    std::transform(extension.begin(), extension.end(), extension.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return extension;
}

bool shouldRedetectMime(const metadata::DocumentInfo& doc) {
    if (doc.mimeType.empty() || doc.mimeType == "application/octet-stream")
        return true;

    if (doc.mimeType != "text/plain")
        return false;

    const auto extension = normalizedRepairExtension(doc);
    if (extension.empty())
        return true;

    const auto hintedMime = detection::FileTypeDetector::getMimeTypeFromExtension(extension);
    return !hintedMime.empty() && hintedMime != "text/plain";
}

std::string bestEffortMimeForDocument(const metadata::DocumentInfo& doc,
                                      const std::shared_ptr<api::IContentStore>& store) {
    try {
        (void)detection::FileTypeDetector::initializeWithMagicNumbers();
        auto& detector = detection::FileTypeDetector::instance();
        const auto extension = normalizedRepairExtension(doc);
        const auto hintedMime =
            extension.empty() ? std::string{}
                              : detection::FileTypeDetector::getMimeTypeFromExtension(extension);

        if (store) {
            auto bytesResult = store->retrieveBytes(doc.sha256Hash);
            if (bytesResult && !bytesResult.value().empty()) {
                constexpr std::size_t kMimeSniffBytes = 8192;
                const auto& bytes = bytesResult.value();
                const std::size_t sniffSize = std::min(bytes.size(), kMimeSniffBytes);
                auto detected =
                    detector.detectFromBuffer(std::span<const std::byte>(bytes.data(), sniffSize));
                if (detected) {
                    std::string mime = detected.value().mimeType;
                    if (!hintedMime.empty() && hintedMime != "application/octet-stream") {
                        const auto detectedType = detector.getFileTypeCategory(mime);
                        const auto hintedType = detector.getFileTypeCategory(hintedMime);
                        if (mime.empty() || mime == "application/octet-stream" ||
                            (mime == "text/plain" && hintedMime != "text/plain") ||
                            (detectedType == "executable" && hintedType == "executable" &&
                             mime != hintedMime)) {
                            mime = hintedMime;
                        }
                    }
                    if (!mime.empty())
                        return mime;
                }
            }
        }

        if (!doc.filePath.empty() && std::filesystem::exists(doc.filePath)) {
            if (auto detected = detector.detectFromFile(doc.filePath)) {
                const auto& mime = detected.value().mimeType;
                if (!mime.empty())
                    return mime;
            }
        }

        if (!extension.empty()) {
            const auto mime = hintedMime;
            if (!mime.empty())
                return mime;
        }
    } catch (...) {
    }

    return "application/octet-stream";
}

// Shared thread pool for RepairService background coroutines
struct RepairThreadPool {
    std::unique_ptr<boost::asio::thread_pool> pool_;
    std::once_flag init_flag_;

    static RepairThreadPool& instance() {
        static RepairThreadPool inst;
        return inst;
    }

    boost::asio::any_io_executor get_executor() {
        std::call_once(init_flag_, [this]() {
            uint32_t threads = TuneAdvisor::repairTokensIdle();
            if (threads < 1)
                threads = 1;
            threads = std::max(threads + 1, 2u);
            pool_ = std::make_unique<boost::asio::thread_pool>(threads);
            spdlog::debug("[RepairThreadPool] Initialized with {} threads", threads);
        });
        return pool_->get_executor();
    }

    ~RepairThreadPool() = default;

private:
    RepairThreadPool() = default;
};

// Build extension-to-language map from loaded symbol extractor plugins
std::unordered_map<std::string, std::string> buildExtensionLanguageMap(
    const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>& extractors) {
    std::unordered_map<std::string, std::string> result;
    for (const auto& extractor : extractors) {
        if (!extractor || !extractor->table())
            continue;
        auto supported = extractor->getSupportedExtensions();
        for (const auto& [ext, lang] : supported)
            result[ext] = lang;
    }
    return result;
}

// Check if a document is extractable based on MIME type, extension, and plugins
bool canExtractDocument(
    const std::string& mimeType, const std::string& extension,
    const std::vector<std::shared_ptr<extraction::IContentExtractor>>& customExtractors,
    const std::shared_ptr<yams::api::IContentStore>& contentStore, const std::string& hash) {
    for (const auto& extractor : customExtractors) {
        if (extractor && extractor->supports(mimeType, extension))
            return true;
    }
    auto& detector = yams::detection::FileTypeDetector::instance();
    if (!mimeType.empty() && detector.isTextMimeType(mimeType))
        return true;
    if (mimeType.empty() || mimeType == "application/octet-stream" ||
        mimeType == "application/x-octet-stream") {
        if (!extension.empty()) {
            auto detectedMime =
                yams::detection::FileTypeDetector::getMimeTypeFromExtension(extension);
            if (!detectedMime.empty() && detector.isTextMimeType(detectedMime))
                return true;
        }
        if (contentStore) {
            auto bytesRes = contentStore->retrieveBytes(hash);
            if (bytesRes) {
                const auto& bytes = bytesRes.value();
                size_t checkSize = std::min(bytes.size(), size_t(8192));
                std::span<const std::byte> sample(bytes.data(), checkSize);
                if (!yams::detection::isBinaryData(sample))
                    return true;
            }
        }
    }
    return false;
}

// Template helper: queue job with exponential backoff when full
template <typename JobT>
boost::asio::awaitable<bool>
queueWithBackoff(std::shared_ptr<SpscQueue<JobT>> queue, JobT&& job,
                 boost::asio::steady_timer& timer, std::atomic<bool>& running,
                 const std::string& jobTypeName, size_t jobSize, int initialDelayMs = 50,
                 int maxDelayMs = 1000, std::function<bool()> shouldStop = {}) {
    const auto queueStart = std::chrono::steady_clock::now();
    uint64_t totalWaitMs = 0;
    int retries = 0;
    while (!queue->try_push(std::forward<JobT>(job))) {
        if (!running.load(std::memory_order_relaxed) || (shouldStop && shouldStop()))
            co_return false;
        const int cappedRetries = std::min(retries, 5);
        const int delayMs = std::min(initialDelayMs * (1 << cappedRetries), maxDelayMs);
        totalWaitMs += static_cast<uint64_t>(delayMs);
        const auto queued = queue ? queue->size_approx() : 0;
        const auto capacity = queue ? queue->capacity() : 0;
        spdlog::warn("RepairService: {} queue full, retry={} sleep_ms={} queued={} capacity={} "
                     "batch_size={}",
                     jobTypeName, retries + 1, delayMs, queued, capacity, jobSize);
        timer.expires_after(std::chrono::milliseconds(delayMs));
        co_await timer.async_wait(boost::asio::use_awaitable);
        retries++;
    }
    if (retries > 0) {
        const auto pushLatencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::steady_clock::now() - queueStart)
                                       .count();
        spdlog::info("RepairService: queued {} job after {} retries (batch_size={} wait_ms={} "
                     "push_latency_ms={})",
                     jobTypeName, retries, jobSize, totalWaitMs, pushLatencyMs);
    }
    co_return true;
}

} // namespace

// ============================================================================
// Construction / Destruction / Lifecycle
// ============================================================================

RepairServiceContext makeRepairServiceContext(ServiceManager* services) {
    RepairServiceContext ctx;
    if (!services)
        return ctx;
    ctx.getMetadataRepo = [services] { return services->getMetadataRepo(); };
    ctx.getContentStore = [services] { return services->getContentStore(); };
    ctx.getVectorDatabase = [services] { return services->getVectorDatabase(); };
    ctx.getKgStore = [services] { return services->getKgStore(); };
    ctx.getGraphComponent = [services] { return services->getGraphComponent(); };
    ctx.getPostIngestQueue = [services] { return services->getPostIngestQueue(); };
    ctx.getRepairManager = [services] { return services->getRepairManager(); };
    ctx.getModelProvider = [services] { return services->getModelProvider(); };
    ctx.getEmbeddingQueuedJobs = [services] { return services->getEmbeddingQueuedJobs(); };
    ctx.getEmbeddingInFlightJobs = [services] { return services->getEmbeddingInFlightJobs(); };
    ctx.getContentExtractors =
        [services]() -> const std::vector<std::shared_ptr<extraction::IContentExtractor>>& {
        return services->getContentExtractors();
    };
    ctx.getSymbolExtractors =
        [services]() -> const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>& {
        return services->getSymbolExtractors();
    };
    ctx.resolvePreferredModel = [services] { return services->resolvePreferredModel(); };
    ctx.getEmbeddingModelName = [services] { return services->getEmbeddingModelName(); };
    ctx.rebuildTopologyArtifacts = [services](const std::string& reason, bool dryRun,
                                              const std::vector<std::string>& hashes) {
        return services->rebuildTopologyArtifacts(reason, dryRun, hashes);
    };
    ctx.rebuildSemanticNeighborGraph = [services](const std::string& reason) {
        return services->rebuildSemanticNeighborGraph(reason);
    };
    ctx.vectorIndexCoordinator = services->getVectorIndexCoordinator().get();
    return ctx;
}

RepairService::RepairService(ServiceManager* services, StateComponent* state,
                             std::function<size_t()> activeConnFn, Config cfg)
    : RepairService(makeRepairServiceContext(services), state, std::move(activeConnFn),
                    std::move(cfg)) {}

RepairService::RepairService(RepairServiceContext ctx, StateComponent* state,
                             std::function<size_t()> activeConnFn, Config cfg)
    : ctx_(std::move(ctx)), state_(state), activeConnFn_(std::move(activeConnFn)),
      cfg_(std::move(cfg)), shutdownState_(std::make_shared<ShutdownState>()) {
    coordinator_ = ctx_.vectorIndexCoordinator;
}

RepairService::~RepairService() {
    stop();
}

std::shared_ptr<metadata::IMetadataRepository> RepairService::getMetadataRepoForRepair() const {
    if (!ctx_.getMetadataRepo)
        return nullptr;
    return std::static_pointer_cast<metadata::IMetadataRepository>(ctx_.getMetadataRepo());
}

std::shared_ptr<GraphComponent> RepairService::getGraphComponentForScheduling() const {
    return ctx_.getGraphComponent ? ctx_.getGraphComponent() : nullptr;
}

std::shared_ptr<metadata::KnowledgeGraphStore> RepairService::getKgStoreForScheduling() const {
    return ctx_.getKgStore ? ctx_.getKgStore() : nullptr;
}

const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>&
RepairService::getSymbolExtractorsForScheduling() const {
    static const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> kEmpty;
    return ctx_.getSymbolExtractors ? ctx_.getSymbolExtractors() : kEmpty;
}

void RepairService::start() {
    if (!cfg_.enable || running_.exchange(true))
        return;
    if (state_) {
        state_->stats.repairRunning.store(true, std::memory_order_relaxed);
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
            spdlog::debug("RepairService coroutine starting");
            try {
                if (!shutdownState->running.load(std::memory_order_acquire))
                    co_return;
                co_await self->backgroundLoop(shutdownState.get());
            } catch (const std::exception& e) {
                spdlog::error("RepairService coroutine exception: {}", e.what());
            }
        },
        [shutdownState](const std::exception_ptr& eptr) {
            if (eptr) {
                try {
                    std::rethrow_exception(eptr);
                } catch (const std::exception& e) {
                    spdlog::error("RepairService completion handler exception: {}", e.what());
                }
            }
            shutdownState->finished.store(true, std::memory_order_release);
            shutdownState->cv.notify_all();
        });

    spdlog::debug("RepairService started (enable={}, batch={})", cfg_.enable, cfg_.maxBatch);

    // Enqueue initial PathTreeRepair job
    static auto pathTreeQueue =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PathTreeJob>(
            "path_tree_repair_jobs", 32);
    InternalEventBus::PathTreeJob initialJob{1, true};
    (void)pathTreeQueue->try_push(initialJob);
}

void RepairService::stop() {
    if (!running_.exchange(false))
        return;
    if (state_) {
        state_->stats.repairRunning.store(false, std::memory_order_relaxed);
        state_->stats.repairInProgress.store(false, std::memory_order_relaxed);
    }
    shutdownState_->running.store(false, std::memory_order_release);
    queueCv_.notify_all();
    {
        std::unique_lock<std::mutex> lk(shutdownState_->mutex);
        shutdownState_->cv.wait_for(lk, std::chrono::milliseconds(2000),
                                    [this] { return shutdownState_->finished.load(); });
    }
    {
        std::unique_lock<std::mutex> lk(activeRepairMutex_);
        activeRepairCv_.wait(lk, [this] { return activeRepairExecutions_ == 0; });
    }
    spdlog::debug("RepairService stopped");
}

bool RepairService::maintenanceAllowed() const {
    if (!activeConnFn_)
        return false;
    return activeConnFn_() == 0;
}

// ============================================================================
// Event-driven interface
// ============================================================================

void RepairService::onDocumentAdded(const DocumentAddedEvent& event) {
    if (!cfg_.enable || !running_)
        return;

    // If PostIngestQueue is handling this document, skip
    if (ctx_.getPostIngestQueue) {
        auto piq = ctx_.getPostIngestQueue();
        if (piq && piq->started()) {
            spdlog::debug("RepairService: skipping DocumentAdded {} -- handled by PostIngestQueue",
                          event.hash);
            return;
        }
    }

    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (pendingSet_.find(event.hash) == pendingSet_.end()) {
            if (cfg_.maxPendingRepairs > 0 && pendingDocuments_.size() >= cfg_.maxPendingRepairs) {
                if (state_) {
                    state_->stats.repairQueueDepth.store(
                        static_cast<uint64_t>(pendingDocuments_.size()));
                }
                spdlog::warn("RepairService: dropping DocumentAdded {} -- pending queue at cap {}",
                             event.hash, cfg_.maxPendingRepairs);
                return;
            }
            pendingSet_.insert(event.hash);
            pendingDocuments_.push(event.hash);
        }
        if (state_)
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
    }
    queueCv_.notify_one();

    // Opportunistically schedule symbol/entity extraction
    try {
        {
            auto gc = getGraphComponentForScheduling();
            const auto& symbolExtractors = getSymbolExtractorsForScheduling();
            if (gc && !symbolExtractors.empty()) {
                static thread_local std::unordered_map<std::string, std::string> extToLang;
                static thread_local bool mapInitialized = false;
                if (!mapInitialized) {
                    extToLang = buildExtensionLanguageMap(symbolExtractors);
                    mapInitialized = true;
                }
                std::string extension;
                auto dotPos = event.path.find_last_of('.');
                if (dotPos != std::string::npos)
                    extension = event.path.substr(dotPos + 1);
                std::string dottedExt;
                if (!extension.empty())
                    dottedExt = "." + extension;
                auto it = extToLang.find(extension);
                if (it == extToLang.end() && !dottedExt.empty())
                    it = extToLang.find(dottedExt);
                if (it != extToLang.end()) {
                    auto kg = getKgStoreForScheduling();
                    std::optional<std::int64_t> docId;
                    if (kg) {
                        auto idRes = kg->getDocumentIdByHash(event.hash);
                        if (idRes.has_value())
                            docId = idRes.value();
                    }
                    bool alreadyExtracted = false;
                    if (kg && docId.has_value()) {
                        auto entRes = kg->getDocEntitiesForDocument(docId.value(), 1, 0);
                        if (entRes.has_value() && !entRes.value().empty())
                            alreadyExtracted = true;
                    }
                    if (!alreadyExtracted) {
                        GraphComponent::EntityExtractionJob job{.documentHash = event.hash,
                                                                .filePath = event.path};
                        job.language.append(it->second.data(), it->second.size());
                        (void)gc->submitEntityExtraction(std::move(job));
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("RepairService: symbol extraction scheduling failed: {}", e.what());
    } catch (...) {
    }
}

void RepairService::onDocumentRemoved(const DocumentRemovedEvent& event) {
    if (!cfg_.enable || !running_)
        return;
    spdlog::debug("RepairService: document {} removed", event.hash);
}

void RepairService::enqueueEmbeddingRepair(const std::vector<std::string>& hashes) {
    if (!cfg_.enable || !running_ || hashes.empty())
        return;
    size_t enqueuedCount = 0;
    size_t droppedAtCap = 0;
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        for (const auto& hash : hashes) {
            if (pendingSet_.find(hash) == pendingSet_.end()) {
                if (cfg_.maxPendingRepairs > 0 &&
                    pendingDocuments_.size() >= cfg_.maxPendingRepairs) {
                    ++droppedAtCap;
                    continue;
                }
                pendingSet_.insert(hash);
                pendingDocuments_.push(hash);
                ++enqueuedCount;
            }
        }
        if (state_)
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
    }
    queueCv_.notify_one();
    spdlog::debug("RepairService: queued {} docs for embedding repair", enqueuedCount);
    if (droppedAtCap > 0) {
        spdlog::warn("RepairService: dropped {} embedding-repair hashes -- pending queue at cap {}",
                     droppedAtCap, cfg_.maxPendingRepairs);
    }
}

// ============================================================================
// Background Loop (ported from RepairCoordinator::runAsync)
// ============================================================================

boost::asio::awaitable<void> RepairService::backgroundLoop(ShutdownState* shutdownState) {
    using namespace std::chrono_literals;
    auto ex = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(ex);

    core::RepairFsm::Config fsmConfig;
    fsmConfig.enable_online_repair = true;
    fsmConfig.max_repair_concurrency = shutdownState->config.maintenanceTokens;
    fsmConfig.repair_backoff_ms = 250;
    fsmConfig.max_retries = static_cast<uint32_t>(shutdownState->config.maxRetries);

    core::RepairFsm fsm(fsmConfig);
    fsm.set_on_state_change([](core::RepairFsm::State state) {
        spdlog::debug("RepairFsm state: {}", core::RepairFsm::to_string(state));
    });

    bool initialScanEnqueued = false;
    bool vectorCleanupDone = false;
    int deferTicks = 0;
    const int minDeferTicks = 50;

    static auto pruneQueue =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PruneJob>("prune_jobs",
                                                                                       128);
    static auto pathTreeQueue =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PathTreeJob>(
            "path_tree_repair_jobs", 32);

    bool pathTreeRepairDone = false;

    while (running_.load(std::memory_order_relaxed) &&
           shutdownState_->running.load(std::memory_order_acquire)) {
        core::RepairFsm::SchedulingHints hints;
        hints.closing = !running_.load(std::memory_order_relaxed) ||
                        !shutdownState_->running.load(std::memory_order_acquire);
        hints.maintenance_allowed = maintenanceAllowed();
        fsm.set_scheduling_hints(hints);

        if (fsm.hints().closing)
            break;

        bool didWork = false;

        // Process prune jobs
        InternalEventBus::PruneJob pruneJob;
        if (pruneQueue->try_pop(pruneJob)) {
            didWork = true;
            InternalEventBus::instance().incPostConsumed();
            spdlog::debug("RepairService: prune job {} needs storage engine refactor",
                          pruneJob.requestId);
        }

        // Process PathTreeRepair jobs
        if (!pathTreeRepairDone) {
            InternalEventBus::PathTreeJob pathTreeJob;
            if (pathTreeQueue->try_pop(pathTreeJob)) {
                didWork = true;
                InternalEventBus::instance().incPostConsumed();
                co_await processPathTreeRepair();
                pathTreeRepairDone = true;
            }
        }

        // Idle backoff
        if (!didWork) {
            auto snap = TuningSnapshotRegistry::instance().get();
            uint32_t pollMs = snap ? snap->workerPollMs : TuneAdvisor::workerPollMs();
            if (snap && snap->daemonIdle) {
                pollMs = std::max<uint32_t>(TuneAdvisor::idleTickMs(), pollMs);
            }
            timer.expires_after(std::chrono::milliseconds(std::max<uint32_t>(10, pollMs)));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }

        if (!vectorCleanupDone && maintenanceAllowed()) {
            performVectorCleanup();
            vectorCleanupDone = true;
        }

        // Deferred initial scan
        bool queueEmpty;
        {
            std::lock_guard<std::mutex> lk(queueMutex_);
            queueEmpty = pendingDocuments_.empty();
        }
        if (queueEmpty && !initialScanEnqueued) {
            if (deferTicks < minDeferTicks) {
                ++deferTicks;
            } else if (maintenanceAllowed()) {
                auto scanExec = RepairThreadPool::instance().get_executor();
                boost::asio::co_spawn(
                    scanExec,
                    [this]() -> boost::asio::awaitable<void> { co_await spawnInitialScan(); },
                    boost::asio::detached);
                initialScanEnqueued = true;
            }
        }

        // Drain pending batch
        std::vector<std::string> batch;
        {
            std::lock_guard<std::mutex> lk(queueMutex_);
            while (!pendingDocuments_.empty() && batch.size() < shutdownState->config.maxBatch) {
                auto hash = std::move(pendingDocuments_.front());
                pendingDocuments_.pop();
                pendingSet_.erase(hash);
                batch.push_back(std::move(hash));
            }
            if (state_)
                state_->stats.repairQueueDepth.store(
                    static_cast<uint64_t>(pendingDocuments_.size()));
        }
        if (batch.empty()) {
            timer.expires_after(100ms);
            co_await timer.async_wait(boost::asio::use_awaitable);
            continue;
        }

        if (!running_.load(std::memory_order_relaxed))
            continue;

        if (state_)
            state_->stats.repairIdleTicks++;

        auto [missingEmbeddings, missingFts5] = detectMissingWork(batch);
        auto meta_repo = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;

        // Check ResourceGovernor before queuing repair work.
        // Under elevated pressure the ingestion pipeline is already competing for
        // embed/FTS5 channel capacity; repair should yield to avoid amplifying
        // the pressure that caused the governor to throttle in the first place.
        auto& governor = ResourceGovernor::instance();
        const auto pressureLevel = governor.getPressureLevel();
        if (pressureLevel >= ResourcePressureLevel::Critical) {
            // Re-enqueue the batch so it is not lost; back off and retry later.
            {
                std::lock_guard<std::mutex> lk(queueMutex_);
                for (auto& h : batch) {
                    if (pendingSet_.find(h) == pendingSet_.end()) {
                        pendingSet_.insert(h);
                        pendingDocuments_.push(std::move(h));
                    }
                }
                if (state_)
                    state_->stats.repairQueueDepth.store(
                        static_cast<uint64_t>(pendingDocuments_.size()));
            }
            spdlog::debug("RepairService: deferring batch ({} docs) under {} pressure",
                          batch.size(), pressureLevelName(pressureLevel));
            timer.expires_after(std::chrono::milliseconds(500));
            co_await timer.async_wait(boost::asio::use_awaitable);
            continue;
        }

        // Under Warning pressure, reduce batch sizes so repair cooperates with
        // the ingestion pipeline instead of competing for channel capacity.
        if (pressureLevel >= ResourcePressureLevel::Warning) {
            const size_t reducedCap = std::max<size_t>(1, missingEmbeddings.size() / 2);
            if (missingEmbeddings.size() > reducedCap) {
                // Put excess back into the pending queue for the next iteration.
                std::lock_guard<std::mutex> lk(queueMutex_);
                for (size_t i = reducedCap; i < missingEmbeddings.size(); ++i) {
                    if (pendingSet_.find(missingEmbeddings[i]) == pendingSet_.end()) {
                        pendingSet_.insert(missingEmbeddings[i]);
                        pendingDocuments_.push(missingEmbeddings[i]);
                    }
                }
                missingEmbeddings.resize(reducedCap);
                if (state_)
                    state_->stats.repairQueueDepth.store(
                        static_cast<uint64_t>(pendingDocuments_.size()));
            }
        }

        // Queue embedding repair jobs
        if (!missingEmbeddings.empty()) {
            if (state_)
                state_->stats.repairBatchesAttempted++;
            if (meta_repo) {
                // Mark as Processing before queuing
                auto statusRes = meta_repo->batchUpdateDocumentRepairStatuses(
                    missingEmbeddings, yams::metadata::RepairStatus::Processing);
                (void)statusRes;
            }

            InternalEventBus::EmbedJob job{
                missingEmbeddings, static_cast<uint32_t>(shutdownState->config.maxBatch), true,
                std::string{},     std::vector<InternalEventBus::EmbedPreparedDoc>{},     nullptr};
            job.updateSemanticGraph = false;
            static std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> embedQ =
                InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
                    "embed_jobs", TuneAdvisor::embedChannelCapacity());
            bool queued = co_await queueWithBackoff(embedQ, std::move(job), timer, running_,
                                                    "embed", missingEmbeddings.size(), 50, 1000);
            if (queued) {
                InternalEventBus::instance().incEmbedQueued();
            } else {
                if (meta_repo) {
                    // Avoid leaving documents stuck in Processing when enqueue fails (shutdown,
                    // channel pressure). They can be retried by subsequent repair passes.
                    (void)meta_repo->batchUpdateDocumentRepairStatuses(
                        missingEmbeddings, yams::metadata::RepairStatus::Pending);
                }
                InternalEventBus::instance().incEmbedDropped();
            }
        }

        // Queue FTS5 jobs — also gated by governor admission control.
        // FTS5 indexing is less resource-intensive than embedding but still adds
        // I/O pressure; skip under Critical/Emergency to let ingestion proceed.
        bool allowFts5 = (pressureLevel < ResourcePressureLevel::Critical) &&
                         (maintenanceAllowed() ||
                          (shutdownState->config.allowDegraded && activeConnFn_ &&
                           activeConnFn_() <= shutdownState->config.maxActiveDuringDegraded));
        if (!missingFts5.empty() && allowFts5) {
            InternalEventBus::Fts5Job ftsJob{
                .hashes = missingFts5,
                .ids = {},
                .batchSize = static_cast<uint32_t>(shutdownState->config.maxBatch),
                .operation = InternalEventBus::Fts5Operation::ExtractAndIndex};
            static std::shared_ptr<SpscQueue<InternalEventBus::Fts5Job>> fts5Q =
                InternalEventBus::instance().get_or_create_channel<InternalEventBus::Fts5Job>(
                    "fts5_jobs", 512);
            bool queued = co_await queueWithBackoff(fts5Q, std::move(ftsJob), timer, running_,
                                                    "FTS5", missingFts5.size(), 100, 2000);
            if (queued)
                InternalEventBus::instance().incFts5Queued();
            else
                InternalEventBus::instance().incFts5Dropped();
        }

        timer.expires_after(100ms);
        co_await timer.async_wait(boost::asio::use_awaitable);
    }

    spdlog::debug("RepairService background loop stopped");
    co_return;
}

boost::asio::awaitable<void> RepairService::spawnInitialScan() {
    try {
        auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
        if (!meta)
            co_return;

        const bool vectorsDisabled = vectorsDisabledByEnv();
        const size_t batchSize = TuneAdvisor::repairStartupBatchSize();
        size_t offset = 0;
        size_t totalEnqueued = 0;

        // Preload all FTS5-indexed rowids once for O(1) lookups in the scan loop.
        std::unordered_set<int64_t> ftsRowIds;
        {
            auto ftsRes = meta->getFts5IndexedRowIdSet();
            if (ftsRes)
                ftsRowIds = std::move(ftsRes.value());
        }

        while (running_.load(std::memory_order_relaxed)) {
            metadata::DocumentQueryOptions opts;
            opts.limit = static_cast<int>(batchSize);
            opts.offset = static_cast<int>(offset);
            auto batchDocs = meta->queryDocuments(opts);
            if (!batchDocs || batchDocs.value().empty())
                break;

            size_t batchEnqueued = 0;
            {
                std::lock_guard<std::mutex> ql(queueMutex_);
                for (const auto& d : batchDocs.value()) {
                    if (!running_.load(std::memory_order_relaxed))
                        break;
                    if (d.repairStatus == yams::metadata::RepairStatus::Processing)
                        continue;

                    // NEW: recover stalled Processing on startup
                    // (handled by detectStuckDocuments during on-demand repair)

                    bool missingEmb = false;
                    if (!vectorsDisabled) {
                        auto hasEmbedRes = meta->hasDocumentEmbeddingByHash(d.sha256Hash);
                        missingEmb = !hasEmbedRes || !hasEmbedRes.value();
                    }
                    bool missingFts =
                        (!d.contentExtracted) &&
                        (d.extractionStatus != yams::metadata::ExtractionStatus::Skipped) &&
                        (d.extractionStatus != yams::metadata::ExtractionStatus::Success);
                    // Also detect successful extraction with missing FTS5 entry.
                    if (!missingFts &&
                        d.extractionStatus == yams::metadata::ExtractionStatus::Success &&
                        ftsRowIds.count(d.id) == 0) {
                        missingFts = true;
                    }

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
            if (batchDocs.value().size() < batchSize)
                break;
        }

        if (totalEnqueued > 0) {
            totalBacklog_.store(totalEnqueued, std::memory_order_relaxed);
            processed_.store(0, std::memory_order_relaxed);
            if (state_) {
                state_->stats.repairTotalBacklog.store(totalEnqueued, std::memory_order_relaxed);
                state_->stats.repairProcessed.store(0, std::memory_order_relaxed);
            }
            spdlog::debug("RepairService: initial scan queued {} documents", totalEnqueued);
        }
    } catch (const std::exception& e) {
        spdlog::warn("RepairService: initial scan exception: {}", e.what());
    }
    co_return;
}

boost::asio::awaitable<void> RepairService::processPathTreeRepair() {
    using namespace std::chrono_literals;
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    auto repairMgr = ctx_.getRepairManager ? ctx_.getRepairManager() : nullptr;
    auto metaRepo = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!repairMgr || !metaRepo)
        co_return;

    try {
        auto docsResult = metaRepo->queryDocuments(metadata::DocumentQueryOptions{});
        if (!docsResult || docsResult.value().empty())
            co_return;

        uint64_t created = 0, errors = 0;
        const size_t batchSz = TuneAdvisor::repairStartupBatchSize();

        for (size_t i = 0; i < docsResult.value().size(); i += batchSz) {
            if (!running_.load(std::memory_order_relaxed))
                break;
            size_t batchEnd = std::min(i + batchSz, docsResult.value().size());
            for (size_t j = i; j < batchEnd; ++j) {
                const auto& doc = docsResult.value()[j];
                if (doc.filePath.empty())
                    continue;
                auto existingNode = metaRepo->findPathTreeNodeByFullPath(doc.filePath);
                if (existingNode && existingNode.value().has_value())
                    continue;
                try {
                    auto treeRes = metaRepo->upsertPathTreeForDocument(doc, doc.id, true,
                                                                       std::span<const float>());
                    if (treeRes)
                        ++created;
                    else
                        ++errors;
                } catch (...) {
                    ++errors;
                }
            }
            timer.expires_after(10ms);
            co_await timer.async_wait(boost::asio::use_awaitable);
        }

        spdlog::debug("RepairService: PathTreeRepair complete (created={}, errors={})", created,
                      errors);
    } catch (const std::exception& e) {
        spdlog::debug("RepairService: PathTreeRepair exception: {}", e.what());
    }
}

void RepairService::performVectorCleanup() {
    if (vectorsDisabledByEnv())
        return;
    try {
        auto vectorDb = ctx_.getVectorDatabase ? ctx_.getVectorDatabase() : nullptr;
        if (vectorDb) {
            auto cleanup = vectorDb->cleanupOrphanRows();
            if (cleanup)
                spdlog::debug("RepairService: vector orphan cleanup done");
            else
                spdlog::warn("RepairService: vector cleanup failed: {}", cleanup.error().message);
        }
    } catch (const std::exception& e) {
        spdlog::warn("RepairService: vector cleanup exception: {}", e.what());
    }
}

RepairService::MissingWorkFlags RepairService::analyzeMissingWorkForHash(
    const std::string& hash, bool checkEmbeddings,
    const std::shared_ptr<api::IContentStore>& contentStore,
    const std::shared_ptr<metadata::MetadataRepository>& metaRepo,
    const std::vector<std::shared_ptr<extraction::IContentExtractor>>& customExtractors) const {
    MissingWorkFlags flags;
    if (!metaRepo) {
        return flags;
    }

    if (checkEmbeddings) {
        auto hasEmbedRes = metaRepo->hasDocumentEmbeddingByHash(hash);
        flags.missingEmbedding = !hasEmbedRes || !hasEmbedRes.value();
    }

    auto docRes = metaRepo->getDocumentByHash(hash);
    if (!(docRes && docRes.value().has_value())) {
        return flags;
    }

    const auto& d = docRes.value().value();
    if (d.repairStatus == yams::metadata::RepairStatus::Completed ||
        d.repairStatus == yams::metadata::RepairStatus::Processing) {
        return flags;
    }

    if (!d.contentExtracted || d.extractionStatus != yams::metadata::ExtractionStatus::Success) {
        flags.missingFts5 =
            canExtractDocument(d.mimeType, d.fileExtension, customExtractors, contentStore, hash);
        return flags;
    }

    auto ftsRes = metaRepo->hasFtsEntry(d.id);
    flags.missingFts5 = ftsRes && !ftsRes.value();
    return flags;
}

RepairService::MissingWorkResult
RepairService::detectMissingWork(const std::vector<std::string>& batch) {
    MissingWorkResult result;
    auto content = ctx_.getContentStore ? ctx_.getContentStore() : nullptr;
    auto meta_repo = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!(content && meta_repo))
        return result;

    const bool checkEmbeddings = !vectorsDisabledByEnv();
    static const std::vector<std::shared_ptr<extraction::IContentExtractor>> kEmptyExtractors;
    const auto& customExtractors =
        ctx_.getContentExtractors ? ctx_.getContentExtractors() : kEmptyExtractors;

    std::vector<MissingWorkFlags> flags(batch.size());
    std::vector<std::exception_ptr> errors(batch.size());

    for (std::size_t i = 0; i < batch.size(); ++i) {
        try {
            flags[i] = analyzeMissingWorkForHash(batch[i], checkEmbeddings, content, meta_repo,
                                                 customExtractors);
        } catch (...) {
            errors[i] = std::current_exception();
        }
    }

    for (std::size_t i = 0; i < batch.size(); ++i) {
        if (errors[i]) {
            try {
                std::rethrow_exception(errors[i]);
            } catch (const std::exception& e) {
                spdlog::warn("RepairService: detectMissingWork failed for {}: {}", batch[i],
                             e.what());
            } catch (...) {
                spdlog::warn("RepairService: detectMissingWork failed for {}", batch[i]);
            }
            continue;
        }
        if (flags[i].missingEmbedding) {
            result.missingEmbeddings.push_back(batch[i]);
        }
        if (flags[i].missingFts5) {
            result.missingFts5.push_back(batch[i]);
        }
    }

    if (!result.missingEmbeddings.empty()) {
        auto provider = ctx_.getModelProvider ? ctx_.getModelProvider() : nullptr;
        auto vectorDb = ctx_.getVectorDatabase ? ctx_.getVectorDatabase() : nullptr;
        if (provider && provider->isAvailable()) {
            size_t modelDim = 0;
            if (ctx_.resolvePreferredModel) {
                const auto preferredModel = ctx_.resolvePreferredModel();
                if (!preferredModel.empty())
                    modelDim = provider->getEmbeddingDim(preferredModel);
            }
            if (modelDim == 0)
                modelDim = provider->getEmbeddingDim("");

            size_t storedDim = 0;
            if (vectorDb)
                storedDim = vectorDb->getConfig().embedding_dim;

            if (modelDim > 0 && storedDim > 0 && modelDim != storedDim) {
                if (cfg_.autoRebuildOnDimMismatch) {
                    static std::atomic<bool> dimMismatchLogged{false};
                    if (!dimMismatchLogged.exchange(true)) {
                        spdlog::warn("RepairService: dim mismatch (model={} db={}); "
                                     "requesting VectorIndexCoordinator rebuild",
                                     modelDim, storedDim);
                    }
                    if (coordinator_) {
                        // Coalescing rebuild — the coordinator handles beginBulkLoad / finalize.
                        // Fire-and-forget; the background loop picks up missing embeddings on
                        // the next tick after the rebuild completes.
                        coordinator_->postRebuild(yams::daemon::RebuildReason::DimensionMismatch);
                    }
                    // Also recreate the tables with the new dimension so embeddings can be stored.
                    if (vectorDb) {
                        try {
                            const auto dbPath = vectorDb->getConfig().database_path;
                            yams::vector::SqliteVecBackend backend;
                            auto initRes = backend.initialize(dbPath);
                            if (initRes) {
                                auto dropRes = backend.dropTables();
                                if (dropRes) {
                                    auto createRes = backend.createTables(modelDim);
                                    if (createRes)
                                        ConfigResolver::writeVectorSentinel(cfg_.dataDir, modelDim,
                                                                            "vec0", 1);
                                }
                            }
                            backend.close();
                        } catch (...) {
                        }
                    }
                } else {
                    result.missingEmbeddings.clear();
                }
            }
        }
    }

    return result;
}

void RepairService::updateProgressPct() {
    if (!state_)
        return;
    auto tot = totalBacklog_.load(std::memory_order_relaxed);
    state_->stats.repairTotalBacklog.store(tot, std::memory_order_relaxed);
    if (tot == 0)
        return;
    auto done = processed_.load(std::memory_order_relaxed);
    state_->stats.repairProcessed.store(done, std::memory_order_relaxed);
    int pct = static_cast<int>(std::min<std::uint64_t>(100, (done * 100) / tot));
    state_->readiness.vectorIndexProgress.store(pct, std::memory_order_relaxed);
}

// ============================================================================
// On-demand Repair (RPC entry point)
// ============================================================================

RepairResponse RepairService::executeRepair(const RepairRequest& request, ProgressFn progress,
                                            std::atomic<bool>* cancelRequested) {
    std::unique_lock<std::mutex> lock(repairMutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
        // Another repair RPC is already running — return immediately.
        RepairResponse busy;
        busy.success = false;
        busy.errors.push_back(
            "Repair is already in progress. Please wait for the current run to finish.");
        spdlog::info("[RepairService] executeRepair rejected: repair already in progress");
        return busy;
    }

    repairInProgress_.store(true, std::memory_order_release);
    if (state_) {
        state_->stats.repairInProgress.store(true, std::memory_order_relaxed);
    }
    {
        std::lock_guard<std::mutex> lk(activeRepairMutex_);
        ++activeRepairExecutions_;
    }
    // RAII guard to clear the flag when we leave this scope.
    struct InProgressGuard {
        RepairService* self;
        std::atomic<bool>& flag;
        std::atomic<bool>* statsFlag;
        ~InProgressGuard() {
            flag.store(false, std::memory_order_release);
            if (statsFlag) {
                statsFlag->store(false, std::memory_order_relaxed);
            }
            if (self) {
                std::lock_guard<std::mutex> lk(self->activeRepairMutex_);
                if (self->activeRepairExecutions_ > 0) {
                    --self->activeRepairExecutions_;
                }
                self->activeRepairCv_.notify_all();
            }
        }
    } inProgressGuard{this, repairInProgress_, state_ ? &state_->stats.repairInProgress : nullptr};

    RepairResponse response;
    std::vector<RepairOperationResult> results;
    std::vector<std::string> errors;
    auto isCanceled = [&]() {
        return cancelRequested && cancelRequested->load(std::memory_order_relaxed);
    };

    auto emitCancel = [&](const std::string& operation) {
        if (!progress) {
            return;
        }
        RepairEvent ev;
        ev.phase = "error";
        ev.operation = operation;
        ev.message = "Repair canceled";
        progress(ev);
    };

    auto runOp = [&](const std::string& name, auto&& fn) {
        if (isCanceled()) {
            emitCancel(name);
            errors.push_back("Repair canceled");
            return false;
        }
        if (progress) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = name;
            ev.message = "Starting " + name + "...";
            progress(ev);
        }
        auto result = fn();
        results.push_back(result);
        response.totalOperations++;
        response.totalSucceeded += result.succeeded;
        response.totalFailed += result.failed;
        response.totalSkipped += result.skipped;
        if (result.failed > 0 && !result.message.empty())
            errors.push_back(name + ": " + result.message);
        if (progress) {
            RepairEvent ev;
            ev.phase = "completed";
            ev.operation = name;
            ev.processed = result.processed;
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = std::move(result.message);
            progress(ev);
        }
        if (isCanceled()) {
            emitCancel(name);
            errors.push_back("Repair canceled");
            return false;
        }
        return true;
    };

    bool doOrphans = request.repairOrphans || request.repairAll;
    bool doMime = request.repairMime || request.repairAll;
    bool doDownloads = request.repairDownloads || request.repairAll;
    bool doPathTree = request.repairPathTree || request.repairAll;
    bool doChunks = request.repairChunks || request.repairAll;
    bool doBlockRefs = request.repairBlockRefs || request.repairAll;
    bool doFts5 = request.repairFts5 || request.repairAll;
    bool doEmbeddings = request.repairEmbeddings || request.repairAll;
    bool doStuckDocs = request.repairStuckDocs || request.repairAll;
    bool doGraph = request.repairGraph || request.repairAll;
    bool doTopology = request.repairTopology;
    bool doDedupe = request.repairDedupe || request.repairAll;
    bool doOptimize = request.optimizeDb || request.repairAll;

    // Phase 0: Stuck document recovery
    if (doStuckDocs)
        if (!runOp("stuck_docs", [&] { return recoverStuckDocuments(request, progress); }))
            goto finalize;

    // Phase 1: Metadata repair
    if (doOrphans)
        if (!runOp("orphans", [&] {
                return cleanOrphanedMetadata(request.dryRun, request.verbose, progress);
            }))
            goto finalize;
    if (doMime)
        if (!runOp("mime",
                   [&] { return repairMimeTypes(request.dryRun, request.verbose, progress); }))
            goto finalize;
    if (doDownloads)
        if (!runOp("downloads",
                   [&] { return repairDownloads(request.dryRun, request.verbose, progress); }))
            goto finalize;
    if (doPathTree)
        if (!runOp("path_tree",
                   [&] { return rebuildPathTree(request.dryRun, request.verbose, progress); }))
            goto finalize;
    if (doDedupe)
        if (!runOp("dedupe", [&] { return applySemanticDedupe(request, progress); }))
            goto finalize;

    // Phase 2: Storage repair
    if (doChunks)
        if (!runOp("chunks",
                   [&] { return cleanOrphanedChunks(request.dryRun, request.verbose, progress); }))
            goto finalize;
    if (doBlockRefs)
        if (!runOp("block_refs", [&] {
                return repairBlockReferences(request.dryRun, request.verbose, progress);
            }))
            goto finalize;

    // Phase 2.5: Knowledge graph repair
    if (doGraph)
        if (!runOp("graph", [&] { return repairKnowledgeGraph(request, progress); }))
            goto finalize;

    // Phase 3: Search index repair
    if (doFts5)
        if (!runOp("fts5", [&] { return rebuildFts5Index(request, progress); }))
            goto finalize;
    if (doEmbeddings)
        if (!runOp("embeddings",
                   [&] { return generateMissingEmbeddings(request, progress, cancelRequested); }))
            goto finalize;
    if (doTopology)
        if (!runOp("topology", [&] { return rebuildTopologyArtifacts(request, progress); }))
            goto finalize;

    // Phase 4: Database maintenance
    if (doOptimize)
        if (!runOp("optimize",
                   [&] { return optimizeDatabase(request.dryRun, request.verbose, progress); }))
            goto finalize;

finalize:
    response.success = errors.empty();
    response.errors = std::move(errors);
    response.operationResults = std::move(results);
    return response;
}

boost::asio::awaitable<RepairResponse>
RepairService::executeRepairAsync(const RepairRequest& request, ProgressFn progress,
                                  std::atomic<bool>* cancelRequested) {
    std::unique_lock<std::mutex> lock(repairMutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
        RepairResponse busy;
        busy.success = false;
        busy.errors.push_back(
            "Repair is already in progress. Please wait for the current run to finish.");
        spdlog::info("[RepairService] executeRepairAsync rejected: repair already in progress");
        co_return busy;
    }

    repairInProgress_.store(true, std::memory_order_release);
    if (state_) {
        state_->stats.repairInProgress.store(true, std::memory_order_relaxed);
    }
    {
        std::lock_guard<std::mutex> lk(activeRepairMutex_);
        ++activeRepairExecutions_;
    }
    struct InProgressGuard {
        RepairService* self;
        std::atomic<bool>& flag;
        std::atomic<bool>* statsFlag;
        ~InProgressGuard() {
            flag.store(false, std::memory_order_release);
            if (statsFlag) {
                statsFlag->store(false, std::memory_order_relaxed);
            }
            if (self) {
                std::lock_guard<std::mutex> lk(self->activeRepairMutex_);
                if (self->activeRepairExecutions_ > 0) {
                    --self->activeRepairExecutions_;
                }
                self->activeRepairCv_.notify_all();
            }
        }
    } inProgressGuard{this, repairInProgress_, state_ ? &state_->stats.repairInProgress : nullptr};

    RepairResponse response;
    std::vector<RepairOperationResult> results;
    std::vector<std::string> errors;
    auto isCanceled = [&]() {
        return cancelRequested && cancelRequested->load(std::memory_order_relaxed);
    };

    auto emitCancel = [&](const std::string& operation) {
        if (!progress) {
            return;
        }
        RepairEvent ev;
        ev.phase = "error";
        ev.operation = operation;
        ev.message = "Repair canceled";
        progress(ev);
    };

    auto runOp = [&](const std::string& name, auto&& fn) {
        if (isCanceled()) {
            emitCancel(name);
            errors.push_back("Repair canceled");
            return false;
        }
        if (progress) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = name;
            ev.message = "Starting " + name + "...";
            progress(ev);
        }
        auto result = fn();
        results.push_back(result);
        response.totalOperations++;
        response.totalSucceeded += result.succeeded;
        response.totalFailed += result.failed;
        response.totalSkipped += result.skipped;
        if (result.failed > 0 && !result.message.empty()) {
            errors.push_back(name + ": " + result.message);
        }
        if (progress) {
            RepairEvent ev;
            ev.phase = "completed";
            ev.operation = name;
            ev.processed = result.processed;
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = std::move(result.message);
            progress(ev);
        }
        if (isCanceled()) {
            emitCancel(name);
            errors.push_back("Repair canceled");
            return false;
        }
        return true;
    };

    auto runAsyncOp = [&](const std::string& name, auto&& fn) -> boost::asio::awaitable<bool> {
        if (isCanceled()) {
            emitCancel(name);
            errors.push_back("Repair canceled");
            co_return false;
        }
        if (progress) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = name;
            ev.message = "Starting " + name + "...";
            progress(ev);
        }
        auto result = co_await fn();
        results.push_back(result);
        response.totalOperations++;
        response.totalSucceeded += result.succeeded;
        response.totalFailed += result.failed;
        response.totalSkipped += result.skipped;
        if (result.failed > 0 && !result.message.empty()) {
            errors.push_back(name + ": " + result.message);
        }
        if (progress) {
            RepairEvent ev;
            ev.phase = "completed";
            ev.operation = name;
            ev.processed = result.processed;
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = std::move(result.message);
            progress(ev);
        }
        if (isCanceled()) {
            emitCancel(name);
            errors.push_back("Repair canceled");
            co_return false;
        }
        co_return true;
    };

    const bool doOrphans = request.repairOrphans || request.repairAll;
    const bool doMime = request.repairMime || request.repairAll;
    const bool doDownloads = request.repairDownloads || request.repairAll;
    const bool doPathTree = request.repairPathTree || request.repairAll;
    const bool doChunks = request.repairChunks || request.repairAll;
    const bool doBlockRefs = request.repairBlockRefs || request.repairAll;
    const bool doFts5 = request.repairFts5 || request.repairAll;
    const bool doEmbeddings = request.repairEmbeddings || request.repairAll;
    const bool doStuckDocs = request.repairStuckDocs || request.repairAll;
    const bool doGraph = request.repairGraph || request.repairAll;
    const bool doTopology = request.repairTopology;
    const bool doDedupe = request.repairDedupe || request.repairAll;
    const bool doOptimize = request.optimizeDb || request.repairAll;

    bool keepGoing = true;

    if (keepGoing && doStuckDocs) {
        if (request.foreground) {
            keepGoing =
                runOp("stuck_docs", [&] { return recoverStuckDocuments(request, progress); });
        } else {
            keepGoing = co_await runAsyncOp("stuck_docs", [&]() {
                return recoverStuckDocumentsAsync(request, progress, cancelRequested);
            });
        }
    }
    if (keepGoing && doOrphans) {
        keepGoing = runOp("orphans", [&] {
            return cleanOrphanedMetadata(request.dryRun, request.verbose, progress);
        });
    }
    if (keepGoing && doMime) {
        keepGoing = runOp(
            "mime", [&] { return repairMimeTypes(request.dryRun, request.verbose, progress); });
    }
    if (keepGoing && doDownloads) {
        keepGoing = runOp("downloads", [&] {
            return repairDownloads(request.dryRun, request.verbose, progress);
        });
    }
    if (keepGoing && doPathTree) {
        keepGoing = runOp("path_tree", [&] {
            return rebuildPathTree(request.dryRun, request.verbose, progress);
        });
    }
    if (keepGoing && doDedupe) {
        keepGoing = runOp("dedupe", [&] { return applySemanticDedupe(request, progress); });
    }
    if (keepGoing && doChunks) {
        keepGoing = runOp("chunks", [&] {
            return cleanOrphanedChunks(request.dryRun, request.verbose, progress);
        });
    }
    if (keepGoing && doBlockRefs) {
        keepGoing = runOp("block_refs", [&] {
            return repairBlockReferences(request.dryRun, request.verbose, progress);
        });
    }
    if (keepGoing && doGraph) {
        keepGoing = runOp("graph", [&] { return repairKnowledgeGraph(request, progress); });
    }
    if (keepGoing && doFts5) {
        keepGoing = runOp("fts5", [&] { return rebuildFts5Index(request, progress); });
    }
    if (keepGoing && doEmbeddings) {
        if (request.foreground) {
            keepGoing = runOp("embeddings", [&] {
                return generateMissingEmbeddings(request, progress, cancelRequested);
            });
        } else {
            keepGoing = co_await runAsyncOp("embeddings", [&]() {
                return generateMissingEmbeddingsAsync(request, progress, cancelRequested);
            });
        }
    }
    if (keepGoing && doTopology) {
        keepGoing = runOp("topology", [&] { return rebuildTopologyArtifacts(request, progress); });
    }
    if (keepGoing && doOptimize) {
        (void)runOp("optimize",
                    [&] { return optimizeDatabase(request.dryRun, request.verbose, progress); });
    }

    response.success = errors.empty();
    response.errors = std::move(errors);
    response.operationResults = std::move(results);
    co_return response;
}

// ============================================================================
// Stuck Document Recovery (NEW)
// ============================================================================

std::vector<RepairService::StuckDocumentInfo>
RepairService::detectStuckDocuments(int32_t maxRetries) {
    std::vector<StuckDocumentInfo> stuck;
    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!meta)
        return stuck;

    auto now = std::chrono::system_clock::now();
    auto threshold = std::chrono::duration_cast<std::chrono::seconds>(cfg_.stalledThreshold);
    auto cutoffEpoch =
        std::chrono::duration_cast<std::chrono::seconds>((now - threshold).time_since_epoch())
            .count();

    // Use targeted SQL queries for each stuck category instead of loading all docs.

    // Category 1: Failed extraction
    {
        metadata::DocumentQueryOptions opts;
        opts.extractionStatuses = {metadata::ExtractionStatus::Failed};
        opts.maxRepairAttempts = maxRetries;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::FailedExtraction, d.id, d.sha256Hash,
                                 d.filePath, d.repairAttempts});
            }
        }
    }

    // Category 2: Ghost success (Success but no content row)
    {
        metadata::DocumentQueryOptions opts;
        opts.extractionStatuses = {metadata::ExtractionStatus::Success};
        opts.maxRepairAttempts = maxRetries;
        opts.onlyMissingContent = true;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::GhostSuccess, d.id, d.sha256Hash, d.filePath,
                                 d.repairAttempts});
            }
        }
    }

    // Category 3: Stalled Pending (indexed/modified before cutoff)
    {
        metadata::DocumentQueryOptions opts;
        opts.extractionStatuses = {metadata::ExtractionStatus::Pending};
        opts.maxRepairAttempts = maxRetries;
        opts.stalledBefore = cutoffEpoch;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::StalledPending, d.id, d.sha256Hash, d.filePath,
                                 d.repairAttempts});
            }
        }
    }

    // Category 4: Stalled Processing (repairStatus stuck in Processing)
    {
        metadata::DocumentQueryOptions opts;
        opts.repairStatuses = {metadata::RepairStatus::Processing};
        opts.maxRepairAttempts = maxRetries;
        opts.repairAttemptedBefore = cutoffEpoch;

        auto docsResult = meta->queryDocuments(opts);
        if (docsResult) {
            for (const auto& d : docsResult.value()) {
                stuck.push_back({StuckDocumentInfo::StalledProcessing, d.id, d.sha256Hash,
                                 d.filePath, d.repairAttempts});
            }
        }
    }

    return stuck;
}

boost::asio::awaitable<RepairOperationResult>
RepairService::recoverStuckDocumentsAsync(const RepairRequest& req, const ProgressFn& progress,
                                          std::atomic<bool>* cancelRequested) {
    RepairOperationResult result;
    result.operation = "stuck_docs";

    auto isCanceled = [&]() {
        return cancelRequested && cancelRequested->load(std::memory_order_relaxed);
    };

    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata repository not available";
        co_return result;
    }

    auto stuckDocs = detectStuckDocuments(req.maxRetries);
    result.processed = stuckDocs.size();

    if (stuckDocs.empty()) {
        result.message = "No stuck documents found";
        co_return result;
    }

    spdlog::info("RepairService: found {} stuck documents", stuckDocs.size());

    if (req.dryRun) {
        result.message = "Would recover " + std::to_string(stuckDocs.size()) + " stuck documents";
        result.skipped = stuckDocs.size();
        co_return result;
    }

    const std::size_t rpcCapacity = static_cast<std::size_t>(TuneAdvisor::postIngestRpcQueueMax());
    const bool useRpcChannel = (rpcCapacity > 0);
    auto postIngestChannel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            useRpcChannel ? "post_ingest_rpc" : "post_ingest",
            useRpcChannel ? rpcCapacity : std::size_t(4096));
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    for (std::size_t i = 0; i < stuckDocs.size(); ++i) {
        if (isCanceled()) {
            result.failed += (stuckDocs.size() - i);
            result.message = "Repair canceled";
            co_return result;
        }

        const auto& s = stuckDocs[i];
        InternalEventBus::PostIngestTask task;
        task.hash = s.hash;
        task.mime = "";

        const std::string jobLabel =
            std::string(useRpcChannel ? "post_ingest_rpc" : "post_ingest") + " stuck-doc " +
            std::to_string(i + 1) + "/" + std::to_string(stuckDocs.size());
        const bool pushed = co_await queueWithBackoff(postIngestChannel, std::move(task), timer,
                                                      running_, jobLabel, 1, 500, 500, isCanceled);
        if (pushed) {
            TuningManager::notifyWakeup();
            auto docRes = meta->getDocument(s.docId);
            if (docRes && docRes.value().has_value()) {
                auto doc = docRes.value().value();
                doc.repairAttempts = s.repairAttempts + 1;
                doc.repairAttemptedAt = std::chrono::time_point_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now());
                (void)meta->updateDocument(doc);
            }

            (void)meta->updateDocumentExtractionStatus(s.docId, false,
                                                       metadata::ExtractionStatus::Pending,
                                                       "RepairService: recovery attempt");
            (void)meta->batchUpdateDocumentRepairStatuses({s.hash},
                                                          metadata::RepairStatus::Pending);
            ++result.succeeded;
        } else {
            ++result.failed;
            spdlog::warn("RepairService: PostIngest channel full after retries, could not "
                         "re-enqueue {} (consider increasing YAMS_POST_INGEST_RPC_QUEUE_MAX)",
                         s.hash.substr(0, 12));
        }

        if (progress && ((i + 1) % 10 == 0 || i + 1 == stuckDocs.size())) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "stuck_docs";
            ev.processed = i + 1;
            ev.total = stuckDocs.size();
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            progress(ev);
        }
    }

    result.message = "Recovered " + std::to_string(result.succeeded) + " stuck documents";
    if (result.failed > 0) {
        result.message += " (" + std::to_string(result.failed) +
                          " could not be enqueued - re-run repair to retry)";
    }
    co_return result;
}

RepairOperationResult RepairService::recoverStuckDocuments(const RepairRequest& req,
                                                           const ProgressFn& progress) {
    RepairOperationResult result;
    result.operation = "stuck_docs";

    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata repository not available";
        return result;
    }

    auto stuckDocs = detectStuckDocuments(req.maxRetries);
    result.processed = stuckDocs.size();

    if (stuckDocs.empty()) {
        result.message = "No stuck documents found";
        return result;
    }

    spdlog::info("RepairService: found {} stuck documents", stuckDocs.size());

    if (req.dryRun) {
        result.message = "Would recover " + std::to_string(stuckDocs.size()) + " stuck documents";
        result.skipped = stuckDocs.size();
        return result;
    }

    // Re-enqueue stuck documents for re-extraction via a high-priority PostIngestQueue channel.
    // Use back-pressure retries with drain polling to avoid channel overflow and silent doc loss.
    const std::size_t rpcCapacity = static_cast<std::size_t>(TuneAdvisor::postIngestRpcQueueMax());
    const bool useRpcChannel = (rpcCapacity > 0);
    auto postIngestChannel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            useRpcChannel ? "post_ingest_rpc" : "post_ingest",
            useRpcChannel ? rpcCapacity : std::size_t(4096));

    constexpr int kMaxRetryRounds = 20; // 20 rounds × 500ms = 10s max wait per document
    constexpr auto kDrainPollInterval = std::chrono::milliseconds(500);

    for (size_t i = 0; i < stuckDocs.size(); ++i) {
        const auto& s = stuckDocs[i];

        // Re-enqueue for extraction (do not mutate metadata unless enqueue succeeds).
        InternalEventBus::PostIngestTask task;
        task.hash = s.hash;
        task.mime = ""; // will be re-detected

        // Attempt the push with back-pressure retries
        bool pushed = postIngestChannel->try_push(task);
        if (!pushed) {
            // Channel full — wait for consumer to drain, then retry
            for (int retry = 0; retry < kMaxRetryRounds && !pushed; ++retry) {
                std::this_thread::sleep_for(kDrainPollInterval);
                pushed = postIngestChannel->try_push(task);
            }
        }

        if (pushed) {
            TuningManager::notifyWakeup();
            // Increment repair attempts
            auto docRes = meta->getDocument(s.docId);
            if (docRes && docRes.value().has_value()) {
                auto doc = docRes.value().value();
                doc.repairAttempts = s.repairAttempts + 1;
                doc.repairAttemptedAt = std::chrono::time_point_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now());
                (void)meta->updateDocument(doc);
            }

            // Reset extraction status to Pending
            (void)meta->updateDocumentExtractionStatus(s.docId, false,
                                                       metadata::ExtractionStatus::Pending,
                                                       "RepairService: recovery attempt");

            // Reset repair status to Pending
            (void)meta->batchUpdateDocumentRepairStatuses({s.hash},
                                                          metadata::RepairStatus::Pending);

            ++result.succeeded;
        } else {
            ++result.failed;
            spdlog::warn("RepairService: PostIngest channel full after retries, could not "
                         "re-enqueue {} (consider increasing YAMS_POST_INGEST_RPC_QUEUE_MAX)",
                         s.hash.substr(0, 12));
        }

        if (progress && (i + 1) % 10 == 0) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "stuck_docs";
            ev.processed = i + 1;
            ev.total = stuckDocs.size();
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            progress(ev);
        }
    }

    result.message = "Recovered " + std::to_string(result.succeeded) + " stuck documents";
    if (result.failed > 0) {
        result.message += " (" + std::to_string(result.failed) +
                          " could not be enqueued — re-run repair to retry)";
    }
    return result;
}

// ============================================================================
// Core Repair Operations
// ============================================================================

RepairOperationResult RepairService::cleanOrphanedMetadata(bool dryRun, bool verbose,
                                                           ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "orphans";

    auto store = ctx_.getContentStore ? ctx_.getContentStore() : nullptr;
    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!store || !meta) {
        result.message = "Store or metadata not available";
        return result;
    }

    auto docsResult = metadata::queryDocumentsByPattern(*meta, "%");
    if (!docsResult) {
        result.message = "Failed to query: " + docsResult.error().message;
        return result;
    }

    std::vector<int64_t> orphanedIds;
    for (const auto& doc : docsResult.value()) {
        auto existsResult = store->exists(doc.sha256Hash);
        if (!existsResult || !existsResult.value())
            orphanedIds.push_back(doc.id);
    }

    result.processed = docsResult.value().size();

    if (orphanedIds.empty()) {
        result.message = "No orphaned entries";
        return result;
    }

    if (dryRun) {
        result.skipped = orphanedIds.size();
        result.message = "Would clean " + std::to_string(orphanedIds.size()) + " orphans";
        return result;
    }

    auto batchResult = meta->deleteDocumentsBatch(orphanedIds);
    if (batchResult) {
        result.succeeded = batchResult.value();
    } else {
        // Fallback to individual deletes
        for (int64_t id : orphanedIds) {
            auto del = meta->deleteDocument(id);
            if (del)
                result.succeeded++;
            else
                result.failed++;
        }
    }

    result.message = "Cleaned " + std::to_string(result.succeeded) + " orphans";
    return result;
}

RepairOperationResult RepairService::repairMimeTypes(bool dryRun, bool verbose,
                                                     ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "mime";

    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    auto store = ctx_.getContentStore ? ctx_.getContentStore() : nullptr;
    (void)detection::FileTypeDetector::initializeWithMagicNumbers();
    auto& detector = detection::FileTypeDetector::instance();

    constexpr int kBatchSize = 5000;
    constexpr uint64_t kProgressStride = 100;
    const auto emit = [&](std::string phase, uint64_t processed, const std::string& message) {
        if (!progress)
            return;
        RepairEvent ev;
        ev.phase = std::move(phase);
        ev.operation = "mime";
        ev.processed = processed;
        ev.succeeded = result.succeeded;
        ev.failed = result.failed;
        ev.skipped = result.skipped;
        ev.message = message;
        progress(ev);
    };

    uint64_t totalScanned = 0;
    uint64_t totalCandidates = 0;
    int offset = 0;
    while (true) {
        metadata::DocumentQueryOptions opts;
        opts.limit = kBatchSize;
        opts.offset = offset;
        auto batchResult = meta->queryDocuments(opts);
        if (!batchResult) {
            result.message = "Failed to query: " + batchResult.error().message;
            return result;
        }
        const auto& batch = batchResult.value();
        if (batch.empty())
            break;

        std::vector<std::pair<int64_t, std::string>> toRepair;
        toRepair.reserve(batch.size());
        for (const auto& doc : batch) {
            ++totalScanned;
            if (shouldRedetectMime(doc)) {
                const auto detectedMime = bestEffortMimeForDocument(doc, store);
                if (!detectedMime.empty() && detectedMime != doc.mimeType) {
                    toRepair.push_back({doc.id, detectedMime});
                    ++totalCandidates;
                }
            }
            if (totalScanned % kProgressStride == 0) {
                emit("repairing", totalScanned, "Scanning documents");
            }
        }

        if (dryRun) {
            result.skipped += toRepair.size();
        } else {
            for (auto& [id, mimeType] : toRepair) {
                auto docResult = meta->getDocument(id);
                if (!(docResult && docResult.value())) {
                    result.failed++;
                    continue;
                }

                auto doc = *docResult.value();
                const bool oldWasText = detector.isTextMimeType(doc.mimeType);
                const bool newIsText = detector.isTextMimeType(mimeType);
                doc.mimeType = std::move(mimeType);

                if (meta->updateDocument(doc)) {
                    if (oldWasText && !newIsText) {
                        (void)meta->deleteContent(id);
                        (void)meta->removeFromIndex(id);
                        (void)meta->updateDocumentExtractionStatus(
                            id, false, metadata::ExtractionStatus::Pending,
                            "MIME repaired; stale text content cleared");
                    }
                    result.succeeded++;
                } else {
                    result.failed++;
                }
                if ((result.succeeded + result.failed) % kProgressStride == 0) {
                    emit("repairing", totalScanned, "Repairing MIME types");
                }
            }
        }

        emit("repairing", totalScanned, "Processed batch");

        if (static_cast<int>(batch.size()) < kBatchSize)
            break;
        offset += kBatchSize;
    }

    result.processed = totalScanned;

    if (totalCandidates == 0) {
        result.message = "All documents have valid MIME types";
    } else if (dryRun) {
        result.message = "Would repair " + std::to_string(result.skipped) + " MIME types";
    } else {
        result.message = "Repaired " + std::to_string(result.succeeded) + " MIME types";
    }
    return result;
}

RepairOperationResult RepairService::repairDownloads(bool dryRun, bool verbose,
                                                     ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "downloads";

    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    auto docsResult = metadata::queryDocumentsByPattern(*meta, "%");
    if (!docsResult) {
        result.message = "Failed to query";
        return result;
    }

    auto is_url = [](const std::string& s) { return s.find("://") != std::string::npos; };
    auto extract_host = [](const std::string& url) -> std::string {
        auto p = url.find("://");
        if (p == std::string::npos)
            return {};
        auto rest = url.substr(p + 3);
        auto slash = rest.find('/');
        return (slash == std::string::npos) ? rest : rest.substr(0, slash);
    };
    auto extract_scheme = [](const std::string& url) -> std::string {
        auto p = url.find("://");
        return (p == std::string::npos) ? std::string{} : url.substr(0, p);
    };
    auto filename_from_url = [](std::string url) -> std::string {
        auto lastSlash = url.find_last_of('/');
        if (lastSlash != std::string::npos)
            url = url.substr(lastSlash + 1);
        auto q = url.find('?');
        if (q != std::string::npos)
            url = url.substr(0, q);
        if (url.empty())
            url = "downloaded_file";
        return url;
    };

    for (auto& doc : docsResult.value()) {
        std::string sourceUrl;
        if (is_url(doc.filePath))
            sourceUrl = doc.filePath;
        if (sourceUrl.empty())
            continue;

        result.processed++;
        if (dryRun) {
            result.skipped++;
            continue;
        }

        std::string filename = filename_from_url(sourceUrl);
        std::string ext;
        auto dotPos = filename.rfind('.');
        if (dotPos != std::string::npos)
            ext = filename.substr(dotPos);
        doc.fileName = filename;
        doc.filePath = std::move(filename);
        doc.fileExtension = std::move(ext);

        if (auto up = meta->updateDocument(doc); up) {
            result.succeeded++;
        } else {
            result.failed++;
        }

        try {
            meta->setMetadata(doc.id, "source_url", metadata::MetadataValue(sourceUrl));
            meta->setMetadata(doc.id, "tag", metadata::MetadataValue("downloaded"));
            auto host = extract_host(sourceUrl);
            auto scheme = extract_scheme(sourceUrl);
            if (!host.empty())
                meta->setMetadata(doc.id, "tag", metadata::MetadataValue("host:" + host));
            if (!scheme.empty())
                meta->setMetadata(doc.id, "tag", metadata::MetadataValue("scheme:" + scheme));
        } catch (...) {
        }
    }

    result.message = "Updated " + std::to_string(result.succeeded) + " download documents";
    return result;
}

RepairOperationResult RepairService::applySemanticDedupe(const RepairRequest& req,
                                                         const ProgressFn& progress) {
    RepairOperationResult result;
    result.operation = "dedupe";

    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    auto groupsResult = meta->listSemanticDuplicateGroups(1000);
    if (!groupsResult) {
        result.message =
            "Failed to load semantic duplicate groups: " + groupsResult.error().message;
        return result;
    }

    std::vector<metadata::SemanticDuplicateGroup> groups;
    groups.reserve(groupsResult.value().size());
    for (const auto& group : groupsResult.value()) {
        if (group.status == "suggested") {
            groups.push_back(group);
        }
    }

    result.processed = groups.size();
    if (groups.empty()) {
        result.message = "No semantic duplicate suggestions to apply";
        return result;
    }

    std::vector<int64_t> canonicalIds;
    canonicalIds.reserve(groups.size());
    for (const auto& group : groups) {
        if (group.canonicalDocumentId.has_value()) {
            canonicalIds.push_back(*group.canonicalDocumentId);
        }
    }

    auto detailsResult = meta->getSemanticDuplicateGroupsForDocuments(canonicalIds);
    if (!detailsResult) {
        result.message =
            "Failed to load semantic duplicate members: " + detailsResult.error().message;
        return result;
    }

    for (size_t i = 0; i < groups.size(); ++i) {
        const auto& group = groups[i];
        if (!group.canonicalDocumentId.has_value()) {
            result.failed++;
            continue;
        }

        auto detailIt = detailsResult.value().find(*group.canonicalDocumentId);
        if (detailIt == detailsResult.value().end()) {
            result.failed++;
            continue;
        }

        std::vector<int64_t> toDelete;
        for (const auto& member : detailIt->second.members) {
            if (member.role != "canonical") {
                toDelete.push_back(member.documentId);
            }
        }

        if (req.dryRun) {
            result.skipped += toDelete.size();
        } else {
            if (!toDelete.empty()) {
                auto batchDelete = meta->deleteDocumentsBatch(toDelete);
                if (batchDelete) {
                    result.succeeded += batchDelete.value();
                    if (batchDelete.value() < toDelete.size()) {
                        result.failed += (toDelete.size() - batchDelete.value());
                    }
                } else {
                    for (int64_t docId : toDelete) {
                        auto del = meta->deleteDocument(docId);
                        if (del)
                            result.succeeded++;
                        else
                            result.failed++;
                    }
                }
            }

            auto statusResult = meta->updateSemanticDuplicateGroupStatus(group.groupKey, "applied");
            if (!statusResult) {
                result.failed++;
            }
        }

        if (progress) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "dedupe";
            ev.processed = i + 1;
            ev.total = groups.size();
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = req.dryRun ? "Previewing semantic duplicate removals"
                                    : "Removing semantic duplicates";
            progress(ev);
        }
    }

    if (req.dryRun) {
        result.message =
            "Would remove " + std::to_string(result.skipped) + " semantic duplicate documents";
    } else {
        result.message =
            "Removed " + std::to_string(result.succeeded) + " semantic duplicate documents";
    }
    return result;
}

RepairOperationResult RepairService::rebuildPathTree(bool dryRun, bool verbose,
                                                     ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "path_tree";

    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    // Use RepairManager for path tree repair
    auto concreteRepo = std::dynamic_pointer_cast<metadata::MetadataRepository>(meta);
    if (!concreteRepo) {
        result.message = "Could not get concrete MetadataRepository";
        return result;
    }

    if (dryRun) {
        auto countResult = concreteRepo->countDocsMissingPathTree();
        if (!countResult) {
            result.message = "Failed to count missing path tree entries";
            return result;
        }
        uint64_t missing = countResult.value();
        result.processed = missing;
        result.skipped = missing;
        result.message = "Would rebuild " + std::to_string(missing) + " path tree entries";
        return result;
    }

    integrity::RepairManager repairMgr(*concreteRepo);
    auto repairResult = repairMgr.repairPathTree([](uint64_t, uint64_t) {});
    if (!repairResult) {
        result.message = "Path tree repair failed: " + repairResult.error().message;
        result.failed = 1;
        return result;
    }

    const auto& stats = repairResult.value();
    result.processed = stats.documentsScanned;
    result.succeeded = stats.nodesCreated;
    result.failed = stats.errors;
    result.message = "Created " + std::to_string(stats.nodesCreated) + " path tree entries";
    return result;
}

RepairOperationResult RepairService::repairKnowledgeGraph(const RepairRequest& req,
                                                          const ProgressFn& progress) {
    RepairOperationResult result;
    result.operation = "graph";

    auto graphComponent = ctx_.getGraphComponent ? ctx_.getGraphComponent() : nullptr;
    auto kgStore = ctx_.getKgStore ? ctx_.getKgStore() : nullptr;
    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!graphComponent || !kgStore || !meta) {
        result.message = "Knowledge graph components not available";
        return result;
    }

    auto graphProgress = [&progress](uint64_t processed, uint64_t total,
                                     const GraphComponent::RepairStats& snap) {
        if (!progress)
            return;
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "graph";
        ev.processed = processed;
        ev.total = total;
        ev.succeeded = snap.nodesCreated + snap.nodesUpdated + snap.edgesCreated;
        ev.failed = snap.errors;
        ev.message = "graph repair scanning";
        progress(ev);
    };
    auto repairRes = graphComponent->repairGraph(req.dryRun, graphProgress);
    if (!repairRes) {
        result.failed = 1;
        result.message = "Graph repair failed: " + repairRes.error().message;
        return result;
    }

    const auto& stats = repairRes.value();
    result.processed += stats.nodesCreated + stats.nodesUpdated + stats.edgesCreated;
    result.succeeded += stats.nodesCreated + stats.nodesUpdated + stats.edgesCreated;
    result.failed += stats.errors;
    for (const auto& issue : stats.issues) {
        spdlog::warn("[RepairService] graph repair issue: {}", issue);
    }

    auto pathMigration = repairLegacyPathNodesInPlace(req.dryRun, req.verbose, progress);
    result.processed += pathMigration.nodesScanned;
    result.succeeded += pathMigration.nodesMigrated + pathMigration.edgesRewired;
    result.skipped += pathMigration.skipped;
    result.failed += pathMigration.errors;
    for (const auto& issue : pathMigration.issues) {
        spdlog::warn("[RepairService] graph path migration issue: {}", issue);
    }

    auto cleanup = cleanOrphanedKgEntries(req.dryRun, req.verbose, progress);
    result.processed += cleanup.nodesScanned;
    result.succeeded += cleanup.nodesDeleted + cleanup.edgesDeleted + cleanup.docEntitiesDeleted;
    result.skipped += cleanup.skipped;
    result.failed += cleanup.errors;
    for (const auto& issue : cleanup.issues) {
        spdlog::warn("[RepairService] graph cleanup issue: {}", issue);
    }

    std::string message = "Rebuilt graph nodes/edges (nodes=" + std::to_string(stats.nodesCreated) +
                          ", edges=" + std::to_string(stats.edgesCreated) + ")";
    if (pathMigration.nodesMigrated > 0 || pathMigration.edgesRewired > 0) {
        message +=
            "; migrated legacy path nodes (nodes=" + std::to_string(pathMigration.nodesMigrated) +
            ", rewired_edges=" + std::to_string(pathMigration.edgesRewired) + ")";
    }
    if (cleanup.nodesDeleted > 0 || cleanup.edgesDeleted > 0 || cleanup.docEntitiesDeleted > 0) {
        message += "; cleaned orphans (nodes=" + std::to_string(cleanup.nodesDeleted) +
                   ", edges=" + std::to_string(cleanup.edgesDeleted) +
                   ", doc_entities=" + std::to_string(cleanup.docEntitiesDeleted) + ")";
    }
    if (req.dryRun) {
        message += " (dry-run: changes were not committed)";
    }
    result.message = std::move(message);
    return result;
}

RepairOperationResult RepairService::rebuildTopologyArtifacts(const RepairRequest& req,
                                                              const ProgressFn& progress) {
    (void)progress;

    RepairOperationResult result;
    result.operation = "topology";

    if (!ctx_.rebuildTopologyArtifacts) {
        result.failed = 1;
        result.message = "Topology rebuild callback unavailable";
        return result;
    }

    auto rebuildResult = ctx_.rebuildTopologyArtifacts(
        req.dryRun ? std::string{"repair.topology.dry_run"} : std::string{"repair.topology"},
        req.dryRun, {});
    if (!rebuildResult) {
        result.failed = 1;
        result.message = "Topology rebuild failed: " + rebuildResult.error().message;
        return result;
    }

    const auto& stats = rebuildResult.value();
    result.processed = stats.documentsProcessed;
    if (stats.skipped || req.dryRun) {
        result.skipped = stats.documentsProcessed;
    } else {
        result.succeeded = stats.documentsProcessed;
    }

    std::string message = (stats.skipped || req.dryRun) ? "Topology rebuild analyzed "
                                                        : "Topology rebuild stored artifacts for ";
    message += std::to_string(stats.documentsProcessed) +
               " docs (clusters=" + std::to_string(stats.clustersBuilt) +
               ", memberships=" + std::to_string(stats.membershipsBuilt) + ")";
    if (!stats.snapshotId.empty()) {
        message += ", snapshot=" + stats.snapshotId;
    }
    if (stats.documentsMissingEmbeddings > 0 || stats.documentsMissingGraphNodes > 0) {
        message += " [missing_embeddings=" + std::to_string(stats.documentsMissingEmbeddings) +
                   ", missing_graph_nodes=" + std::to_string(stats.documentsMissingGraphNodes) +
                   "]";
    }
    if (!stats.issues.empty()) {
        message += ": " + stats.issues.front();
    }
    result.message = std::move(message);
    return result;
}

RepairService::PathNodeMigrationStats
RepairService::repairLegacyPathNodesInPlace(bool dryRun, bool verbose, ProgressFn progress) {
    (void)verbose;

    PathNodeMigrationStats stats;
    auto kgStore = ctx_.getKgStore ? ctx_.getKgStore() : nullptr;
    if (!kgStore) {
        stats.errors = 1;
        stats.issues.push_back("Knowledge graph store unavailable");
        return stats;
    }

    auto emitProgress = [&](const std::string& message) {
        if (!progress)
            return;
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "graph";
        ev.processed = stats.nodesScanned;
        ev.succeeded = stats.nodesMigrated + stats.edgesRewired;
        ev.failed = stats.errors;
        ev.skipped = stats.skipped;
        ev.message = message;
        progress(ev);
    };

    auto fetchAllEdges = [&](std::int64_t nodeId,
                             bool outgoing) -> Result<std::vector<metadata::KGEdge>> {
        constexpr std::size_t kPage = 1000;
        std::size_t offset = 0;
        std::vector<metadata::KGEdge> allEdges;

        while (true) {
            Result<std::vector<metadata::KGEdge>> edgesRes =
                outgoing ? kgStore->getEdgesFrom(nodeId, std::nullopt, kPage, offset)
                         : kgStore->getEdgesTo(nodeId, std::nullopt, kPage, offset);

            if (!edgesRes) {
                return edgesRes.error();
            }

            auto edges = std::move(edgesRes.value());
            if (edges.empty()) {
                break;
            }

            allEdges.insert(allEdges.end(), edges.begin(), edges.end());
            if (edges.size() < kPage) {
                break;
            }
            offset += edges.size();
        }

        return allEdges;
    };

    auto migrateType = [&](std::string_view type, std::string_view oldPrefix,
                           std::string_view newPrefix) {
        constexpr std::size_t kBatchSize = 300;
        std::size_t offset = 0;

        while (true) {
            auto nodesRes = kgStore->findNodesByType(type, kBatchSize, offset);
            if (!nodesRes) {
                ++stats.errors;
                stats.issues.push_back("findNodesByType(" + std::string(type) +
                                       ") failed: " + nodesRes.error().message);
                break;
            }

            const auto& nodes = nodesRes.value();
            if (nodes.empty()) {
                break;
            }

            std::size_t migratedInBatch = 0;
            for (const auto& node : nodes) {
                ++stats.nodesScanned;
                if (node.nodeKey.compare(0, oldPrefix.size(), oldPrefix) != 0) {
                    continue;
                }

                const std::string suffix = node.nodeKey.substr(oldPrefix.size());
                if (suffix.empty()) {
                    ++stats.skipped;
                    continue;
                }

                const std::string newNodeKey = std::string(newPrefix) + suffix;
                if (newNodeKey == node.nodeKey) {
                    ++stats.skipped;
                    continue;
                }

                std::optional<std::int64_t> newNodeId;
                auto existingNew = kgStore->getNodeByKey(newNodeKey);
                if (!existingNew) {
                    ++stats.errors;
                    stats.issues.push_back("getNodeByKey(" + newNodeKey +
                                           ") failed: " + existingNew.error().message);
                    continue;
                }

                if (existingNew.value().has_value()) {
                    newNodeId = existingNew.value()->id;
                } else if (!dryRun) {
                    metadata::KGNode migratedNode;
                    migratedNode.nodeKey = newNodeKey;
                    migratedNode.label = node.label;
                    migratedNode.type = node.type;
                    migratedNode.properties = node.properties;
                    auto upsertRes = kgStore->upsertNode(migratedNode);
                    if (!upsertRes) {
                        ++stats.errors;
                        stats.issues.push_back("upsertNode(" + newNodeKey +
                                               ") failed: " + upsertRes.error().message);
                        continue;
                    }
                    newNodeId = upsertRes.value();
                }

                if (dryRun) {
                    ++stats.nodesMigrated;
                    continue;
                }

                auto outEdgesRes = fetchAllEdges(node.id, true);
                if (!outEdgesRes) {
                    ++stats.errors;
                    stats.issues.push_back("getEdgesFrom failed for " + node.nodeKey + ": " +
                                           outEdgesRes.error().message);
                    continue;
                }
                auto inEdgesRes = fetchAllEdges(node.id, false);
                if (!inEdgesRes) {
                    ++stats.errors;
                    stats.issues.push_back("getEdgesTo failed for " + node.nodeKey + ": " +
                                           inEdgesRes.error().message);
                    continue;
                }

                std::vector<metadata::KGEdge> rewiredEdges;
                rewiredEdges.reserve(outEdgesRes.value().size() + inEdgesRes.value().size());
                std::unordered_set<std::int64_t> seenEdgeIds;

                auto appendRewired = [&](const metadata::KGEdge& edge) {
                    if (edge.id > 0 && !seenEdgeIds.insert(edge.id).second) {
                        return;
                    }

                    metadata::KGEdge rewired = edge;
                    if (rewired.srcNodeId == node.id) {
                        rewired.srcNodeId = *newNodeId;
                    }
                    if (rewired.dstNodeId == node.id) {
                        rewired.dstNodeId = *newNodeId;
                    }

                    if (rewired.srcNodeId == node.id || rewired.dstNodeId == node.id) {
                        return;
                    }
                    rewiredEdges.push_back(std::move(rewired));
                };

                for (const auto& edge : outEdgesRes.value()) {
                    appendRewired(edge);
                }
                for (const auto& edge : inEdgesRes.value()) {
                    appendRewired(edge);
                }

                if (!rewiredEdges.empty()) {
                    auto addRes = kgStore->addEdgesUnique(rewiredEdges);
                    if (!addRes) {
                        ++stats.errors;
                        stats.issues.push_back("addEdgesUnique failed for migrated node " +
                                               node.nodeKey + ": " + addRes.error().message);
                        continue;
                    }
                    stats.edgesRewired += rewiredEdges.size();
                }

                auto deleteRes = kgStore->deleteNodeById(node.id);
                if (!deleteRes) {
                    ++stats.errors;
                    stats.issues.push_back("deleteNodeById failed for " + node.nodeKey + ": " +
                                           deleteRes.error().message);
                    continue;
                }

                ++stats.nodesMigrated;
                ++migratedInBatch;

                if (stats.nodesScanned % 200 == 0) {
                    emitProgress(
                        "Migrated legacy path nodes=" + std::to_string(stats.nodesMigrated) +
                        ", rewired_edges=" + std::to_string(stats.edgesRewired));
                }
            }

            if (nodes.size() < kBatchSize) {
                break;
            }

            if (!dryRun && migratedInBatch > 0) {
                if (offset >= migratedInBatch) {
                    offset -= migratedInBatch;
                } else {
                    offset = 0;
                }
            }
            offset += nodes.size();
        }
    };

    migrateType("file", "file:", "path:file:");
    migrateType("directory", "dir:", "path:dir:");

    if (stats.nodesMigrated > 0) {
        emitProgress("Migrated " + std::to_string(stats.nodesMigrated) +
                     " legacy path nodes in-place");
    }

    return stats;
}

RepairService::KgCleanupStats RepairService::cleanOrphanedKgEntries(bool dryRun, bool verbose,
                                                                    const ProgressFn& progress) {
    (void)verbose;
    KgCleanupStats stats;
    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    auto kgStore = ctx_.getKgStore ? ctx_.getKgStore() : nullptr;
    if (!meta || !kgStore) {
        stats.errors = 1;
        stats.issues.push_back("Metadata or KG store unavailable");
        return stats;
    }

    auto emitProgress = [&](const std::string& message) {
        if (!progress)
            return;
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "graph";
        ev.processed = stats.nodesScanned;
        ev.succeeded = stats.nodesDeleted + stats.edgesDeleted + stats.docEntitiesDeleted;
        ev.failed = stats.errors;
        ev.skipped = stats.skipped;
        ev.message = message;
        progress(ev);
    };

    auto scanType = [&](const std::string& type, const std::string& prefix, bool deleteByHash) {
        constexpr std::size_t kBatchSize = 500;
        std::size_t offset = 0;
        while (true) {
            auto nodesRes = kgStore->findNodesByType(type, kBatchSize, offset);
            if (!nodesRes) {
                ++stats.errors;
                stats.issues.push_back("findNodesByType(" + type +
                                       ") failed: " + nodesRes.error().message);
                break;
            }

            const auto& nodes = nodesRes.value();
            if (nodes.empty())
                break;

            std::size_t deletedNodesInBatch = 0;
            for (const auto& node : nodes) {
                ++stats.nodesScanned;
                if (node.nodeKey.compare(0, prefix.size(), prefix) != 0)
                    continue;

                std::string hash = node.nodeKey.substr(prefix.size());
                if (hash.empty())
                    continue;

                auto docRes = meta->getDocumentByHash(hash);
                if (!docRes) {
                    ++stats.errors;
                    stats.issues.push_back("getDocumentByHash failed for " + hash + ": " +
                                           docRes.error().message);
                    continue;
                }
                if (!docRes.value().has_value()) {
                    ++stats.orphanNodes;
                    if (dryRun) {
                        ++stats.skipped;
                        continue;
                    }
                    if (deleteByHash) {
                        auto delRes = kgStore->deleteNodesForDocumentHash(hash);
                        if (!delRes) {
                            ++stats.errors;
                            stats.issues.push_back("deleteNodesForDocumentHash failed for " + hash +
                                                   ": " + delRes.error().message);
                            continue;
                        }
                        stats.nodesDeleted += static_cast<uint64_t>(delRes.value());
                        ++deletedNodesInBatch;
                    } else {
                        auto delRes = kgStore->deleteNodeById(node.id);
                        if (!delRes) {
                            ++stats.errors;
                            stats.issues.push_back("deleteNodeById failed for " + node.nodeKey +
                                                   ": " + delRes.error().message);
                            continue;
                        }
                        ++stats.nodesDeleted;
                        ++deletedNodesInBatch;
                    }
                }

                if (stats.nodesScanned % 200 == 0) {
                    emitProgress("Scanned " + std::to_string(stats.nodesScanned) +
                                 " KG nodes (orphans=" + std::to_string(stats.orphanNodes) + ")");
                }
            }

            if (nodes.size() < kBatchSize)
                break;

            if (!dryRun && deletedNodesInBatch > 0) {
                if (offset >= deletedNodesInBatch) {
                    offset -= deletedNodesInBatch;
                } else {
                    offset = 0;
                }
            }
            offset += nodes.size();
        }
    };

    scanType("document", "doc:", true);
    scanType("blob", "blob:", false);

    if (!dryRun) {
        auto edgeRes = kgStore->deleteOrphanedEdges();
        if (!edgeRes) {
            ++stats.errors;
            stats.issues.push_back("deleteOrphanedEdges failed: " + edgeRes.error().message);
        } else {
            stats.edgesDeleted = static_cast<uint64_t>(edgeRes.value());
        }

        auto entityRes = kgStore->deleteOrphanedDocEntities();
        if (!entityRes) {
            ++stats.errors;
            stats.issues.push_back("deleteOrphanedDocEntities failed: " +
                                   entityRes.error().message);
        } else {
            stats.docEntitiesDeleted = static_cast<uint64_t>(entityRes.value());
        }
    } else {
        emitProgress("Dry-run: skipped orphan edge/doc_entities cleanup");
    }

    if (stats.edgesDeleted > 0 || stats.docEntitiesDeleted > 0) {
        emitProgress("Cleaned orphan edges=" + std::to_string(stats.edgesDeleted) +
                     ", stale doc_entities=" + std::to_string(stats.docEntitiesDeleted));
    }

    return stats;
}

RepairOperationResult RepairService::cleanOrphanedChunks(bool dryRun, bool verbose,
                                                         const ProgressFn& progress) {
    RepairOperationResult result;
    result.operation = "chunks";

    namespace fs = std::filesystem;
    fs::path refsDbPath = cfg_.dataDir / "storage" / "refs.db";
    fs::path objectsPath = cfg_.dataDir / "storage" / "objects";

    if (!fs::exists(refsDbPath) || !fs::exists(objectsPath)) {
        result.message = "No refs.db or objects directory found";
        return result;
    }

    sqlite3* db = nullptr;
    if (sqlite3_open(refsDbPath.string().c_str(), &db) != SQLITE_OK) {
        result.message = "Failed to open refs.db";
        return result;
    }

    // Quick-check: count unreferenced block_references entries.
    // If zero, the common case (no orphans expected) can skip the expensive FS walk.
    {
        sqlite3_stmt* countStmt = nullptr;
        const char* countSql = "SELECT COUNT(*) FROM block_references WHERE ref_count = 0";
        int zeroRefCount = 0;
        if (sqlite3_prepare_v2(db, countSql, -1, &countStmt, nullptr) == SQLITE_OK) {
            if (sqlite3_step(countStmt) == SQLITE_ROW)
                zeroRefCount = sqlite3_column_int(countStmt, 0);
            sqlite3_finalize(countStmt);
        }

        if (zeroRefCount == 0) {
            // Also do a quick FS sanity check: count prefix dirs. If the count matches
            // the number of referenced hashes' unique prefixes, no orphans are possible.
            // For now, if refs.db tracks no zero-ref entries, trust it and skip the walk.
            sqlite3_close(db);
            result.message = "No orphaned chunks (quick-check: 0 unreferenced blocks)";
            return result;
        }

        spdlog::info("[RepairService] chunks: {} unreferenced block_references, proceeding "
                     "with filesystem scan",
                     zeroRefCount);
    }

    // Get referenced hashes
    std::set<std::string> referencedHashes;
    sqlite3_stmt* stmt;
    const char* sql = "SELECT block_hash FROM block_references WHERE ref_count > 0";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            if (hash)
                referencedHashes.insert(hash);
        }
        sqlite3_finalize(stmt);
    }

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "chunks";
        ev.message = "Loaded " + std::to_string(referencedHashes.size()) +
                     " referenced hashes, scanning filesystem...";
        progress(ev);
    }

    // Scan manifests for additional references
    size_t manifestDirsScanned = 0;
    if (fs::exists(objectsPath)) {
        for (const auto& dirEntry : fs::directory_iterator(objectsPath)) {
            if (!fs::is_directory(dirEntry))
                continue;
            ++manifestDirsScanned;
            if (progress && manifestDirsScanned % 64 == 0) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "chunks";
                ev.message =
                    "Scanning manifests (" + std::to_string(manifestDirsScanned) + " dirs)...";
                progress(ev);
            }
            for (const auto& fileEntry : fs::directory_iterator(dirEntry.path())) {
                if (!fs::is_regular_file(fileEntry))
                    continue;
                std::string filename = fileEntry.path().filename().string();
                if (filename.size() > 9 && filename.substr(filename.size() - 9) == ".manifest") {
                    try {
                        std::ifstream mf(fileEntry.path(), std::ios::binary);
                        if (!mf)
                            continue;
                        std::vector<char> data((std::istreambuf_iterator<char>(mf)),
                                               std::istreambuf_iterator<char>());
                        if (data.size() >= 28 && data[0] == 'Y' && data[1] == 'M' &&
                            data[2] == 'N' && data[3] == 'F') {
                            size_t pos = 24;
                            uint32_t numChunks = 0;
                            if (pos + 4 <= data.size()) {
                                std::memcpy(&numChunks, data.data() + pos, 4);
                                pos += 4;
                            }
                            for (uint32_t i = 0; i < numChunks && pos + 4 <= data.size(); ++i) {
                                uint32_t hashLen = 0;
                                std::memcpy(&hashLen, data.data() + pos, 4);
                                pos += 4;
                                if (hashLen > 0 && hashLen < 256 && pos + hashLen <= data.size()) {
                                    referencedHashes.insert(
                                        std::string(data.data() + pos, hashLen));
                                    pos += hashLen + 16;
                                } else {
                                    break;
                                }
                            }
                        }
                    } catch (...) {
                    }
                }
            }
        }
    }

    // Find orphaned chunks
    std::vector<std::pair<std::string, fs::path>> orphanedChunks;
    size_t dirsScanned = 0;
    if (fs::exists(objectsPath)) {
        for (const auto& dirEntry : fs::directory_iterator(objectsPath)) {
            if (!fs::is_directory(dirEntry))
                continue;
            ++dirsScanned;
            // Emit progress every 64 prefix dirs to keep the stream alive
            if (progress && dirsScanned % 64 == 0) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "chunks";
                ev.message = "Scanning directory " + std::to_string(dirsScanned) + "...";
                progress(ev);
            }
            std::string dirName = dirEntry.path().filename().string();
            for (const auto& fileEntry : fs::directory_iterator(dirEntry.path())) {
                if (!fs::is_regular_file(fileEntry))
                    continue;
                std::string fn = fileEntry.path().filename().string();
                if (fn.size() > 9 && fn.substr(fn.size() - 9) == ".manifest")
                    continue;
                std::string fullHash = dirName + fn;
                if (referencedHashes.find(fullHash) == referencedHashes.end()) {
                    orphanedChunks.push_back({fullHash, fileEntry.path()});
                }
            }
        }
    }

    result.processed = orphanedChunks.size();

    if (orphanedChunks.empty()) {
        result.message = "No orphaned chunks";
        sqlite3_close(db);
        return result;
    }

    if (dryRun) {
        result.skipped = orphanedChunks.size();
        result.message = "Would delete " + std::to_string(orphanedChunks.size()) + " chunks";
        sqlite3_close(db);
        return result;
    }

    for (const auto& [hash, path] : orphanedChunks) {
        try {
            fs::remove(path);
            result.succeeded++;
        } catch (...) {
            result.failed++;
        }
    }

    // Clean zero-ref entries from refs.db
    sqlite3_exec(db, "DELETE FROM block_references WHERE ref_count = 0", nullptr, nullptr, nullptr);

    sqlite3_close(db);
    result.message = "Deleted " + std::to_string(result.succeeded) + " chunks";
    return result;
}

RepairOperationResult RepairService::repairBlockReferences(bool dryRun, bool verbose,
                                                           const ProgressFn& progress) {
    RepairOperationResult result;
    result.operation = "block_refs";

    namespace fs = std::filesystem;
    fs::path objectsPath = cfg_.dataDir / "storage" / "objects";
    fs::path refsDbPath = cfg_.dataDir / "storage" / "refs.db";

    if (!fs::exists(objectsPath) || !fs::exists(refsDbPath)) {
        result.message = "No objects directory or refs.db";
        return result;
    }

    auto repairResult = integrity::RepairManager::repairBlockReferences(
        objectsPath, refsDbPath, dryRun, [&progress](uint64_t processed, uint64_t total) {
            if (!progress)
                return;
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "block_refs";
            ev.processed = processed;
            ev.total = total;
            ev.message = "Scanning block references";
            progress(ev);
        });
    if (!repairResult) {
        result.message = "Block refs repair failed: " + repairResult.error().message;
        result.failed = 1;
        return result;
    }

    const auto& stats = repairResult.value();
    result.processed = stats.blocksScanned;
    result.succeeded = stats.blocksUpdated;
    result.skipped = stats.blocksSkipped;
    result.failed = stats.errors;
    result.message = "Scanned " + std::to_string(stats.blocksScanned) + ", updated " +
                     std::to_string(stats.blocksUpdated);
    return result;
}

RepairOperationResult RepairService::rebuildFts5Index(const RepairRequest& req,
                                                      const ProgressFn& progress) {
    RepairOperationResult result;
    result.operation = "fts5";

    const bool dryRun = req.dryRun;
    const bool force = req.force;

    auto store = ctx_.getContentStore ? ctx_.getContentStore() : nullptr;
    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!store || !meta) {
        result.message = "Store or metadata not available";
        return result;
    }

    auto docs = metadata::queryDocumentsByPattern(*meta, "%");
    if (!docs) {
        result.message = "Failed to enumerate: " + docs.error().message;
        return result;
    }

    // Reset ghost-success documents (Success but no content row)
    for (const auto& d : docs.value()) {
        if (d.extractionStatus != metadata::ExtractionStatus::Success)
            continue;
        auto contentRes = meta->getContent(d.id);
        if (contentRes && !contentRes.value().has_value()) {
            (void)meta->updateDocumentExtractionStatus(d.id, false,
                                                       metadata::ExtractionStatus::Pending,
                                                       "Missing content row; reset by repair");
        }
    }

    static const std::vector<std::shared_ptr<extraction::IContentExtractor>> kEmptyExtractors;
    const auto& customExtractors =
        ctx_.getContentExtractors ? ctx_.getContentExtractors() : kEmptyExtractors;

    // Pre-load all FTS5 rowids so the incremental skip check below is O(1)
    // per document instead of one SQL round-trip each.
    std::unordered_set<int64_t> ftsRowIds;
    if (!force) {
        auto ftsResult = meta->getFts5IndexedRowIdSet();
        if (ftsResult)
            ftsRowIds = std::move(ftsResult.value());
    }

    result.processed = docs.value().size();
    const size_t totalDocs = docs.value().size();
    size_t docIdx = 0;

    // Emit an initial progress event so the client knows how many documents
    // to expect and doesn't appear to hang on large stores.
    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "fts5";
        ev.processed = 0;
        ev.total = totalDocs;
        ev.message = "Rebuilding FTS5 index (" + std::to_string(totalDocs) + " documents)";
        progress(ev);
    }

    for (const auto& d : docs.value()) {
        ++docIdx;

        // Emit progress frequently so the streaming layer has events to
        // send (prevents client read timeouts during large rebuilds).
        // Using a small interval (25) keeps the client alive and gives
        // users visible feedback on long operations.
        if (progress && docIdx % 25 == 0) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "fts5";
            ev.processed = docIdx;
            ev.total = totalDocs;
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = "Rebuilding FTS5 index (" + std::to_string(docIdx) + "/" +
                         std::to_string(totalDocs) + ")";
            progress(ev);
        }

        const std::string extension = normalizedRepairExtension(d);

        // Re-detect MIME for unhelpful or clearly wrong types
        std::string effectiveMime = d.mimeType;
        if (shouldRedetectMime(d)) {
            effectiveMime = bestEffortMimeForDocument(d, store);
            auto updated = d;
            updated.mimeType = effectiveMime;
            (void)meta->updateDocument(updated);
        }

        if (dryRun) {
            result.skipped++;
            continue;
        }

        // Incremental mode: skip documents that already have successful extraction
        // with a valid content row AND a matching FTS5 index entry. The ghost-success
        // loop above already reset any documents that claim Success but lack content,
        // so remaining Success docs have content — but the FTS5 index may still be
        // missing (e.g. after corruption or interrupted rebuild).
        // Use --force to unconditionally rebuild everything.
        if (!force && d.extractionStatus == metadata::ExtractionStatus::Success) {
            auto contentRes = meta->getContent(d.id);
            if (contentRes && contentRes.value().has_value()) {
                if (ftsRowIds.count(d.id)) {
                    result.skipped++;
                    continue;
                }
            }
        }

        try {
            auto extractedOpt = yams::extraction::util::extractDocumentText(
                store, d.sha256Hash, effectiveMime, extension, customExtractors);
            if (extractedOpt && !extractedOpt->empty()) {
                (void)meta->updateDocumentExtractionStatus(
                    d.id, false, metadata::ExtractionStatus::Pending, "repair fts5 processing");

                metadata::DocumentContent content;
                content.documentId = d.id;
                if (extractedOpt->size() > kMaxTextToPersistInMetadataBytes) {
                    content.contentText = extractedOpt->substr(0, kMaxTextToPersistInMetadataBytes);
                } else {
                    content.contentText = *extractedOpt;
                }
                content.extractionMethod = "repair";
                auto contentResult = meta->insertContent(content);

                // Cap text for FTS5 indexing too — SQLite cannot bind strings > ~2 GB
                // and very large documents cause "string or blob too big" errors.
                const auto& textForIndex = (extractedOpt->size() > kMaxTextToPersistInMetadataBytes)
                                               ? content.contentText
                                               : *extractedOpt;
                auto ir = meta->indexDocumentContent(d.id, d.fileName, textForIndex, effectiveMime);

                if (ir && contentResult) {
                    (void)meta->updateDocumentExtractionStatus(d.id, true,
                                                               metadata::ExtractionStatus::Success);
                    result.succeeded++;
                } else {
                    static const std::string kUnknownFailure = "unknown";
                    const std::string& failMsg = !contentResult
                                                     ? contentResult.error().message
                                                     : (!ir ? ir.error().message : kUnknownFailure);
                    (void)meta->updateDocumentExtractionStatus(
                        d.id, false, metadata::ExtractionStatus::Failed, failMsg);
                    result.failed++;
                }
            } else {
                (void)meta->updateDocumentExtractionStatus(
                    d.id, false, metadata::ExtractionStatus::Skipped, "No extractable text");
                result.skipped++;
            }
        } catch (const std::exception& e) {
            (void)meta->updateDocumentExtractionStatus(
                d.id, false, metadata::ExtractionStatus::Failed, e.what());
            result.failed++;
        }
    }

    result.message = "FTS5 rebuild: " + std::to_string(result.succeeded) + " ok, " +
                     std::to_string(result.failed) + " failed, " + std::to_string(result.skipped) +
                     " skipped";
    return result;
}

boost::asio::awaitable<RepairOperationResult>
RepairService::generateMissingEmbeddingsAsync(const RepairRequest& req, const ProgressFn& progress,
                                              std::atomic<bool>* cancelRequested) {
    RepairOperationResult result;
    result.operation = "embeddings";

    auto isCanceled = [&]() {
        return cancelRequested && cancelRequested->load(std::memory_order_relaxed);
    };

    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        co_return result;
    }

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "embeddings";
        ev.message = "Scanning metadata candidates for missing embeddings";
        progress(ev);
    }

    metadata::DocumentQueryOptions queryOpts;
    if (!req.force) {
        queryOpts.hasEmbedding = false;
    }
    auto docs = meta->queryDocumentsForGrepCandidates(queryOpts);
    if (!docs) {
        result.message = "Failed to query";
        co_return result;
    }

    auto isEmbeddable = [&req](const std::string& m) -> bool {
        if (m.rfind("text/", 0) == 0)
            return true;
        if (m == "application/json" || m == "application/xml" || m == "application/x-yaml" ||
            m == "application/yaml")
            return true;
        for (const auto& inc : req.includeMime) {
            if (!inc.empty() && (m == inc || m.rfind(inc, 0) == 0))
                return true;
        }
        return false;
    };

    std::vector<std::string> hashes;
    size_t eligibleByMime = 0;
    size_t eligibleByExtractedText = 0;
    std::vector<std::string> excludedSamples;
    for (const auto& d : docs.value()) {
        const bool hasExtractedText = d.contentExtracted;
        if (isEmbeddable(d.mimeType)) {
            ++eligibleByMime;
            hashes.push_back(d.sha256Hash);
        } else if (hasExtractedText) {
            ++eligibleByExtractedText;
            hashes.push_back(d.sha256Hash);
        } else if (excludedSamples.size() < 8) {
            excludedSamples.push_back(d.filePath + " mime=" + d.mimeType +
                                      " extracted=" + std::string(d.contentExtracted ? "1" : "0"));
        }
    }

    spdlog::info("RepairService::generateMissingEmbeddingsAsync candidates: scanned={} eligible={} "
                 "eligible_by_mime={} eligible_by_extracted_text={} excluded_samples=[{}] "
                 "force={} missing_only_query={}",
                 docs.value().size(), hashes.size(), eligibleByMime, eligibleByExtractedText,
                 excludedSamples.size(), req.force ? 1 : 0, req.force ? 0 : 1);

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "embeddings";
        ev.total = hashes.size();
        ev.message =
            "Found " + std::to_string(hashes.size()) + " eligible documents missing embeddings";
        progress(ev);
    }

    result.processed = hashes.size();
    result.skipped = 0;

    if (hashes.empty()) {
        result.message = "No eligible documents";
        co_return result;
    }

    if (req.dryRun) {
        result.skipped += hashes.size();
        result.message = "Would generate embeddings for " + std::to_string(hashes.size()) + " docs";
        co_return result;
    }

    std::string modelName = req.embeddingModel;
    if (modelName.empty() && ctx_.resolvePreferredModel) {
        try {
            modelName = ctx_.resolvePreferredModel();
        } catch (...) {
        }
    }
    if (modelName.empty() && ctx_.getEmbeddingModelName) {
        try {
            modelName = ctx_.getEmbeddingModelName();
        } catch (...) {
        }
    }
    if (isCanceled()) {
        result.failed = hashes.size();
        result.message = "Repair canceled";
        co_return result;
    }

    const uint32_t embedCap = TuneAdvisor::embedChannelCapacity();
    auto embedQ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", embedCap);
    const std::size_t batchSize = std::max<std::size_t>(1u, cfg_.maxBatch);
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    for (std::size_t i = 0; i < hashes.size(); i += batchSize) {
        if (isCanceled()) {
            result.failed += (hashes.size() - i);
            result.message = "Repair canceled";
            co_return result;
        }

        const std::size_t end = std::min(i + batchSize, hashes.size());
        std::vector<std::string> batch(hashes.begin() + static_cast<std::ptrdiff_t>(i),
                                       hashes.begin() + static_cast<std::ptrdiff_t>(end));
        const std::size_t batchIndex = (i / batchSize) + 1;
        const std::size_t totalBatches = (hashes.size() + batchSize - 1) / batchSize;

        if (meta) {
            (void)meta->batchUpdateDocumentRepairStatuses(batch,
                                                          metadata::RepairStatus::Processing);
        }

        InternalEventBus::EmbedJob job{batch,
                                       static_cast<uint32_t>(batch.size()),
                                       !req.force,
                                       modelName,
                                       std::vector<InternalEventBus::EmbedPreparedDoc>{},
                                       nullptr};
        // Batch semantic graph maintenance outside the per-job embedding hot path.
        // Per-job corpus scans cause large transient heap spikes during repair.
        job.updateSemanticGraph = false;

        const std::string jobLabel =
            "embed batch " + std::to_string(batchIndex) + "/" + std::to_string(totalBatches);
        const bool queued = co_await queueWithBackoff(embedQ, std::move(job), timer, running_,
                                                      jobLabel, batch.size(), 50, 1000, isCanceled);
        if (!queued) {
            result.failed += batch.size();
            if (meta) {
                (void)meta->batchUpdateDocumentRepairStatuses(batch,
                                                              metadata::RepairStatus::Pending);
            }
            InternalEventBus::instance().incEmbedDropped(batch.size());
            continue;
        }

        InternalEventBus::instance().incEmbedQueued(batch.size());
        TuningManager::notifyWakeup();
        result.succeeded += batch.size();
        result.processed = result.succeeded + result.failed + result.skipped;

        if (progress) {
            RepairEvent ev;
            ev.phase = "repairing";
            ev.operation = "embeddings";
            ev.processed = std::min(end, hashes.size());
            ev.total = hashes.size();
            ev.succeeded = result.succeeded;
            ev.failed = result.failed;
            ev.skipped = result.skipped;
            ev.message = "queued daemon embed batch " + std::to_string(batchIndex) + "/" +
                         std::to_string(totalBatches);
            progress(ev);
        }
    }

    result.message = "Queued " + std::to_string(result.succeeded) + " docs for embedding";
    co_return result;
}

RepairOperationResult RepairService::generateMissingEmbeddings(const RepairRequest& req,
                                                               const ProgressFn& progress,
                                                               std::atomic<bool>* cancelRequested) {
    RepairOperationResult result;
    result.operation = "embeddings";

    auto isCanceled = [&]() {
        return cancelRequested && cancelRequested->load(std::memory_order_relaxed);
    };

    auto meta = ctx_.getMetadataRepo ? ctx_.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "embeddings";
        ev.message = "Scanning metadata candidates for missing embeddings";
        progress(ev);
    }

    metadata::DocumentQueryOptions queryOpts;
    if (!req.force) {
        queryOpts.hasEmbedding = false;
    }
    auto docs = meta->queryDocumentsForGrepCandidates(queryOpts);
    if (!docs) {
        result.message = "Failed to query";
        return result;
    }

    // Filter for embeddable MIME types
    auto isEmbeddable = [&req](const std::string& m) -> bool {
        if (m.rfind("text/", 0) == 0)
            return true;
        if (m == "application/json" || m == "application/xml" || m == "application/x-yaml" ||
            m == "application/yaml")
            return true;
        for (const auto& inc : req.includeMime)
            if (!inc.empty() && (m == inc || m.rfind(inc, 0) == 0))
                return true;
        return false;
    };

    std::vector<std::string> hashes;
    size_t eligibleByMime = 0;
    size_t eligibleByExtractedText = 0;
    std::vector<std::string> excludedSamples;
    for (const auto& d : docs.value()) {
        const bool hasExtractedText = d.contentExtracted;
        if (isEmbeddable(d.mimeType)) {
            ++eligibleByMime;
            hashes.push_back(d.sha256Hash);
        } else if (hasExtractedText) {
            ++eligibleByExtractedText;
            hashes.push_back(d.sha256Hash);
        } else if (excludedSamples.size() < 8) {
            excludedSamples.push_back(d.filePath + " mime=" + d.mimeType +
                                      " extracted=" + std::string(d.contentExtracted ? "1" : "0"));
        }
    }

    spdlog::info("RepairService::generateMissingEmbeddings candidates: scanned={} eligible={} "
                 "eligible_by_mime={} eligible_by_extracted_text={} excluded_samples=[{}] "
                 "force={} missing_only_query={}",
                 docs.value().size(), hashes.size(), eligibleByMime, eligibleByExtractedText,
                 excludedSamples.size(), req.force ? 1 : 0, req.force ? 0 : 1);

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "embeddings";
        ev.total = hashes.size();
        ev.message =
            "Found " + std::to_string(hashes.size()) + " eligible documents missing embeddings";
        progress(ev);
    }

    size_t skippedExisting = 0;

    result.processed = hashes.size() + skippedExisting;
    result.skipped = skippedExisting;

    if (hashes.empty()) {
        if (skippedExisting > 0) {
            result.message = "All " + std::to_string(skippedExisting) +
                             " eligible documents already have embeddings";
        } else {
            result.message = "No eligible documents";
        }
        return result;
    }

    if (req.dryRun) {
        result.skipped += hashes.size();
        result.message = "Would generate embeddings for " + std::to_string(hashes.size()) + " docs";
        if (skippedExisting > 0) {
            result.message +=
                " (" + std::to_string(skippedExisting) + " already embedded, skipped)";
        }
        return result;
    }

    std::string modelName = req.embeddingModel;
    if (modelName.empty() && ctx_.resolvePreferredModel) {
        try {
            modelName = ctx_.resolvePreferredModel();
        } catch (...) {
        }
    }
    if (modelName.empty() && ctx_.getEmbeddingModelName) {
        try {
            modelName = ctx_.getEmbeddingModelName();
        } catch (...) {
        }
    }
    if (req.foreground && modelName.empty()) {
        result.failed = hashes.size();
        result.message = "No embedding model configured";
        return result;
    }
    if (isCanceled()) {
        result.failed = hashes.size();
        result.message = "Repair canceled";
        return result;
    }

    const uint32_t embedCap = TuneAdvisor::embedChannelCapacity();
    auto embedQ = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", embedCap);

    const std::size_t batchSize = std::max<std::size_t>(1u, cfg_.maxBatch);
    const std::size_t totalDocs = hashes.size() + skippedExisting;
    const std::size_t totalBatches = (hashes.size() + batchSize - 1) / batchSize;
    std::size_t completedDocs = 0;
    std::size_t failedDocs = 0;
    std::size_t skippedDocs = skippedExisting;

    for (std::size_t i = 0; i < hashes.size(); i += batchSize) {
        if (isCanceled()) {
            result.failed += (hashes.size() - i);
            result.message = "Repair canceled";
            return result;
        }

        const std::size_t end = std::min(i + batchSize, hashes.size());
        std::vector<std::string> batch(hashes.begin() + i, hashes.begin() + end);
        const std::size_t batchIndex = i / batchSize;

        if (meta) {
            (void)meta->batchUpdateDocumentRepairStatuses(batch,
                                                          metadata::RepairStatus::Processing);
        }

        std::shared_ptr<InternalEventBus::EmbedJobMonitor> monitor;
        if (req.foreground) {
            monitor = std::make_shared<InternalEventBus::EmbedJobMonitor>();
            monitor->totalDocs = batch.size();
            monitor->phase = "queued";
            monitor->detail = "queued daemon embed batch";
        }

        InternalEventBus::EmbedJob job{batch,
                                       static_cast<uint32_t>(batch.size()),
                                       !req.force,
                                       modelName,
                                       std::vector<InternalEventBus::EmbedPreparedDoc>{},
                                       std::move(monitor)};
        // Batch semantic graph maintenance outside the per-job embedding hot path.
        // Per-job corpus scans cause large transient heap spikes during repair.
        job.updateSemanticGraph = false;

        int retries = 0;
        bool pushed = false;
        while (!pushed && retries < 20) {
            pushed = embedQ->try_push(job);
            if (!pushed) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(50 * (1 << std::min(retries, 5))));
                retries++;
            }
        }

        if (!pushed) {
            result.failed += batch.size();
            if (meta) {
                (void)meta->batchUpdateDocumentRepairStatuses(batch,
                                                              metadata::RepairStatus::Pending);
            }
            InternalEventBus::instance().incEmbedDropped();
            continue;
        }

        InternalEventBus::instance().incEmbedQueued();
        TuningManager::notifyWakeup();

        if (!req.foreground) {
            result.succeeded += batch.size();
            if (progress) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "embeddings";
                ev.processed = std::min(i + batchSize, hashes.size());
                ev.total = hashes.size();
                ev.succeeded = result.succeeded;
                ev.failed = result.failed;
                ev.skipped = result.skipped;
                progress(ev);
            }
            continue;
        }

        std::size_t lastReportedProcessed = static_cast<std::size_t>(-1);
        auto lastReport = std::chrono::steady_clock::time_point{};
        while (true) {
            if (isCanceled()) {
                if (monitor) {
                    monitor->cancelRequested.store(true, std::memory_order_relaxed);
                    monitor->cv.notify_all();
                }
                result.message = "Repair canceled";
                return result;
            }

            std::size_t batchCompleted = 0;
            std::size_t batchFailed = 0;
            std::size_t batchSkipped = 0;
            std::size_t batchProcessed = 0;
            std::string batchPhase = "queued";
            std::string batchDetail;
            bool batchDone = false;
            {
                std::unique_lock<std::mutex> lk(monitor->mutex);
                monitor->cv.wait_for(lk, std::chrono::milliseconds(250));
                batchCompleted = static_cast<std::size_t>(monitor->succeededDocs);
                batchFailed = static_cast<std::size_t>(monitor->failedDocs);
                batchSkipped = static_cast<std::size_t>(monitor->skippedDocs);
                batchProcessed = static_cast<std::size_t>(monitor->processedDocs);
                batchPhase = monitor->phase;
                batchDetail = monitor->detail;
                batchDone = monitor->done;
            }

            const auto now = std::chrono::steady_clock::now();
            if (progress && (batchProcessed != lastReportedProcessed ||
                             lastReport.time_since_epoch().count() == 0 ||
                             (now - lastReport) >= std::chrono::seconds(1))) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "embeddings";
                ev.processed = completedDocs + failedDocs + skippedDocs + batchProcessed;
                ev.total = totalDocs;
                ev.succeeded = completedDocs + batchCompleted;
                ev.failed = failedDocs + batchFailed;
                ev.skipped = skippedDocs + batchSkipped;
                if (batchDone) {
                    ev.message = "completed daemon embed batch " + std::to_string(batchIndex + 1) +
                                 "/" + std::to_string(totalBatches);
                } else {
                    ev.message = "daemon embed batch " + std::to_string(batchIndex + 1) + "/" +
                                 std::to_string(totalBatches) + " phase=" + batchPhase;
                    if (!batchDetail.empty()) {
                        ev.message += " " + batchDetail;
                    }
                }
                progress(ev);
                lastReportedProcessed = batchProcessed;
                lastReport = now;
            }

            if (batchDone) {
                completedDocs += batchCompleted;
                failedDocs += batchFailed;
                skippedDocs += batchSkipped;
                result.processed = completedDocs + failedDocs + skippedDocs;
                result.succeeded = completedDocs;
                result.failed = failedDocs;
                result.skipped = skippedDocs;
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
    }

    if (req.foreground) {
        // No explicit finalize needed — the coordinator manages the bulk window.
        if (req.repairTopology && result.succeeded > 0 && ctx_.rebuildSemanticNeighborGraph) {
            auto semanticResult =
                ctx_.rebuildSemanticNeighborGraph("repair_service.embedding_batch");
            if (!semanticResult) {
                spdlog::warn("RepairService: deferred semantic neighbor rebuild failed: {}",
                             semanticResult.error().message);
            } else if (progress) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "embeddings";
                ev.processed = result.processed;
                ev.total = totalDocs;
                ev.succeeded = result.succeeded;
                ev.failed = result.failed;
                ev.skipped = result.skipped;
                ev.message = "rebuilt semantic neighbor graph edges=" +
                             std::to_string(semanticResult.value());
                progress(ev);
            }
        }
        result.message = "Generated " + std::to_string(result.succeeded) + " embeddings";
        if (result.skipped > 0 || result.failed > 0) {
            result.message += ", skipped=" + std::to_string(result.skipped) +
                              ", failed=" + std::to_string(result.failed);
        }
        if (state_) {
            state_->stats.repairEmbeddingsGenerated.store(result.succeeded,
                                                          std::memory_order_relaxed);
            state_->stats.repairEmbeddingsSkipped.store(result.skipped, std::memory_order_relaxed);
            state_->stats.repairFailedOperations.store(result.failed, std::memory_order_relaxed);
        }
    } else {
        result.message = "Queued " + std::to_string(result.succeeded) + " docs for embedding";
        if (result.skipped > 0) {
            result.message += " (" + std::to_string(result.skipped) + " already embedded, skipped)";
        }
    }
    return result;
}

RepairOperationResult RepairService::optimizeDatabase(bool dryRun, bool verbose,
                                                      ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "optimize";

    namespace fs = std::filesystem;
    fs::path dbPath = cfg_.dataDir / "yams.db";
    fs::path vecDbPath = cfg_.dataDir / "vectors.db";

    int dbCount = 0;
    if (fs::exists(dbPath))
        dbCount++;
    if (fs::exists(vecDbPath))
        dbCount++;

    if (dbCount == 0) {
        result.message = "No databases found";
        return result;
    }

    result.processed = dbCount;

    if (dryRun) {
        result.skipped = dbCount;
        result.message = "Would optimize " + std::to_string(dbCount) + " database(s)";
        return result;
    }

    int succeeded = 0;
    std::string failMsg;

    // Helper: checkpoint WAL + VACUUM + ANALYZE for a single database
    auto optimizeOne = [&](const fs::path& path, const char* label) {
        sqlite3* db = nullptr;
        if (sqlite3_open(path.string().c_str(), &db) != SQLITE_OK) {
            failMsg += std::string(label) + ": failed to open; ";
            return false;
        }

        // Checkpoint WAL first to reduce its size before VACUUM
        int walLog = 0, walCkpt = 0;
        sqlite3_wal_checkpoint_v2(db, nullptr, SQLITE_CHECKPOINT_TRUNCATE, &walLog, &walCkpt);
        spdlog::info("[Optimize] {} WAL checkpoint: log={} checkpointed={}", label, walLog,
                     walCkpt);

        char* errMsg = nullptr;
        if (sqlite3_exec(db, "VACUUM", nullptr, nullptr, &errMsg) == SQLITE_OK) {
            sqlite3_exec(db, "ANALYZE", nullptr, nullptr, nullptr);
            spdlog::info("[Optimize] {} VACUUM + ANALYZE completed", label);
            sqlite3_close(db);
            return true;
        } else {
            std::string error = errMsg ? errMsg : "Unknown error";
            sqlite3_free(errMsg);
            failMsg += std::string(label) + ": " + error + "; ";
            sqlite3_close(db);
            return false;
        }
    };

    if (fs::exists(dbPath) && optimizeOne(dbPath, "yams.db")) {
        succeeded++;
    }
    if (fs::exists(vecDbPath) && optimizeOne(vecDbPath, "vectors.db")) {
        succeeded++;
    }

    result.succeeded = succeeded;
    result.failed = dbCount - succeeded;

    if (result.failed == 0) {
        result.message = "All databases optimized";
    } else {
        result.message = "Optimize partially failed: " + failMsg;
    }

    return result;
}

} // namespace yams::daemon
