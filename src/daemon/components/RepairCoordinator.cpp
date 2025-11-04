#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/RepairCoordinator.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
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
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/repair_scheduling_adapter.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/content_extractor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/vector_database.h>

namespace yams::daemon {

namespace {
// Shared thread pool for all RepairCoordinator instances
// Uses 2 threads: one for coordination, one for DB queries
// Static singleton - threads are detached and cleaned up by OS at exit
struct RepairThreadPool {
    boost::asio::thread_pool pool{2};

    static RepairThreadPool& instance() {
        static RepairThreadPool instance;
        return instance;
    }

    boost::asio::any_io_executor get_executor() { return pool.get_executor(); }

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
    std::unordered_map<std::string, std::string> result;

    for (const auto& extractor : extractors) {
        if (!extractor)
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

RepairCoordinator::~RepairCoordinator() {
    stop();
}

void RepairCoordinator::start() {
    if (!cfg_.enable || running_.exchange(true)) {
        return;
    }
    // Initialize maintenance tokens based on config
    tokens_.store(cfg_.maintenanceTokens);
    shutdownState_->finished.store(false, std::memory_order_relaxed);
    shutdownState_->running.store(true, std::memory_order_relaxed);

    auto exec = RepairThreadPool::instance().get_executor();
    // Capture shared_ptr to shutdown state - do NOT capture this
    auto shutdownState = shutdownState_;
    auto* self = this; // Capture raw pointer - we guarantee lifetime via stop()

    boost::asio::co_spawn(
        exec,
        [self, shutdownState]() -> boost::asio::awaitable<void> {
            spdlog::debug("RepairCoordinator coroutine starting");
            try {
                // Only proceed if still running
                if (!shutdownState->running.load(std::memory_order_acquire)) {
                    spdlog::debug(
                        "RepairCoordinator coroutine - already stopped, exiting immediately");
                    co_return;
                }
                co_await self->runAsync();
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
}

void RepairCoordinator::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    // Signal the coroutine to stop via shared state
    shutdownState_->running.store(false, std::memory_order_release);
    spdlog::debug("RepairCoordinator::stop() - signaled coroutine to stop");
    // Clear services pointer to avoid use-after-free races during shutdown.
    services_ = nullptr;
    queueCv_.notify_all();
    // Wait for coroutine to signal completion
    {
        std::unique_lock<std::mutex> lk(shutdownState_->mutex);
        bool completed = shutdownState_->cv.wait_for(lk, std::chrono::milliseconds(2000), [this] {
            return shutdownState_->finished.load(std::memory_order_relaxed);
        });
        if (!completed) {
            spdlog::warn("RepairCoordinator::stop() - coroutine did not finish within timeout");
        } else {
            spdlog::debug("RepairCoordinator::stop() - coroutine finished");
        }
    }
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
    try {
        if (services_ && services_->getPostIngestQueue()) {
            spdlog::debug(
                "RepairCoordinator: skipping DocumentAdded {} — handled by PostIngestQueue",
                event.hash);
            return;
        }
    } catch (...) {
        // fall through to best-effort queue if introspection fails
    }

    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        pendingDocuments_.push(event.hash);
        if (state_)
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
    }
    queueCv_.notify_one();
    spdlog::debug("RepairCoordinator: queued document {} for embedding check", event.hash);

    // Opportunistically schedule symbol/entity extraction for newly added documents
    // Query plugins dynamically for supported languages - no static checks
    try {
        if (services_) {
            auto eg = services_->getEntityGraphService();
            auto symbolExtractors = services_->getSymbolExtractors();

            if (eg && !symbolExtractors.empty()) {
                // Build extension->language map from loaded plugins
                static thread_local std::unordered_map<std::string, std::string> extToLang;
                static thread_local bool mapInitialized = false;

                if (!mapInitialized) {
                    extToLang = buildExtensionLanguageMap(symbolExtractors);
                    mapInitialized = true;
                    spdlog::debug(
                        "RepairCoordinator: initialized symbol extraction for {} extensions",
                        extToLang.size());
                }

                // Extract file extension from path
                std::string extension;
                auto dotPos = event.path.find_last_of('.');
                if (dotPos != std::string::npos) {
                    extension = event.path.substr(dotPos); // includes the dot
                }

                // Check if any plugin supports this extension
                auto it = extToLang.find(extension);
                if (it != extToLang.end()) {
                    EntityGraphService::Job j;
                    j.documentHash = event.hash;
                    j.filePath = event.path;
                    j.language = it->second; // Language from plugin!

                    (void)eg->submitExtraction(std::move(j));
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

void RepairCoordinator::onDocumentRemoved(const DocumentRemovedEvent& event) {
    if (!cfg_.enable || !running_) {
        return;
    }

    // For now, just log - in future could clean up orphaned embeddings
    spdlog::debug("RepairCoordinator: document {} removed", event.hash);
}

boost::asio::awaitable<void> RepairCoordinator::runAsync() {
    using namespace std::chrono_literals;
    auto ex = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(ex);

    core::RepairFsm::Config fsmConfig;
    fsmConfig.enable_online_repair = true;
    fsmConfig.max_repair_concurrency = cfg_.maintenanceTokens;
    fsmConfig.repair_backoff_ms = 250;
    fsmConfig.max_retries = 3;

    core::RepairFsm fsm(fsmConfig);
    fsm.set_on_state_change([](core::RepairFsm::State state) {
        spdlog::debug("RepairFsm state changed to: {}", core::RepairFsm::to_string(state));
    });

    bool initialScanEnqueued = false;
    // Defer initial scan to avoid blocking during startup
    int deferTicks = 0;
    const int minDeferTicks = 50; // ~5 seconds at 100ms/tick

    // Get or create prune job channel
    static auto pruneQueue =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PruneJob>("prune_jobs",
                                                                                       128);

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

        // Process prune jobs if available
        InternalEventBus::PruneJob pruneJob;
        if (pruneQueue->try_pop(pruneJob)) {
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
                spdlog::warn("RepairCoordinator: prune job {} - storage engine access needed",
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

        // Defer initial scan to avoid blocking during startup (grace period)
        if (pendingDocuments_.empty() && !initialScanEnqueued) {
            if (deferTicks < minDeferTicks) {
                ++deferTicks;
            } else if (maintenance_allowed()) {
                spdlog::debug(
                    "RepairCoordinator: starting async initial scan (batched, non-blocking)");
                // Spawn async scan on dedicated executor to avoid blocking this coroutine
                auto scanExec = RepairThreadPool::instance().get_executor();
                boost::asio::co_spawn(
                    scanExec,
                    [this]() -> boost::asio::awaitable<void> {
                        try {
                            auto meta = services_ ? services_->getMetadataRepo() : nullptr;
                            auto vectorDb = services_ ? services_->getVectorDatabase() : nullptr;
                            if (!meta || !vectorDb) {
                                co_return;
                            }

                            // Query in batches to avoid locking DB for too long
                            const size_t batchSize = 1000;
                            size_t offset = 0;
                            size_t totalEnqueued = 0;

                            while (running_.load(std::memory_order_relaxed)) {
                                // Query batch of documents (limit + offset)
                                metadata::DocumentQueryOptions opts;
                                opts.limit = static_cast<int>(batchSize);
                                opts.offset = static_cast<int>(offset);
                                auto batchDocs = meta->queryDocuments(opts);

                                if (!batchDocs || batchDocs.value().empty()) {
                                    break; // No more documents
                                }

                                size_t batchEnqueued = 0;
                                {
                                    std::lock_guard<std::mutex> ql(queueMutex_);
                                    for (const auto& d : batchDocs.value()) {
                                        if (!running_.load(std::memory_order_relaxed))
                                            break;

                                        // Check if document needs repair
                                        const bool missingEmb =
                                            !vectorDb->hasEmbedding(d.sha256Hash);
                                        const bool missingFts =
                                            (!d.contentExtracted) ||
                                            (d.extractionStatus !=
                                             yams::metadata::ExtractionStatus::Success);

                                        if (missingEmb || missingFts) {
                                            pendingDocuments_.push(d.sha256Hash);
                                            ++batchEnqueued;
                                        }
                                    }
                                }

                                totalEnqueued += batchEnqueued;
                                offset += batchDocs.value().size();

                                // Yield between batches to let other work proceed
                                co_await boost::asio::post(
                                    co_await boost::asio::this_coro::executor,
                                    boost::asio::use_awaitable);

                                // Stop if batch was incomplete (reached end)
                                if (batchDocs.value().size() < batchSize) {
                                    break;
                                }
                            }

                            if (totalEnqueued > 0) {
                                totalBacklog_.store(totalEnqueued, std::memory_order_relaxed);
                                processed_.store(0, std::memory_order_relaxed);
                                spdlog::debug("RepairCoordinator: async scan complete, queued "
                                              "{} documents from {} total",
                                              totalEnqueued, offset);
                            } else {
                                spdlog::debug("RepairCoordinator: async scan complete, no "
                                              "documents need repair (scanned {})",
                                              offset);
                            }
                        } catch (const std::exception& e) {
                            spdlog::warn("RepairCoordinator: async scan exception: {}", e.what());
                        }
                        co_return;
                    },
                    boost::asio::detached);
                initialScanEnqueued = true;
            }
        }

        std::vector<std::string> batch;
        {
            std::lock_guard<std::mutex> lk(queueMutex_);
            while (!pendingDocuments_.empty() && batch.size() < cfg_.maxBatch) {
                batch.push_back(std::move(pendingDocuments_.front()));
                pendingDocuments_.pop();
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

        // Verify required services are available
        auto content = services_ ? services_->getContentStore() : nullptr;
        auto meta_repo = services_ ? services_->getMetadataRepo() : nullptr;
        if (!(content && meta_repo)) {
            continue;
        }

        // Detect documents missing embeddings (vector repair)
        std::vector<std::string> missingEmbeddings;
        auto vectorDb = services_ ? services_->getVectorDatabase() : nullptr;
        if (vectorDb) {
            for (const auto& hash : batch) {
                if (!vectorDb->hasEmbedding(hash))
                    missingEmbeddings.push_back(hash);
            }

            // Check if model provider is available for embedding generation
            if (!missingEmbeddings.empty()) {
                auto provider = services_ ? services_->getModelProvider() : nullptr;
                if (provider && provider->isAvailable()) {
                    spdlog::debug(
                        "RepairCoordinator: {} docs need embeddings, model provider ready",
                        missingEmbeddings.size());
                }
            }
        }

        // Detect documents missing FTS5 content (symbolic repair)
        std::vector<std::string> missingFts5;
        auto meta = services_ ? services_->getMetadataRepo() : nullptr;
        if (meta) {
            // Get custom extractors to check if we can handle non-text files
            auto customExtractors =
                services_ ? services_->getContentExtractors()
                          : std::vector<std::shared_ptr<extraction::IContentExtractor>>{};

            for (const auto& hash : batch) {
                auto docRes = meta->getDocumentByHash(hash);
                if (docRes && docRes.value().has_value()) {
                    const auto& d = docRes.value().value();
                    // Check if FTS5 content needs extraction
                    if (!d.contentExtracted ||
                        d.extractionStatus != yams::metadata::ExtractionStatus::Success) {
                        // Only queue if extractable (uses FileTypeDetector + magic_numbers.hpp +
                        // plugins)
                        if (canExtractDocument(d.mimeType, d.fileExtension, customExtractors,
                                               content, hash)) {
                            missingFts5.push_back(hash);
                        }
                    }
                }
            }
        }

        // Queue embedding repair job with backpressure handling
        if (!missingEmbeddings.empty()) {
            if (state_)
                state_->stats.repairBatchesAttempted++;

            InternalEventBus::EmbedJob job{
                missingEmbeddings, static_cast<uint32_t>(cfg_.maxBatch),
                true,         // skipExisting
                std::string{} // modelName (use default)
            };

            static std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> embedQ =
                InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
                    "embed_jobs", 1024);

            // Queue with backoff (goal: healthy DB, not speed)
            bool queued = co_await queueWithBackoff(embedQ, std::move(job), timer, running_,
                                                    "embed", missingEmbeddings.size(), 50, 1000);

            if (queued) {
                InternalEventBus::instance().incEmbedQueued();
            } else {
                InternalEventBus::instance().incEmbedDropped();
            }
        }

        // Queue FTS5 repair job (allow during light load if degraded mode enabled)
        bool allowFts5 = maintenance_allowed() || (cfg_.allowDegraded && activeConnFn_ &&
                                                   activeConnFn_() <= cfg_.maxActiveDuringDegraded);
        if (!missingFts5.empty() && allowFts5) {
            InternalEventBus::Fts5Job ftsJob{missingFts5, static_cast<uint32_t>(cfg_.maxBatch),
                                             InternalEventBus::Fts5Operation::ExtractAndIndex};

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
