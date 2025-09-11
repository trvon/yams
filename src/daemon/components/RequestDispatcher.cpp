#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <regex>
#include <sstream>
#include <thread>
#include <tuple>
#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/task.h>
#include <mach/task_info.h>
#endif
#include <yams/api/content_store.h>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/common/name_resolver.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/latency_registry.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/ipc/request_context_registry.h>
#include <yams/daemon/ipc/retrieval_session.h>
#include <yams/daemon/ipc/stream_metrics_registry.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/daemon/resource/plugin_loader.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/document_metadata.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/system_executor.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/extraction/extraction_util.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_engine_builder.h>
#include <yams/search/search_executor.h>
#include <yams/search/search_results.h>
#include <yams/vector/batch_metrics.h>
#include <yams/vector/dynamic_batcher.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/embedding_service.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>
#include <yams/version.hpp>

namespace yams::daemon {

// Helper functions for system metrics (moved from daemon.cpp)
double getMemoryUsage() {
#ifdef __APPLE__
    task_vm_info_data_t info;
    mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_VM_INFO, (task_info_t)&info, &count) == KERN_SUCCESS) {
        return static_cast<double>(info.resident_size) / (1024.0 * 1024.0); // MB
    }
#else
    // Linux implementation using /proc/self/status
    std::ifstream status("/proc/self/status");
    if (status.is_open()) {
        std::string line;
        while (std::getline(status, line)) {
            if (line.find("VmRSS:") == 0) {
                std::istringstream iss(line);
                std::string label;
                long rss_kb;
                iss >> label >> rss_kb;
                return static_cast<double>(rss_kb) / 1024.0; // Convert KB to MB
            }
        }
    }
#endif
    return 0.0;
}

double getCpuUsage() {
    // Simple CPU usage approximation - in a real implementation,
    // you'd track CPU time over intervals
#ifdef __APPLE__
    task_info_data_t tinfo __attribute__((unused));
    unsigned thread_count;
    thread_act_array_t thread_list;

    if (task_threads(mach_task_self(), &thread_list, &thread_count) == KERN_SUCCESS) {
        vm_deallocate(mach_task_self(), (vm_offset_t)thread_list, thread_count * sizeof(thread_t));
        return static_cast<double>(thread_count) * 0.1; // Rough approximation
    }
#else
    // Linux implementation - read thread count from /proc/self/status (robust to spaces)
    std::ifstream status("/proc/self/status");
    if (status.is_open()) {
        std::string line;
        while (std::getline(status, line)) {
            if (line.rfind("Threads:", 0) == 0) {
                try {
                    std::istringstream iss(line);
                    std::string label;
                    int thread_count = 0;
                    iss >> label >> thread_count;
                    if (thread_count > 0) {
                        return static_cast<double>(thread_count) * 0.1; // Very rough proxy
                    }
                } catch (...) {
                }
            }
        }
    }
#endif
    return 0.0;
}

RequestDispatcher::RequestDispatcher(YamsDaemon* daemon, ServiceManager* serviceManager,
                                     StateComponent* state)
    : daemon_(daemon), serviceManager_(serviceManager), state_(state) {}

RequestDispatcher::RequestDispatcher(YamsDaemon* daemon, ServiceManager* serviceManager,
                                     StateComponent* state, DaemonMetrics* metrics)
    : daemon_(daemon), serviceManager_(serviceManager), state_(state), metrics_(metrics) {}

RequestDispatcher::~RequestDispatcher() = default;

ServiceManager* RequestDispatcher::getServiceManager() const {
    return serviceManager_;
}

boost::asio::awaitable<Response> RequestDispatcher::dispatch(const Request& req) {
    // Immediately handle status and ping requests for responsiveness, bypassing other checks.
    if (std::holds_alternative<StatusRequest>(req)) {
        co_return co_await handleStatusRequest(std::get<StatusRequest>(req));
    }
    if (std::holds_alternative<PingRequest>(req)) {
        co_return co_await handlePingRequest(std::get<PingRequest>(req));
    }

    // For all other requests, check daemon readiness.
    auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
    if (lifecycleSnapshot.state != LifecycleState::Ready &&
        lifecycleSnapshot.state != LifecycleState::Degraded) {
        // Core services must be ready to proceed.
        bool coreReady = state_ && state_->readiness.metadataRepoReady.load() &&
                         state_->readiness.contentStoreReady.load();
        if (!coreReady) {
            // If not ready, return a status response to inform the client.
            StatusRequest statusReq;
            statusReq.detailed = true;
            co_return co_await handleStatusRequest(statusReq);
        } else {
            spdlog::debug("Proceeding with request during initialization (core ready)");
        }
    }

    // For requests that need services, check readiness.
    bool needs_services = !std::holds_alternative<ShutdownRequest>(req) &&
                          !std::holds_alternative<GetStatsRequest>(req);

    if (needs_services) {
        if (!state_->readiness.metadataRepoReady.load()) {
            co_return ErrorResponse{ErrorCode::InvalidState,
                                    "Metadata repository not ready. Please try again shortly."};
        }
        if (!state_->readiness.contentStoreReady.load()) {
            co_return ErrorResponse{ErrorCode::InvalidState,
                                    "Content store not ready. Please try again shortly."};
        }
    }

    Response out;
    try {
        if (req.valueless_by_exception()) {
            spdlog::warn(
                "RequestDispatcher: received valueless request variant; replying with error");
            co_return ErrorResponse{ErrorCode::InvalidData,
                                    "Malformed request (variant is valueless)"};
        }
    } catch (...) {
        co_return ErrorResponse{ErrorCode::InvalidData, "Malformed request (variant check failed)"};
    }
    try {
        out = co_await std::visit(
            [this](auto&& arg) -> boost::asio::awaitable<Response> {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, StatusRequest>) {
                    co_return co_await handleStatusRequest(arg);
                } else if constexpr (std::is_same_v<T, ShutdownRequest>) {
                    co_return co_await handleShutdownRequest(arg);
                } else if constexpr (std::is_same_v<T, PingRequest>) {
                    co_return co_await handlePingRequest(arg);
                } else if constexpr (std::is_same_v<T, SearchRequest>) {
                    co_return co_await handleSearchRequest(arg);
                } else if constexpr (std::is_same_v<T, GetRequest>) {
                    co_return co_await handleGetRequest(arg);
                } else if constexpr (std::is_same_v<T, AddDocumentRequest>) {
                    co_return co_await handleAddDocumentRequest(arg);
                } else if constexpr (std::is_same_v<T, ListRequest>) {
                    co_return co_await handleListRequest(arg);
                } else if constexpr (std::is_same_v<T, DeleteRequest>) {
                    co_return co_await handleDeleteRequest(arg);
                } else if constexpr (std::is_same_v<T, GetStatsRequest>) {
                    co_return co_await handleGetStatsRequest(arg);
                } else if constexpr (std::is_same_v<T, GenerateEmbeddingRequest>) {
                    co_return co_await handleGenerateEmbeddingRequest(arg);
                } else if constexpr (std::is_same_v<T, BatchEmbeddingRequest>) {
                    co_return co_await handleBatchEmbeddingRequest(arg);
                } else if constexpr (std::is_same_v<T, LoadModelRequest>) {
                    co_return co_await handleLoadModelRequest(arg);
                } else if constexpr (std::is_same_v<T, UnloadModelRequest>) {
                    co_return co_await handleUnloadModelRequest(arg);
                } else if constexpr (std::is_same_v<T, ModelStatusRequest>) {
                    co_return co_await handleModelStatusRequest(arg);
                } else if constexpr (std::is_same_v<T, UpdateDocumentRequest>) {
                    co_return co_await handleUpdateDocumentRequest(arg);
                } else if constexpr (std::is_same_v<T, GrepRequest>) {
                    co_return co_await handleGrepRequest(arg);
                } else if constexpr (std::is_same_v<T, DownloadRequest>) {
                    co_return co_await handleDownloadRequest(arg);
                } else if constexpr (std::is_same_v<T, PrepareSessionRequest>) {
                    co_return co_await handlePrepareSessionRequest(arg);
                } else if constexpr (std::is_same_v<T, PluginScanRequest>) {
                    co_return co_await handlePluginScanRequest(arg);
                } else if constexpr (std::is_same_v<T, PluginLoadRequest>) {
                    co_return co_await handlePluginLoadRequest(arg);
                } else if constexpr (std::is_same_v<T, PluginUnloadRequest>) {
                    co_return co_await handlePluginUnloadRequest(arg);
                } else if constexpr (std::is_same_v<T, PluginTrustListRequest>) {
                    co_return co_await handlePluginTrustListRequest(arg);
                } else if constexpr (std::is_same_v<T, PluginTrustAddRequest>) {
                    co_return co_await handlePluginTrustAddRequest(arg);
                } else if constexpr (std::is_same_v<T, PluginTrustRemoveRequest>) {
                    co_return co_await handlePluginTrustRemoveRequest(arg);
                } else if constexpr (std::is_same_v<T, CancelRequest>) {
                    co_return co_await handleCancelRequest(arg);
                } else if constexpr (std::is_same_v<T, EmbedDocumentsRequest>) {
                    co_return co_await handleEmbedDocumentsRequest(arg);
                } else {
                    co_return ErrorResponse{ErrorCode::NotImplemented,
                                            "Request type not yet implemented"};
                }
            },
            req);
    } catch (const std::exception& e) {
        spdlog::error("RequestDispatcher::dispatch exception: {}", e.what());
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("Failed to process request: ") + e.what()};
    } catch (...) {
        spdlog::error("RequestDispatcher::dispatch unknown exception");
        co_return ErrorResponse{ErrorCode::InternalError,
                                "Failed to process request: unknown error"};
    }

    try {
        if (state_) {
            state_->stats.requestsProcessed.fetch_add(1, std::memory_order_relaxed);
        }
    } catch (...) {
    }

    co_return out;
}

boost::asio::any_io_executor RequestDispatcher::getWorkerExecutor() const {
    try {
        if (serviceManager_) {
            return serviceManager_->getWorkerExecutor();
        }
    } catch (...) {
    }
    return boost::asio::system_executor();
}
std::function<void(bool)> RequestDispatcher::getWorkerJobSignal() const {
    try {
        if (serviceManager_)
            return serviceManager_->getWorkerJobSignal();
    } catch (...) {
    }
    return {};
}

boost::asio::awaitable<Response>
RequestDispatcher::handleStatusRequest(const StatusRequest& /*req*/) {
    // Minimal and safe status path using centralized DaemonMetrics when available
    StatusResponse res;
    try {
        // Fast mode toggles optional, potentially noisy probes or payloads
        const bool fastMode = []() {
            if (const char* v = std::getenv("YAMS_STATUS_FAST")) {
                std::string s(v);
                std::transform(s.begin(), s.end(), s.begin(),
                               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                return (s == "1" || s == "true" || s == "on" || s == "yes");
            }
            return false;
        }();
        if (metrics_) {
            // Ensure snapshot reflects current worker pool state (avoid stale cache)
            metrics_->refresh();
            auto snap = metrics_->getSnapshot();
            res.running = snap.running;
            res.version = snap.version;
            res.uptimeSeconds = snap.uptimeSeconds;
            res.requestsProcessed = snap.requestsProcessed;
            res.activeConnections = snap.activeConnections;
            res.memoryUsageMb = snap.memoryUsageMb;
            res.cpuUsagePercent = snap.cpuUsagePercent;
            res.ready = snap.ready;
            res.overallStatus = snap.overallStatus;
            res.lifecycleState = snap.lifecycleState;
            res.lastError = snap.lastError;
            res.fsmTransitions = snap.fsmTransitions;
            res.fsmHeaderReads = snap.fsmHeaderReads;
            res.fsmPayloadReads = snap.fsmPayloadReads;
            res.fsmPayloadWrites = snap.fsmPayloadWrites;
            res.fsmBytesSent = snap.fsmBytesSent;
            res.fsmBytesReceived = snap.fsmBytesReceived;
            res.muxActiveHandlers = snap.muxActiveHandlers;
            res.muxQueuedBytes = snap.muxQueuedBytes;
            res.muxWriterBudgetBytes = snap.muxWriterBudgetBytes;
            res.requestCounts["worker_threads"] = snap.workerThreads;
            res.requestCounts["worker_active"] = snap.workerActive;
            res.requestCounts["worker_queued"] = snap.workerQueued;
            res.retryAfterMs = snap.retryAfterMs;
            for (const auto& [k, v] : snap.readinessStates)
                res.readinessStates[k] = v;
            for (const auto& [k, v] : snap.initProgress)
                res.initProgress[k] = v;
        } else {
            // Fallback to legacy direct reads
            auto uptime = std::chrono::steady_clock::now() - state_->stats.startTime;
            res.running = true;
            res.version = YAMS_VERSION_STRING;
            res.uptimeSeconds = std::chrono::duration_cast<std::chrono::seconds>(uptime).count();
            res.requestsProcessed = state_->stats.requestsProcessed.load();
            res.activeConnections = state_->stats.activeConnections.load();
            try {
                auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
                if (lifecycleSnapshot.state == LifecycleState::Ready ||
                    lifecycleSnapshot.state == LifecycleState::Degraded) {
                    res.memoryUsageMb = getMemoryUsage();
                    res.cpuUsagePercent = getCpuUsage();
                }
            } catch (...) {
            }
        }

        // If metrics are not present, keep legacy readiness fill
        if (!metrics_) {
            res.ready = state_->readiness.fullyReady();
            res.readinessStates["ipc_server"] = state_->readiness.ipcServerReady.load();
            res.readinessStates["content_store"] = state_->readiness.contentStoreReady.load();
            res.readinessStates["database"] = state_->readiness.databaseReady.load();
            res.readinessStates["metadata_repo"] = state_->readiness.metadataRepoReady.load();
            res.readinessStates["search_engine"] = state_->readiness.searchEngineReady.load();
            res.readinessStates["model_provider"] = state_->readiness.modelProviderReady.load();
            res.readinessStates["vector_index"] = state_->readiness.vectorIndexReady.load();
            res.readinessStates["plugins"] = state_->readiness.pluginsReady.load();
        }
        // Expose degraded flag when hybrid engine is not yet available
        try {
            bool searchDegraded = true;
            if (serviceManager_) {
                auto engine = serviceManager_->getSearchEngineSnapshot();
                searchDegraded = (engine == nullptr);
            }
            res.readinessStates["search_engine_degraded"] = searchDegraded;
        } catch (...) {
            res.readinessStates["search_engine_degraded"] = true;
        }

        // Surface vector scoring availability for doctor/status consumers
        try {
            bool vectorEmbeddingsAvailable = false;
            bool vectorScoringEnabled = false;
            if (serviceManager_) {
                auto eng = serviceManager_->getSearchEngineSnapshot();
                auto gen = serviceManager_->getEmbeddingGenerator();
                if (gen) {
                    try {
                        vectorEmbeddingsAvailable = gen->isInitialized();
                    } catch (...) {
                    }
                }
                if (eng) {
                    try {
                        const auto& cfg = eng->getConfig();
                        vectorScoringEnabled =
                            (cfg.vector_weight > 0.0f) && vectorEmbeddingsAvailable;
                    } catch (...) {
                    }
                }
            }
            res.readinessStates["vector_embeddings_available"] = vectorEmbeddingsAvailable;
            res.readinessStates["vector_scoring_enabled"] = vectorScoringEnabled;
        } catch (...) {
        }

        // Progress for long-running initializations
        if (!state_->readiness.searchEngineReady.load())
            res.initProgress["search_engine"] = state_->readiness.searchProgress.load();
        if (!state_->readiness.vectorIndexReady.load())
            res.initProgress["vector_index"] = state_->readiness.vectorIndexProgress.load();
        if (!state_->readiness.modelProviderReady.load())
            res.initProgress["model_provider"] = state_->readiness.modelLoadProgress.load();

        // Populate model provider and loaded model details when ready (expose memory usage)
        try {
            auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
            if ((lifecycleSnapshot.state == LifecycleState::Ready ||
                 lifecycleSnapshot.state == LifecycleState::Degraded) &&
                !fastMode) {
                auto provider = serviceManager_->getModelProvider();
                if (provider) {
                    // Provider summary row
                    StatusResponse::ModelInfo mi;
                    mi.name = "(provider)";
                    mi.type = provider->getProviderName();
                    mi.memoryMb = provider->getMemoryUsage() / (1024ull * 1024ull);
                    mi.requestCount = 0;
                    res.models.push_back(std::move(mi));
                    // Loaded models with per-model memory when available
                    try {
                        auto loaded = provider->getLoadedModels();
                        for (const auto& m : loaded) {
                            StatusResponse::ModelInfo di;
                            di.name = m;
                            di.type = provider->getProviderName();
                            size_t memBytes = 0;
                            uint64_t reqCount = 0;
                            if (auto infoR = provider->getModelInfo(m); infoR) {
                                const auto& info = infoR.value();
                                memBytes = info.memoryUsageBytes;
                                reqCount = info.requestCount;
                            }
                            di.memoryMb = memBytes / (1024ull * 1024ull);
                            di.requestCount = reqCount;
                            res.models.push_back(std::move(di));
                        }
                    } catch (...) {
                    }
                }
            }
        } catch (...) {
        }

        // Derive overall status from authoritative lifecycle FSM rather than
        // readiness booleans (which include optional subsystems like models).
        try {
            auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
            switch (lifecycleSnapshot.state) {
                case LifecycleState::Ready:
                    res.overallStatus = "ready";
                    res.lifecycleState = "ready";
                    break;
                case LifecycleState::Degraded:
                    res.overallStatus = "degraded";
                    res.lifecycleState = "degraded";
                    break;
                case LifecycleState::Initializing:
                    res.overallStatus = "initializing";
                    res.lifecycleState = "initializing";
                    break;
                case LifecycleState::Starting:
                    res.overallStatus = "starting";
                    res.lifecycleState = "starting";
                    break;
                case LifecycleState::Failed:
                    res.overallStatus = "failed";
                    res.lifecycleState = "failed";
                    break;
                case LifecycleState::Stopping:
                    res.overallStatus = "stopping";
                    res.lifecycleState = "stopping";
                    break;
                case LifecycleState::Stopped:
                    res.overallStatus = "stopped";
                    res.lifecycleState = "stopped";
                    break;
                case LifecycleState::Unknown:
                default:
                    res.overallStatus = "initializing";
                    res.lifecycleState = "initializing";
                    break;
            }
            if (!lifecycleSnapshot.lastError.empty()) {
                res.lastError = lifecycleSnapshot.lastError;
            }
        } catch (...) {
            // Fallback to legacy readiness view
            res.overallStatus = state_->readiness.overallStatus();
            res.lifecycleState = res.overallStatus;
        }

        // FSM/MUX metrics: expose only when daemon is Ready/Degraded to avoid early init races
        if (!metrics_) {
            try {
                auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
                if (lifecycleSnapshot.state == LifecycleState::Ready ||
                    lifecycleSnapshot.state == LifecycleState::Degraded) {
                    auto snap = FsmMetricsRegistry::instance().snapshot();
                    res.fsmTransitions = snap.transitions;
                    res.fsmHeaderReads = snap.headerReads;
                    res.fsmPayloadReads = snap.payloadReads;
                    res.fsmPayloadWrites = snap.payloadWrites;
                    res.fsmBytesSent = snap.bytesSent;
                    res.fsmBytesReceived = snap.bytesReceived;

                    auto msnap = MuxMetricsRegistry::instance().snapshot();
                    res.muxActiveHandlers = msnap.activeHandlers;
                    res.muxQueuedBytes = msnap.queuedBytes;
                    res.muxWriterBudgetBytes = msnap.writerBudgetBytes;
                }
            } catch (...) {
                // best-effort only
            }
        }

        if (!metrics_) {
            try {
                auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
                if (lifecycleSnapshot.state == LifecycleState::Ready ||
                    lifecycleSnapshot.state == LifecycleState::Degraded) {
                    std::size_t threads = 0, active = 0, queued = 0;
                    if (serviceManager_) {
                        threads = serviceManager_->getWorkerThreads();
                        active = serviceManager_->getWorkerActive();
                        queued = 0;
                    } else {
                        threads = std::max(1u, std::thread::hardware_concurrency());
                    }
                    res.requestCounts["worker_threads"] = threads;
                    res.requestCounts["worker_active"] = active;
                    res.requestCounts["worker_queued"] = queued;
                }
            } catch (...) {
            }
        }

    } catch (...) {
        // On any unexpected issue, return a minimal running=false stub to avoid crash
        StatusResponse fallback;
        fallback.running = true;
        fallback.ready = false;
        fallback.uptimeSeconds = 0;
        fallback.requestsProcessed = 0;
        fallback.activeConnections = 0;
        fallback.memoryUsageMb = 0;
        fallback.cpuUsagePercent = 0;
        fallback.version = YAMS_VERSION_STRING;
        fallback.overallStatus = "Starting";
        co_return fallback;
    }
    co_return res;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleShutdownRequest(const ShutdownRequest& req) {
    spdlog::info("Received shutdown request (graceful={})", req.graceful);

    // Important: ensure the shutdown response is flushed back to the client before the
    // socket server begins tearing down the IO context. Schedule the stop slightly later
    // on a detached thread to give RequestHandler time to write the response frame.
    if (daemon_) {
        bool graceful = req.graceful;
        std::thread([d = daemon_, graceful]() {
            try {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            } catch (...) {
            }
            d->requestStop();
            (void)graceful; // reserved for future policy differences
        }).detach();
    }

    co_return SuccessResponse{"Shutdown initiated"};
}

// Ensure error messages are valid UTF-8 for protobuf transport
static inline std::string sanitizeUtf8(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    const unsigned char* p = reinterpret_cast<const unsigned char*>(s.data());
    size_t i = 0, n = s.size();
    auto append_replacement = [&]() { out += "\xEF\xBF\xBD"; };
    while (i < n) {
        unsigned char c = p[i];
        if (c < 0x80) {
            out.push_back(static_cast<char>(c));
            i++;
            continue;
        }
        // 2-byte
        if ((c & 0xE0) == 0xC0 && i + 1 < n) {
            unsigned char c1 = p[i + 1];
            if ((c1 & 0xC0) == 0x80 && (c >= 0xC2)) {
                out.push_back(static_cast<char>(c));
                out.push_back(static_cast<char>(c1));
                i += 2;
                continue;
            }
        }
        // 3-byte
        if ((c & 0xF0) == 0xE0 && i + 2 < n) {
            unsigned char c1 = p[i + 1], c2 = p[i + 2];
            if ((c1 & 0xC0) == 0x80 && (c2 & 0xC0) == 0x80) {
                // Avoid surrogates
                if (!(c == 0xE0 && c1 < 0xA0) && !(c == 0xED && c1 >= 0xA0)) {
                    out.push_back(static_cast<char>(c));
                    out.push_back(static_cast<char>(c1));
                    out.push_back(static_cast<char>(c2));
                    i += 3;
                    continue;
                }
            }
        }
        // 4-byte
        if ((c & 0xF8) == 0xF0 && i + 3 < n) {
            unsigned char c1 = p[i + 1], c2 = p[i + 2], c3 = p[i + 3];
            if ((c1 & 0xC0) == 0x80 && (c2 & 0xC0) == 0x80 && (c3 & 0xC0) == 0x80) {
                // valid range 0x010000..0x10FFFF
                if (!(c == 0xF0 && c1 < 0x90) && !(c == 0xF4 && c1 >= 0x90) && c <= 0xF4) {
                    out.push_back(static_cast<char>(c));
                    out.push_back(static_cast<char>(c1));
                    out.push_back(static_cast<char>(c2));
                    out.push_back(static_cast<char>(c3));
                    i += 4;
                    continue;
                }
            }
        }
        append_replacement();
        i++;
    }
    return out;
}

static inline ErrorResponse makeError(ErrorCode code, const std::string& msg) {
    return ErrorResponse{code, sanitizeUtf8(msg)};
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGenerateEmbeddingRequest(const GenerateEmbeddingRequest& req) {
    try {
        auto provider = serviceManager_->getModelProvider();
        if (!provider || !provider->isAvailable()) {
            // Opportunistic adoption: try to adopt a provider from loaded hosts
            try {
                if (serviceManager_) {
                    (void)serviceManager_->adoptModelProviderFromHosts();
                    provider = serviceManager_->getModelProvider();
                }
            } catch (...) {
            }
            if (!provider || !provider->isAvailable()) {
                co_return makeError(ErrorCode::InvalidState, "Model provider unavailable");
            }
        }
        spdlog::info("Embedding request: model='{}' normalize={} text_len={}", req.modelName,
                     req.normalize ? "true" : "false", req.text.size());
        // If a model name is provided, best-effort load it first with soft timeout
        if (!req.modelName.empty() && !provider->isModelLoaded(req.modelName)) {
            int timeout_ms = 30000;
            if (const char* env = std::getenv("YAMS_MODEL_LOAD_TIMEOUT_MS")) {
                try {
                    timeout_ms = std::stoi(env);
                    if (timeout_ms < 1000)
                        timeout_ms = 1000;
                } catch (...) {
                }
            }
            auto fut = std::async(std::launch::async,
                                  [&]() { return provider->loadModel(req.modelName); });
            if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) ==
                std::future_status::timeout) {
                co_return makeError(ErrorCode::Timeout, "Model load timed out");
            }
            auto lr = fut.get();
            if (!lr) {
                co_return makeError(lr.error().code, lr.error().message);
            }
        }
        Result<std::vector<float>> r =
            req.modelName.empty() ? provider->generateEmbedding(req.text)
                                  : provider->generateEmbeddingFor(req.modelName, req.text);
        if (!r) {
            co_return makeError(r.error().code, r.error().message);
        }
        EmbeddingResponse resp;
        resp.embedding = std::move(r.value());
        resp.dimensions = resp.embedding.size();
        resp.modelUsed = req.modelName;
        resp.processingTimeMs = 0;
        co_return resp;
    } catch (const std::exception& e) {
        co_return makeError(ErrorCode::InternalError,
                            std::string("Embedding generation failed: ") + e.what());
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handleBatchEmbeddingRequest(const BatchEmbeddingRequest& req) {
    try {
        auto t0 = std::chrono::steady_clock::now();
        auto hex_preview = [](const std::string& s, std::size_t n = 8) {
            std::ostringstream oss;
            oss << std::hex;
            std::size_t lim = std::min(n, s.size());
            for (std::size_t i = 0; i < lim; ++i) {
                oss << static_cast<unsigned int>(static_cast<unsigned char>(s[i]));
                if (i + 1 < lim)
                    oss << ' ';
            }
            return oss.str();
        };
        spdlog::info("BatchEmbedding dispatch: model='{}' (len={}, hex[:8]={}) count={} "
                     "normalize={} batchSize={}",
                     req.modelName, req.modelName.size(), hex_preview(req.modelName),
                     req.texts.size(), req.normalize ? "true" : "false", req.batchSize);
        auto provider = serviceManager_->getModelProvider();
        if (!provider || !provider->isAvailable()) {
            // Opportunistic adoption: try to adopt a provider from loaded hosts
            try {
                if (serviceManager_) {
                    (void)serviceManager_->adoptModelProviderFromHosts();
                    provider = serviceManager_->getModelProvider();
                }
            } catch (...) {
            }
            if (!provider || !provider->isAvailable()) {
                co_return makeError(ErrorCode::InvalidState, "Model provider unavailable");
            }
        }
        spdlog::info("Batch embedding request: model='{}' count={} normalize={} batchSize={}",
                     req.modelName, req.texts.size(), req.normalize ? "true" : "false",
                     req.batchSize);
        if (!req.modelName.empty() && !provider->isModelLoaded(req.modelName)) {
            int timeout_ms = 30000;
            if (const char* env = std::getenv("YAMS_MODEL_LOAD_TIMEOUT_MS")) {
                try {
                    timeout_ms = std::stoi(env);
                    if (timeout_ms < 1000)
                        timeout_ms = 1000;
                } catch (...) {
                }
            }
            auto fut = std::async(std::launch::async,
                                  [&]() { return provider->loadModel(req.modelName); });
            if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) ==
                std::future_status::timeout) {
                co_return makeError(ErrorCode::Timeout, "Model load timed out");
            }
            auto lr = fut.get();
            if (!lr) {
                co_return makeError(lr.error().code, lr.error().message);
            }
        }
        Result<std::vector<std::vector<float>>> r =
            req.modelName.empty() ? provider->generateBatchEmbeddings(req.texts)
                                  : provider->generateBatchEmbeddingsFor(req.modelName, req.texts);

        // Fallback: some providers do not implement batch APIs. Fall back to per-item generation.
        if (!r && (r.error().code == ErrorCode::NotImplemented ||
                   r.error().code == ErrorCode::NotSupported)) {
            spdlog::warn("Batch embeddings not implemented for provider; falling back to per-item "
                         "generation (count={})",
                         req.texts.size());
            std::vector<std::vector<float>> out;
            out.reserve(req.texts.size());
            for (const auto& t : req.texts) {
                Result<std::vector<float>> one =
                    req.modelName.empty() ? provider->generateEmbedding(t)
                                          : provider->generateEmbeddingFor(req.modelName, t);
                if (!one) {
                    co_return makeError(one.error().code, one.error().message);
                }
                out.push_back(std::move(one.value()));
            }
            r = std::move(out);
        }

        if (!r) {
            co_return makeError(r.error().code, r.error().message);
        }
        auto embeddings = std::move(r.value());
        const size_t dim = embeddings.empty() ? 0 : embeddings.front().size();
        const size_t count = embeddings.size();
        auto t1 = std::chrono::steady_clock::now();
        const uint32_t proc_ms = static_cast<uint32_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
        spdlog::info("BatchEmbedding completed: model='{}' count={} dim={} time_ms={} batchSize={}",
                     req.modelName, count, dim, proc_ms, req.batchSize);

        // Prefer handle-based finalization to avoid large frames: create a retrieval session
        bool use_handle = true;
        if (const char* env = std::getenv("YAMS_EMBED_RETURN_HANDLE")) {
            std::string s(env);
            for (auto& c : s)
                c = static_cast<char>(std::tolower(c));
            if (s == "0" || s == "false" || s == "off" || s == "no")
                use_handle = false;
        }
        if (use_handle && serviceManager_ && serviceManager_->getRetrievalSessionManager()) {
            try {
                // Serialize embeddings as contiguous f32 little-endian row-major
                std::vector<std::byte> bytes;
                bytes.reserve(dim * count * sizeof(float));
                for (const auto& v : embeddings) {
                    const float* p = v.data();
                    bytes.insert(bytes.end(), reinterpret_cast<const std::byte*>(p),
                                 reinterpret_cast<const std::byte*>(p) +
                                     (v.size() * sizeof(float)));
                }
                auto* rsm = serviceManager_->getRetrievalSessionManager();
                const uint32_t chunk = TuneAdvisor::chunkSize();
                const uint64_t tid = rsm->create(std::move(bytes), chunk, 0);
                GetInitResponse init;
                init.transferId = tid;
                init.totalSize = static_cast<uint64_t>(dim * count * sizeof(float));
                init.chunkSize = chunk;
                init.metadata["format"] = "embeddings_f32le";
                init.metadata["dim"] = std::to_string(dim);
                init.metadata["count"] = std::to_string(count);
                init.metadata["model"] = req.modelName;
                init.metadata["processing_ms"] = std::to_string(proc_ms);
                co_return init;
            } catch (const std::exception& ex) {
                spdlog::debug("Handle-based embedding finalize failed: {} — falling back to inline",
                              ex.what());
            }
        }

        // Fallback: inline response
        BatchEmbeddingResponse resp;
        resp.embeddings = std::move(embeddings);
        resp.dimensions = dim;
        resp.modelUsed = req.modelName;
        resp.processingTimeMs = proc_ms;
        resp.successCount = count;
        resp.failureCount = 0;

        // Best-effort: upsert embeddings into in-memory vector index when available
        try {
            auto vecMgr = serviceManager_->getVectorIndexManager();
            if (vecMgr && vecMgr->isInitialized() && !resp.embeddings.empty()) {
                std::vector<std::string> ids;
                ids.reserve(resp.embeddings.size());
                // Create deterministic but ad-hoc IDs from input text
                auto to_hex = [](uint64_t v) {
                    char buf[17];
                    snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(v));
                    return std::string(buf);
                };
                for (size_t i = 0; i < resp.embeddings.size(); ++i) {
                    uint64_t h = std::hash<std::string>{}(i < req.texts.size() ? req.texts[i]
                                                                               : std::to_string(i));
                    ids.emplace_back(std::string("adhoc_") + to_hex(h));
                }
                std::vector<std::map<std::string, std::string>> meta(ids.size());
                auto addRes = vecMgr->addVectors(ids, resp.embeddings, meta);
                if (!addRes) {
                    spdlog::debug("VectorIndexManager addVectors failed: {}",
                                  addRes.error().message);
                }
            }
        } catch (const std::exception& e) {
            spdlog::debug("Vector index upsert skipped: {}", e.what());
        }
        co_return resp;
    } catch (const std::exception& e) {
        co_return makeError(ErrorCode::InternalError,
                            std::string("Batch embedding failed: ") + e.what());
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handleLoadModelRequest(const LoadModelRequest& req) {
    try {
        auto provider = serviceManager_->getModelProvider();
        if (!provider || !provider->isAvailable()) {
            // Opportunistic adoption: try to adopt a provider from loaded hosts before failing
            try {
                if (serviceManager_) {
                    (void)serviceManager_->adoptModelProviderFromHosts();
                    provider = serviceManager_->getModelProvider();
                }
            } catch (...) {
            }
            if (!provider || !provider->isAvailable()) {
                co_return makeError(ErrorCode::InvalidState, "Model provider unavailable");
            }
        }
        if (req.modelName.empty()) {
            co_return makeError(ErrorCode::InvalidData, "modelName is required");
        }
        // Load model with soft timeout (options-aware)
        int timeout_ms = 30000;
        if (const char* env = std::getenv("YAMS_MODEL_LOAD_TIMEOUT_MS")) {
            try {
                timeout_ms = std::stoi(env);
                if (timeout_ms < 1000)
                    timeout_ms = 1000;
            } catch (...) {
            }
        }
        Result<void> r;
        if (!req.optionsJson.empty()) {
            auto fut = std::async(std::launch::async, [&]() {
                return provider->loadModelWithOptions(req.modelName, req.optionsJson);
            });
            if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) ==
                std::future_status::timeout) {
                co_return makeError(ErrorCode::Timeout, "Model load timed out");
            }
            r = fut.get();
        } else {
            auto fut = std::async(std::launch::async,
                                  [&]() { return provider->loadModel(req.modelName); });
            if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) ==
                std::future_status::timeout) {
                co_return makeError(ErrorCode::Timeout, "Model load timed out");
            }
            r = fut.get();
        }
        if (!r) {
            co_return makeError(r.error().code, r.error().message);
        }
        ModelLoadResponse resp;
        resp.success = true;
        resp.modelName = req.modelName;
        resp.memoryUsageMb = provider->getMemoryUsage() / (1024 * 1024);
        resp.loadTimeMs = 0;
        co_return resp;
    } catch (const std::exception& e) {
        co_return makeError(ErrorCode::InternalError,
                            std::string("Load model failed: ") + e.what());
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handleUnloadModelRequest(const UnloadModelRequest& req) {
    try {
        auto provider = serviceManager_->getModelProvider();
        if (!provider || !provider->isAvailable()) {
            co_return ErrorResponse{ErrorCode::InvalidState, "Model provider unavailable"};
        }
        if (req.modelName.empty()) {
            co_return ErrorResponse{ErrorCode::InvalidData, "modelName is required"};
        }
        auto r = provider->unloadModel(req.modelName);
        if (!r) {
            co_return ErrorResponse{r.error().code, r.error().message};
        }
        SuccessResponse resp{"Model unloaded"};
        co_return resp;
    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("Unload model failed: ") + e.what()};
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handleModelStatusRequest(const ModelStatusRequest& req) {
    try {
        auto provider = serviceManager_->getModelProvider();
        ModelStatusResponse resp;
        if (!provider || !provider->isAvailable()) {
            co_return resp;
        }
        auto loaded = provider->getLoadedModels();
        for (const auto& name : loaded) {
            if (!req.modelName.empty() && req.modelName != name) {
                continue;
            }
            ModelStatusResponse::ModelDetails d{};
            d.name = name;
            d.path = "";
            d.loaded = true;
            // After loadModel warmup, a loaded model has a ready session; treat as hot
            d.isHot = true;
            d.memoryMb = 0;
            if (auto mi = provider->getModelInfo(name); mi) {
                d.memoryMb = mi.value().memoryUsageBytes / (1024 * 1024);
                d.maxSequenceLength = mi.value().maxSequenceLength;
            }
            d.embeddingDim = provider->getEmbeddingDim(name);
            d.requestCount = 0;
            d.errorCount = 0;
            d.loadTime = {};
            d.lastAccess = {};
            resp.models.push_back(std::move(d));
        }
        resp.totalMemoryMb = provider->getMemoryUsage() / (1024 * 1024);
        resp.maxMemoryMb = 0;
        co_return resp;
    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("Model status failed: ") + e.what()};
    }
}

boost::asio::awaitable<Response> RequestDispatcher::handlePingRequest(const PingRequest& /*req*/) {
    PongResponse res;
    res.serverTime = std::chrono::steady_clock::now();
    co_return res;
}

boost::asio::awaitable<Response> RequestDispatcher::handleSearchRequest(const SearchRequest& req) {
    try {
        auto appContext = serviceManager_->getAppContext();
        appContext.workerExecutor = getWorkerExecutor();
        auto searchService = app::services::makeSearchService(appContext);

        app::services::SearchRequest serviceReq;
        serviceReq.query = req.query;
        serviceReq.limit = req.limit;
        serviceReq.fuzzy = req.fuzzy;
        serviceReq.similarity = static_cast<float>(req.similarity);
        serviceReq.hash = req.hashQuery;
        // Default type; may be overridden by fallback below
        // Prefer keyword search by default to maximize portability in environments
        // where vector acceleration may be unavailable. Clients can still request
        // explicit types via req.searchType.
        serviceReq.type = req.searchType.empty() ? "keyword" : req.searchType;
        serviceReq.verbose = req.verbose;
        serviceReq.literalText = req.literalText;
        serviceReq.showHash = req.showHash;
        serviceReq.pathsOnly = req.pathsOnly;
        serviceReq.jsonOutput = req.jsonOutput;
        serviceReq.showLineNumbers = req.showLineNumbers;
        serviceReq.beforeContext = req.beforeContext;
        serviceReq.afterContext = req.afterContext;
        serviceReq.context = req.context;
        // New: map engine-level filters from daemon request
        serviceReq.pathPattern = req.pathPattern;
        serviceReq.tags = req.tags;
        serviceReq.matchAllTags = req.matchAllTags;
        serviceReq.extension = req.extension;
        serviceReq.mimeType = req.mimeType;
        serviceReq.fileType = req.fileType;
        serviceReq.textOnly = req.textOnly;
        serviceReq.binaryOnly = req.binaryOnly;
        serviceReq.createdAfter = req.createdAfter;
        serviceReq.createdBefore = req.createdBefore;
        serviceReq.modifiedAfter = req.modifiedAfter;
        serviceReq.modifiedBefore = req.modifiedBefore;
        serviceReq.indexedAfter = req.indexedAfter;
        serviceReq.indexedBefore = req.indexedBefore;

        // If vector scoring is disabled via env, prefer metadata search.
        if (const char* disVec = std::getenv("YAMS_DISABLE_VECTOR");
            disVec && *disVec && std::string(disVec) != "0" && std::string(disVec) != "false") {
            serviceReq.type = "metadata";
            spdlog::debug("YAMS_DISABLE_VECTOR set; forcing metadata-only search.");
        }

        // If the hybrid engine is not ready, fall back to a metadata-only search.
        if (!state_->readiness.searchEngineReady.load()) {
            serviceReq.type = "metadata";
            spdlog::debug("Hybrid search engine not ready, falling back to metadata search.");
        }

        auto result = co_await searchService->search(serviceReq); // Task<Result<...>> is awaitable
        if (!result) {
            co_return ErrorResponse{result.error().code, result.error().message};
        }

        const auto& serviceResp = result.value();

        SearchResponse response;
        response.totalCount = serviceResp.total;
        response.elapsed = std::chrono::milliseconds(serviceResp.executionTimeMs);

        for (const auto& item : serviceResp.results) {
            SearchResult resultItem;
            resultItem.id = std::to_string(item.id);
            resultItem.title = item.title;
            resultItem.path = item.path;
            resultItem.score = item.score;
            resultItem.snippet = item.snippet;
            if (!item.hash.empty()) {
                resultItem.metadata["hash"] = item.hash;
            }
            if (!item.path.empty()) {
                resultItem.metadata["path"] = item.path;
            }
            if (!item.title.empty()) {
                resultItem.metadata["title"] = item.title;
            }
            response.results.push_back(std::move(resultItem));
        }

        std::stable_sort(
            response.results.begin(), response.results.end(),
            [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
        co_return response;

    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("Search failed: ") + e.what()};
    }
}

boost::asio::awaitable<Response> RequestDispatcher::handleGetRequest(const GetRequest& req) {
    try {
        auto appContext = serviceManager_->getAppContext();
        auto documentService = app::services::makeDocumentService(appContext);

        app::services::RetrieveDocumentRequest serviceReq;
        serviceReq.hash = req.hash;
        serviceReq.name = req.name;
        serviceReq.fileType = req.fileType;
        serviceReq.mimeType = req.mimeType;
        serviceReq.extension = req.extension;
        serviceReq.latest = req.latest;
        serviceReq.oldest = req.oldest;
        serviceReq.outputPath = req.outputPath;
        serviceReq.metadataOnly = req.metadataOnly;
        serviceReq.maxBytes = req.maxBytes;
        serviceReq.chunkSize = req.chunkSize;
        serviceReq.includeContent = !req.metadataOnly;
        serviceReq.raw = req.raw;
        serviceReq.extract = req.extract;
        serviceReq.graph = req.showGraph;
        serviceReq.depth = req.graphDepth;

        spdlog::debug("RequestDispatcher: Mapping GetRequest to DocumentService (hash='{}', "
                      "name='{}', metadataOnly={})",
                      req.hash, req.name, req.metadataOnly);

        auto result = documentService->retrieve(serviceReq);
        if (!result) {
            spdlog::warn("RequestDispatcher: DocumentService::retrieve failed: {}",
                         result.error().message);
            co_return ErrorResponse{result.error().code, result.error().message};
        }

        const auto& serviceResp = result.value();
        GetResponse response;

        if (serviceResp.document.has_value()) {
            const auto& doc = serviceResp.document.value();
            response.hash = doc.hash;
            response.path = doc.path;
            response.name = doc.name;
            response.fileName = doc.fileName;
            response.size = doc.size;
            response.mimeType = doc.mimeType;
            response.fileType = doc.fileType;
            response.created = doc.created;
            response.modified = doc.modified;
            response.indexed = doc.indexed;
            if (doc.content.has_value()) {
                response.content = doc.content.value();
                response.hasContent = true;
            } else {
                response.hasContent = false;
            }
            for (const auto& [key, value] : doc.metadata) {
                response.metadata[key] = value;
            }
        } else if (!serviceResp.documents.empty()) {
            const auto& doc = serviceResp.documents[0];
            response.hash = doc.hash;
            response.path = doc.path;
            response.name = doc.name;
            response.fileName = doc.fileName;
            response.size = doc.size;
            response.mimeType = doc.mimeType;
            response.fileType = doc.fileType;
            response.created = doc.created;
            response.modified = doc.modified;
            response.indexed = doc.indexed;
            if (doc.content.has_value()) {
                response.content = doc.content.value();
                response.hasContent = true;
            } else {
                response.hasContent = false;
            }
            for (const auto& [key, value] : doc.metadata) {
                response.metadata[key] = value;
            }
        } else {
            co_return ErrorResponse{ErrorCode::NotFound, "No documents found matching criteria"};
        }

        response.graphEnabled = serviceResp.graphEnabled;
        if (serviceResp.graphEnabled) {
            response.related.reserve(serviceResp.related.size());
            for (const auto& rel : serviceResp.related) {
                RelatedDocumentEntry entry;
                entry.hash = rel.hash;
                entry.path = rel.path;
                entry.name = rel.name;
                entry.relationship = rel.relationship.value_or("unknown");
                entry.distance = rel.distance;
                entry.relevanceScore = rel.relevanceScore;
                response.related.push_back(std::move(entry));
            }
        }

        response.totalBytes = serviceResp.totalBytes;
        response.outputWritten = serviceResp.outputPath.has_value();

        co_return response;

    } catch (const std::exception& e) {
        spdlog::error("RequestDispatcher: GetRequest handling failed: {}", e.what());
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("Get request failed: ") + e.what()};
    }
}

// PBI-008-11 scaffold: prepare session using app services (no IPC exposure yet)
int RequestDispatcher::prepareSession(const PrepareSessionOptions& opts) {
    try {
        auto appContext = serviceManager_->getAppContext();
        auto svc = yams::app::services::makeSessionService(&appContext);
        if (!svc)
            return -1;
        if (!opts.sessionName.empty()) {
            if (!svc->exists(opts.sessionName))
                return -2;
            svc->use(opts.sessionName);
        }
        yams::app::services::PrepareBudget b;
        b.maxCores = opts.maxCores;
        b.maxMemoryGb = opts.maxMemoryGb;
        b.maxTimeMs = opts.maxTimeMs;
        b.aggressive = opts.aggressive;
        auto warmed = svc->prepare(b, opts.limit, opts.snippetLen);
        return static_cast<int>(warmed);
    } catch (...) {
        return -3;
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePrepareSessionRequest(const PrepareSessionRequest& req) {
    PrepareSessionOptions opts;
    opts.sessionName = req.sessionName;
    opts.maxCores = req.cores;
    opts.maxMemoryGb = req.memoryGb;
    opts.maxTimeMs = req.timeMs;
    opts.aggressive = req.aggressive;
    opts.limit = req.limit;
    opts.snippetLen = req.snippetLen;

    int warmed = prepareSession(opts);
    if (warmed < 0) {
        co_return ErrorResponse{ErrorCode::InternalError, "Session prepare failed"};
    }
    PrepareSessionResponse resp;
    resp.warmedCount = static_cast<uint64_t>(warmed);
    resp.message = "OK";
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleEmbedDocumentsRequest(const EmbedDocumentsRequest& req) {
    if (!serviceManager_) {
        co_return ErrorResponse{ErrorCode::NotInitialized, "ServiceManager not available"};
    }

    auto executor = getWorkerExecutor();
    auto signal = getWorkerJobSignal();
    if (signal) {
        signal(true);
    }

    // Run the potentially long-running operation on a worker thread.
    auto result = co_await boost::asio::co_spawn(
        executor,
        [this, req]() -> boost::asio::awaitable<
                          std::optional<yams::Result<yams::repair::EmbeddingRepairStats>>> {
            auto contentStore = serviceManager_->getContentStore();
            auto metadataRepo = serviceManager_->getMetadataRepo();
            auto embeddingGenerator = serviceManager_->getEmbeddingGenerator();
            auto contentExtractors = serviceManager_->getContentExtractors();

            if (!embeddingGenerator) {
                auto ensure = serviceManager_->ensureEmbeddingGeneratorReady();
                if (ensure) {
                    embeddingGenerator = serviceManager_->getEmbeddingGenerator();
                }
            }

            if (!embeddingGenerator) {
                co_return std::optional<yams::Result<yams::repair::EmbeddingRepairStats>>{
                    Error{ErrorCode::NotInitialized, "Embedding generator not available (provider "
                                                     "not adopted or model not loaded)"}};
            }
            if (!contentStore) {
                co_return std::optional<yams::Result<yams::repair::EmbeddingRepairStats>>{
                    Error{ErrorCode::NotInitialized, "Content store not available"}};
            }
            if (!metadataRepo) {
                co_return std::optional<yams::Result<yams::repair::EmbeddingRepairStats>>{
                    Error{ErrorCode::NotInitialized, "Metadata repository not available"}};
            }

            yams::repair::EmbeddingRepairConfig repairConfig;
            repairConfig.batchSize = req.batchSize;
            repairConfig.skipExisting = req.skipExisting;
            // Ensure repair uses the daemon's resolved data directory for vectors.db
            try {
                repairConfig.dataPath = serviceManager_->getResolvedDataDir();
            } catch (...) {
            }

            auto stats = yams::repair::repairMissingEmbeddings(
                contentStore, metadataRepo, embeddingGenerator, repairConfig, req.documentHashes,
                nullptr, // progress callback
                contentExtractors);

            co_return std::optional<yams::Result<yams::repair::EmbeddingRepairStats>>{
                std::move(stats)};
        },
        boost::asio::use_awaitable);

    if (signal) {
        signal(false);
    }

    if (!result) {
        co_return ErrorResponse{ErrorCode::InternalError, "Embedding repair failed"};
    }

    if (!result->has_value()) {
        co_return ErrorResponse{result->error().code, result->error().message};
    }

    const auto& stats = result->value();

    EmbedDocumentsResponse resp;
    resp.requested = stats.documentsProcessed;
    resp.embedded = stats.embeddingsGenerated;
    resp.skipped = stats.embeddingsSkipped;
    resp.failed = stats.failedOperations;
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleAddDocumentRequest(const AddDocumentRequest& req) {
    try {
        // Check if this is a directory operation
        if (req.recursive && !req.path.empty() && std::filesystem::is_directory(req.path)) {
            // Use IndexingService for directory operations
            auto indexingService =
                app::services::makeIndexingService(serviceManager_->getAppContext());

            // Convert daemon AddDocumentRequest to service AddDirectoryRequest
            app::services::AddDirectoryRequest serviceReq;
            serviceReq.directoryPath = req.path;
            serviceReq.collection = req.collection;
            serviceReq.includePatterns = req.includePatterns;
            serviceReq.excludePatterns = req.excludePatterns;
            serviceReq.recursive = req.recursive;
            // Convert std::map to std::unordered_map
            for (const auto& [key, value] : req.metadata) {
                serviceReq.metadata[key] = value;
            }

            auto result = indexingService->addDirectory(serviceReq);
            if (!result) {
                co_return ErrorResponse{result.error().code, result.error().message};
            }

            const auto& serviceResp = result.value();

            // Map app::services::AddDirectoryResponse to daemon AddDocumentResponse
            // Use filesIndexed (successfully added) rather than filesProcessed for accuracy
            AddDocumentResponse response;
            response.hash = ""; // Directory operations don't have a single hash
            response.path = req.path;
            response.documentsAdded = static_cast<size_t>(serviceResp.filesIndexed);

            // Note: For directory operations, individual files are notified via the indexing
            // service No need to notify here as it would be redundant
            // Optionally trigger embeddings for all added files when enabled
            try {
                if (!req.noEmbeddings && serviceManager_ &&
                    serviceManager_->isEmbeddingsAutoOnAdd()) {
                    std::vector<std::string> hashes;
                    hashes.reserve(serviceResp.results.size());
                    for (const auto& r : serviceResp.results) {
                        if (r.success && !r.hash.empty())
                            hashes.push_back(r.hash);
                    }
                    if (!hashes.empty()) {
                        // Auto-embed policy via TuneAdvisor (no env dependency)
                        auto shouldAuto = [this]() -> bool {
                            using TA = yams::daemon::TuneAdvisor;
                            try {
                                auto pol = TA::autoEmbedPolicy();
                                if (pol == TA::AutoEmbedPolicy::Never)
                                    return false;
                                if (pol == TA::AutoEmbedPolicy::Always)
                                    return true;
                                if (metrics_) {
                                    auto snap = metrics_->getSnapshot();
                                    if (snap.activeConnections > 0)
                                        return false;
                                    if (snap.cpuUsagePercent > TA::cpuIdleThresholdPercent())
                                        return false;
                                    if (snap.muxQueuedBytes >
                                        static_cast<int64_t>(TA::muxBacklogHighBytes() / 8))
                                        return false; // conservative
                                    return true;
                                }
                            } catch (...) {
                            }
                            return false;
                        }();

                        if (!shouldAuto) {
                            // Defer to RepairCoordinator by notifying daemon about each doc
                            if (daemon_) {
                                for (const auto& h : hashes) {
                                    daemon_->onDocumentAdded(h, "");
                                }
                            }
                        } else {
                            auto ex = getWorkerExecutor();
                            auto signal = getWorkerJobSignal();
                            if (signal)
                                signal(true);
                            auto* sm = this->serviceManager_;
                            boost::asio::dispatch(ex, [sm, hs = std::move(hashes),
                                                       signal]() mutable {
                                try {
                                    if (sm) {
                                        auto store = sm->getContentStore();
                                        auto meta = sm->getMetadataRepo();
                                        auto dataDir = sm->getConfig().dataDir;
                                        yams::vector::EmbeddingService esvc(store, meta, dataDir);
                                        (void)esvc.generateEmbeddingsForDocuments(hs);
                                    }
                                } catch (...) {
                                }
                                if (signal)
                                    signal(false);
                            });
                        }
                    }
                }
            } catch (...) {
            }
            co_return response;
        } else {
            // Use DocumentService for single file operations
            auto documentService =
                app::services::makeDocumentService(serviceManager_->getAppContext());

            // Convert daemon AddDocumentRequest to service StoreDocumentRequest
            app::services::StoreDocumentRequest serviceReq;
            serviceReq.path = req.path;
            serviceReq.content = req.content;
            serviceReq.name = req.name;
            serviceReq.mimeType = req.mimeType;
            serviceReq.disableAutoMime = req.disableAutoMime;
            serviceReq.tags = req.tags;
            // Convert std::map to std::unordered_map
            for (const auto& [key, value] : req.metadata) {
                serviceReq.metadata[key] = value;
            }
            serviceReq.collection = req.collection;
            serviceReq.snapshotId = req.snapshotId;
            serviceReq.snapshotLabel = req.snapshotLabel;
            serviceReq.noEmbeddings = req.noEmbeddings;

            auto t0 = std::chrono::steady_clock::now();
            auto result = documentService->store(serviceReq);
            if (!result) {
                co_return ErrorResponse{result.error().code, result.error().message};
            }
            auto t1 = std::chrono::steady_clock::now();
            const auto& serviceResp = result.value();

            // Map app::services::StoreDocumentResponse to daemon AddDocumentResponse
            AddDocumentResponse response;
            response.hash = serviceResp.hash;
            response.path = req.path.empty() ? req.name : req.path;
            response.documentsAdded = 1; // Service returns single document info

            // Notify daemon about the added document for repair coordination
            if (daemon_ && !serviceResp.hash.empty()) {
                daemon_->onDocumentAdded(serviceResp.hash, response.path);
            }

            // Optionally trigger embeddings automatically after indexing
            try {
                if (!req.noEmbeddings && serviceManager_ &&
                    serviceManager_->isEmbeddingsAutoOnAdd() && !response.hash.empty()) {
                    // Auto-embed policy via TuneAdvisor (no env dependency)
                    auto shouldAuto = [this]() -> bool {
                        using TA = yams::daemon::TuneAdvisor;
                        try {
                            auto pol = TA::autoEmbedPolicy();
                            if (pol == TA::AutoEmbedPolicy::Never)
                                return false;
                            if (pol == TA::AutoEmbedPolicy::Always)
                                return true;
                            if (metrics_) {
                                auto snap = metrics_->getSnapshot();
                                if (snap.activeConnections > 0)
                                    return false;
                                if (snap.cpuUsagePercent > TA::cpuIdleThresholdPercent())
                                    return false;
                                if (snap.muxQueuedBytes >
                                    static_cast<int64_t>(TA::muxBacklogHighBytes() / 8))
                                    return false;
                                return true;
                            }
                        } catch (...) {
                        }
                        return false;
                    }();

                    if (!shouldAuto) {
                        if (daemon_)
                            daemon_->onDocumentAdded(response.hash, response.path);
                    } else {
                        auto ex = getWorkerExecutor();
                        auto signal = getWorkerJobSignal();
                        if (signal)
                            signal(true);
                        auto* sm = this->serviceManager_;
                        boost::asio::dispatch(ex, [sm, h = response.hash, signal]() {
                            try {
                                if (sm) {
                                    auto store = sm->getContentStore();
                                    auto meta = sm->getMetadataRepo();
                                    auto dataDir = sm->getConfig().dataDir;
                                    yams::vector::EmbeddingService esvc(store, meta, dataDir);
                                    (void)esvc.generateEmbeddingsForDocuments(
                                        std::vector<std::string>{h});
                                }
                            } catch (...) {
                            }
                            if (signal)
                                signal(false);
                        });
                    }
                }
            } catch (...) {
            }

            // Diagnostics: per-stage timing for singleton AddDocument
            try {
                auto ms_store =
                    std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
                spdlog::info("AddDocument(store): hash={} name='{}' size={}B mime='{}' store_ms={}",
                             serviceResp.hash, response.path,
                             static_cast<long long>(serviceReq.content.size()), serviceReq.mimeType,
                             static_cast<long long>(ms_store));
            } catch (...) {
            }

            co_return response;
        }

    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("AddDocument request failed: ") + e.what()};
    }
}

Result<std::pair<std::string, std::string>>
RequestDispatcher::handleSingleFileAdd(const std::filesystem::path& filePath,
                                       const AddDocumentRequest& req,
                                       std::shared_ptr<api::IContentStore> contentStore,
                                       std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    // Build content metadata
    api::ContentMetadata metadata;
    metadata.mimeType = req.mimeType;

    // Auto-detect MIME type if not provided and not disabled
    if (metadata.mimeType.empty() && !req.disableAutoMime) {
        // Initialize detector (best-effort)
        (void)yams::detection::FileTypeDetector::initializeWithMagicNumbers();
        auto& det = yams::detection::FileTypeDetector::instance();
        std::string detected;
        if (auto sig = det.detectFromFile(filePath)) {
            detected = sig.value().mimeType;
        }
        if (detected.empty()) {
            detected = yams::detection::FileTypeDetector::getMimeTypeFromExtension(
                filePath.extension().string());
        }
        if (!detected.empty())
            metadata.mimeType = detected;
    }

    // Store the file
    auto t_store0 = std::chrono::steady_clock::now();
    auto storeResult = contentStore->store(filePath, metadata);
    if (!storeResult) {
        return Error{storeResult.error().code, storeResult.error().message};
    }
    auto t_store1 = std::chrono::steady_clock::now();
    const auto& result = storeResult.value();

    // Store metadata in database
    metadata::DocumentInfo docInfo;
    docInfo.filePath = filePath.string();
    docInfo.fileName = req.name.empty() ? filePath.filename().string() : req.name;
    docInfo.fileExtension = filePath.extension().string();
    docInfo.fileSize = static_cast<int64_t>(result.bytesStored);
    docInfo.sha256Hash = result.contentHash;
    docInfo.mimeType = metadata.mimeType;

    auto now = std::chrono::system_clock::now();
    docInfo.createdTime = now;
    docInfo.modifiedTime = now;
    docInfo.indexedTime = now;

    // Check if document already exists (deduplication case)
    bool isNewDocument = true;
    int64_t docId = -1;

    if (result.dedupRatio() >= 0.99) { // Near 100% dedup
        auto existingDoc = metadataRepo->getDocumentByHash(result.contentHash);
        if (existingDoc && existingDoc.value().has_value()) {
            // Document already exists, update it
            isNewDocument = false;
            docId = existingDoc.value()->id;

            // Update indexed time
            auto doc = existingDoc.value().value();
            doc.indexedTime = now;
            metadataRepo->updateDocument(doc);
        }
    }

    // Insert new document if it doesn't exist
    bool did_insert = false;
    std::chrono::steady_clock::time_point t_db0;
    if (isNewDocument) {
        t_db0 = std::chrono::steady_clock::now();
        auto insertResult = metadataRepo->insertDocument(docInfo);
        if (insertResult) {
            docId = insertResult.value();
            did_insert = true;
        } else {
            return Error{insertResult.error().code,
                         "Failed to store metadata: " + insertResult.error().message};
        }
    }

    // Add tags and custom metadata
    if (docId > 0) {
        auto t_tags0 = std::chrono::steady_clock::now();
        addTagsAndMetadata(docId, req.tags, req.metadata, metadataRepo);
        auto t_tags1 = std::chrono::steady_clock::now();
        // Diagnostics summary
        try {
            auto ms_store =
                std::chrono::duration_cast<std::chrono::milliseconds>(t_store1 - t_store0).count();
            long long ms_db = 0;
            if (did_insert) {
                ms_db =
                    std::chrono::duration_cast<std::chrono::milliseconds>(t_tags0 - t_db0).count();
            }
            auto ms_tags =
                std::chrono::duration_cast<std::chrono::milliseconds>(t_tags1 - t_tags0).count();
            spdlog::info(
                "AddDocument(file): hash={} path='{}' mime='{}' store_ms={} db_ms={} tags_ms={}",
                result.contentHash, filePath.string(), metadata.mimeType,
                static_cast<long long>(ms_store), ms_db, static_cast<long long>(ms_tags));
        } catch (...) {
        }
    }

    return std::make_pair(result.contentHash, filePath.string());
}

Result<std::pair<std::string, std::string>>
RequestDispatcher::handleContentAdd(const AddDocumentRequest& req,
                                    std::shared_ptr<api::IContentStore> contentStore,
                                    std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    if (req.content.empty()) {
        return Error{ErrorCode::InvalidArgument, "Content cannot be empty"};
    }

    // Convert string content to bytes
    std::vector<std::byte> contentBytes;
    contentBytes.reserve(req.content.size());
    for (char c : req.content) {
        contentBytes.push_back(static_cast<std::byte>(c));
    }

    // Build metadata
    api::ContentMetadata metadata;
    metadata.mimeType = req.mimeType;
    if (metadata.mimeType.empty() && !req.disableAutoMime) {
        metadata.mimeType = "text/plain"; // Default for content-based adds
    }

    // Store the content
    auto storeResult = contentStore->storeBytes(contentBytes, metadata);
    if (!storeResult) {
        return Error{storeResult.error().code, storeResult.error().message};
    }

    const auto& result = storeResult.value();

    // Store metadata in database
    metadata::DocumentInfo docInfo;
    docInfo.filePath = req.name.empty() ? "content" : req.name;
    docInfo.fileName = req.name.empty() ? "content" : req.name;
    docInfo.fileExtension = "";
    docInfo.fileSize = static_cast<int64_t>(req.content.size());
    docInfo.sha256Hash = result.contentHash;
    docInfo.mimeType = metadata.mimeType;

    auto now = std::chrono::system_clock::now();
    docInfo.createdTime = now;
    docInfo.modifiedTime = now;
    docInfo.indexedTime = now;

    auto insertResult = metadataRepo->insertDocument(docInfo);
    if (!insertResult) {
        return Error{insertResult.error().code,
                     "Failed to store metadata: " + insertResult.error().message};
    }

    int64_t docId = insertResult.value();

    // Add tags and custom metadata
    addTagsAndMetadata(docId, req.tags, req.metadata, metadataRepo);

    return std::make_pair(result.contentHash, docInfo.filePath);
}

Result<size_t>
RequestDispatcher::handleDirectoryAdd(const std::filesystem::path& dirPath,
                                      const AddDocumentRequest& req,
                                      std::shared_ptr<api::IContentStore> contentStore,
                                      std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    // Collect files first to enable parallel processing and deterministic counts
    std::vector<std::filesystem::path> files;
    try {
        auto opts = std::filesystem::directory_options::skip_permission_denied;
        if (req.recursive) {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(dirPath, opts)) {
                if (entry.is_regular_file()) {
                    files.push_back(entry.path());
                }
            }
        } else {
            for (const auto& entry : std::filesystem::directory_iterator(dirPath, opts)) {
                if (entry.is_regular_file()) {
                    files.push_back(entry.path());
                }
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return Error{ErrorCode::IOError,
                     "Filesystem error during directory traversal: " + std::string(e.what())};
    }

    if (files.empty()) {
        return static_cast<size_t>(0);
    }

    // Very simple glob-like filter for include/exclude patterns (*)
    auto matches_any = [](const std::string& text, const std::vector<std::string>& patterns) {
        if (patterns.empty())
            return true; // no include patterns => include all
        for (const auto& pat : patterns) {
            // Convert '*' to '.*' regex; escape dots
            std::string rx;
            rx.reserve(pat.size() * 2);
            for (char c : pat) {
                if (c == '*')
                    rx += ".*";
                else if (c == '.')
                    rx += "\\.";
                else
                    rx += c;
            }
            try {
                if (std::regex_match(text, std::regex(rx)))
                    return true;
            } catch (...) {
                // On invalid pattern, skip
            }
        }
        return false;
    };

    auto matches_none = [&](const std::string& text, const std::vector<std::string>& patterns) {
        for (const auto& pat : patterns) {
            std::string rx;
            rx.reserve(pat.size() * 2);
            for (char c : pat) {
                if (c == '*')
                    rx += ".*";
                else if (c == '.')
                    rx += "\\.";
                else
                    rx += c;
            }
            try {
                if (std::regex_match(text, std::regex(rx)))
                    return false;
            } catch (...) {
            }
        }
        return true;
    };

    // Filter files based on patterns
    std::vector<std::filesystem::path> filtered;
    filtered.reserve(files.size());
    for (const auto& p : files) {
        const std::string name = p.filename().string();
        if (!matches_any(name, req.includePatterns))
            continue; // if includePatterns present
        if (!matches_none(name, req.excludePatterns))
            continue; // excluded
        filtered.push_back(p);
    }

    if (filtered.empty()) {
        return static_cast<size_t>(0);
    }

    // Parallel add with a simple worker pool
    const unsigned hw = std::max(1u, std::thread::hardware_concurrency());
    const unsigned workers = std::min<unsigned>(hw, 8); // cap to avoid I/O saturation

    std::atomic<size_t> index{0};
    std::atomic<size_t> added{0};

    auto worker = [&]() {
        for (;;) {
            size_t i = index.fetch_add(1);
            if (i >= filtered.size())
                break;
            const auto& path = filtered[i];

            AddDocumentRequest fileReq = req;
            fileReq.path = path.string();
            fileReq.content.clear();

            auto res = handleSingleFileAdd(path, fileReq, contentStore, metadataRepo);
            if (res) {
                added.fetch_add(1);
            } else {
                spdlog::warn("Failed to add file {}: {}", path.string(), res.error().message);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(workers);
    for (unsigned i = 0; i < workers; ++i) {
        threads.emplace_back(worker);
    }
    for (auto& t : threads)
        t.join();

    return static_cast<size_t>(added.load());
}

void RequestDispatcher::addTagsAndMetadata(
    int64_t docId, const std::vector<std::string>& tags,
    const std::map<std::string, std::string>& metadata,
    std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    // Add tags
    for (const auto& tagStr : tags) {
        // Each element might contain comma-separated tags
        std::stringstream ss(tagStr);
        std::string tag;
        while (std::getline(ss, tag, ',')) {
            // Trim whitespace
            tag.erase(0, tag.find_first_not_of(" \t"));
            tag.erase(tag.find_last_not_of(" \t") + 1);
            if (!tag.empty()) {
                metadataRepo->setMetadata(docId, "tag", metadata::MetadataValue(tag));
            }
        }
    }

    // Add custom metadata
    for (const auto& [key, value] : metadata) {
        metadataRepo->setMetadata(docId, key, metadata::MetadataValue(value));
    }
}

boost::asio::awaitable<Response> RequestDispatcher::handleListRequest(const ListRequest& req) {
    try {
        // Use app services for list with full feature parity
        auto appContext = serviceManager_->getAppContext();
        auto docService = app::services::makeDocumentService(appContext);

        // Map daemon ListRequest to app::services::ListDocumentsRequest
        // Full mapping of all protocol fields for PBI-001 compliance
        app::services::ListDocumentsRequest serviceReq;

        // Basic pagination and sorting
        serviceReq.limit = req.limit;
        serviceReq.offset = req.offset;
        if (req.recentCount > 0) {
            serviceReq.recent = req.recentCount;
        } else if (req.recent) {
            serviceReq.recent = req.limit;
        }

        // Format and display options
        serviceReq.format = req.format;
        serviceReq.sortBy = req.sortBy;
        serviceReq.reverse = req.reverse;
        serviceReq.verbose = req.verbose;
        serviceReq.showSnippets = req.showSnippets && !req.noSnippets;
        serviceReq.snippetLength = req.snippetLength;
        serviceReq.showMetadata = req.showMetadata;
        serviceReq.showTags = req.showTags;
        serviceReq.groupBySession = req.groupBySession;
        // Propagate paths-only hint and minimize hydration when requested
        serviceReq.pathsOnly = req.pathsOnly;
        if (req.pathsOnly) {
            serviceReq.showSnippets = false;
            serviceReq.showMetadata = false;
            serviceReq.showTags = false;
        }

        // File type filters
        serviceReq.type = req.fileType;
        serviceReq.mime = req.mimeType;
        serviceReq.extension = req.extensions;
        serviceReq.binary = req.binaryOnly;
        serviceReq.text = req.textOnly;

        // Time filters
        serviceReq.createdAfter = req.createdAfter;
        serviceReq.createdBefore = req.createdBefore;
        serviceReq.modifiedAfter = req.modifiedAfter;
        serviceReq.modifiedBefore = req.modifiedBefore;
        serviceReq.indexedAfter = req.indexedAfter;
        serviceReq.indexedBefore = req.indexedBefore;

        // Change tracking
        serviceReq.changes = req.showChanges;
        serviceReq.since = req.sinceTime;
        serviceReq.diffTags = req.showDiffTags;
        serviceReq.showDeleted = req.showDeleted;
        serviceReq.changeWindow = req.changeWindow;

        // Tag filtering - combine both tag sources
        serviceReq.tags = req.tags;
        if (!req.filterTags.empty()) {
            // Parse comma-separated filterTags and merge with tags vector
            std::istringstream ss(req.filterTags);
            std::string tag;
            while (std::getline(ss, tag, ',')) {
                // Trim whitespace
                tag.erase(0, tag.find_first_not_of(" \t"));
                tag.erase(tag.find_last_not_of(" \t") + 1);
                if (!tag.empty()) {
                    serviceReq.tags.push_back(tag);
                }
            }
        }
        serviceReq.matchAllTags = req.matchAllTags;

        // Name pattern filtering
        serviceReq.pattern = req.namePattern;

        auto result = docService->list(serviceReq);
        if (!result) {
            co_return ErrorResponse{result.error().code, result.error().message};
        }

        const auto& serviceResp = result.value();

        // Map app::services::ListDocumentsResponse to enhanced daemon ListResponse
        ListResponse response;
        response.items.reserve(serviceResp.documents.size());

        for (const auto& doc : serviceResp.documents) {
            ListEntry item;

            // Basic file information
            item.hash = doc.hash;
            item.path = doc.path;
            item.name = doc.name;
            item.fileName = doc.fileName;
            item.size = doc.size;

            // File type and format information
            item.mimeType = doc.mimeType;
            item.fileType = doc.fileType;
            item.extension = doc.extension;

            // Timestamps
            item.created = doc.created;
            item.modified = doc.modified;
            item.indexed = doc.indexed;

            // Content and metadata
            if (doc.snippet) {
                item.snippet = doc.snippet.value();
            }
            item.tags = doc.tags;
            // Convert unordered_map to map for protocol compatibility
            for (const auto& [key, value] : doc.metadata) {
                item.metadata[key] = value;
            }

            // Change tracking info
            if (doc.changeType) {
                item.changeType = doc.changeType.value();
            }
            if (doc.changeTime) {
                item.changeTime = doc.changeTime.value();
            }

            // Display helpers
            item.relevanceScore = doc.relevanceScore;
            if (doc.matchReason) {
                item.matchReason = doc.matchReason.value();
            }

            response.items.push_back(std::move(item));
        }

        response.totalCount = serviceResp.totalFound;

        co_return response;

    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("List request failed: ") + e.what()};
    }
}

boost::asio::awaitable<Response> RequestDispatcher::handleDeleteRequest(const DeleteRequest& req) {
    try {
        // Use DocumentService for business logic
        auto documentService = app::services::makeDocumentService(serviceManager_->getAppContext());

        // Convert daemon DeleteRequest to service DeleteByNameRequest
        app::services::DeleteByNameRequest serviceReq;

        // Map all fields from daemon request to service request
        serviceReq.hash = req.hash;
        serviceReq.name = req.name;
        serviceReq.names = req.names;
        serviceReq.pattern = req.pattern;
        serviceReq.dryRun = req.dryRun;
        serviceReq.force = req.force || req.purge; // Map purge to force for backward compat
        serviceReq.keepRefs = req.keepRefs;
        serviceReq.recursive = req.recursive;
        serviceReq.verbose = req.verbose;

        // Handle directory deletion
        if (!req.directory.empty()) {
            if (!req.recursive) {
                co_return ErrorResponse{ErrorCode::InvalidArgument,
                                        "Directory deletion requires recursive flag for safety"};
            }
            // Convert directory to pattern
            serviceReq.pattern = req.directory;
            if (!serviceReq.pattern.empty() && serviceReq.pattern.back() != '/') {
                serviceReq.pattern += '/';
            }
            serviceReq.pattern += "*";
        }

        auto result = documentService->deleteByName(serviceReq);
        if (!result) {
            co_return ErrorResponse{result.error().code, result.error().message};
        }

        const auto& serviceResp = result.value();

        // Build DeleteResponse from service response
        DeleteResponse response;
        response.dryRun = serviceResp.dryRun;
        response.successCount = 0;
        response.failureCount = 0;

        // Convert service results to daemon results
        for (const auto& deleteResult : serviceResp.deleted) {
            DeleteResponse::DeleteResult daemonResult;
            daemonResult.name = deleteResult.name;
            daemonResult.hash = deleteResult.hash;
            daemonResult.success = deleteResult.deleted;
            daemonResult.error = deleteResult.error.value_or("");

            if (daemonResult.success) {
                response.successCount++;
                // Notify daemon about the removed document for repair coordination
                if (daemon_ && !deleteResult.hash.empty()) {
                    daemon_->onDocumentRemoved(deleteResult.hash);
                }
            } else {
                response.failureCount++;
            }

            response.results.push_back(daemonResult);
        }

        // Add error results
        for (const auto& errorResult : serviceResp.errors) {
            DeleteResponse::DeleteResult daemonResult;
            daemonResult.name = errorResult.name;
            daemonResult.hash = errorResult.hash;
            daemonResult.success = false;
            daemonResult.error = errorResult.error.value_or("Unknown error");

            response.failureCount++;
            response.results.push_back(daemonResult);
        }

        // If no results at all, return error
        if (response.results.empty() && !response.dryRun) {
            co_return ErrorResponse{ErrorCode::NotFound, "No documents found matching criteria"};
        }

        co_return response;

    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("Delete request failed: ") + e.what()};
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGetStatsRequest(const GetStatsRequest& req) {
    // Early fast-path: if lifecycle not ready/degraded, return minimal not_ready stats
    {
        auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
        if (lifecycleSnapshot.state != LifecycleState::Ready &&
            lifecycleSnapshot.state != LifecycleState::Degraded) {
            GetStatsResponse response;
            response.totalDocuments = 0;
            response.totalSize = 0;
            response.indexedDocuments = 0;
            response.vectorIndexSize = 0;
            response.compressionRatio = 0.0;
            response.additionalStats["not_ready"] = "true";
            try {
                response.additionalStats["daemon_version"] = YAMS_VERSION_STRING;
            } catch (...) {
            }
            try {
                auto uptime = std::chrono::steady_clock::now() - state_->stats.startTime;
                auto uptimeSeconds =
                    std::chrono::duration_cast<std::chrono::seconds>(uptime).count();
                response.additionalStats["daemon_uptime"] = std::to_string(uptimeSeconds);
            } catch (...) {
            }
            co_return response;
        }
    }
    try {
        auto contentStore = serviceManager_->getContentStore();
        auto metadataRepo = serviceManager_->getMetadataRepo();

        if (!contentStore || !metadataRepo) {
            GetStatsResponse response;
            response.totalDocuments = 0;
            response.totalSize = 0;
            response.indexedDocuments = 0;
            response.vectorIndexSize = 0;
            response.compressionRatio = 0.0;
            // Explicit readiness indicator for consumers
            response.additionalStats["not_ready"] = "true";
            // Service availability hints
            response.additionalStats["service_contentstore"] =
                contentStore ? "running" : "unavailable";
            response.additionalStats["service_metadatarepo"] =
                metadataRepo ? "running" : "unavailable";
            // Basic daemon info where available (non-blocking)
            try {
                response.additionalStats["daemon_version"] = YAMS_VERSION_STRING;
            } catch (...) {
            }
            try {
                auto uptime = std::chrono::steady_clock::now() - state_->stats.startTime;
                auto uptimeSeconds =
                    std::chrono::duration_cast<std::chrono::seconds>(uptime).count();
                response.additionalStats["daemon_uptime"] = std::to_string(uptimeSeconds);
            } catch (...) {
            }
            co_return response;
        }

        // Get content store statistics (authoritative on stored content)
        auto storeStats = contentStore->getStats();

        // Get metadata repository statistics using available methods (may lag behind store)
        size_t metaDocCount = 0;
        if (auto docCountResult = metadataRepo->getDocumentCount(); docCountResult) {
            metaDocCount = static_cast<size_t>(docCountResult.value());
        } else {
            // Surface error via additionalStats but continue with store-derived counts
            // so stats remain useful even if metadata is initializing
            // Note: additionalStats populated later when available
        }

        auto indexedCountResult = metadataRepo->getIndexedDocumentCount();
        size_t indexedCount = indexedCountResult ? indexedCountResult.value() : 0;

        // Create stats response. Prefer content store object count when available; fall back to
        // metadata.
        GetStatsResponse response;
        // Explicit readiness indicator for consumers (CLI can avoid showing zeros blindly)
        try {
            response.additionalStats["ready"] = state_->readiness.fullyReady() ? "true" : "false";
        } catch (...) {
        }
        const size_t storeDocCount = static_cast<size_t>(storeStats.totalObjects);
        response.totalDocuments = (storeDocCount > 0) ? storeDocCount : metaDocCount;
        response.totalSize = storeStats.totalBytes;

        // Get vector database stats from DaemonMetrics and resolved data dir (no legacy fallbacks)
        size_t vectorDbSize = 0;
        size_t vectorRowsExact = 0;
        try {
            auto snap = metrics_->getSnapshot();
            vectorDbSize = snap.vectorDbSizeBytes;
            vectorRowsExact = snap.vectorRowsExact;
        } catch (...) {
        }

        // Try to get exact vector row count using VectorDatabase when possible (always),
        // and open a temporary handle only in detailed mode.
        try {
            auto vdb = serviceManager_->getVectorDatabase();
            if (vdb && vdb->isInitialized()) {
                vectorRowsExact = vdb->getVectorCount();
            } else if (req.detailed) {
                // Fallback: resolved data dir only
                try {
                    auto dd = serviceManager_->getResolvedDataDir();
                    if (!dd.empty()) {
                        auto p = dd / "vectors.db";
                        if (std::filesystem::exists(p)) {
                            yams::vector::VectorDatabaseConfig cfg;
                            cfg.database_path = p.string();
                            auto tmp = std::make_unique<yams::vector::VectorDatabase>(cfg);
                            if (tmp->initialize())
                                vectorRowsExact = tmp->getVectorCount();
                        }
                    }
                } catch (...) {
                }
            }
        } catch (...) {
            // best effort
        }

        // Annotate vectordb path using resolved data dir
        try {
            auto dd = serviceManager_->getResolvedDataDir();
            if (!dd.empty())
                response.additionalStats["vectordb_path"] = (dd / "vectors.db").string();
        } catch (...) {
        }

        // Indexed documents: use metadata/FTS count; keep vector counts separate
        size_t reportedIndexed = indexedCount;
        if (reportedIndexed > response.totalDocuments) {
            spdlog::warn(
                "Reported indexed (metadata) count ({}) is greater than total documents ({}). "
                "Capping to total for display.",
                reportedIndexed, response.totalDocuments);
            response.indexedDocuments = response.totalDocuments;
        } else {
            response.indexedDocuments = reportedIndexed;
        }
        response.vectorIndexSize = vectorDbSize;
        response.compressionRatio = storeStats.dedupRatio();

        // Surface exact vector row count when available
        if (vectorRowsExact > 0)
            response.additionalStats["vector_rows"] = std::to_string(vectorRowsExact);

        // Extraction progress indicators
        if (response.totalDocuments >= response.indexedDocuments) {
            size_t pending = response.totalDocuments - response.indexedDocuments;
            response.additionalStats["extraction_pending"] = std::to_string(pending);
            response.additionalStats["documents_total"] = std::to_string(response.totalDocuments);
            response.additionalStats["documents_extracted"] =
                std::to_string(response.indexedDocuments);
        }
        // Precise counts by extraction status (if available)
        try {
            auto pendingCount = metadataRepo->getDocumentCountByExtractionStatus(
                metadata::ExtractionStatus::Pending);
            if (pendingCount) {
                response.additionalStats["extraction_queue_size"] =
                    std::to_string(pendingCount.value());
            }
            auto failedCount = metadataRepo->getDocumentCountByExtractionStatus(
                metadata::ExtractionStatus::Failed);
            if (failedCount) {
                response.additionalStats["extraction_failed_count"] =
                    std::to_string(failedCount.value());
            }
        } catch (...) {
            // best effort
        }

        // Vector/embedding service status
        try {
            auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
            if (lifecycleSnapshot.state != LifecycleState::Ready &&
                lifecycleSnapshot.state != LifecycleState::Degraded) {
                response.additionalStats["service_embeddingservice"] = "unavailable";
                response.additionalStats["onnx_models_loaded"] = "0";
            } else {
                // Centralized: DaemonMetrics
                auto einfo = metrics_->getEmbeddingServiceInfo();
                response.additionalStats["service_embeddingservice"] =
                    einfo.available ? "available" : "unavailable";
                response.additionalStats["onnx_models_loaded"] =
                    std::to_string(std::max(0, einfo.modelsLoaded));
                response.additionalStats["onnx_runtime"] =
                    einfo.onnxRuntimeEnabled ? "enabled" : "disabled";
            }
            // Preferred model and local presence hint
            try {
                std::string preferred;
                if (const char* env = std::getenv("YAMS_PREFERRED_MODEL"))
                    preferred = env;
                if (preferred.empty()) {
                    const char* home = std::getenv("HOME");
                    if (home) {
                        namespace fs = std::filesystem;
                        fs::path base = fs::path(home) / ".yams" / "models";
                        std::error_code ec;
                        if (fs::exists(base, ec) && fs::is_directory(base, ec)) {
                            for (const auto& e : fs::directory_iterator(base, ec)) {
                                if (!e.is_directory())
                                    continue;
                                if (fs::exists(e.path() / "model.onnx", ec)) {
                                    preferred = e.path().filename().string();
                                    break;
                                }
                            }
                        }
                    }
                }
                if (!preferred.empty()) {
                    response.additionalStats["preferred_model"] = preferred;
                    bool exists = false;
                    try {
                        const char* home = std::getenv("HOME");
                        if (home) {
                            namespace fs = std::filesystem;
                            exists = fs::exists(fs::path(home) / ".yams" / "models" / preferred /
                                                "model.onnx");
                        }
                    } catch (...) {
                    }
                    response.additionalStats["preferred_model_path_exists"] =
                        exists ? "true" : "false";
                }
            } catch (...) {
            }
        } catch (...) {
            response.additionalStats["service_embeddingservice"] = "error";
        }

        // Always include basic additional stats and centralized service flags
        response.additionalStats["store_objects"] = std::to_string(storeStats.totalObjects);
        response.additionalStats["unique_blocks"] = std::to_string(storeStats.uniqueBlocks);
        response.additionalStats["deduplicated_bytes"] =
            std::to_string(storeStats.deduplicatedBytes);
        try {
            auto msnap = metrics_->getSnapshot();
            if (!msnap.serviceContentStore.empty())
                response.additionalStats["service_contentstore"] = msnap.serviceContentStore;
            if (!msnap.serviceMetadataRepo.empty())
                response.additionalStats["service_metadatarepo"] = msnap.serviceMetadataRepo;
            if (!msnap.serviceSearchExecutor.empty())
                response.additionalStats["service_searchexecutor"] = msnap.serviceSearchExecutor;
            if (!msnap.searchExecutorReason.empty())
                response.additionalStats["searchexecutor_reason"] = msnap.searchExecutorReason;
        } catch (...) {
        }

        // Expose dynamic batching metrics for embeddings
        try {
            const auto& bm = yams::vector::batchmetrics::get();
            response.additionalStats["embed_batch_effective_tokens"] =
                std::to_string(bm.effectiveTokens.load());
            response.additionalStats["embed_batch_recent_avg_docs"] =
                std::to_string(bm.recentAvgDocs.load());
            response.additionalStats["embed_batch_successes"] = std::to_string(bm.successes.load());
            response.additionalStats["embed_batch_failures"] = std::to_string(bm.failures.load());
            response.additionalStats["embed_batch_backoffs"] = std::to_string(bm.backoffs.load());
        } catch (...) {
            // best effort only
        }

        // Expose RepairCoordinator metrics for observability
        response.additionalStats["repair_idle_ticks"] =
            std::to_string(state_->stats.repairIdleTicks.load());
        response.additionalStats["repair_busy_ticks"] =
            std::to_string(state_->stats.repairBusyTicks.load());
        response.additionalStats["repair_batches_attempted"] =
            std::to_string(state_->stats.repairBatchesAttempted.load());
        response.additionalStats["repair_embeddings_generated"] =
            std::to_string(state_->stats.repairEmbeddingsGenerated.load());
        response.additionalStats["repair_embeddings_skipped"] =
            std::to_string(state_->stats.repairEmbeddingsSkipped.load());
        response.additionalStats["repair_failed_operations"] =
            std::to_string(state_->stats.repairFailedOperations.load());

        // Calculate block metrics (informational only)
        double blockToDocRatio =
            response.totalDocuments > 0
                ? static_cast<double>(storeStats.uniqueBlocks) / response.totalDocuments
                : 0.0;
        response.additionalStats["block_to_doc_ratio"] = std::to_string(blockToDocRatio);

        // Calculate average block size for informational purposes
        double avgBlockSize =
            storeStats.uniqueBlocks > 0
                ? static_cast<double>(storeStats.totalBytes) / storeStats.uniqueBlocks
                : 0.0;
        response.additionalStats["avg_block_size"] =
            std::to_string(static_cast<size_t>(avgBlockSize));

        // Deduplication effectiveness (how much space we're saving)
        if (storeStats.deduplicatedBytes > 0) {
            double dedupSavings = static_cast<double>(storeStats.deduplicatedBytes) /
                                  (storeStats.totalBytes + storeStats.deduplicatedBytes) * 100.0;
            response.additionalStats["dedup_savings_percent"] = std::to_string(dedupSavings);
        }

        if (req.detailed) {
            // Add detailed statistics if requested
            response.additionalStats["store_bytes"] = std::to_string(storeStats.totalBytes);
            response.additionalStats["store_operations"] =
                std::to_string(storeStats.storeOperations);
            response.additionalStats["retrieve_operations"] =
                std::to_string(storeStats.retrieveOperations);
            response.additionalStats["delete_operations"] =
                std::to_string(storeStats.deleteOperations);

            // Add metadata repo statistics using available methods
            response.additionalStats["meta_documents"] = std::to_string(metaDocCount);
            response.additionalStats["meta_indexed_documents"] = std::to_string(indexedCount);
        }

        // Always include currently loaded plugins (fast path; no directory scans)
        try {
            nlohmann::json pj = nlohmann::json::array();
            size_t count = 0;
            if (auto abi = serviceManager_->getAbiPluginHost()) {
                auto descs = abi->listLoaded();
                count += descs.size();
                for (const auto& d : descs) {
                    nlohmann::json rec;
                    rec["name"] = d.name;
                    rec["path"] = d.path.string();
                    if (!d.interfaces.empty())
                        rec["interfaces"] = d.interfaces;
                    // Enrich adopted provider with degraded/error details and health when available
                    try {
                        if (serviceManager_) {
                            if (!serviceManager_->adoptedProviderPluginName().empty() &&
                                serviceManager_->adoptedProviderPluginName() == d.name) {
                                rec["provider"] = true;
                                if (auto mp = serviceManager_->getModelProvider()) {
                                    rec["models_loaded"] = mp->getLoadedModels().size();
                                }
                                if (serviceManager_->isModelProviderDegraded()) {
                                    rec["degraded"] = true;
                                    if (!serviceManager_->lastModelError().empty())
                                        rec["error"] = serviceManager_->lastModelError();
                                }
                            }
                        }
                        // Health best-effort
                        try {
                            if (auto hr = abi->health(d.name))
                                rec["health"] = hr.value();
                        } catch (...) {
                        }
                    } catch (...) {
                    }
                    pj.push_back(rec);
                }
            } else {
                auto plugins = serviceManager_->getLoadedPlugins();
                count = plugins.size();
                for (const auto& pi : plugins) {
                    nlohmann::json rec;
                    rec["name"] = pi.name;
                    rec["path"] = pi.path.string();
                    pj.push_back(rec);
                }
            }
            response.additionalStats["plugins_loaded"] = std::to_string(count);
            response.additionalStats["plugins_json"] = pj.dump();
        } catch (const std::exception& ex) {
            // Fail-safe: ensure keys exist even on error
            response.additionalStats["plugins_loaded"] = "0";
            response.additionalStats["plugins_json"] = "[]";
            response.additionalStats["plugins_error"] = ex.what();
        } catch (...) {
            response.additionalStats["plugins_loaded"] = "0";
            response.additionalStats["plugins_json"] = "[]";
            response.additionalStats["plugins_error"] = "unknown error";
        }

        // Defensive fallback: if content store reports zero but configured storage exists,
        // scan objects directory to estimate counts and total size.
        // Cap the scan to avoid blocking status requests on large stores.
        if (req.detailed && (response.totalDocuments == 0 || response.totalSize == 0)) {
            try {
                auto cfg = serviceManager_->getConfig();
                std::filesystem::path objectsDir = cfg.dataDir / "storage" / "objects";
                if (std::filesystem::exists(objectsDir)) {
                    const uint64_t maxFiles = 1000; // hard cap to avoid long scans
                    const auto maxDuration = std::chrono::milliseconds(150);
                    const auto startScan = std::chrono::steady_clock::now();

                    uint64_t count = 0;
                    uint64_t bytes = 0;
                    for (auto it = std::filesystem::recursive_directory_iterator(objectsDir);
                         it != std::filesystem::recursive_directory_iterator(); ++it) {
                        if (it->is_regular_file()) {
                            ++count;
                            std::error_code ec;
                            bytes += std::filesystem::file_size(it->path(), ec);
                            if (count >= maxFiles)
                                break;
                        }
                        if (std::chrono::steady_clock::now() - startScan > maxDuration) {
                            break;
                        }
                    }
                    if (count > 0) {
                        response.totalDocuments = std::max<size_t>(response.totalDocuments, count);
                        response.totalSize = std::max<size_t>(response.totalSize, bytes);
                    }
                }
            } catch (...) {
                // best effort only
            }
        }

        // File type breakdown if requested
        if (req.showFileTypes) {
            auto extStatsResult = metadataRepo->getDocumentCountsByExtension();
            if (extStatsResult) {
                for (const auto& [ext, count] : extStatsResult.value()) {
                    response.documentsByType[ext] = static_cast<size_t>(count);
                }
            }
        }

        // Performance metrics if requested
        if (req.showPerformance) {
            response.additionalStats["performance_store_ops"] =
                std::to_string(storeStats.storeOperations);
            response.additionalStats["performance_retrieve_ops"] =
                std::to_string(storeStats.retrieveOperations);
            response.additionalStats["performance_delete_ops"] =
                std::to_string(storeStats.deleteOperations);

            // Add timing stats if available
            response.additionalStats["performance_avg_store_time"] = "0.0ms";    // TODO: implement
            response.additionalStats["performance_avg_retrieve_time"] = "0.0ms"; // TODO: implement
        }

        // Compression stats if requested
        if (req.showCompression) {
            response.additionalStats["compression_ratio"] =
                std::to_string(response.compressionRatio);
            response.additionalStats["compression_saved_bytes"] =
                std::to_string(storeStats.deduplicatedBytes);
            response.additionalStats["compression_algorithm"] =
                "zstd"; // TODO: get actual algorithm
        }

        // Duplicate analysis if requested
        if (req.showDuplicates) {
            // Estimate duplicates from deduplication info
            if (response.totalDocuments > 0 && storeStats.uniqueBlocks > 0) {
                size_t estimatedDupes = response.totalDocuments > storeStats.uniqueBlocks
                                            ? response.totalDocuments - storeStats.uniqueBlocks
                                            : 0;
                response.additionalStats["duplicates_estimated"] = std::to_string(estimatedDupes);
                response.additionalStats["duplicates_saved_bytes"] =
                    std::to_string(storeStats.deduplicatedBytes);
            }
        }

        // Block-level deduplication if requested
        if (req.showDedup) {
            response.additionalStats["dedup_unique_blocks"] =
                std::to_string(storeStats.uniqueBlocks);
            response.additionalStats["dedup_total_objects"] =
                std::to_string(storeStats.totalObjects);
            response.additionalStats["dedup_ratio"] = std::to_string(storeStats.dedupRatio());
            response.additionalStats["dedup_saved_space"] =
                std::to_string(storeStats.deduplicatedBytes);
        }

        // Health information if requested
        if (req.includeHealth) {
            auto healthStatus = contentStore->checkHealth();

            // Overall health based on actual storage health, not block counts
            response.additionalStats["health_status"] =
                healthStatus.isHealthy ? "healthy" : "unhealthy";
            response.additionalStats["health_message"] = healthStatus.status;

            // Block health is always healthy unless we detect actual corruption
            // Multiple blocks per document is normal due to chunking
            response.additionalStats["block_health"] = "healthy";

            if (!healthStatus.warnings.empty()) {
                response.additionalStats["health_warnings"] =
                    std::to_string(healthStatus.warnings.size());
            }

            // Add daemon version and uptime
            response.additionalStats["daemon_version"] = YAMS_VERSION_STRING;
            auto uptime = std::chrono::steady_clock::now() - state_->stats.startTime;
            auto uptimeSeconds = std::chrono::duration_cast<std::chrono::seconds>(uptime).count();
            response.additionalStats["daemon_uptime"] = std::to_string(uptimeSeconds);

            // Service-specific diagnostics
            response.additionalStats["service_contentstore"] =
                contentStore ? "running" : "unavailable";
            response.additionalStats["service_metadatarepo"] =
                metadataRepo ? "running" : "unavailable";

            // Expose configured data directory for clarity
            try {
                const auto& dd = serviceManager_->getResolvedDataDir();
                if (!dd.empty()) {
                    response.additionalStats["data_dir"] = dd.string();
                }
            } catch (...) {
                // ignore
            }

            // Check search executor with reason if unavailable
            {
                auto searchExecutor = serviceManager_->getSearchExecutor();
                if (searchExecutor) {
                    response.additionalStats["service_searchexecutor"] = "available";
                } else {
                    response.additionalStats["service_searchexecutor"] = "unavailable";
                    std::string reason;
                    if (!state_->readiness.databaseReady.load())
                        reason = "database_not_ready";
                    else if (!state_->readiness.metadataRepoReady.load())
                        reason = "metadata_repo_not_ready";
                    else
                        reason = "not_initialized";
                    response.additionalStats["searchexecutor_reason"] = reason;
                }
            }

            // Check vector database status
            try {
                response.additionalStats["service_vectordb"] =
                    response.vectorIndexSize > 0 ? "initialized" : "not_initialized";
                if (response.vectorIndexSize > 0) {
                    response.additionalStats["vectordb_documents"] =
                        std::to_string(response.indexedDocuments);
                    response.additionalStats["vectordb_size"] =
                        std::to_string(response.vectorIndexSize);
                    // provide vector DB path if known
                    try {
                        const auto& dd = serviceManager_->getResolvedDataDir();
                        if (!dd.empty()) {
                            response.additionalStats["vectordb_path"] =
                                (dd / "vectors.db").string();
                        }
                    } catch (...) {
                    }
                } else {
                    response.additionalStats["vectordb_status"] = "will_initialize_on_first_search";
                }
            } catch (const std::exception& e) {
                response.additionalStats["service_vectordb"] = "error";
                response.additionalStats["vectordb_error"] = e.what();
            }

            // Embedding / ONNX provider status (non-blocking; avoid locking provider internals)
            try {
                auto provider = serviceManager_->getModelProvider();
                auto generator = serviceManager_->getEmbeddingGenerator();
                // Set only if not already populated earlier with precise values
                if (response.additionalStats.find("service_embeddingservice") ==
                    response.additionalStats.end()) {
                    response.additionalStats["service_embeddingservice"] =
                        generator ? "available" : "unavailable";
                }
                if (response.additionalStats.find("onnx_models_loaded") ==
                    response.additionalStats.end()) {
                    if (provider) {
                        response.additionalStats["onnx_models_loaded"] = "unknown";
                    } else {
                        response.additionalStats["onnx_models_loaded"] = "0";
                    }
                }
            } catch (...) {
                response.additionalStats["service_embeddingservice"] = "unavailable";
            }

            // Embedding usage metrics (best-effort)
            try {
                auto generator = serviceManager_->getEmbeddingGenerator();
                if (generator) {
                    auto estats = generator->getStats();
                    response.additionalStats["embed_total_texts"] =
                        std::to_string(estats.total_texts_processed.load());
                    response.additionalStats["embed_total_tokens"] =
                        std::to_string(estats.total_tokens_processed.load());
                    response.additionalStats["embed_total_time_ms"] =
                        std::to_string(estats.total_inference_time.load());
                    response.additionalStats["embed_avg_time_ms"] =
                        std::to_string(estats.avg_inference_time.load());
                    response.additionalStats["embed_throughput_txtps"] =
                        std::to_string(estats.throughput_texts_per_sec.load());
                    response.additionalStats["embed_throughput_tokps"] =
                        std::to_string(estats.throughput_tokens_per_sec.load());
                    // Model identity and dimension when available
                    try {
                        auto cfg = generator->getConfig();
                        response.additionalStats["embed_model_name"] = cfg.model_name;
                        response.additionalStats["embed_dim"] =
                            std::to_string(generator->getEmbeddingDimension());
                    } catch (...) {
                    }
                }
            } catch (...) {
                // best-effort only
            }

            // Service startup order and timing
            response.additionalStats["service_startup_order"] =
                "contentstore,metadatarepo,searchexecutor,embeddingservice";
            response.additionalStats["service_initialization_time"] =
                std::to_string(uptimeSeconds); // Time since daemon start
        }

        // Add explicit readiness indicator for stats consumers
        {
            auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
            bool readyish = (lifecycleSnapshot.state == LifecycleState::Ready ||
                             lifecycleSnapshot.state == LifecycleState::Degraded);
            response.additionalStats["not_ready"] = readyish ? "false" : "true";
        }

        // Provide a compact JSON payload so proto path can carry richer details
        try {
            nlohmann::json j;
            j["total_documents"] = response.totalDocuments;
            j["total_size"] = response.totalSize;
            j["indexed_documents"] = response.indexedDocuments;
            j["vector_index_size"] = response.vectorIndexSize;
            j["compression_ratio"] = response.compressionRatio;
            // Basic runtime stats for CLI without relying on Status proto
            j["memory_mb"] = getMemoryUsage();
            j["cpu_pct"] = getCpuUsage();
            auto uptime = std::chrono::steady_clock::now() - state_->stats.startTime;
            j["uptime_seconds"] = std::chrono::duration_cast<std::chrono::seconds>(uptime).count();
            j["version"] = std::string(YAMS_VERSION_STRING);

            // Readiness snapshot
            nlohmann::json rd;
            rd["ipc_server"] = state_->readiness.ipcServerReady.load();
            rd["content_store"] = state_->readiness.contentStoreReady.load();
            rd["database"] = state_->readiness.databaseReady.load();
            rd["metadata_repo"] = state_->readiness.metadataRepoReady.load();
            rd["search_engine"] = state_->readiness.searchEngineReady.load();
            rd["model_provider"] = state_->readiness.modelProviderReady.load();
            rd["vector_index"] = state_->readiness.vectorIndexReady.load();
            rd["plugins"] = state_->readiness.pluginsReady.load();
            j["readiness"] = rd;

            // Progress
            nlohmann::json pr;
            pr["search_engine"] = state_->readiness.searchProgress.load();
            pr["vector_index"] = state_->readiness.vectorIndexProgress.load();
            pr["model_provider"] = state_->readiness.modelLoadProgress.load();
            j["progress"] = pr;

            // Durations and top_slowest from state
            if (!state_->initDurationsMs.empty()) {
                nlohmann::json dur;
                for (const auto& [k, v] : state_->initDurationsMs)
                    dur[k] = v;
                j["durations_ms"] = dur;
                std::vector<std::pair<std::string, uint64_t>> items(state_->initDurationsMs.begin(),
                                                                    state_->initDurationsMs.end());
                std::sort(items.begin(), items.end(),
                          [](const auto& a, const auto& b) { return a.second > b.second; });
                nlohmann::json top;
                size_t count = std::min<size_t>(3, items.size());
                for (size_t i = 0; i < count; ++i) {
                    top.push_back({{"name", items[i].first}, {"elapsed_ms", items[i].second}});
                }
                if (!top.empty())
                    j["top_slowest"] = top;
            }

            // Latency snapshot (approximate percentiles)
            {
                auto lsnap = LatencyRegistry::instance().snapshot();
                j["latency_p50_ms"] = lsnap.p50_ms;
                j["latency_p95_ms"] = lsnap.p95_ms;
            }

            // Services snapshot for JSON consumers
            try {
                nlohmann::json sj;
                // Copy from additionalStats when available
                auto it = response.additionalStats.find("service_contentstore");
                if (it != response.additionalStats.end())
                    sj["contentstore"] = it->second;
                it = response.additionalStats.find("service_metadatarepo");
                if (it != response.additionalStats.end())
                    sj["metadatarepo"] = it->second;
                it = response.additionalStats.find("service_searchexecutor");
                if (it != response.additionalStats.end())
                    sj["searchexecutor"] = it->second;
                it = response.additionalStats.find("searchexecutor_reason");
                if (it != response.additionalStats.end())
                    sj["searchexecutor_reason"] = it->second;
                it = response.additionalStats.find("service_vectordb");
                if (it != response.additionalStats.end())
                    sj["vectordb"] = it->second;
                it = response.additionalStats.find("service_embeddingservice");
                if (it != response.additionalStats.end())
                    sj["embeddingservice"] = it->second;
                it = response.additionalStats.find("onnx_models_loaded");
                if (it != response.additionalStats.end())
                    sj["onnx_models_loaded"] = it->second;
                if (!sj.empty())
                    j["services"] = sj;
            } catch (...) {
            }

            // Overall readiness as explicit boolean for banner logic
            auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
            bool readyish = (lifecycleSnapshot.state == LifecycleState::Ready ||
                             lifecycleSnapshot.state == LifecycleState::Degraded);
            j["not_ready"] = !readyish;

            // Add worker pool metrics into JSON for doctor scripting
            try {
                std::size_t threads = 0, active = 0, queued = 0, util = 0;
                if (serviceManager_) {
                    threads = serviceManager_->getWorkerThreads();
                    active = serviceManager_->getWorkerActive();
                    auto posted = serviceManager_->getWorkerPosted();
                    auto completed = serviceManager_->getWorkerCompleted();
                    if (posted >= completed + active)
                        queued = posted - completed - active;
                    if (threads > 0)
                        util = static_cast<std::size_t>((100.0 * active) / threads);
                }
                nlohmann::json wj;
                wj["threads"] = threads;
                wj["active"] = active;
                wj["queued"] = queued;
                wj["utilizationPct"] = util;
                j["worker_pool"] = wj;
            } catch (...) {
            }

            response.additionalStats["json"] = j.dump();
        } catch (...) {
            // ignore JSON build issues
        }

        // Streaming metrics (aggregated)
        {
            auto ssnap = StreamMetricsRegistry::instance().snapshot();
            response.additionalStats["stream_total_streams"] = std::to_string(ssnap.totalStreams);
            response.additionalStats["stream_batches_emitted"] =
                std::to_string(ssnap.batchesEmitted);
            response.additionalStats["stream_keepalives"] = std::to_string(ssnap.keepalives);
            uint64_t avg = (ssnap.ttfbCount > 0) ? (ssnap.ttfbSumMs / ssnap.ttfbCount) : 0;
            response.additionalStats["stream_ttfb_avg_ms"] = std::to_string(avg);
        }

        // Worker pool metrics
        try {
            std::size_t threads = 0, active = 0, queued = 0, util = 0;
            if (serviceManager_) {
                threads = serviceManager_->getWorkerThreads();
                active = serviceManager_->getWorkerActive();
                auto posted = serviceManager_->getWorkerPosted();
                auto completed = serviceManager_->getWorkerCompleted();
                if (posted >= completed + active)
                    queued = posted - completed - active;
                if (threads > 0)
                    util = static_cast<std::size_t>((100.0 * active) / threads);
            }
            response.additionalStats["worker_threads"] = std::to_string(threads);
            response.additionalStats["worker_active"] = std::to_string(active);
            response.additionalStats["worker_queued"] = std::to_string(queued);
            response.additionalStats["worker_utilization_pct"] = std::to_string(util);
        } catch (...) {
        }

        // Plugin loader snapshot (prefer host descriptors; include basic health when available)
        if (req.detailed) {
            try {
                if (serviceManager_) {
                    nlohmann::json pj = nlohmann::json::array();
                    size_t count = 0;
                    if (auto* abi = serviceManager_->getAbiPluginHost()) {
                        auto descs = abi->listLoaded();
                        count += descs.size();
                        for (const auto& d : descs) {
                            nlohmann::json rec;
                            rec["name"] = d.name;
                            rec["path"] = d.path.string();
                            if (!d.interfaces.empty())
                                rec["interfaces"] = d.interfaces;
                            // Health best-effort
                            try {
                                if (auto hr = abi->health(d.name))
                                    rec["health"] = hr.value();
                            } catch (...) {
                            }
                            // Enrich provider status
                            try {
                                if (!serviceManager_->adoptedProviderPluginName().empty() &&
                                    serviceManager_->adoptedProviderPluginName() == d.name) {
                                    rec["provider"] = true;
                                    if (auto mp = serviceManager_->getModelProvider()) {
                                        rec["models_loaded"] = mp->getLoadedModels().size();
                                    }
                                    if (serviceManager_->isModelProviderDegraded()) {
                                        rec["degraded"] = true;
                                        if (!serviceManager_->lastModelError().empty())
                                            rec["error"] = serviceManager_->lastModelError();
                                    }
                                }
                            } catch (...) {
                            }
                            pj.push_back(rec);
                        }
                    } else {
                        auto plugins = serviceManager_->getLoadedPlugins();
                        count = plugins.size();
                        for (const auto& pi : plugins) {
                            nlohmann::json rec;
                            rec["name"] = pi.name;
                            rec["path"] = pi.path.string();
                            pj.push_back(rec);
                        }
                    }
                    response.additionalStats["plugins_loaded"] = std::to_string(count);
                    response.additionalStats["plugins_json"] = pj.dump();
                }
            } catch (const std::exception& ex) {
                response.additionalStats["plugins_loaded"] = "0";
                response.additionalStats["plugins_json"] = "[]";
                response.additionalStats["plugins_error"] = ex.what();
            } catch (...) {
                response.additionalStats["plugins_loaded"] = "0";
                response.additionalStats["plugins_json"] = "[]";
                response.additionalStats["plugins_error"] = "unknown error";
            }
        }

        // Auto-repair metrics for doctor/stats visibility
        response.additionalStats["repair_queue_depth"] =
            std::to_string(state_->stats.repairQueueDepth.load());
        response.additionalStats["repair_batches_attempted"] =
            std::to_string(state_->stats.repairBatchesAttempted.load());
        response.additionalStats["repair_embeddings_generated"] =
            std::to_string(state_->stats.repairEmbeddingsGenerated.load());
        response.additionalStats["repair_failed_operations"] =
            std::to_string(state_->stats.repairFailedOperations.load());

        // WAL metrics (via ServiceManager provider; returns zeros until attached)
        try {
            if (serviceManager_) {
                auto wp = serviceManager_->getWalMetricsProvider();
                if (wp) {
                    auto w = wp->getStats();
                    response.additionalStats["wal_active_transactions"] =
                        std::to_string(w.activeTransactions);
                    response.additionalStats["wal_pending_entries"] =
                        std::to_string(w.pendingEntries);
                }
            }
        } catch (...) {
        }

        co_return response;

    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("GetStats request failed: ") + e.what()};
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handleUpdateDocumentRequest(const UpdateDocumentRequest& req) {
    try {
        // Use DocumentService for business logic
        auto documentService = app::services::makeDocumentService(serviceManager_->getAppContext());

        // Convert daemon UpdateDocumentRequest to service UpdateMetadataRequest
        app::services::UpdateMetadataRequest serviceReq;
        serviceReq.hash = req.hash;
        serviceReq.name = req.name;

        // Map metadata updates
        for (const auto& [key, value] : req.metadata) {
            serviceReq.keyValues[key] = value;
        }

        // Map content update
        serviceReq.newContent = req.newContent;

        // Map tag operations
        serviceReq.addTags = req.addTags;
        serviceReq.removeTags = req.removeTags;

        // Set atomic transaction mode (default true)
        serviceReq.atomic = req.atomic;
        serviceReq.createBackup = req.createBackup;
        serviceReq.verbose = req.verbose;

        // Execute the update
        auto result = documentService->updateMetadata(serviceReq);
        if (!result) {
            co_return ErrorResponse{result.error().code, result.error().message};
        }

        const auto& serviceResp = result.value();

        // Map app::services::UpdateMetadataResponse to daemon UpdateDocumentResponse
        UpdateDocumentResponse response;
        response.hash = serviceResp.hash;
        response.metadataUpdated = serviceResp.updatesApplied > 0;
        response.tagsUpdated = (serviceResp.tagsAdded > 0) || (serviceResp.tagsRemoved > 0);
        response.contentUpdated = serviceResp.contentUpdated;

        co_return response;

    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("UpdateDocument request failed: ") + e.what()};
    }
}

boost::asio::awaitable<Response> RequestDispatcher::handleGrepRequest(const GrepRequest& req) {
    try {
        // Use app services for grep
        auto appContext = serviceManager_->getAppContext();
        auto grepService = app::services::makeGrepService(appContext);

        // Map daemon GrepRequest to app::services::GrepRequest with complete field mapping
        app::services::GrepRequest serviceReq;
        serviceReq.pattern = req.pattern;

        // Use new paths field if available, fall back to path for backward compatibility
        if (!req.paths.empty()) {
            serviceReq.paths = req.paths;
        } else if (!req.path.empty()) {
            serviceReq.paths.push_back(req.path);
        }

        serviceReq.includePatterns = req.includePatterns;
        serviceReq.recursive = req.recursive; // Now using actual field from daemon protocol

        // Handle context options with proper precedence
        if (req.contextLines > 0) {
            // contextLines overrides before/after if set
            serviceReq.context = req.contextLines;
            serviceReq.beforeContext = req.contextLines;
            serviceReq.afterContext = req.contextLines;
        } else {
            // Use separate before/after context
            serviceReq.beforeContext = req.beforeContext;
            serviceReq.afterContext = req.afterContext;
            serviceReq.context = 0;
        }

        serviceReq.ignoreCase = req.caseInsensitive;
        serviceReq.word = req.wholeWord;
        serviceReq.invert = req.invertMatch;
        serviceReq.literalText = req.literalText;
        serviceReq.lineNumbers = req.showLineNumbers;
        serviceReq.withFilename = req.showFilename && !req.noFilename;
        serviceReq.count = req.countOnly;
        serviceReq.filesWithMatches = req.filesOnly; // Map filesOnly to filesWithMatches
        serviceReq.filesWithoutMatch = req.filesWithoutMatch;
        serviceReq.pathsOnly = req.pathsOnly;
        serviceReq.colorMode = req.colorMode;
        serviceReq.regexOnly = req.regexOnly;
        serviceReq.semanticLimit = static_cast<int>(req.semanticLimit);
        serviceReq.tags = req.filterTags; // Use filterTags field name
        serviceReq.matchAllTags = req.matchAllTags;
        serviceReq.maxCount = static_cast<int>(req.maxMatches);

        auto result = grepService->grep(serviceReq);
        if (!result) {
            co_return ErrorResponse{result.error().code, result.error().message};
        }

        const auto& serviceResp = result.value();

        // Map app::services::GrepResponse to daemon GrepResponse with default cap
        const std::size_t defaultCap = 20;
        // Do not cap when special output modes are requested; emit full set (including semantic)
        const bool applyDefaultCap =
            !(req.countOnly || req.filesOnly || req.filesWithoutMatch || req.pathsOnly) &&
            req.maxMatches == 0;

        GrepResponse response;
        response.filesSearched = serviceResp.filesSearched;
        std::size_t emitted = 0;

        for (const auto& fileResult : serviceResp.results) {
            for (const auto& match : fileResult.matches) {
                if (applyDefaultCap && emitted >= defaultCap)
                    break;
                GrepMatch daemonMatch;
                daemonMatch.file = fileResult.file;
                daemonMatch.lineNumber = match.lineNumber;
                daemonMatch.line = match.line;
                daemonMatch.contextBefore = match.before;
                daemonMatch.contextAfter = match.after;
                daemonMatch.matchType = match.matchType.empty() ? "regex" : match.matchType;
                daemonMatch.confidence = match.confidence;
                response.matches.push_back(std::move(daemonMatch));
                emitted++;
            }
            if (applyDefaultCap && emitted >= defaultCap)
                break;
        }

        // Totals: reflect emitted matches if default cap applied
        response.totalMatches = applyDefaultCap ? emitted : serviceResp.totalMatches;

        // Note: daemon protocol doesn't support filesWithMatches/filesWithoutMatches

        co_return response;

    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("Grep request failed: ") + e.what()};
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handleDownloadRequest(const DownloadRequest& req) {
    try {
        // Determine if daemon-side download is enabled.
        // Primary switch is intended to be a policy flag (daemon.download.enable=true).
        // As a safe test override, honor YAMS_ENABLE_DAEMON_DOWNLOAD=1.
        const bool envEnabled = []() {
            if (const char* v = std::getenv("YAMS_ENABLE_DAEMON_DOWNLOAD")) {
                return std::string(v) == "1" || std::string(v) == "true";
            }
            return false;
        }();

        // If policy is not wired through ServiceManager/daemon config yet, the env flag
        // allows tests to exercise the DownloadService path safely.
        const bool policyEnabled = envEnabled;

        if (!policyEnabled) {
            DownloadResponse response;
            response.url = req.url;
            response.success = false;
            response.error =
                "Daemon download is disabled. Perform download locally (MCP/CLI) and index, or "
                "enable daemon.download policy (enable=true, allowed_hosts, allowed_schemes, "
                "require_checksum, store_only, sandbox).";
            if (!req.quiet) {
                spdlog::info("Download request received; responding with policy reminder (daemon "
                             "downloads disabled by default).");
            }
            co_return response;
        }

        // Execute via app::services::DownloadService
        auto appContext = serviceManager_->getAppContext();
        auto downloadService = app::services::makeDownloadService(appContext);
        if (!downloadService) {
            co_return ErrorResponse{ErrorCode::NotInitialized,
                                    "Download service not available in daemon"};
        }

        // Map daemon DownloadRequest -> app::services::DownloadServiceRequest
        app::services::DownloadServiceRequest sreq;
        sreq.url = req.url;
        // Defaults chosen to be safe; follow redirects and store into CAS only.
        sreq.followRedirects = true;
        sreq.storeOnly = true;
        // Honor reasonable defaults for chunk size, concurrency, timeout
        // (DownloadService has internal defaults; we set conservative overrides)
        sreq.concurrency = 4;
        sreq.chunkSizeBytes = 8'388'608; // 8MB
        sreq.timeout = std::chrono::milliseconds(60'000);
        sreq.resume = true;

        // Execute download
        auto sres = downloadService->download(sreq);
        if (!sres) {
            co_return ErrorResponse{sres.error().code, sres.error().message};
        }

        const auto& ok = sres.value();

        // Build daemon DownloadResponse
        DownloadResponse response;
        response.url = ok.url;
        response.hash = ok.hash;
        response.localPath = ok.storedPath.string();
        response.size = static_cast<size_t>(ok.sizeBytes);
        response.success = ok.success;

        if (!response.success) {
            response.error = "Download failed";
        } else {
            // Notify daemon about the downloaded document for repair coordination
            if (daemon_ && !response.hash.empty()) {
                daemon_->onDocumentAdded(response.hash, response.localPath);
            }
        }

        co_return response;

    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("Download request failed: ") + e.what()};
    }
}

// Helper method for grep functionality
bool RequestDispatcher::matchesAnyPattern(const std::string& filename,
                                          const std::vector<std::string>& patterns) {
    for (const auto& pattern : patterns) {
        // Simple wildcard matching (* and ?)
        std::string regexPattern = pattern;

        // Escape special regex characters except * and ?
        std::string escaped;
        for (char c : regexPattern) {
            if (c == '*') {
                escaped += ".*";
            } else if (c == '?') {
                escaped += ".";
            } else if (std::string(".^$+{}[]|()\\").find(c) != std::string::npos) {
                escaped += "\\";
                escaped += c;
            } else {
                escaped += c;
            }
        }

        try {
            std::regex regex(escaped, std::regex_constants::icase);
            if (std::regex_match(filename, regex)) {
                return true;
            }
        } catch (const std::regex_error&) {
            // If regex fails, fall back to simple string comparison
            if (filename == pattern) {
                return true;
            }
        }
    }
    return false;
}

// Helper method to escape regex special characters
std::string RequestDispatcher::escapeRegex(const std::string& text) {
    std::string escaped;
    for (char c : text) {
        if (std::string(".^$*+?{}[]|()\\").find(c) != std::string::npos) {
            escaped += "\\";
        }
        escaped += c;
    }
    return escaped;
}

double RequestDispatcher::getMemoryUsage() {
    // TODO: Implement platform-specific memory usage retrieval
    return 0.0;
}

double RequestDispatcher::getCpuUsage() {
    // TODO: Implement platform-specific CPU usage retrieval
    return 0.0;
}

bool RequestDispatcher::isValidHash(const std::string& hash) {
    if (hash.length() < 8 || hash.length() > 64) {
        return false;
    }
    return std::all_of(hash.begin(), hash.end(), [](char c) { return std::isxdigit(c); });
}

std::string RequestDispatcher::truncateSnippet(const std::string& snippet, size_t maxLength) {
    if (snippet.length() <= maxLength) {
        return snippet;
    }
    return snippet.substr(0, maxLength) + "...";
}

Response
RequestDispatcher::handleHashSearch(const SearchRequest& req,
                                    std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    if (!isValidHash(req.hashQuery)) {
        return ErrorResponse{ErrorCode::InvalidArgument,
                             "Invalid hash format. Must be 8-64 hexadecimal characters."};
    }

    try {
        auto docResult = metadataRepo->getDocumentByHash(req.hashQuery);
        if (!docResult || !docResult.value()) {
            // Return empty results rather than error for not found
            SearchResponse response;
            return response;
        }

        const auto& doc = docResult.value().value();
        SearchResponse response;

        // Convert document to search result format
        yams::daemon::SearchResult result;
        result.id = doc.sha256Hash;
        result.title = doc.fileName;
        result.path = doc.filePath;
        result.score = 1.0;  // Exact match
        result.snippet = ""; // Hash searches don't need snippets

        // Add metadata
        result.metadata["path"] = doc.filePath;
        result.metadata["title"] = doc.fileName;
        result.metadata["hash"] = doc.sha256Hash;
        result.metadata["size"] = std::to_string(doc.fileSize);
        result.metadata["mimeType"] = doc.mimeType;

        response.results.push_back(result);
        return response;

    } catch (const std::exception& e) {
        return ErrorResponse{ErrorCode::InternalError,
                             std::string("Hash search failed: ") + e.what()};
    }
}

Response
RequestDispatcher::handleFuzzySearch(const SearchRequest& req,
                                     std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    try {
        auto searchResult = metadataRepo->fuzzySearch(req.query, req.similarity, req.limit);
        if (!searchResult) {
            return ErrorResponse{ErrorCode::InternalError, searchResult.error().message};
        }

        SearchResponse response;
        const auto& fuzzyResults = searchResult.value();

        // Convert metadata::SearchResults to daemon response format
        for (const auto& item : fuzzyResults.results) {
            yams::daemon::SearchResult result;
            result.id = item.document.sha256Hash;
            result.title = item.document.fileName;
            result.path = item.document.filePath;
            result.score = item.score;
            result.snippet = truncateSnippet(item.snippet, 200);

            // Add metadata from the fuzzy search result
            result.metadata["path"] = item.document.filePath;
            result.metadata["title"] = item.document.fileName;
            result.metadata["hash"] = item.document.sha256Hash;
            result.metadata["mimeType"] = item.document.mimeType;
            result.metadata["fileSize"] = std::to_string(item.document.fileSize);

            response.results.push_back(result);
        }

        return response;

    } catch (const std::exception& e) {
        return ErrorResponse{ErrorCode::InternalError,
                             std::string("Fuzzy search failed: ") + e.what()};
    }
}

Response
RequestDispatcher::handleHybridSearch(const SearchRequest& req,
                                      std::shared_ptr<metadata::MetadataRepository> metadataRepo) {
    try {
        // Get vector index manager and embedding generator from service manager
        auto vecMgr = serviceManager_->getVectorIndexManager();
        auto embeddingGen = serviceManager_->getEmbeddingGenerator();

        if (!vecMgr) {
            spdlog::warn("VectorIndexManager not available, falling back to metadata search");
            return handleMetadataSearch(req, metadataRepo);
        }

        // Build hybrid search engine
        yams::search::SearchEngineBuilder builder;
        builder.withVectorIndex(vecMgr).withMetadataRepo(metadataRepo);

        // Add embedding generator if available
        if (embeddingGen) {
            builder.withEmbeddingGenerator(embeddingGen);
        }

        // Configure build options (defaults), then overlay from config.toml if present
        auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
        opts.hybrid.final_top_k = req.limit;
        opts.hybrid.generate_explanations = req.verbose || req.jsonOutput;

        // Best-effort: overlay [search.hybrid] from ~/.config/yams/config.toml
        // Lightweight parser (line-based) to avoid hard dependency here
        auto trim = [](std::string& t) {
            if (t.empty())
                return;
            t.erase(0, t.find_first_not_of(" \t"));
            auto p = t.find_last_not_of(" \t");
            if (p != std::string::npos)
                t.erase(p + 1);
        };
        auto parseBool = [](const std::string& v) -> std::optional<bool> {
            std::string s = v;
            std::transform(s.begin(), s.end(), s.begin(), ::tolower);
            if (s == "true" || s == "1" || s == "yes" || s == "on")
                return true;
            if (s == "false" || s == "0" || s == "no" || s == "off")
                return false;
            return std::nullopt;
        };
        auto parseFloat = [](const std::string& v) -> std::optional<float> {
            try {
                return std::stof(v);
            } catch (...) {
                return std::nullopt;
            }
        };
        auto parseU64 = [](const std::string& v) -> std::optional<uint64_t> {
            try {
                return static_cast<uint64_t>(std::stoull(v));
            } catch (...) {
                return std::nullopt;
            }
        };
        auto parseSizeT = [](const std::string& v) -> std::optional<size_t> {
            try {
                return static_cast<size_t>(std::stoull(v));
            } catch (...) {
                return std::nullopt;
            }
        };
        auto parseStrategy = [](const std::string& v)
            -> std::optional<yams::search::HybridSearchConfig::FusionStrategy> {
            std::string s = v;
            std::transform(s.begin(), s.end(), s.begin(), ::toupper);
            if (s == "LINEAR_COMBINATION")
                return yams::search::HybridSearchConfig::FusionStrategy::LINEAR_COMBINATION;
            if (s == "RECIPROCAL_RANK")
                return yams::search::HybridSearchConfig::FusionStrategy::RECIPROCAL_RANK;
            if (s == "CASCADE")
                return yams::search::HybridSearchConfig::FusionStrategy::CASCADE;
            if (s == "HYBRID_CASCADE")
                return yams::search::HybridSearchConfig::FusionStrategy::HYBRID_CASCADE;
            return std::nullopt;
        };

        try {
            namespace fs = std::filesystem;
            fs::path cfgHome;
            if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
                cfgHome = fs::path(xdg);
            else if (const char* home = std::getenv("HOME"))
                cfgHome = fs::path(home) / ".config";
            fs::path cfgPath = cfgHome / "yams" / "config.toml";
            if (!cfgPath.empty() && fs::exists(cfgPath)) {
                std::ifstream in(cfgPath);
                std::string line;
                while (std::getline(in, line)) {
                    std::string l = line;
                    trim(l);
                    if (l.empty() || l[0] == '#')
                        continue;
                    // Only consider keys with prefix 'search.hybrid.'
                    if (l.rfind("search.hybrid.", 0) != 0)
                        continue;
                    auto eq = l.find('=');
                    if (eq == std::string::npos)
                        continue;
                    std::string key = l.substr(0, eq);
                    trim(key);
                    std::string val = l.substr(eq + 1);
                    trim(val);
                    if (!val.empty() && val.front() == '"' && val.back() == '"')
                        val = val.substr(1, val.size() - 2);

                    auto applyFloat = [&](const char* name, float& dst) {
                        if (key == name) {
                            if (auto f = parseFloat(val))
                                dst = *f;
                        }
                    };
                    auto applySize = [&](const char* name, size_t& dst) {
                        if (key == name) {
                            if (auto n = parseSizeT(val))
                                dst = *n;
                        }
                    };
                    auto applyBool = [&](const char* name, bool& dst) {
                        if (key == name) {
                            if (auto b = parseBool(val))
                                dst = *b;
                        }
                    };

                    applyFloat("search.hybrid.vector_weight", opts.hybrid.vector_weight);
                    applyFloat("search.hybrid.keyword_weight", opts.hybrid.keyword_weight);
                    applyFloat("search.hybrid.kg_entity_weight", opts.hybrid.kg_entity_weight);
                    applyFloat("search.hybrid.structural_weight", opts.hybrid.structural_weight);
                    applySize("search.hybrid.vector_top_k", opts.hybrid.vector_top_k);
                    applySize("search.hybrid.keyword_top_k", opts.hybrid.keyword_top_k);
                    applySize("search.hybrid.final_top_k", opts.hybrid.final_top_k);
                    applySize("search.hybrid.rerank_top_k", opts.hybrid.rerank_top_k);
                    applyBool("search.hybrid.enable_reranking", opts.hybrid.enable_reranking);
                    applyBool("search.hybrid.enable_query_expansion",
                              opts.hybrid.enable_query_expansion);
                    applySize("search.hybrid.expansion_terms", opts.hybrid.expansion_terms);
                    applyBool("search.hybrid.enable_kg", opts.hybrid.enable_kg);
                    applySize("search.hybrid.kg_max_neighbors", opts.hybrid.kg_max_neighbors);
                    applySize("search.hybrid.kg_max_hops", opts.hybrid.kg_max_hops);
                    if (key == "search.hybrid.kg_budget_ms") {
                        if (auto n = parseU64(val))
                            opts.hybrid.kg_budget_ms = std::chrono::milliseconds{*n};
                    }
                    applyBool("search.hybrid.normalize_scores", opts.hybrid.normalize_scores);
                    applyBool("search.hybrid.generate_explanations",
                              opts.hybrid.generate_explanations);
                    applyBool("search.hybrid.parallel_search", opts.hybrid.parallel_search);
                    if (key == "search.hybrid.rrf_k") {
                        if (auto f = parseFloat(val))
                            opts.hybrid.rrf_k = *f;
                    }
                    if (key == "search.hybrid.fusion_strategy") {
                        if (auto s = parseStrategy(val))
                            opts.hybrid.fusion_strategy = *s;
                    }
                }
                // Ensure weights are normalized after overlay
                opts.hybrid.normalizeWeights();
                // Honor request limit over config final_top_k
                opts.hybrid.final_top_k = req.limit;
            }
        } catch (...) {
            // Best-effort only; continue with defaults
        }

        // Build and execute search
        auto engRes = builder.buildEmbedded(opts);
        if (!engRes) {
            spdlog::warn("Hybrid search engine build failed, falling back to metadata search");
            return handleMetadataSearch(req, metadataRepo);
        }

        auto engine = engRes.value();
        auto searchRes = engine->search(req.query, opts.hybrid.final_top_k);
        if (!searchRes) {
            return ErrorResponse{ErrorCode::InternalError,
                                 std::string("Hybrid search failed: ") + searchRes.error().message};
        }

        // Optionally apply enhanced scoring hooks (no-op by default; bounded by timeout)
        std::vector<yams::search::HybridSearchResult> items = searchRes.value();

        // Read enhanced config (best-effort) from config.toml
        struct EnhancedCfg {
            bool enable{false};
            float classification_weight{0.0f};
            float kg_expansion_weight{0.0f};
            float hotzone_weight{0.0f};
            size_t max_expansion_concepts{0};
            std::chrono::milliseconds timeout{2000};
        } ecfg;
        try {
            namespace fs = std::filesystem;
            fs::path cfgHome;
            if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
                cfgHome = fs::path(xdg);
            else if (const char* home = std::getenv("HOME"))
                cfgHome = fs::path(home) / ".config";
            fs::path cfgPath = cfgHome / "yams" / "config.toml";
            if (!cfgPath.empty() && fs::exists(cfgPath)) {
                std::ifstream in(cfgPath);
                std::string line;
                // Reuse outer trim/parse lambdas to avoid shadowing (-Wshadow)
                while (std::getline(in, line)) {
                    std::string l = line;
                    trim(l);
                    if (l.empty() || l[0] == '#')
                        continue;
                    if (l.rfind("search.enhanced.", 0) != 0)
                        continue;
                    auto eq = l.find('=');
                    if (eq == std::string::npos)
                        continue;
                    std::string key = l.substr(0, eq);
                    trim(key);
                    std::string val = l.substr(eq + 1);
                    trim(val);
                    if (!val.empty() && val.front() == '"' && val.back() == '"')
                        val = val.substr(1, val.size() - 2);
                    if (key == "search.enhanced.enable") {
                        if (auto b = parseBool(val))
                            ecfg.enable = *b;
                    }
                    if (key == "search.enhanced.classification_weight") {
                        if (auto f = parseFloat(val))
                            ecfg.classification_weight = *f;
                    }
                    if (key == "search.enhanced.kg_expansion_weight") {
                        if (auto f = parseFloat(val))
                            ecfg.kg_expansion_weight = *f;
                    }
                    if (key == "search.enhanced.hotzone_weight") {
                        if (auto f = parseFloat(val))
                            ecfg.hotzone_weight = *f;
                    }
                    if (key == "search.enhanced.max_expansion_concepts") {
                        if (auto u = parseU64(val))
                            ecfg.max_expansion_concepts = static_cast<size_t>(*u);
                    }
                    if (key == "search.enhanced.enhanced_search_timeout_ms") {
                        if (auto u = parseU64(val))
                            ecfg.timeout = std::chrono::milliseconds{*u};
                    }
                }
            }
        } catch (...) {
        }

        auto analyze_simple = [](const std::string& q) {
            std::string lower = q;
            std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
            // Tokenize to count words
            size_t words = 0;
            bool in = false;
            for (char c : lower) {
                if (std::isalnum(static_cast<unsigned char>(c))) {
                    if (!in) {
                        in = true;
                        ++words;
                    }
                } else {
                    in = false;
                }
            }
            bool single = (words == 1);
            bool structured = (lower.find('"') != std::string::npos) ||
                              (lower.find(" and ") != std::string::npos) ||
                              (lower.find(" or ") != std::string::npos) ||
                              (lower.find(" not ") != std::string::npos) ||
                              (lower.find('(') != std::string::npos) ||
                              (lower.find(')') != std::string::npos);
            return std::make_pair(single, structured);
        };

        if (ecfg.enable) {
            auto [single, structured] = analyze_simple(req.query);
            if (!single && !structured) {
                // Compute lightweight enhancements within budget
                auto t0 = std::chrono::steady_clock::now();
                // Tokenize query
                std::vector<std::string> qtokens;
                qtokens.reserve(req.query.size() / 4 + 1);
                {
                    std::string s = req.query;
                    std::string tok;
                    for (char c : s) {
                        if (std::isalnum(static_cast<unsigned char>(c))) {
                            tok.push_back(static_cast<char>(std::tolower(c)));
                        } else {
                            if (!tok.empty()) {
                                qtokens.push_back(tok);
                                tok.clear();
                            }
                        }
                    }
                    if (!tok.empty())
                        qtokens.push_back(tok);
                    // dedup
                    std::sort(qtokens.begin(), qtokens.end());
                    qtokens.erase(std::unique(qtokens.begin(), qtokens.end()), qtokens.end());
                }
                auto contains_token = [](const std::string& hay, const std::string& needle) {
                    if (needle.empty())
                        return false;
                    auto h = hay;
                    std::transform(h.begin(), h.end(), h.begin(), ::tolower);
                    return h.find(needle) != std::string::npos;
                };
                // Hotzone memory (simple static across process)
                static std::unordered_map<std::string,
                                          std::pair<size_t, std::chrono::steady_clock::time_point>>
                    hot;
                // Update hot counts
                for (const auto& t : qtokens)
                    hot[t] = {hot[t].first + 1, std::chrono::steady_clock::now()};

                const float cw = std::max(0.0f, ecfg.classification_weight);
                const float kw = std::max(0.0f, ecfg.kg_expansion_weight);
                const float hw = std::max(0.0f, ecfg.hotzone_weight);
                const float totalw = (cw + kw + hw);
                if (totalw > 0.0f) {
                    for (auto& it : items) {
                        // Extract quick fields
                        std::string title, path;
                        if (auto t = it.metadata.find("title"); t != it.metadata.end())
                            title = t->second;
                        if (auto p = it.metadata.find("path"); p != it.metadata.end())
                            path = p->second;

                        // Classification-like: fraction of query tokens present in
                        // snippet/title/path
                        size_t match = 0;
                        for (const auto& t : qtokens) {
                            if (contains_token(it.content, t) || contains_token(title, t) ||
                                contains_token(path, t))
                                match++;
                        }
                        float cls = qtokens.empty() ? 0.0f
                                                    : static_cast<float>(match) /
                                                          static_cast<float>(qtokens.size());

                        // KG expansion-like: simple boost if any token plural/singular variant
                        // appears
                        size_t expHits = 0;
                        for (const auto& t : qtokens) {
                            std::string v1 = t + "s";
                            std::string v2 = t;
                            if (!t.empty() && t.back() == 's')
                                v2 = t.substr(0, t.size() - 1);
                            if (contains_token(it.content, v1) || contains_token(title, v1) ||
                                contains_token(path, v1))
                                expHits++;
                            if (v2 != t && (contains_token(it.content, v2) ||
                                            contains_token(title, v2) || contains_token(path, v2)))
                                expHits++;
                        }
                        float kgex = qtokens.empty()
                                         ? 0.0f
                                         : std::min(1.0f, static_cast<float>(expHits) /
                                                              static_cast<float>(qtokens.size()));

                        // Hotzone: boost by top hot token presence
                        float hotb = 0.0f;
                        for (const auto& t : qtokens) {
                            auto itHot = hot.find(t);
                            if (itHot == hot.end())
                                continue;
                            size_t freq = itHot->second.first;
                            if (freq == 0)
                                continue;
                            float pres = (contains_token(title, t) || contains_token(path, t) ||
                                          contains_token(it.content, t))
                                             ? 1.0f
                                             : 0.0f;
                            hotb = std::max(hotb,
                                            pres * std::min(2.0f, static_cast<float>(std::log1p(
                                                                      static_cast<double>(freq)))));
                        }

                        // Combine
                        float add = cw * cls + kw * kgex + hw * hotb;
                        it.hybrid_score += add;
                    }
                    // Re-sort
                    std::sort(items.begin(), items.end());
                    if (items.size() > static_cast<size_t>(opts.hybrid.final_top_k))
                        items.resize(opts.hybrid.final_top_k);
                }
                (void)t0; // future: enforce ecfg.timeout budget across computations
            }
        }

        // Convert results to response format
        SearchResponse response;
        const auto& finalItems = items;

        for (const auto& item : finalItems) {
            yams::daemon::SearchResult result;
            result.id = item.id;
            result.score = static_cast<double>(item.hybrid_score);
            result.snippet = truncateSnippet(item.content, 200);

            // Extract metadata fields
            auto titleIt = item.metadata.find("title");
            if (titleIt != item.metadata.end()) {
                result.title = titleIt->second;
            }

            auto pathIt = item.metadata.find("path");
            if (pathIt != item.metadata.end()) {
                result.path = pathIt->second;
            }

            // Copy all metadata
            result.metadata = item.metadata;

            // Add score breakdown if verbose
            if (req.verbose) {
                result.metadata["vector_score"] = std::to_string(item.vector_score);
                result.metadata["keyword_score"] = std::to_string(item.keyword_score);
                result.metadata["kg_entity_score"] = std::to_string(item.kg_entity_score);
                result.metadata["structural_score"] = std::to_string(item.structural_score);
            }

            response.results.push_back(result);
        }

        return response;

    } catch (const std::exception& e) {
        return ErrorResponse{ErrorCode::InternalError,
                             std::string("Hybrid search failed: ") + e.what()};
    }
}

Response RequestDispatcher::handleMetadataSearch(
    const SearchRequest& /*req*/, std::shared_ptr<metadata::MetadataRepository> /*metadataRepo*/) {
    try {
        // Use search executor for fallback metadata search
        auto searchExecutor = serviceManager_->getSearchExecutor();
        if (!searchExecutor) {
            return ErrorResponse{ErrorCode::NotInitialized, "Search executor not available"};
        }

        // Build search parameters - this is a simplified fallback
        // In a full implementation, you'd want to use the search executor's capabilities
        SearchResponse response;
        // For now, return empty results - this could be enhanced to use SearchExecutor
        // or implement a basic metadata-only search
        return response;

    } catch (const std::exception& e) {
        return ErrorResponse{ErrorCode::InternalError,
                             std::string("Metadata search failed: ") + e.what()};
    }
}

boost::asio::awaitable<Response> RequestDispatcher::handleCancelRequest(const CancelRequest& req) {
    bool ok = RequestContextRegistry::instance().cancel(req.targetRequestId);
    if (ok) {
        co_return SuccessResponse{"Cancel accepted"};
    }
    co_return ErrorResponse{ErrorCode::NotFound, "RequestId not found or already completed"};
}

} // namespace yams::daemon

// Re-open daemon namespace for plugin loader helpers and handlers
namespace yams::daemon {
static PluginRecord toRecord(const PluginDescriptor& sr) {
    PluginRecord pr;
    pr.name = sr.name;
    pr.version = sr.version;
    pr.abiVersion = sr.abiVersion;
    pr.path = sr.path.string();
    pr.manifestJson = sr.manifestJson;
    pr.interfaces = sr.interfaces;
    return pr;
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginScanRequest(const PluginScanRequest& req) {
    try {
        auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
        auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
        auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
        if (!abi && !wasm && !ext)
            co_return ErrorResponse{ErrorCode::NotImplemented, "No plugin host available"};
        PluginScanResponse resp;
        if (!req.target.empty()) {
            bool any = false;
            if (abi) {
                if (auto r = abi->scanTarget(req.target)) {
                    resp.plugins.push_back(toRecord(r.value()));
                    any = true;
                }
            }
            if (wasm) {
                if (auto r = wasm->scanTarget(req.target)) {
                    resp.plugins.push_back(toRecord(r.value()));
                    any = true;
                }
            }
            if (ext) {
                if (auto r = ext->scanTarget(req.target)) {
                    resp.plugins.push_back(toRecord(r.value()));
                    any = true;
                }
            }
            if (!any)
                co_return ErrorResponse{ErrorCode::NotFound, "No plugin found at target"};
        } else if (!req.dir.empty()) {
            if (abi) {
                if (auto r = abi->scanDirectory(req.dir))
                    for (auto& sr : r.value())
                        resp.plugins.push_back(toRecord(sr));
            }
            if (wasm) {
                if (auto r = wasm->scanDirectory(req.dir))
                    for (auto& sr : r.value())
                        resp.plugins.push_back(toRecord(sr));
            }
            if (ext) {
                if (auto r = ext->scanDirectory(req.dir))
                    for (auto& sr : r.value())
                        resp.plugins.push_back(toRecord(sr));
            }
        } else {
            // Default directories: reuse existing model PluginLoader directories
            for (const auto& dir : PluginLoader::getDefaultPluginDirectories()) {
                if (abi)
                    if (auto r = abi->scanDirectory(dir))
                        for (auto& sr : r.value())
                            resp.plugins.push_back(toRecord(sr));
                if (wasm)
                    if (auto r = wasm->scanDirectory(dir))
                        for (auto& sr : r.value())
                            resp.plugins.push_back(toRecord(sr));
                if (ext)
                    if (auto r = ext->scanDirectory(dir))
                        for (auto& sr : r.value())
                            resp.plugins.push_back(toRecord(sr));
            }
        }
        co_return resp;
    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::Unknown, e.what()};
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginLoadRequest(const PluginLoadRequest& req) {
    try {
        auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
        auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
        auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
        auto appContext =
            serviceManager_ ? serviceManager_->getAppContext() : yams::app::services::AppContext{};
        if (!abi && !wasm && !ext)
            co_return ErrorResponse{ErrorCode::NotImplemented, "No plugin host available"};
        PluginLoadResponse lr;
        std::filesystem::path target(req.pathOrName);
        if (req.dryRun) {
            if (abi) {
                if (auto r = abi->scanTarget(target)) {
                    lr.loaded = false;
                    lr.message = "dry-run";
                    lr.record = toRecord(r.value());
                    co_return lr;
                }
            }
            if (wasm) {
                if (auto r = wasm->scanTarget(target)) {
                    lr.loaded = false;
                    lr.message = "dry-run";
                    lr.record = toRecord(r.value());
                    co_return lr;
                }
            }
            co_return ErrorResponse{ErrorCode::NotFound, "Plugin not found"};
        }
        if (!std::filesystem::exists(target)) {
            // Try default directories by file name first
            bool found = false;
            for (const auto& dir : PluginLoader::getDefaultPluginDirectories()) {
                auto candidate = dir / req.pathOrName;
                if (std::filesystem::exists(candidate)) {
                    target = candidate;
                    found = true;
                    break;
                }
            }
            // If still not found and arg looks like a logical name (no path separators),
            // scan default directories and match by plugin descriptor name
            if (!found && req.pathOrName.find('/') == std::string::npos &&
                req.pathOrName.find('\\') == std::string::npos) {
                for (const auto& dir : PluginLoader::getDefaultPluginDirectories()) {
                    // Prefer ABI host scan
                    if (abi) {
                        if (auto r = abi->scanDirectory(dir)) {
                            for (const auto& d : r.value()) {
                                if (d.name == req.pathOrName) {
                                    target = d.path;
                                    found = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (!found && wasm) {
                        if (auto r = wasm->scanDirectory(dir)) {
                            for (const auto& d : r.value()) {
                                if (d.name == req.pathOrName) {
                                    target = d.path;
                                    found = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (found)
                        break;
                }
            }
            if (!found)
                co_return ErrorResponse{ErrorCode::FileNotFound, "Plugin not found"};
        }
        // Choose host by target type
        auto extn = target.extension().string();
        // External: directory with yams-plugin.json
        if (ext && std::filesystem::is_directory(target) &&
            std::filesystem::exists(target / "yams-plugin.json")) {
            if (auto r = ext->load(target, req.configJson)) {
                lr.loaded = true;
                lr.message = "loaded";
                lr.record = toRecord(r.value());
                co_return lr;
            }
        }
        if (extn == ".wasm" && wasm) {
            if (auto r = wasm->load(target, req.configJson)) {
                lr.loaded = true;
                lr.message = "loaded";
                lr.record = toRecord(r.value());
                co_return lr;
            }
        }
        if (abi) {
            if (auto r = abi->load(target, req.configJson)) {
                lr.loaded = true;
                lr.message = "loaded";
                lr.record = toRecord(r.value());
                // Attempt to adopt model provider dynamically if this plugin provides it
                if (serviceManager_) {
                    (void)serviceManager_->adoptModelProviderFromHosts(lr.record.name);
                }
                co_return lr;
            }
        }
        co_return ErrorResponse{ErrorCode::InvalidState, "Load failed"};
    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::Unknown, e.what()};
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginUnloadRequest(const PluginUnloadRequest& req) {
    try {
        auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
        auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
        auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
        bool ok = false;
        if (abi) {
            if (auto r = abi->unload(req.name))
                ok = true;
        }
        if (wasm && !ok) {
            if (auto r = wasm->unload(req.name))
                ok = true;
        }
        if (ext && !ok) {
            if (auto r = ext->unload(req.name))
                ok = true;
        }
        if (!ok)
            co_return ErrorResponse{ErrorCode::NotFound, "Plugin not found or unload failed"};
        co_return SuccessResponse{"unloaded"};
    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::Unknown, e.what()};
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginTrustListRequest(const PluginTrustListRequest& /*req*/) {
    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
    auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
    auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
    PluginTrustListResponse resp;
    if (abi)
        for (auto& p : abi->trustList())
            resp.paths.push_back(p.string());
    if (wasm)
        for (auto& p : wasm->trustList())
            resp.paths.push_back(p.string());
    if (ext)
        for (auto& p : ext->trustList())
            resp.paths.push_back(p.string());
    // de-dup
    std::sort(resp.paths.begin(), resp.paths.end());
    resp.paths.erase(std::unique(resp.paths.begin(), resp.paths.end()), resp.paths.end());
    co_return resp;
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginTrustAddRequest(const PluginTrustAddRequest& req) {
    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
    auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
    auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
    bool ok = false;
    if (abi)
        if (auto r = abi->trustAdd(req.path))
            ok = true;
    if (wasm)
        if (auto r = wasm->trustAdd(req.path))
            ok = true;
    if (ext)
        if (auto r = ext->trustAdd(req.path))
            ok = true;
    if (!ok)
        co_return ErrorResponse{ErrorCode::Unknown, "Trust add failed"};
    // Opportunistic autoload: if a directory/file was just trusted, load matching plugins now.
    try {
        std::filesystem::path p = req.path;
        if (std::filesystem::is_directory(p)) {
            if (abi) {
                if (auto r = abi->scanDirectory(p)) {
                    for (const auto& d : r.value())
                        (void)abi->load(d.path, "");
                }
            }
            if (wasm) {
                if (auto r = wasm->scanDirectory(p)) {
                    for (const auto& d : r.value())
                        (void)wasm->load(d.path, "");
                }
            }
        } else if (std::filesystem::is_regular_file(p)) {
            if (abi)
                (void)abi->load(p, "");
            if (wasm && p.extension() == ".wasm")
                (void)wasm->load(p, "");
        }
        // Attempt adoption of model provider if relevant
        if (serviceManager_)
            (void)serviceManager_->adoptModelProviderFromHosts();
    } catch (...) {
        // best-effort; ignore failures here
    }
    co_return SuccessResponse{"ok"};
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePluginTrustRemoveRequest(const PluginTrustRemoveRequest& req) {
    auto abi = serviceManager_ ? serviceManager_->getAbiPluginHost() : nullptr;
    auto wasm = serviceManager_ ? serviceManager_->getWasmPluginHost() : nullptr;
    auto ext = serviceManager_ ? serviceManager_->getExternalPluginHost() : nullptr;
    bool ok = false;
    if (abi)
        if (auto r = abi->trustRemove(req.path))
            ok = true;
    if (wasm)
        if (auto r = wasm->trustRemove(req.path))
            ok = true;
    if (ext)
        if (auto r = ext->trustRemove(req.path))
            ok = true;
    if (!ok)
        co_return ErrorResponse{ErrorCode::Unknown, "Trust remove failed"};
    co_return SuccessResponse{"ok"};
}

} // namespace yams::daemon
