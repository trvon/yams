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
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN 1
#endif
#ifndef NOMINMAX
#define NOMINMAX 1
#endif
#include <Windows.h>
#include <Psapi.h>
#endif
#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/task.h>
#include <mach/task_info.h>
#endif
#include <boost/asio/thread_pool.hpp>
#include <yams/api/content_store.h>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/common/name_resolver.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/dispatch_utils.hpp>
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
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/document_metadata.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/extraction/extraction_util.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/repair/embedding_repair_util.h>
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

// Trait to map request types to their handler methods
template <typename T> struct RequestHandlerTraits;

#define DEFINE_REQUEST_HANDLER(RequestType, HandlerMethod)                                         \
    template <> struct RequestHandlerTraits<RequestType> {                                         \
        static boost::asio::awaitable<Response> handle(RequestDispatcher* dispatcher,              \
                                                       const RequestType& req) {                   \
            return dispatcher->HandlerMethod(req);                                                 \
        }                                                                                          \
    }

DEFINE_REQUEST_HANDLER(StatusRequest, handleStatusRequest);
DEFINE_REQUEST_HANDLER(ShutdownRequest, handleShutdownRequest);
DEFINE_REQUEST_HANDLER(PingRequest, handlePingRequest);
DEFINE_REQUEST_HANDLER(SearchRequest, handleSearchRequest);
DEFINE_REQUEST_HANDLER(GetRequest, handleGetRequest);
DEFINE_REQUEST_HANDLER(GetInitRequest, handleGetInitRequest);
DEFINE_REQUEST_HANDLER(GetChunkRequest, handleGetChunkRequest);
DEFINE_REQUEST_HANDLER(GetEndRequest, handleGetEndRequest);
DEFINE_REQUEST_HANDLER(CatRequest, handleCatRequest);
DEFINE_REQUEST_HANDLER(ListSessionsRequest, handleListSessionsRequest);
DEFINE_REQUEST_HANDLER(UseSessionRequest, handleUseSessionRequest);
DEFINE_REQUEST_HANDLER(AddPathSelectorRequest, handleAddPathSelectorRequest);
DEFINE_REQUEST_HANDLER(RemovePathSelectorRequest, handleRemovePathSelectorRequest);
DEFINE_REQUEST_HANDLER(ListTreeDiffRequest, handleListTreeDiffRequest);
DEFINE_REQUEST_HANDLER(FileHistoryRequest, handleFileHistoryRequest);
DEFINE_REQUEST_HANDLER(PruneRequest, handlePruneRequest);
DEFINE_REQUEST_HANDLER(ListCollectionsRequest, handleListCollectionsRequest);
DEFINE_REQUEST_HANDLER(ListSnapshotsRequest, handleListSnapshotsRequest);
DEFINE_REQUEST_HANDLER(RestoreCollectionRequest, handleRestoreCollectionRequest);
DEFINE_REQUEST_HANDLER(RestoreSnapshotRequest, handleRestoreSnapshotRequest);
DEFINE_REQUEST_HANDLER(AddDocumentRequest, handleAddDocumentRequest);
DEFINE_REQUEST_HANDLER(ListRequest, handleListRequest);
DEFINE_REQUEST_HANDLER(DeleteRequest, handleDeleteRequest);
DEFINE_REQUEST_HANDLER(GetStatsRequest, handleGetStatsRequest);
DEFINE_REQUEST_HANDLER(GenerateEmbeddingRequest, handleGenerateEmbeddingRequest);
DEFINE_REQUEST_HANDLER(BatchEmbeddingRequest, handleBatchEmbeddingRequest);
DEFINE_REQUEST_HANDLER(LoadModelRequest, handleLoadModelRequest);
DEFINE_REQUEST_HANDLER(UnloadModelRequest, handleUnloadModelRequest);
DEFINE_REQUEST_HANDLER(ModelStatusRequest, handleModelStatusRequest);
DEFINE_REQUEST_HANDLER(UpdateDocumentRequest, handleUpdateDocumentRequest);
DEFINE_REQUEST_HANDLER(GrepRequest, handleGrepRequest);
DEFINE_REQUEST_HANDLER(DownloadRequest, handleDownloadRequest);
DEFINE_REQUEST_HANDLER(PrepareSessionRequest, handlePrepareSessionRequest);
DEFINE_REQUEST_HANDLER(PluginScanRequest, handlePluginScanRequest);
DEFINE_REQUEST_HANDLER(PluginLoadRequest, handlePluginLoadRequest);
DEFINE_REQUEST_HANDLER(PluginUnloadRequest, handlePluginUnloadRequest);
DEFINE_REQUEST_HANDLER(PluginTrustListRequest, handlePluginTrustListRequest);
DEFINE_REQUEST_HANDLER(PluginTrustAddRequest, handlePluginTrustAddRequest);
DEFINE_REQUEST_HANDLER(PluginTrustRemoveRequest, handlePluginTrustRemoveRequest);
DEFINE_REQUEST_HANDLER(CancelRequest, handleCancelRequest);
DEFINE_REQUEST_HANDLER(EmbedDocumentsRequest, handleEmbedDocumentsRequest);
DEFINE_REQUEST_HANDLER(GraphQueryRequest, handleGraphQueryRequest);
DEFINE_REQUEST_HANDLER(GraphPathHistoryRequest, handleGraphPathHistoryRequest);
DEFINE_REQUEST_HANDLER(GraphRepairRequest, handleGraphRepairRequest);
DEFINE_REQUEST_HANDLER(GraphValidateRequest, handleGraphValidateRequest);
DEFINE_REQUEST_HANDLER(KgIngestRequest, handleKgIngestRequest);

#undef DEFINE_REQUEST_HANDLER

// Helper functions for system metrics (moved from daemon.cpp)
double getMemoryUsage() {
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS_EX pmc{};
    if (GetProcessMemoryInfo(GetCurrentProcess(), reinterpret_cast<PROCESS_MEMORY_COUNTERS*>(&pmc),
                             sizeof(pmc))) {
        return static_cast<double>(pmc.WorkingSetSize) / (1024.0 * 1024.0);
    }
    return 0.0;
#elif defined(__APPLE__)
    task_vm_info_data_t info;
    mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_VM_INFO, (task_info_t)&info, &count) == KERN_SUCCESS) {
        return static_cast<double>(info.resident_size) / (1024.0 * 1024.0); // MB
    }
    return 0.0;
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
    return 0.0;
#endif
}

double getCpuUsage() {
    // Simple CPU usage approximation - in a real implementation,
    // you'd track CPU time over intervals
#if defined(_WIN32)
    FILETIME idleFT{}, kernelFT{}, userFT{};
    FILETIME createFT{}, exitFT{}, procKernelFT{}, procUserFT{};
    if (!GetSystemTimes(&idleFT, &kernelFT, &userFT) ||
        !GetProcessTimes(GetCurrentProcess(), &createFT, &exitFT, &procKernelFT, &procUserFT)) {
        return 0.0;
    }
    auto to64 = [](const FILETIME& ft) {
        ULARGE_INTEGER li{};
        li.LowPart = ft.dwLowDateTime;
        li.HighPart = ft.dwHighDateTime;
        return li.QuadPart;
    };
    const std::uint64_t proc = to64(procKernelFT) + to64(procUserFT);
    const std::uint64_t total = to64(kernelFT) + to64(userFT);
    static std::uint64_t lastProc = 0;
    static std::uint64_t lastTotal = 0;
    if (lastProc == 0 || lastTotal == 0 || proc < lastProc || total < lastTotal) {
        lastProc = proc;
        lastTotal = total;
        return 0.0;
    }
    const std::uint64_t dProc = proc - lastProc;
    const std::uint64_t dTotal = total - lastTotal;
    lastProc = proc;
    lastTotal = total;
    if (dTotal == 0)
        return 0.0;
    SYSTEM_INFO sysInfo{};
    GetSystemInfo(&sysInfo);
    const double maxPct =
        100.0 *
        static_cast<double>(sysInfo.dwNumberOfProcessors ? sysInfo.dwNumberOfProcessors : 1);
    double pct = (static_cast<double>(dProc) / static_cast<double>(dTotal)) * 100.0;
    return std::clamp(pct, 0.0, maxPct);
#elif defined(__APPLE__)
    task_info_data_t tinfo __attribute__((unused));
    unsigned thread_count;
    thread_act_array_t thread_list;

    if (task_threads(mach_task_self(), &thread_list, &thread_count) == KERN_SUCCESS) {
        vm_deallocate(mach_task_self(), (vm_offset_t)thread_list, thread_count * sizeof(thread_t));
        return static_cast<double>(thread_count) * 0.1; // Rough approximation
    }
    return 0.0;
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
    return 0.0;
#endif
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
    const bool isAddDocumentRequest = std::holds_alternative<AddDocumentRequest>(req);
    const bool isGetStatsRequest = std::holds_alternative<GetStatsRequest>(req);
    const bool isShutdownRequest = std::holds_alternative<ShutdownRequest>(req);

    auto lifecycleSnapshot = daemon_->getLifecycle().snapshot();
    // Allow GetStatsRequest to bypass the readiness gating so it can return
    // a minimal GetStatsResponse (the handler itself has an early not_ready
    // fast path). Previously this branch caused a StatusResponse to be
    // returned for GetStatsRequest during initialization, breaking the
    // contract that GetStats always yields GetStatsResponse.
    if (lifecycleSnapshot.state != LifecycleState::Ready &&
        lifecycleSnapshot.state != LifecycleState::Degraded && !isGetStatsRequest) {
        // Core services must be ready to proceed. However, for AddDocumentRequest we can
        // accept the request early as long as the content store is ready; the operation is
        // asynchronous and metadata indexing will catch up when the repository is ready.
        bool csReady = state_ && state_->readiness.contentStoreReady.load();
        bool dbReady = state_ && state_->readiness.metadataRepoReady.load();
        bool coreReady = csReady && dbReady;
        if (!coreReady && !(isAddDocumentRequest && csReady)) {
            // If not ready (and not an early-accepted add), return a status response.
            StatusRequest statusReq;
            statusReq.detailed = true;
            co_return co_await handleStatusRequest(statusReq);
        } else if (!coreReady && isAddDocumentRequest && csReady) {
            spdlog::debug(
                "Proceeding with AddDocument during initialization (content store ready)");
        } else {
            spdlog::debug("Proceeding with request during initialization (core ready)");
        }
    }

    // For requests that need services, check readiness.
    bool needs_services = !isShutdownRequest && !isGetStatsRequest;
    const bool contentStoreReady = state_ && state_->readiness.contentStoreReady.load();
    const bool metadataRepoReady = state_ && state_->readiness.metadataRepoReady.load();

    if (needs_services) {
        if (!metadataRepoReady) {
            if (isAddDocumentRequest && contentStoreReady) {
                spdlog::debug("Proceeding with AddDocument while metadata repository initializes");
            } else {
                co_return ErrorResponse{ErrorCode::InvalidState,
                                        "Metadata repository not ready. Please try again shortly."};
            }
        }
        if (!contentStoreReady) {
            std::string error_detail = "Content store not available. Please try again shortly.";
            if (serviceManager_) {
                std::string cs_error = serviceManager_->getContentStoreError();
                if (!cs_error.empty()) {
                    error_detail += " Details: " + cs_error;
                }
            }
            co_return ErrorResponse{ErrorCode::InvalidState, error_detail};
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
                return RequestHandlerTraits<T>::handle(this, arg);
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
    throw std::runtime_error("RequestDispatcher: ServiceManager not available");
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
RequestDispatcher::handleShutdownRequest(const ShutdownRequest& req) {
    spdlog::info("Received shutdown request (graceful={})", req.graceful);

    // Detach thread to perform clean shutdown then force process exit.
    // This prevents orphaned daemon processes (see PBI-070).
    if (daemon_) {
        bool graceful = req.graceful;
        std::thread([d = daemon_, graceful]() {
            try {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));

                spdlog::info("Initiating daemon shutdown sequence...");
                auto result = d->stop();
                if (!result) {
                    spdlog::error("Daemon shutdown encountered error: {}", result.error().message);
                }
            } catch (const std::exception& e) {
                spdlog::error("Exception during daemon shutdown: {}", e.what());
            } catch (...) {
                spdlog::error("Unknown exception during daemon shutdown");
            }

            d->requestStop();

            // Force exit after clean shutdown to prevent orphaned processes
            spdlog::info("Daemon shutdown complete, exiting process");
            std::exit(0);

            (void)graceful;
        }).detach();
    }

    co_return SuccessResponse{"Shutdown initiated"};
}
} // namespace yams::daemon
