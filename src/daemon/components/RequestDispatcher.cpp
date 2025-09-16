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
    // Allow GetStatsRequest to bypass the readiness gating so it can return
    // a minimal GetStatsResponse (the handler itself has an early not_ready
    // fast path). Previously this branch caused a StatusResponse to be
    // returned for GetStatsRequest during initialization, breaking the
    // contract that GetStats always yields GetStatsResponse.
    if (lifecycleSnapshot.state != LifecycleState::Ready &&
        lifecycleSnapshot.state != LifecycleState::Degraded &&
        !std::holds_alternative<GetStatsRequest>(req)) {
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
                } else if constexpr (std::is_same_v<T, GetInitRequest>) {
                    co_return co_await handleGetInitRequest(arg);
                } else if constexpr (std::is_same_v<T, GetChunkRequest>) {
                    co_return co_await handleGetChunkRequest(arg);
                } else if constexpr (std::is_same_v<T, GetEndRequest>) {
                    co_return co_await handleGetEndRequest(arg);
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
} // namespace yams::daemon
