#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
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
// clang-format off
#include <Windows.h>
#include <Psapi.h>
// clang-format on
#endif
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
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/ipc/request_context_registry.h>
#include <yams/daemon/ipc/retrieval_session.h>
#include <yams/daemon/ipc/stream_metrics_registry.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/document_metadata.h>
#include <yams/profiling.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/extraction/extraction_util.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/search/search_engine_builder.h>
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
// ListCollectionsRequest removed - use getMetadataValueCounts(["collection"], {})
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
DEFINE_REQUEST_HANDLER(MetadataValueCountsRequest, handleMetadataValueCountsRequest);
DEFINE_REQUEST_HANDLER(BatchRequest, handleBatchRequest);
DEFINE_REQUEST_HANDLER(RepairRequest, handleRepairRequest);

#undef DEFINE_REQUEST_HANDLER

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
    YAMS_ZONE_SCOPED_N("RequestDispatcher::dispatch");
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
    } catch (...) { // NOLINT(bugprone-empty-catch): stats failures must not interrupt response
    }

    co_return out;
}

boost::asio::any_io_executor RequestDispatcher::getWorkerExecutor() const {
    try {
        if (serviceManager_) {
            return serviceManager_->getWorkerExecutor();
        }
    } catch (...) { // NOLINT(bugprone-empty-catch): fall through to throw
    }
    throw std::runtime_error("RequestDispatcher: ServiceManager not available");
}

boost::asio::any_io_executor RequestDispatcher::getCliExecutor() const {
    try {
        if (serviceManager_) {
            return serviceManager_->getCliExecutor();
        }
    } catch (...) {
    }
    return getWorkerExecutor();
}
std::function<void(bool)> RequestDispatcher::getWorkerJobSignal() const {
    try {
        if (serviceManager_)
            return serviceManager_->getWorkerJobSignal();
    } catch (...) { // NOLINT(bugprone-empty-catch): fall through to return empty
    }
    return {};
}

boost::asio::awaitable<Response>
RequestDispatcher::handleShutdownRequest(const ShutdownRequest& req) {
    spdlog::info("Received shutdown request (graceful={})", req.graceful);

    if (daemon_) {
        bool graceful = req.graceful;
        bool inTestMode = std::getenv("YAMS_TESTING") != nullptr ||
                          std::getenv("YAMS_TEST_SAFE_SINGLE_INSTANCE") != nullptr;
        daemon_->spawnShutdownThread([d = daemon_, graceful, inTestMode]() {
            std::atomic<bool> shutdownComplete{false};
            std::thread watchdog;
            auto finalizeWatchdog = [&]() {
                shutdownComplete.store(true, std::memory_order_release);
                if (watchdog.joinable()) {
                    watchdog.join();
                }
            };
            try {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));

                if (!inTestMode) {
                    int timeoutMs = 15000;
                    if (const char* env = std::getenv("YAMS_SHUTDOWN_FORCE_EXIT_MS")) {
                        try {
                            int parsed = std::stoi(env);
                            if (parsed <= 0) {
                                timeoutMs = 0;
                            } else {
                                timeoutMs = std::max(parsed, 1000);
                            }
                        } catch (...) {
                        }
                    }
                    if (timeoutMs > 0) {
                        watchdog = std::thread([timeoutMs, &shutdownComplete]() {
                            auto deadline = std::chrono::steady_clock::now() +
                                            std::chrono::milliseconds(timeoutMs);
                            while (std::chrono::steady_clock::now() < deadline) {
                                if (shutdownComplete.load(std::memory_order_acquire)) {
                                    return;
                                }
                                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                            }
                            try {
                                spdlog::error("Shutdown exceeded {}ms; forcing process exit",
                                              timeoutMs);
                            } catch (...) {
                            }
#if !defined(_WIN32)
                            raise(SIGKILL);
#endif
                            std::_Exit(1);
                        });
                    }
                }

                // NOLINTBEGIN(bugprone-empty-catch): logger may be unavailable during shutdown
                try {
                    spdlog::info("Initiating daemon shutdown sequence...");
                } catch (...) {
                }
                auto result = d->stop();
                finalizeWatchdog();
                if (!result) {
                    try {
                        spdlog::error("Daemon shutdown encountered error: {}",
                                      result.error().message);
                    } catch (...) {
                    }
                }
            } catch (const std::exception& e) {
                finalizeWatchdog();
                try {
                    spdlog::error("Exception during daemon shutdown: {}", e.what());
                } catch (...) {
                }
            } catch (...) {
                finalizeWatchdog();
                try {
                    spdlog::error("Unknown exception during daemon shutdown");
                } catch (...) {
                }
            }

            d->requestStop();

            if (!inTestMode) {
                try {
                    spdlog::info("Daemon shutdown complete, exiting process");
                } catch (...) {
                }
                std::exit(0);
            } else {
                try {
                    spdlog::info("Daemon shutdown complete (test mode, not exiting)");
                } catch (...) {
                }
            }
            // NOLINTEND(bugprone-empty-catch)

            (void)graceful;
        });
    }

    co_return SuccessResponse{"Shutdown initiated"};
}
} // namespace yams::daemon
