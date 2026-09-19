// pi-lens-ignore: fatal error
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
#include <type_traits>
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
#include <yams/daemon/components/dispatch_response.hpp>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/daemon_lifecycle.h>
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
DEFINE_REQUEST_HANDLER(DownloadStatusRequest, handleDownloadStatusRequest);
DEFINE_REQUEST_HANDLER(CancelDownloadJobRequest, handleCancelDownloadJobRequest);
DEFINE_REQUEST_HANDLER(ListDownloadJobsRequest, handleListDownloadJobsRequest);
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
DEFINE_REQUEST_HANDLER(GraphExploreRequest, handleGraphExploreRequest);
DEFINE_REQUEST_HANDLER(GraphSymbolLookupRequest, handleGraphSymbolLookupRequest);
DEFINE_REQUEST_HANDLER(GraphTraceRequest, handleGraphTraceRequest);
DEFINE_REQUEST_HANDLER(GraphImpactRequest, handleGraphImpactRequest);
DEFINE_REQUEST_HANDLER(GraphAffectedTestsRequest, handleGraphAffectedTestsRequest);
DEFINE_REQUEST_HANDLER(GraphPathHistoryRequest, handleGraphPathHistoryRequest);
DEFINE_REQUEST_HANDLER(GraphRepairRequest, handleGraphRepairRequest);
DEFINE_REQUEST_HANDLER(GraphValidateRequest, handleGraphValidateRequest);
DEFINE_REQUEST_HANDLER(KgIngestRequest, handleKgIngestRequest);
DEFINE_REQUEST_HANDLER(MetadataValueCountsRequest, handleMetadataValueCountsRequest);
DEFINE_REQUEST_HANDLER(MemorySyncRequest, handleMemorySyncRequest);
DEFINE_REQUEST_HANDLER(BatchRequest, handleBatchRequest);
template <> struct RequestHandlerTraits<RepairRequest> {
    static boost::asio::awaitable<Response> handle(RequestDispatcher*, const RepairRequest&) {
        co_return dispatch::makeErrorResponse(
            ErrorCode::InvalidArgument,
            "RepairRequest is streaming-only; use DaemonClient::callRepair or streaming IPC");
    }
};

#undef DEFINE_REQUEST_HANDLER

RequestDispatcher::RequestDispatcher(IDaemonLifecycle* lifecycle, ServiceManager* serviceManager,
                                     StateComponent* state)
    : lifecycle_(lifecycle), serviceManager_(serviceManager), state_(state) {}

RequestDispatcher::RequestDispatcher(IDaemonLifecycle* lifecycle, ServiceManager* serviceManager,
                                     StateComponent* state, DaemonMetrics* metrics)
    : lifecycle_(lifecycle), serviceManager_(serviceManager), state_(state), metrics_(metrics) {}

ServiceManager* RequestDispatcher::getServiceManager() const {
    return serviceManager_;
}

boost::asio::awaitable<Response>
RequestDispatcher::handleMemorySyncRequest(const MemorySyncRequest& req) {
    if (serviceManager_ == nullptr) {
        co_return dispatch::makeErrorResponse(ErrorCode::NotInitialized,
                                              "memory sync service manager is unavailable");
    }

    MemorySyncResponse response;
    switch (req.operation) {
        case MemorySyncOperation::Publish: {
            auto published = co_await dispatch::offload_to_worker(
                serviceManager_, [manager = serviceManager_, key = req.key, value = req.value] {
                    return manager->publishMemorySync(key, value);
                });
            if (!published) {
                co_return dispatch::makeErrorResponse(published.error().code,
                                                      published.error().message);
            }
            response.published = true;
            break;
        }
        case MemorySyncOperation::Read: {
            auto value = serviceManager_->readMemorySyncCached(req.key);
            if (!value) {
                co_return dispatch::makeErrorResponse(value.error().code, value.error().message);
            }
            response.value = std::move(value.value());
            break;
        }
        case MemorySyncOperation::Delete: {
            auto deleted = co_await dispatch::offload_to_worker(
                serviceManager_, [manager = serviceManager_, key = req.key] {
                    return manager->deleteMemorySync(key);
                });
            if (!deleted) {
                co_return dispatch::makeErrorResponse(deleted.error().code,
                                                      deleted.error().message);
            }
            response.published = true;
            break;
        }
        case MemorySyncOperation::Connect: {
            auto connected = co_await dispatch::offload_to_worker(
                serviceManager_, [manager = serviceManager_, connection = req.key] {
                    return manager->connectP2p(connection);
                });
            if (!connected) {
                co_return dispatch::makeErrorResponse(connected.error().code,
                                                      connected.error().message);
            }
            const auto& result = connected.value();
            response.value =
                nlohmann::json{
                    {"peer_node_id", result.peerNodeId}, {"peer_pin", result.peerPin},
                    {"deltas_sent", result.deltasSent},  {"deltas_received", result.deltasReceived},
                    {"merged", result.merged},           {"quarantined", result.quarantined}}
                    .dump();
            break;
        }
        case MemorySyncOperation::Disconnect: {
            auto disconnected = co_await dispatch::offload_to_worker(
                serviceManager_, [manager = serviceManager_, nodeId = req.key] {
                    return manager->disconnectP2p(nodeId);
                });
            if (!disconnected) {
                co_return dispatch::makeErrorResponse(disconnected.error().code,
                                                      disconnected.error().message);
            }
            response.value = nlohmann::json{{"node_id", req.key}, {"remembered", false}}.dump();
            break;
        }
        case MemorySyncOperation::Forget: {
            auto forgotten = co_await dispatch::offload_to_worker(
                serviceManager_, [manager = serviceManager_, nodeId = req.key] {
                    return manager->forgetP2pPeer(nodeId);
                });
            if (!forgotten) {
                co_return dispatch::makeErrorResponse(forgotten.error().code,
                                                      forgotten.error().message);
            }
            response.value = nlohmann::json{{"node_id", req.key}}.dump();
            break;
        }
        case MemorySyncOperation::Peers: {
            auto peers = serviceManager_->listP2pPeers();
            if (!peers) {
                co_return dispatch::makeErrorResponse(peers.error().code, peers.error().message);
            }
            nlohmann::json rows = nlohmann::json::array();
            for (const auto& peer : peers.value()) {
                rows.push_back({{"node_id", peer.nodeId},
                                {"spki_pin", peer.spkiPin},
                                {"corpus_id", peer.corpusId},
                                {"corpus_epoch", peer.corpusEpoch},
                                {"last_seen_version", peer.lastSeenVersion},
                                {"last_connected_ms", peer.lastConnectedMs},
                                {"endpoint", peer.endpoint},
                                {"remembered", peer.remembered},
                                {"pinned_by_operator", peer.pinnedByOperator}});
            }
            response.value = rows.dump();
            break;
        }
        case MemorySyncOperation::Identity: {
            auto identity = serviceManager_->getP2pIdentity();
            if (!identity) {
                co_return dispatch::makeErrorResponse(identity.error().code,
                                                      identity.error().message);
            }
            response.value = nlohmann::json{{"node_id", identity.value().nodeId},
                                            {"spki_pin", identity.value().spkiPin}}
                                 .dump();
            break;
        }
        case MemorySyncOperation::Enroll: {
            auto enrolled = co_await dispatch::offload_to_worker(
                serviceManager_, [manager = serviceManager_, nodeId = req.key, pin = req.value] {
                    return manager->enrollP2pPeer(nodeId, pin);
                });
            if (!enrolled) {
                co_return dispatch::makeErrorResponse(enrolled.error().code,
                                                      enrolled.error().message);
            }
            response.value = nlohmann::json{
                {"node_id", req.key},
                {"spki_pin", req.value},
                {"pinned_by_operator",
                 true}}.dump();
            break;
        }
        case MemorySyncOperation::Status:
            break;
        default:
            co_return dispatch::makeErrorResponse(ErrorCode::InvalidArgument,
                                                  "invalid memory sync operation");
    }

    auto status = serviceManager_->getMemorySyncStatus();
    if (!status) {
        co_return dispatch::makeErrorResponse(status.error().code, status.error().message);
    }
    response.started = status.value().started;
    response.records = status.value().records;
    response.quarantinedRecords = status.value().quarantinedRecords;
    response.authFailures = status.value().authFailures;
    response.successfulCycles = status.value().successfulCycles;
    response.failedCycles = status.value().failedCycles;
    response.lastSuccessAgeMs = status.value().lastSuccessAgeMs;
    response.backend = std::move(status.value().backend);
    response.nodeId = std::move(status.value().nodeId);
    response.corpusId = std::move(status.value().corpusId);
    response.corpusEpoch = status.value().corpusEpoch;
    response.mode = std::move(status.value().mode);
    response.trustMode = std::move(status.value().trustMode);
    // pi-lens-ignore: clang:no_member -- fields are additive and generated headers lag clangd.
    response.peerCount = status.value().peerCount;
    co_return response;
}

void RequestDispatcher::SearchAdmissionGuard::release() {
    if (serviceManager_ == nullptr) {
        return;
    }
    if (started_) {
        serviceManager_->onSearchRequestFinished();
    } else {
        serviceManager_->onSearchRequestRejected();
    }
    serviceManager_ = nullptr;
    started_ = false;
}

RequestDispatcher::SearchAdmissionGuard::~SearchAdmissionGuard() {
    release();
}

boost::asio::awaitable<RequestDispatcher::AdmissionGuard>
RequestDispatcher::acquireBoundedAdmission(std::atomic<uint64_t>& activeCounter,
                                           std::atomic<uint64_t>& rejectedCounter, uint32_t limit,
                                           uint32_t maxWaitMs,
                                           std::atomic<uint64_t>* testOverride) {
    if (limit == 0) {
        co_return AdmissionGuard{};
    }

    if (testOverride != nullptr && testOverride->load(std::memory_order_acquire) >= limit) {
        rejectedCounter.fetch_add(1, std::memory_order_acq_rel);
        co_return AdmissionGuard{};
    }

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(std::max<uint32_t>(1, maxWaitMs));
    auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor);

    while (true) {
        auto current = activeCounter.load(std::memory_order_acquire);
        while (current < limit) {
            if (activeCounter.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
                co_return AdmissionGuard{&activeCounter};
            }
        }

        if (std::chrono::steady_clock::now() >= deadline) {
            rejectedCounter.fetch_add(1, std::memory_order_acq_rel);
            co_return AdmissionGuard{};
        }

        timer.expires_after(std::chrono::milliseconds(5));
        co_await timer.async_wait(boost::asio::use_awaitable);
    }
}

void RequestDispatcher::recordRejected(std::atomic<uint64_t>& counter) {
    counter.fetch_add(1, std::memory_order_acq_rel);
}

boost::asio::awaitable<RequestDispatcher::SearchAdmissionGuard>
RequestDispatcher::acquireSearchAdmission(uint32_t concurrencyCap) {
    if (serviceManager_ == nullptr) {
        co_return SearchAdmissionGuard{};
    }

    serviceManager_->onSearchRequestQueued();
    const auto maxWaitMs = std::max<uint32_t>(1000u, TuneAdvisor::listAdmissionWaitMs());
    auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(maxWaitMs);

    while (true) {
        if (serviceManager_->tryStartSearchRequest(concurrencyCap)) {
            co_return SearchAdmissionGuard{serviceManager_, true};
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            recordRejected(state_->stats.searchRequestsRejected);
            co_return SearchAdmissionGuard{serviceManager_, false};
        }
        timer.expires_after(std::chrono::milliseconds(5));
        co_await timer.async_wait(boost::asio::use_awaitable);
    }
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

    auto lifecycleSnapshot = lifecycle_ ? lifecycle_->getLifecycleSnapshot() : LifecycleSnapshot{};
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
                co_return dispatch::makeErrorResponse(
                    ErrorCode::InvalidState,
                    "Metadata repository not ready. Please try again shortly.");
            }
        }
        if (!contentStoreReady) {
            std::string error_detail = "Content store not available. Please try again shortly.";
            if (serviceManager_) {
                const auto& cs_error = serviceManager_->getContentStoreError();
                if (!cs_error.empty()) {
                    error_detail += " Details: " + cs_error;
                }
            }
            co_return dispatch::makeErrorResponse(ErrorCode::InvalidState, error_detail);
        }
    }

    Response out;
    try {
        if (req.valueless_by_exception()) {
            spdlog::warn(
                "RequestDispatcher: received valueless request variant; replying with error");
            co_return dispatch::makeErrorResponse(ErrorCode::InvalidData,
                                                  "Malformed request (variant is valueless)");
        }
    } catch (...) {
        co_return dispatch::makeErrorResponse(ErrorCode::InvalidData,
                                              "Malformed request (variant check failed)");
    }
    const bool countCurrentStatusRequest = std::holds_alternative<StatusRequest>(req);
    try {
        if (state_ && !countCurrentStatusRequest) {
            state_->stats.requestsProcessed.fetch_add(1, std::memory_order_relaxed);
        }
    } catch (...) { // NOLINT(bugprone-empty-catch): stats failures must not interrupt response
    }
    try {
        out = co_await std::visit(
            [this](auto&& arg) -> boost::asio::awaitable<Response> {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, CancelDownloadJobRequest>) {
                    return handleCancelDownloadJobRequest(arg);
                }
                return RequestHandlerTraits<T>::handle(this, arg);
            },
            req);
    } catch (const std::exception& e) {
        spdlog::error("RequestDispatcher::dispatch exception: {}", e.what());
        co_return dispatch::makeErrorResponse(
            ErrorCode::InternalError, std::string("Failed to process request: ") + e.what());
    } catch (...) {
        spdlog::error("RequestDispatcher::dispatch unknown exception");
        co_return dispatch::makeErrorResponse(ErrorCode::InternalError,
                                              "Failed to process request: unknown error");
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
    } catch (const std::exception& e) {
        spdlog::trace("RequestDispatcher: CLI executor unavailable: {}", e.what());
    } catch (...) {
        spdlog::trace("RequestDispatcher: CLI executor unavailable");
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

    if (lifecycle_) {
        bool graceful = req.graceful;
        // NOLINTNEXTLINE(concurrency-mt-unsafe): read-only test-mode compatibility check.
        bool inTestMode = std::getenv("YAMS_TESTING") != nullptr;
#if defined(YAMS_TESTING) || defined(YAMS_TEST_LIFECYCLE_CONTROLS)
        // NOLINTNEXTLINE(concurrency-mt-unsafe): read-only test-mode compatibility check.
        inTestMode = inTestMode || std::getenv("YAMS_TEST_SAFE_SINGLE_INSTANCE") != nullptr;
#endif
        lifecycle_->requestShutdown(graceful, inTestMode);
    }

    co_return SuccessResponse{"Shutdown initiated"};
}
} // namespace yams::daemon
