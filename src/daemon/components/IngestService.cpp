#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/DocumentRequestMapper.h>
#include <yams/daemon/components/IngestService.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/profiling.h>

namespace yams::daemon {

// Forward declare helper
using PendingPostIngestByMime = std::unordered_map<std::string, std::vector<PostIngestQueue::Task>>;
static PendingPostIngestByMime processTask(ServiceManager* sm,
                                           const InternalEventBus::StoreDocumentTask& task);
static PendingPostIngestByMime
processTaskBatch(ServiceManager* sm, std::vector<InternalEventBus::StoreDocumentTask> tasks);
static std::future<PendingPostIngestByMime>
dispatchBatchToCoordinator(ServiceManager* sm, WorkCoordinator* coordinator,
                           std::vector<InternalEventBus::StoreDocumentTask> tasks) {
    std::promise<PendingPostIngestByMime> promise;
    auto future = promise.get_future();

    // channelPoller runs on this coordinator and waits on the returned future. With only one
    // worker, posting back to the same executor would deadlock that sole worker.
    if (!coordinator || !coordinator->isRunning() || coordinator->getWorkerCount() < 2) {
        try {
            promise.set_value(processTaskBatch(sm, std::move(tasks)));
        } catch (...) {
            promise.set_exception(std::current_exception());
        }
        return future;
    }

    auto sharedPromise =
        std::make_shared<std::promise<PendingPostIngestByMime>>(std::move(promise));
    auto executor = coordinator->getExecutor();
    boost::asio::post(executor, [sm, tasks = std::move(tasks), sharedPromise]() mutable {
        try {
            sharedPromise->set_value(processTaskBatch(sm, std::move(tasks)));
        } catch (...) {
            sharedPromise->set_exception(std::current_exception());
        }
    });

    return future;
}

static void mergePendingPostIngest(PendingPostIngestByMime& target,
                                   PendingPostIngestByMime&& source) {
    for (auto& [mime, hashes] : source) {
        if (hashes.empty()) {
            continue;
        }
        auto& dst = target[mime];
        dst.insert(dst.end(), std::make_move_iterator(hashes.begin()),
                   std::make_move_iterator(hashes.end()));
    }
}

static bool getEnvIngestCorrectnessMode() {
    static const bool val = []() {
        if (const char* s = std::getenv("YAMS_INGEST_CORRECTNESS_MODE")) {
            std::string value(s);
            std::transform(value.begin(), value.end(), value.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            return !(value == "0" || value == "false" || value == "no" || value == "off");
        }
        return true;
    }();
    return val;
}

static void flushPendingPostIngestBatches(ServiceManager* sm, PendingPostIngestByMime& pending) {
    if (!sm || !sm->getPostIngestQueue() || pending.empty()) {
        pending.clear();
        return;
    }
    for (auto& [mime, tasks] : pending) {
        if (tasks.empty()) {
            continue;
        }
        sm->enqueuePostIngestTasks(std::move(tasks));
    }
    pending.clear();
}

IngestService::IngestService(ServiceManager* sm, WorkCoordinator* coordinator)
    : sm_(sm), coordinator_(coordinator), strand_(coordinator_->makeStrand()) {}

IngestService::~IngestService() {
    stop();
}

void IngestService::start() {
    YAMS_ZONE_SCOPED_N("IngestService::start");
    bool expected = false;
    if (!startGuard_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        spdlog::debug("[IngestService] start() ignored; poller already active or starting");
        return;
    }

    stop_.store(false, std::memory_order_release);
    coordinator_->spawnDetached(strand_, channelPoller());
}

void IngestService::notifyLifecycle() {
    YAMS_ZONE_SCOPED_N("IngestService::notifyLifecycle");
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    lifecycleCv_.notify_all();
}

void IngestService::stop() {
    YAMS_ZONE_SCOPED_N("IngestService::stop");
    stop_.store(true);
    notifyLifecycle();
    spdlog::info("[IngestService] Stop requested");

    constexpr auto kWaitBudget = std::chrono::milliseconds(15000);
    auto deadline = std::chrono::steady_clock::now() + kWaitBudget;
    bool exited = false;
    {
        std::unique_lock<std::mutex> lock(lifecycleMutex_);
        exited = lifecycleCv_.wait_until(
            lock, deadline, [this]() { return !running_.load(std::memory_order_acquire); });
    }

    if (!exited) {
        spdlog::warn("[IngestService] Stop timed out after {}ms waiting for channel poller to "
                     "exit",
                     kWaitBudget.count());
    } else {
        startGuard_.store(false, std::memory_order_release);
    }
}

boost::asio::awaitable<void> IngestService::channelPoller() {
    YAMS_ZONE_SCOPED_N("IngestService::channelPoller");
    running_.store(true, std::memory_order_release);
    struct RunningGuard {
        std::atomic<bool>& running;
        std::atomic<bool>& startGuard;
        IngestService* self;
        ~RunningGuard() {
            running.store(false, std::memory_order_release);
            startGuard.store(false, std::memory_order_release);
            if (self) {
                self->notifyLifecycle();
            }
        }
    } runningGuard{running_, startGuard_, this};

    const std::size_t channelCapacity =
        static_cast<std::size_t>(TuneAdvisor::storeDocumentChannelCapacity());
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::StoreDocumentTask>(
            "store_document_tasks", channelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    auto idleDelay = std::chrono::milliseconds(5);
    const bool correctnessMode = getEnvIngestCorrectnessMode();

    auto maxIdleDelay = []() {
        auto snap = TuningSnapshotRegistry::instance().get();
        if (snap && snap->daemonIdle) {
            return std::chrono::milliseconds(
                std::max<uint32_t>(10u, std::min<uint32_t>(TuneAdvisor::idleTickMs(), 50u)));
        }
        uint32_t pollMs = snap ? snap->workerPollMs : TuneAdvisor::workerPollMs();
        return std::chrono::milliseconds(std::max<uint32_t>(2u, std::min<uint32_t>(pollMs, 10u)));
    };

    while (!stop_.load()) {
        // Keep request admission independent from downstream post-ingest transaction sizing.
        // Reuse the configured post-ingest bound so storage does not fragment a batch before the
        // downstream queue applies the same transaction limit.
        int batchLimit =
            static_cast<int>(std::max<std::uint32_t>(1u, TuneAdvisor::postIngestBatchSize()));
        bool underPressure = false;
        try {
            auto snap = ResourceGovernor::instance().getSnapshot();
            if (snap.cpuUsagePercent > 80)
                batchLimit = 8;
            else if (snap.cpuUsagePercent > 60)
                batchLimit = 10;
            else if (snap.cpuUsagePercent > 40)
                batchLimit = 12;
        } catch (...) {
        }
        if (!ResourceGovernor::instance().canAdmitWork()) {
            // Avoid full ingestion deadlock under pressure: keep draining slowly.
            underPressure = true;
            batchLimit = correctnessMode ? 32 : 4;
        }

        const std::size_t queueDepth = channel->size_approx();
        const std::size_t queueCap = std::max<std::size_t>(1, channel->capacity());
        const bool backlogHigh = (queueDepth * 100u / queueCap) >= 70u;
        if (correctnessMode && backlogHigh) {
            batchLimit = 64;
        }

        InternalEventBus::StoreDocumentTask task;
        int processed = 0;
        std::vector<InternalEventBus::StoreDocumentTask> batch;
        batch.reserve(static_cast<std::size_t>(batchLimit));
        while (processed < batchLimit && channel->try_pop(task)) {
            idleDelay = std::chrono::milliseconds(5);
            batch.push_back(std::move(task));
            ++processed;
        }

        PendingPostIngestByMime pendingPostIngest;
        if (!batch.empty()) {
            auto future = dispatchBatchToCoordinator(sm_, coordinator_, std::move(batch));
            try {
                mergePendingPostIngest(pendingPostIngest, future.get());
            } catch (const std::exception& e) {
                spdlog::error("[IngestService] task batch failed: {}", e.what());
            } catch (...) {
                spdlog::error("[IngestService] task batch failed: unknown exception");
            }
        }

        if (!pendingPostIngest.empty()) {
            flushPendingPostIngestBatches(sm_, pendingPostIngest);
        }
        if (processed == 0) {
            timer.expires_after(idleDelay);
            co_await timer.async_wait(boost::asio::use_awaitable);
            const auto maxIdle = maxIdleDelay();
            if (idleDelay < maxIdle) {
                idleDelay *= 2;
                if (idleDelay > maxIdle) {
                    idleDelay = maxIdle;
                }
            }
        } else if (underPressure) {
            if (correctnessMode && backlogHigh) {
                continue;
            }
            timer.expires_after(std::chrono::milliseconds(10));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    spdlog::info("[IngestService] Channel poller exited");
}

static void appendPostIngestTask(ServiceManager* sm, const AddDocumentRequest& request,
                                 const app::services::StoreDocumentResponse& response,
                                 PendingPostIngestByMime& pendingPostIngest) {
    auto postIngestQueue = sm ? sm->getPostIngestQueue() : nullptr;
    if (!postIngestQueue || response.hash.empty()) {
        spdlog::warn("[IngestService] Post-ingest skipped: sm={} piq={} hash_empty={}",
                     sm != nullptr, postIngestQueue != nullptr, response.hash.empty());
        return;
    }

    PostIngestQueue::Task postIngest;
    postIngest.hash = response.hash;
    postIngest.mime = request.mimeType;
    postIngest.documentId = response.documentId;
    postIngest.noEmbeddings = request.noEmbeddings;
    pendingPostIngest[request.mimeType].push_back(std::move(postIngest));
}

static app::services::StoreDocumentRequest
mapStoreTask(const InternalEventBus::StoreDocumentTask& task) {
    auto request = yams::daemon::dispatch::mapStoreDocumentRequest(task.request);
    request.precomputedHash = task.precomputedHash;
    request.precomputedFileSize = task.precomputedFileSize;
    request.precomputedLastWriteTimeNs = task.precomputedLastWriteTimeNs;
    request.skipInlineContentIndexing = true;
    request.combineMetadataPathTree = true;
    return request;
}

static void collectStoreResult(ServiceManager* sm, const AddDocumentRequest& request,
                               const Result<app::services::StoreDocumentResponse>& result,
                               PendingPostIngestByMime& pendingPostIngest) {
    if (!result) {
        spdlog::error("Failed to store document from ingest queue: {}", result.error().message);
        return;
    }

    const auto& response = result.value();
    spdlog::debug("Successfully stored document from ingest queue: {} "
                  "(bytesStored={}, bytesDeduped={})",
                  response.hash, response.bytesStored, response.bytesDeduped);
    appendPostIngestTask(sm, request, response, pendingPostIngest);
}

static PendingPostIngestByMime processTask(ServiceManager* sm,
                                           const InternalEventBus::StoreDocumentTask& task) {
    PendingPostIngestByMime pendingPostIngest;
    const auto& req = task.request;

    try {
        spdlog::debug(
            "[IngestService] task path='{}' name='{}' recursive={} has_content={} noEmbeddings={} "
            "include_count={} exclude_count={} tag_count={} metadata={}",
            req.path, req.name, req.recursive ? 1 : 0, req.content.empty() ? 0 : 1,
            req.noEmbeddings ? 1 : 0, req.includePatterns.size(), req.excludePatterns.size(),
            req.tags.size(), req.metadata.size());
    } catch (...) { // NOLINT(bugprone-empty-catch): logging failures must not interrupt ingest
    }

    bool isDir = (!req.path.empty() && std::filesystem::is_directory(req.path));

    if (!isDir && req.path.empty() && (req.content.empty() || req.name.empty())) {
        spdlog::warn("Failed to store document from ingest queue: {}",
                     "Provide either 'path' or 'content' + 'name'");
        return pendingPostIngest;
    }

    auto appContext = sm->getAppContext();

    if ((req.recursive || isDir) && !req.path.empty() && isDir) {
        auto indexingService = yams::app::services::makeIndexingService(appContext);
        app::services::AddDirectoryRequest serviceReq;
        serviceReq.directoryPath = req.path;
        serviceReq.collection = req.collection;
        serviceReq.tags = req.tags;
        serviceReq.includePatterns = req.includePatterns;
        serviceReq.excludePatterns = req.excludePatterns;
        serviceReq.recursive = true;
        serviceReq.snapshotId = req.snapshotId;
        serviceReq.snapshotLabel = req.snapshotLabel;
        serviceReq.sessionId = req.sessionId; // Session-isolated memory (PBI-082)
        serviceReq.noEmbeddings = req.noEmbeddings;
        serviceReq.noGitignore = req.noGitignore; // Gitignore handling
        for (const auto& [key, value] : req.metadata) {
            serviceReq.metadata[key] = value;
        }

        auto result = indexingService->addDirectory(serviceReq);
        if (!result) {
            spdlog::error("Failed to store directory from ingest queue: {}",
                          result.error().message);
        } else {
            spdlog::info("Successfully stored directory from ingest queue: {}", req.path);
            const auto& serviceResp = result.value();
            if (sm && !serviceResp.snapshotId.empty()) {
                sm->onSnapshotPersisted();
            }

            // NOTE: addDirectory() already dispatches successful file hashes into the
            // post-ingest InternalEventBus as each file is stored. Re-enqueueing the full
            // response batch here duplicates post-ingest work (including title/NL extraction)
            // and can make benchmark KG readiness wait on hundreds of redundant jobs.
        }
    } else {
        auto docService = yams::app::services::makeDocumentService(appContext);
        auto result = docService->store(mapStoreTask(task));
        collectStoreResult(sm, req, result, pendingPostIngest);
    }

    return pendingPostIngest;
}

static PendingPostIngestByMime
processTaskBatch(ServiceManager* sm, std::vector<InternalEventBus::StoreDocumentTask> tasks) {
    PendingPostIngestByMime pendingPostIngest;
    if (tasks.size() < 2) {
        if (!tasks.empty()) {
            mergePendingPostIngest(pendingPostIngest, processTask(sm, tasks.front()));
        }
        return pendingPostIngest;
    }

    std::vector<const InternalEventBus::StoreDocumentTask*> documentTasks;
    documentTasks.reserve(tasks.size());
    for (const auto& task : tasks) {
        const auto& request = task.request;
        std::error_code pathError;
        const bool isDirectory = !request.path.empty() &&
                                 std::filesystem::is_directory(request.path, pathError) &&
                                 !pathError;
        const bool validDocument =
            !isDirectory &&
            (!request.path.empty() || (!request.content.empty() && !request.name.empty()));
        if (!pathError && validDocument && !request.recursive) {
            documentTasks.push_back(&task);
        } else {
            try {
                mergePendingPostIngest(pendingPostIngest, processTask(sm, task));
            } catch (const std::exception& error) {
                spdlog::error("[IngestService] task failed: {}", error.what());
            } catch (...) {
                spdlog::error("[IngestService] task failed: unknown exception");
            }
        }
    }

    if (documentTasks.empty()) {
        return pendingPostIngest;
    }
    if (documentTasks.size() == 1) {
        mergePendingPostIngest(pendingPostIngest, processTask(sm, *documentTasks.front()));
        return pendingPostIngest;
    }

    auto appContext = sm->getAppContext();
    auto documentService = yams::app::services::makeDocumentService(appContext);
    std::vector<app::services::StoreDocumentRequest> requests;
    requests.reserve(documentTasks.size());
    for (const auto* task : documentTasks) {
        requests.push_back(mapStoreTask(*task));
    }

    auto results = documentService->storeBatch(requests);
    for (std::size_t i = 0; i < documentTasks.size(); ++i) {
        const auto& request = documentTasks[i]->request;
        if (i >= results.size()) {
            spdlog::error("Failed to store document wave item {}: missing batch result", i);
            continue;
        }
        collectStoreResult(sm, request, results[i], pendingPostIngest);
    }

    return pendingPostIngest;
}

} // namespace yams::daemon
