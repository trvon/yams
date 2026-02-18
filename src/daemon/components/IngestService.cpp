#include <spdlog/spdlog.h>
#include <algorithm>
#include <future>
#include <memory>
#include <chrono>
#include <cctype>
#include <string>
#include <thread>
#include <unordered_map>
#include <boost/asio/post.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/IngestService.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>

namespace yams::daemon {

// Forward declare helper
using PendingPostIngestByMime = std::unordered_map<std::string, std::vector<std::string>>;
static PendingPostIngestByMime processTask(ServiceManager* sm,
                                           const InternalEventBus::StoreDocumentTask& task);
static std::future<PendingPostIngestByMime>
dispatchTaskToCoordinator(ServiceManager* sm, WorkCoordinator* coordinator,
                          InternalEventBus::StoreDocumentTask task) {
    std::promise<PendingPostIngestByMime> promise;
    auto future = promise.get_future();

    if (!coordinator || !coordinator->isRunning()) {
        try {
            promise.set_value(processTask(sm, task));
        } catch (...) {
            promise.set_exception(std::current_exception());
        }
        return future;
    }

    auto sharedPromise =
        std::make_shared<std::promise<PendingPostIngestByMime>>(std::move(promise));
    auto executor = coordinator->getExecutor();
    boost::asio::post(executor, [sm, task = std::move(task), sharedPromise]() mutable {
        try {
            sharedPromise->set_value(processTask(sm, task));
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

static std::size_t getEnvIngestParallelism() {
    static const std::size_t val = []() {
        if (const char* env = std::getenv("YAMS_INGEST_PARALLELISM"); env && *env) {
            try {
                return static_cast<std::size_t>(std::stoul(env));
            } catch (...) {
            }
        }
        return std::size_t{0};
    }();
    return val;
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

static std::size_t resolveIngestParallelism(WorkCoordinator* coordinator, bool underPressure) {
    std::size_t parallelism = getEnvIngestParallelism();

    if (parallelism == 0) {
        const std::size_t workers = coordinator ? coordinator->getWorkerCount() : 0;
        if (workers > 1) {
            parallelism = std::max<std::size_t>(2, workers / 2);
        } else {
            parallelism = 2;
        }
    }

    const bool correctnessMode = getEnvIngestCorrectnessMode();

    const std::size_t maxParallel = correctnessMode ? 16 : 8;
    parallelism = std::clamp<std::size_t>(parallelism, 1, maxParallel);
    if (underPressure) {
        if (correctnessMode) {
            parallelism =
                std::min<std::size_t>(maxParallel, std::max<std::size_t>(2, parallelism * 2));
        } else {
            parallelism = std::max<std::size_t>(1, parallelism / 2);
        }
    }
    return parallelism;
}

static void flushPendingPostIngestBatches(ServiceManager* sm, PendingPostIngestByMime& pending) {
    if (!sm || !sm->getPostIngestQueue() || pending.empty()) {
        pending.clear();
        return;
    }
    for (auto& [mime, hashes] : pending) {
        if (hashes.empty()) {
            continue;
        }
        sm->enqueuePostIngestBatch(hashes, mime);
    }
    pending.clear();
}

IngestService::IngestService(ServiceManager* sm, WorkCoordinator* coordinator)
    : sm_(sm), coordinator_(coordinator), strand_(coordinator_->makeStrand()) {}

IngestService::~IngestService() {
    stop();
}

void IngestService::start() {
    if (!stop_.load()) {
        boost::asio::co_spawn(strand_, channelPoller(), boost::asio::detached);
    }
}

void IngestService::stop() {
    stop_.store(true);
    spdlog::info("[IngestService] Stop requested");

    constexpr auto kWaitStep = std::chrono::milliseconds(1);
    constexpr auto kWaitBudget = std::chrono::milliseconds(2000);
    auto waited = std::chrono::milliseconds(0);
    while (running_.load(std::memory_order_acquire) && waited < kWaitBudget) {
        std::this_thread::sleep_for(kWaitStep);
        waited += kWaitStep;
    }

    if (running_.load(std::memory_order_acquire)) {
        spdlog::warn("[IngestService] Stop timed out waiting for channel poller to exit");
    }
}

boost::asio::awaitable<void> IngestService::channelPoller() {
    running_.store(true, std::memory_order_release);
    struct RunningGuard {
        std::atomic<bool>& running;
        ~RunningGuard() { running.store(false, std::memory_order_release); }
    } runningGuard{running_};

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
        uint32_t pollMs = snap ? snap->workerPollMs : TuneAdvisor::workerPollMs();
        return std::chrono::milliseconds(std::max<uint32_t>(50, pollMs));
    };

    while (!stop_.load()) {
        // CPU-aware batch sizing: keep enough fan-out to avoid singleton post-ingest flushes.
        const int tunedBatch =
            std::clamp<int>(static_cast<int>(TuneAdvisor::postIngestBatchSize()) * 4, 16, 128);
        int batchLimit = tunedBatch;
        bool underPressure = false;
        try {
            auto snap = ResourceGovernor::instance().getSnapshot();
            if (snap.cpuUsagePercent > 80)
                batchLimit = std::max(8, tunedBatch / 4);
            else if (snap.cpuUsagePercent > 60)
                batchLimit = std::max(10, tunedBatch / 3);
            else if (snap.cpuUsagePercent > 40)
                batchLimit = std::max(12, tunedBatch / 2);
        } catch (...) {
        }
        if (!ResourceGovernor::instance().canAdmitWork()) {
            // Avoid full ingestion deadlock under pressure: keep draining slowly.
            underPressure = true;
            batchLimit =
                correctnessMode ? std::max(32, tunedBatch * 2) : std::max(4, tunedBatch / 4);
        }

        const std::size_t queueDepth = channel->size_approx();
        const std::size_t queueCap = std::max<std::size_t>(1, channel->capacity());
        const bool backlogHigh = (queueDepth * 100u / queueCap) >= 70u;
        if (correctnessMode && backlogHigh) {
            batchLimit = std::min(512, std::max(batchLimit, tunedBatch * 4));
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
            const std::size_t parallelism = resolveIngestParallelism(coordinator_, underPressure);

            for (std::size_t offset = 0; offset < batch.size();) {
                const std::size_t waveSize =
                    std::min<std::size_t>(parallelism, batch.size() - offset);
                std::vector<std::future<PendingPostIngestByMime>> futures;
                futures.reserve(waveSize);

                for (std::size_t i = 0; i < waveSize; ++i) {
                    auto taskCopy = std::move(batch[offset + i]);
                    futures.push_back(
                        dispatchTaskToCoordinator(sm_, coordinator_, std::move(taskCopy)));
                }

                for (auto& fut : futures) {
                    try {
                        mergePendingPostIngest(pendingPostIngest, fut.get());
                    } catch (const std::exception& e) {
                        spdlog::error("[IngestService] parallel task failed: {}", e.what());
                    } catch (...) {
                        spdlog::error("[IngestService] parallel task failed: unknown exception");
                    }
                }

                offset += waveSize;
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
            timer.expires_after(std::chrono::milliseconds(25));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    spdlog::info("[IngestService] Channel poller exited");
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

            if (sm && sm->getPostIngestQueue()) {
                constexpr std::size_t kBatchSize = 128;
                auto& pending = pendingPostIngest[std::string()];
                pending.reserve(pending.size() + std::min<std::size_t>(kBatchSize, 256));
                for (const auto& r : serviceResp.results) {
                    if (!r.success || r.hash.empty()) {
                        continue;
                    }
                    pending.push_back(r.hash);
                    if (pending.size() >= kBatchSize) {
                        sm->enqueuePostIngestBatch(pending, std::string());
                        pending.clear();
                    }
                }
            }
        }
    } else {
        auto docService = yams::app::services::makeDocumentService(appContext);
        app::services::StoreDocumentRequest serviceReq;
        serviceReq.path = req.path;
        serviceReq.content = req.content;
        serviceReq.name = req.name;
        serviceReq.mimeType = req.mimeType;
        serviceReq.disableAutoMime = req.disableAutoMime;
        serviceReq.tags = req.tags;
        serviceReq.metadata.clear();
        for (const auto& [key, value] : req.metadata) {
            serviceReq.metadata[key] = value;
        }
        serviceReq.collection = req.collection;
        serviceReq.snapshotId = req.snapshotId;
        serviceReq.snapshotLabel = req.snapshotLabel;
        serviceReq.sessionId = req.sessionId; // Session-isolated memory (PBI-082)
        serviceReq.noEmbeddings = req.noEmbeddings;

        auto result = docService->store(serviceReq);
        if (!result) {
            spdlog::error("Failed to store document from ingest queue: {}", result.error().message);
        } else {
            const auto& serviceResp = result.value();
            spdlog::debug("Successfully stored document from ingest queue: {} "
                          "(bytesStored={}, bytesDeduped={})",
                          serviceResp.hash, serviceResp.bytesStored, serviceResp.bytesDeduped);

            if (sm && sm->getPostIngestQueue() && !serviceResp.hash.empty()) {
                spdlog::debug("[IngestService] Enqueuing post-ingest for hash={}",
                              serviceResp.hash);
                pendingPostIngest[req.mimeType].push_back(serviceResp.hash);
            } else {
                spdlog::warn("[IngestService] Post-ingest skipped: sm={} piq={} hash_empty={}",
                             sm != nullptr, sm ? (sm->getPostIngestQueue() != nullptr) : false,
                             serviceResp.hash.empty());
            }
        }
    }

    return pendingPostIngest;
}

} // namespace yams::daemon
