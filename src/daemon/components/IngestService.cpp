#include <spdlog/spdlog.h>
#include <algorithm>
#include <unordered_map>
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
static void processTask(ServiceManager* sm, const InternalEventBus::StoreDocumentTask& task,
                        PendingPostIngestByMime& pendingPostIngest);
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
}

boost::asio::awaitable<void> IngestService::channelPoller() {
    const std::size_t channelCapacity =
        static_cast<std::size_t>(TuneAdvisor::storeDocumentChannelCapacity());
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::StoreDocumentTask>(
            "store_document_tasks", channelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    auto idleDelay = std::chrono::milliseconds(5);
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
            batchLimit = std::max(4, tunedBatch / 4);
        }

        InternalEventBus::StoreDocumentTask task;
        int processed = 0;
        PendingPostIngestByMime pendingPostIngest;
        while (processed < batchLimit && channel->try_pop(task)) {
            idleDelay = std::chrono::milliseconds(5);
            processTask(sm_, task, pendingPostIngest);
            ++processed;
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
            timer.expires_after(std::chrono::milliseconds(25));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    spdlog::info("[IngestService] Channel poller exited");
}

static void processTask(ServiceManager* sm, const InternalEventBus::StoreDocumentTask& task,
                        PendingPostIngestByMime& pendingPostIngest) {
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
        return;
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
}

} // namespace yams::daemon
