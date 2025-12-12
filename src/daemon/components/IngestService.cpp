#include <spdlog/spdlog.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/IngestService.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/WorkCoordinator.h>

namespace yams::daemon {

// Forward declare helper
static void processTask(ServiceManager* sm, const InternalEventBus::StoreDocumentTask& task);

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
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::StoreDocumentTask>(
            "store_document_tasks", 4096);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    while (!stop_.load()) {
        InternalEventBus::StoreDocumentTask task;
        if (channel->try_pop(task)) {
            processTask(sm_, task);
        } else {
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(boost::asio::use_awaitable);
        }
    }

    spdlog::info("[IngestService] Channel poller exited");
}

static void processTask(ServiceManager* sm, const InternalEventBus::StoreDocumentTask& task) {
    const auto& req = task.request;

    try {
        spdlog::info("[IngestService] task path='{}' name='{}' recursive={} has_content={} "
                     "noEmbeddings={} include_count={} exclude_count={} tag_count={} metadata={}",
                     req.path, req.name, req.recursive ? 1 : 0, req.content.empty() ? 0 : 1,
                     req.noEmbeddings ? 1 : 0, req.includePatterns.size(),
                     req.excludePatterns.size(), req.tags.size(), req.metadata.size());
    } catch (...) {
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
                for (const auto& r : serviceResp.results) {
                    if (r.success && !r.hash.empty()) {
                        sm->enqueuePostIngest(r.hash, std::string());
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
            spdlog::info("Successfully stored document from ingest queue: {} "
                         "(bytesStored={}, bytesDeduped={})",
                         serviceResp.hash, serviceResp.bytesStored, serviceResp.bytesDeduped);

            if (sm && sm->getPostIngestQueue() && !serviceResp.hash.empty()) {
                sm->enqueuePostIngest(serviceResp.hash, req.mimeType);
            }
        }
    }
}

} // namespace yams::daemon
