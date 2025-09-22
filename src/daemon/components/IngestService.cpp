#include <spdlog/spdlog.h>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/IngestService.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>

namespace yams::daemon {

IngestService::IngestService(ServiceManager* sm, size_t threads) : sm_(sm) {
    if (threads == 0)
        threads = 1;
    threads_.reserve(threads);
}

IngestService::~IngestService() {
    stop();
}

void IngestService::start() {
    if (threads_.empty()) { // Start threads only once
        size_t num_threads = threads_.capacity();
        for (size_t i = 0; i < num_threads; ++i) {
            threads_.emplace_back([this](yams::compat::stop_token token) { workerLoop(token); });
        }
    }
}

void IngestService::stop() {
    if (!stop_.exchange(true)) {
        for (auto& t : threads_) {
            if (t.joinable()) {
                t.request_stop();
                t.join();
            }
        }
    }
}

void IngestService::workerLoop(yams::compat::stop_token token) {
    auto appContext = sm_->getAppContext();
    auto docService = yams::app::services::makeDocumentService(appContext);
    auto indexingService = yams::app::services::makeIndexingService(appContext);
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::StoreDocumentTask>(
            "store_document_tasks", 4096);

    while (!token.stop_requested()) {
        InternalEventBus::StoreDocumentTask task;
        if (channel->try_pop(task)) {
            const auto& req = task.request;
            try {
                spdlog::debug("[IngestService] Popped StoreDocumentTask from InternalEventBus "
                              "(path='{}', name='{}', recursive={}, noEmbeddings={})",
                              req.path, req.name, req.recursive ? 1 : 0, req.noEmbeddings ? 1 : 0);
            } catch (...) {
            }
            bool isDir = (!req.path.empty() && std::filesystem::is_directory(req.path));

            // Drop clearly invalid tasks early to avoid churn and potential crashes
            // in downstream services. Requirements: either a path, or (content + name).
            if (!isDir && req.path.empty() && (req.content.empty() || req.name.empty())) {
                spdlog::warn("Failed to store document from ingest queue: {}",
                             "Provide either 'path' or 'content' + 'name'");
                continue;
            }

            if ((req.recursive || isDir) && !req.path.empty() && isDir) {
                app::services::AddDirectoryRequest serviceReq;
                serviceReq.directoryPath = req.path;
                serviceReq.collection = req.collection;
                serviceReq.tags = req.tags;
                serviceReq.includePatterns = req.includePatterns;
                serviceReq.excludePatterns = req.excludePatterns;
                serviceReq.recursive = true;
                serviceReq.deferExtraction = true;
                for (const auto& [key, value] : req.metadata) {
                    serviceReq.metadata[key] = value;
                }
                auto result = indexingService->addDirectory(serviceReq);
                if (!result) {
                    spdlog::error("Failed to store directory from ingest queue: {}",
                                  result.error().message);
                } else {
                    spdlog::info("Successfully stored directory from ingest queue: {}", req.path);
                }
            } else {
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
                serviceReq.noEmbeddings = req.noEmbeddings;
                serviceReq.deferExtraction = true; // Always defer for async ingestion

                auto result = docService->store(serviceReq);
                if (!result) {
                    spdlog::error("Failed to store document from ingest queue: {}",
                                  result.error().message);
                } else {
                    const auto& serviceResp = result.value();
                    spdlog::info("Successfully stored document from ingest queue: {} "
                                 "(bytesStored={}, bytesDeduped={})",
                                 serviceResp.hash, serviceResp.bytesStored,
                                 serviceResp.bytesDeduped);
                    if (sm_) {
                        sm_->enqueuePostIngest(serviceResp.hash, req.mimeType);
                    }
                }
            }
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

} // namespace yams::daemon
