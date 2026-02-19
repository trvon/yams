#pragma once

#include <yams/app/services/services.hpp>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon::dispatch {

inline app::services::StoreDocumentRequest mapStoreDocumentRequest(const AddDocumentRequest& req) {
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
    serviceReq.sessionId = req.sessionId;
    serviceReq.noEmbeddings = req.noEmbeddings;
    serviceReq.recursive = req.recursive;
    serviceReq.includePatterns = req.includePatterns;
    serviceReq.excludePatterns = req.excludePatterns;
    return serviceReq;
}

} // namespace yams::daemon::dispatch
