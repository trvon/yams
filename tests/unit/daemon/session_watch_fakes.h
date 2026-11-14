// SPDX-License-Identifier: GPL-3.0-or-later
// Recording stand-ins for the services the session watcher drives.
#pragma once

#include <cstddef>
#include <filesystem>
#include <set>
#include <string>
#include <vector>

#include <yams/app/services/services.hpp>

namespace yams::daemon::test {

class SessionWatchIndexingService final : public app::services::IIndexingService {
public:
    Result<app::services::AddDirectoryResponse>
    addDirectory(const app::services::AddDirectoryRequest& request) override {
        requests.push_back(request);
        if (failuresRemaining > 0) {
            --failuresRemaining;
            return Error{ErrorCode::IOError, "simulated indexing failure"};
        }
        app::services::AddDirectoryResponse response;
        response.filesProcessed = request.explicitFiles.size();
        for (const auto& relative : request.explicitFiles) {
            app::services::IndexedFileResult result;
            result.path = (std::filesystem::path(request.directoryPath) / relative).string();
            result.success = !failingFiles.contains(relative);
            if (result.success) {
                ++response.filesIndexed;
            } else {
                ++response.filesFailed;
                result.error = "simulated per-file failure";
            }
            response.results.push_back(std::move(result));
        }
        return response;
    }

    std::size_t failuresRemaining{0};
    std::set<std::string> failingFiles;
    std::vector<app::services::AddDirectoryRequest> requests;
};

class SessionWatchDocumentService final : public app::services::IDocumentService {
public:
    Result<app::services::StoreDocumentResponse>
    store(const app::services::StoreDocumentRequest&) override {
        return ErrorCode::NotImplemented;
    }
    Result<app::services::RetrieveDocumentResponse>
    retrieve(const app::services::RetrieveDocumentRequest&) override {
        return ErrorCode::NotImplemented;
    }
    Result<app::services::CatDocumentResponse>
    cat(const app::services::CatDocumentRequest&) override {
        return ErrorCode::NotImplemented;
    }
    Result<app::services::ListDocumentsResponse>
    list(const app::services::ListDocumentsRequest&) override {
        return ErrorCode::NotImplemented;
    }
    Result<app::services::UpdateMetadataResponse>
    updateMetadata(const app::services::UpdateMetadataRequest&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::string> resolveNameToHash(const std::string&, bool) override {
        return ErrorCode::NotImplemented;
    }
    Result<app::services::DeleteByNameResponse>
    deleteByName(const app::services::DeleteByNameRequest& request) override {
        deleteRequests.push_back(request);
        if (failuresRemaining > 0) {
            --failuresRemaining;
            return Error{ErrorCode::IOError, "simulated deletion failure"};
        }
        app::services::DeleteByNameResponse response;
        response.count = 1;
        response.deleted.push_back(
            {.name = request.name, .deleted = true, .errorCode = ErrorCode::Success});
        return response;
    }
    Result<app::services::DeleteByNameResponse>
    pruneVersions(const app::services::PruneVersionsRequest&) override {
        return ErrorCode::NotImplemented;
    }

    std::size_t failuresRemaining{0};
    std::vector<app::services::DeleteByNameRequest> deleteRequests;
};

} // namespace yams::daemon::test
