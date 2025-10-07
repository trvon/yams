// Copyright (c) 2024 The YAMS Authors. All rights reserved.
// SPDX-License-Identifier: BSD-3-Clause

#include "yams/daemon/components/RequestDispatcher.h"
#include "yams/daemon/components/ServiceManager.h"
#include "yams/metadata/metadata_repository.h"

#include <spdlog/spdlog.h>

namespace yams::daemon {

using namespace yams::metadata;

boost::asio::awaitable<Response>
RequestDispatcher::handleListTreeDiffRequest(const ListTreeDiffRequest& req) {
    spdlog::debug("ListTreeDiffRequest: base={}, target={}, prefix={}, typeFilter={}, limit={}, "
                  "offset={}",
                  req.baseSnapshotId, req.targetSnapshotId, req.pathPrefix, req.typeFilter,
                  req.limit, req.offset);

    // Validate inputs
    if (req.baseSnapshotId.empty() || req.targetSnapshotId.empty()) {
        ErrorResponse err{};
        err.code = ErrorCode::InvalidArgument;
        err.message = "Both baseSnapshotId and targetSnapshotId are required";
        co_return err;
    }

    // Get metadata repository from service manager
    auto ctx = serviceManager_->getAppContext();
    if (!ctx.metadataRepo) {
        ErrorResponse err{};
        err.code = ErrorCode::InternalError;
        err.message = "Metadata repository not available";
        co_return err;
    }

    // Build TreeDiffQuery
    TreeDiffQuery query{};
    query.baseSnapshotId = req.baseSnapshotId;
    query.targetSnapshotId = req.targetSnapshotId;
    if (!req.pathPrefix.empty()) {
        query.pathPrefix = req.pathPrefix;
    }
    if (!req.typeFilter.empty()) {
        // Convert string to enum
        if (req.typeFilter == "added")
            query.typeFilter = TreeChangeType::Added;
        else if (req.typeFilter == "modified")
            query.typeFilter = TreeChangeType::Modified;
        else if (req.typeFilter == "deleted")
            query.typeFilter = TreeChangeType::Deleted;
        else if (req.typeFilter == "renamed")
            query.typeFilter = TreeChangeType::Renamed;
    }
    query.limit = std::min(req.limit, uint64_t{10000});
    query.offset = req.offset;

    // Execute query
    auto changesRes = ctx.metadataRepo->listTreeChanges(query);
    if (!changesRes) {
        ErrorResponse err{};
        err.code = ErrorCode::DatabaseError;
        err.message = fmt::format("Failed to list tree changes: {}", changesRes.error().message);
        co_return err;
    }

    // Build response
    ListTreeDiffResponse resp{};
    resp.changes.reserve(changesRes.value().size());

    for (const auto& change : changesRes.value()) {
        TreeChangeEntry entry{};
        // Map TreeChangeType enum to string
        switch (change.type) {
            case TreeChangeType::Added:
                entry.changeType = "added";
                break;
            case TreeChangeType::Modified:
                entry.changeType = "modified";
                break;
            case TreeChangeType::Deleted:
                entry.changeType = "deleted";
                break;
            case TreeChangeType::Renamed:
                entry.changeType = "renamed";
                break;
            default:
                entry.changeType = "unknown";
                break;
        }

        // newPath is the primary path
        entry.path = change.newPath;
        // oldPath for renames
        if (!change.oldPath.empty() && change.oldPath != change.newPath) {
            entry.oldPath = change.oldPath;
        }
        // Hashes
        entry.hash = change.newHash;
        entry.oldHash = change.oldHash;

        // Size fields not available in TreeChangeRecord, set to 0
        entry.size = 0;
        entry.oldSize = 0;

        // Optional delta hash
        if (change.contentDeltaHash) {
            entry.contentDeltaHash = *change.contentDeltaHash;
        }

        resp.changes.push_back(std::move(entry));
    }

    // For now, totalCount = size (pagination not fully implemented in DB query yet)
    resp.totalCount = resp.changes.size();

    spdlog::debug("ListTreeDiffRequest completed: returned {} changes", resp.changes.size());
    co_return resp;
}

} // namespace yams::daemon
