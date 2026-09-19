#pragma once

#include <filesystem>
#include <yams/cli/content_reference.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli {

inline nlohmann::json fileHistoryToJson(const daemon::FileHistoryResponse& history) {
    auto documents = nlohmann::json::array();
    for (const auto& version : history.versions) {
        nlohmann::json item = {
            {"path", history.filepath},
            {"name", std::filesystem::path(history.filepath).filename().string()},
            {"snapshot_id", version.snapshotId},
            {"size", version.size},
            {"indexed", version.indexedTimestamp}};
        addContentReference(item, version.hash);
        documents.push_back(std::move(item));
    }
    return {{"documents", std::move(documents)},
            {"total", history.versions.size()},
            {"total_versions", history.totalVersions},
            {"path", history.filepath},
            {"found", history.found},
            {"message", history.message}};
}

} // namespace yams::cli
