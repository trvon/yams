#pragma once

#include <algorithm>
#include <cctype>
#include <string>

#include <nlohmann/json.hpp>
#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli {

// JSON retrieval v1 deliberately represents original UTF-8 bytes, not excerpts or
// extracted text. Serialize before writing anything so failures cannot emit a
// plausible but incomplete evidence record.
inline Result<std::string> serializeGetResponse(const daemon::GetResponse& response,
                                                bool metadataOnly) {
    if (response.hash.size() != 64 ||
        !std::all_of(response.hash.begin(), response.hash.end(),
                     [](unsigned char c) { return std::isxdigit(c) != 0; })) {
        return Error{ErrorCode::InvalidData, "Retrieved document has no full SHA-256 identity"};
    }
    if (!metadataOnly && !response.hasContent) {
        return Error{ErrorCode::NotFound, "Document content not available"};
    }
    if (!metadataOnly && response.content.size() != response.size) {
        return Error{ErrorCode::InvalidData,
                     "Retrieved content size differs from source size; refusing partial JSON"};
    }
    try {
        nlohmann::json document = {
            {"schema_version", 1},
            {"hash", response.hash},
            {"name", response.name.empty() ? response.fileName : response.name},
            {"path", response.path},
            {"size", response.size},
            {"mime_type", response.mimeType},
            {"created", response.created},
            {"modified", response.modified},
            {"indexed", response.indexed},
            {"metadata", response.metadata},
            {"has_content", !metadataOnly},
            {"content", metadataOnly ? nlohmann::json(nullptr) : nlohmann::json(response.content)},
            {"encoding", metadataOnly ? nlohmann::json(nullptr) : nlohmann::json("utf-8")},
            {"representation", metadataOnly ? nlohmann::json(nullptr) : nlohmann::json("original")},
            {"truncated", false}};
        return document.dump(2) + "\n";
    } catch (const nlohmann::json::exception& error) {
        return Error{ErrorCode::InvalidData,
                     std::string("Cannot serialize document as UTF-8 JSON: ") + error.what()};
    }
}

} // namespace yams::cli
