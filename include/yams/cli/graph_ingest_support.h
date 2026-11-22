#pragma once

#include <nlohmann/json.hpp>
#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace yams::cli {

// Rows per KgIngestRequest. Keeps a request well under the 16 MB IPC frame for ordinary rows.
inline constexpr std::size_t kGraphIngestChunkRows = 2000;

// Parse the `yams graph ingest` payload. It is the MCP `graph` action=ingest argument object:
// {"nodes":[...], "edges":[...], "aliases":[...], "skip_existing_nodes":bool,
//  "skip_existing_edges":bool, "document_hash":str}. An "action" key, if present, must be
// "ingest". `properties` may be a JSON object or a JSON-encoded string.
Result<daemon::KgIngestRequest> parseGraphIngestPayload(const nlohmann::json& payload);
Result<daemon::KgIngestRequest> parseGraphIngestPayloadText(std::string_view text);

// Split into requests of at most `chunkRows` rows: all nodes first, then edges, then aliases,
// so an edge never precedes the request that creates its endpoints.
std::vector<daemon::KgIngestRequest> chunkGraphIngestRequest(const daemon::KgIngestRequest& request,
                                                             std::size_t chunkRows);

// Sum per-chunk responses. success is false if any chunk failed.
daemon::KgIngestResponse
mergeGraphIngestResponses(const std::vector<daemon::KgIngestResponse>& responses);

// Same keys as the MCP ingest result, so callers parse one shape for both transports.
nlohmann::json graphIngestResponseToJson(const daemon::KgIngestResponse& response);
nlohmann::json graphRemoveResponseToJson(const daemon::KgRemoveResponse& response,
                                         std::string_view ns, std::string_view relation);

} // namespace yams::cli
