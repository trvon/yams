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
// {"ns":str, "nodes":[...], "edges":[...], "aliases":[...], "skip_existing_nodes":bool,
//  "skip_existing_edges":bool}. Top-level "ns" is the namespace the request acts for; it
// decides which nodes aliases may be added to. An "action" key, if present, must be "ingest".
// `properties` may be a JSON object or a JSON-encoded string; a present properties.ns must be a
// non-empty string. "document_hash" is rejected: ingest never binds rows to a document.
Result<daemon::KgIngestRequest> parseGraphIngestPayload(const nlohmann::json& payload);
Result<daemon::KgIngestRequest> parseGraphIngestPayloadText(std::string_view text);

// Split into requests of at most `chunkRows` rows: all nodes first, then edges, then aliases,
// so an edge never precedes the request that creates its endpoints.
std::vector<daemon::KgIngestRequest> chunkGraphIngestRequest(const daemon::KgIngestRequest& request,
                                                             std::size_t chunkRows);

// Sum per-chunk responses. success is false if any chunk failed.
daemon::KgIngestResponse
mergeGraphIngestResponses(const std::vector<daemon::KgIngestResponse>& responses);

// Tracks a chunked ingest: merged counts of the chunks that committed, and the first failure.
// Ingest stops at the first chunk that fails to send or does not commit, so a failed node chunk
// is never followed by edge chunks that would all be unresolved.
class GraphIngestProgress {
public:
    explicit GraphIngestProgress(std::size_t chunksTotal) : total_(chunksTotal) {}
    // Returns false when the caller must stop sending chunks.
    bool record(const Result<daemon::KgIngestResponse>& chunk);
    const daemon::KgIngestResponse& merged() const { return merged_; }
    std::size_t chunksCommitted() const { return committed_; }
    std::size_t chunksTotal() const { return total_; }
    const std::string& failure() const { return failure_; }
    bool complete() const { return failure_.empty() && committed_ == total_; }
    // Rows the committed chunks left unwritten for ownership or a missing endpoint.
    bool hasRefusals() const {
        return !merged_.nsConflicts.empty() || !merged_.unresolvedEdges.empty();
    }

private:
    daemon::KgIngestResponse merged_{};
    std::vector<daemon::KgIngestResponse> committedChunks_;
    std::size_t committed_{0};
    std::size_t total_{0};
    std::string failure_;
};

// Same keys as the MCP ingest result, so callers parse one shape for both transports.
nlohmann::json graphIngestResponseToJson(const daemon::KgIngestResponse& response);
// The above plus chunks_committed, chunks_total and, on failure, "error". success means every
// chunk committed; the counts always describe exactly the committed chunks.
nlohmann::json graphIngestProgressToJson(const GraphIngestProgress& progress);
nlohmann::json graphRemoveResponseToJson(const daemon::KgRemoveResponse& response,
                                         std::string_view ns, std::string_view relation);

} // namespace yams::cli
