#include <yams/cli/graph_ingest_support.h>

#include <algorithm>

namespace yams::cli {

using json = nlohmann::json;

namespace {

Error invalid(std::string message) {
    return Error{ErrorCode::InvalidArgument, std::move(message)};
}

Result<std::string> stringField(const json& row, const char* key, std::string_view where,
                                bool required) {
    if (!row.contains(key) || row[key].is_null()) {
        if (required) {
            return invalid(std::string(where) + ": missing required '" + key + "'");
        }
        return std::string{};
    }
    if (!row[key].is_string()) {
        return invalid(std::string(where) + ": '" + key + "' must be a string");
    }
    auto value = row[key].get<std::string>();
    if (required && value.empty()) {
        return invalid(std::string(where) + ": '" + key + "' must not be empty");
    }
    return value;
}

Result<std::string> propertiesField(const json& row, std::string_view where) {
    if (!row.contains("properties") || row["properties"].is_null()) {
        return std::string{};
    }
    const auto& props = row["properties"];
    json object;
    if (props.is_object()) {
        object = props;
    } else if (props.is_string()) {
        auto text = props.get<std::string>();
        if (text.empty()) {
            return std::string{};
        }
        object = json::parse(text, nullptr, false);
        if (!object.is_object()) {
            return invalid(std::string(where) + ": 'properties' string is not a JSON object");
        }
    } else {
        return invalid(std::string(where) + ": 'properties' must be an object");
    }
    if (auto ns = object.find("ns");
        ns != object.end() && (!ns->is_string() || ns->get<std::string>().empty())) {
        return invalid(std::string(where) + ": properties.ns must be a non-empty string");
    }
    return object.dump();
}

Result<float> numberField(const json& row, const char* key, std::string_view where,
                          float fallback) {
    if (!row.contains(key) || row[key].is_null()) {
        return fallback;
    }
    if (!row[key].is_number()) {
        return invalid(std::string(where) + ": '" + key + "' must be a number");
    }
    return row[key].get<float>();
}

Result<const json*> arrayField(const json& payload, const char* key) {
    if (!payload.contains(key) || payload[key].is_null()) {
        return static_cast<const json*>(nullptr);
    }
    if (!payload[key].is_array()) {
        return invalid(std::string("'") + key + "' must be an array");
    }
    return &payload[key];
}

Result<bool> boolField(const json& payload, const char* key) {
    if (!payload.contains(key) || payload[key].is_null()) {
        return false;
    }
    if (!payload[key].is_boolean()) {
        return invalid(std::string("'") + key + "' must be a boolean");
    }
    return payload[key].get<bool>();
}

} // namespace

Result<daemon::KgIngestRequest> parseGraphIngestPayload(const json& payload) {
    if (!payload.is_object()) {
        return invalid("graph ingest payload must be a JSON object");
    }
    if (payload.contains("action") &&
        (!payload["action"].is_string() || payload["action"].get<std::string>() != "ingest")) {
        return invalid("graph ingest payload 'action' must be \"ingest\" when present");
    }

    daemon::KgIngestRequest request;
    auto nodes = arrayField(payload, "nodes");
    if (!nodes)
        return nodes.error();
    auto edges = arrayField(payload, "edges");
    if (!edges)
        return edges.error();
    auto aliases = arrayField(payload, "aliases");
    if (!aliases)
        return aliases.error();
    if (!nodes.value() && !edges.value() && !aliases.value()) {
        return invalid("graph ingest payload needs at least one of 'nodes', 'edges', 'aliases'");
    }

    if (const auto* rows = nodes.value()) {
        request.nodes.reserve(rows->size());
        for (std::size_t i = 0; i < rows->size(); ++i) {
            const auto& row = (*rows)[i];
            const auto where = "nodes[" + std::to_string(i) + "]";
            if (!row.is_object())
                return invalid(where + ": must be an object");
            daemon::KgIngestNode node;
            auto key = stringField(row, "node_key", where, true);
            if (!key)
                return key.error();
            auto label = stringField(row, "label", where, false);
            if (!label)
                return label.error();
            auto type = stringField(row, "type", where, false);
            if (!type)
                return type.error();
            auto props = propertiesField(row, where);
            if (!props)
                return props.error();
            node.nodeKey = std::move(key.value());
            node.label = std::move(label.value());
            node.type = std::move(type.value());
            node.properties = std::move(props.value());
            request.nodes.push_back(std::move(node));
        }
    }

    if (const auto* rows = edges.value()) {
        request.edges.reserve(rows->size());
        for (std::size_t i = 0; i < rows->size(); ++i) {
            const auto& row = (*rows)[i];
            const auto where = "edges[" + std::to_string(i) + "]";
            if (!row.is_object())
                return invalid(where + ": must be an object");
            daemon::KgIngestEdge edge;
            auto src = stringField(row, "src_node_key", where, true);
            if (!src)
                return src.error();
            auto dst = stringField(row, "dst_node_key", where, true);
            if (!dst)
                return dst.error();
            auto relation = stringField(row, "relation", where, true);
            if (!relation)
                return relation.error();
            auto weight = numberField(row, "weight", where, 1.0f);
            if (!weight)
                return weight.error();
            auto props = propertiesField(row, where);
            if (!props)
                return props.error();
            edge.srcNodeKey = std::move(src.value());
            edge.dstNodeKey = std::move(dst.value());
            edge.relation = std::move(relation.value());
            edge.weight = weight.value();
            edge.properties = std::move(props.value());
            request.edges.push_back(std::move(edge));
        }
    }

    if (const auto* rows = aliases.value()) {
        request.aliases.reserve(rows->size());
        for (std::size_t i = 0; i < rows->size(); ++i) {
            const auto& row = (*rows)[i];
            const auto where = "aliases[" + std::to_string(i) + "]";
            if (!row.is_object())
                return invalid(where + ": must be an object");
            daemon::KgIngestAlias alias;
            auto key = stringField(row, "node_key", where, true);
            if (!key)
                return key.error();
            auto text = stringField(row, "alias", where, true);
            if (!text)
                return text.error();
            auto source = stringField(row, "source", where, false);
            if (!source)
                return source.error();
            auto confidence = numberField(row, "confidence", where, 1.0f);
            if (!confidence)
                return confidence.error();
            alias.nodeKey = std::move(key.value());
            alias.alias = std::move(text.value());
            alias.source = std::move(source.value());
            alias.confidence = confidence.value();
            request.aliases.push_back(std::move(alias));
        }
    }

    auto skipNodes = boolField(payload, "skip_existing_nodes");
    if (!skipNodes)
        return skipNodes.error();
    auto skipEdges = boolField(payload, "skip_existing_edges");
    if (!skipEdges)
        return skipEdges.error();
    request.skipExistingNodes = skipNodes.value();
    request.skipExistingEdges = skipEdges.value();
    if (payload.contains("document_hash")) {
        return invalid("'document_hash' is not supported: ingest never binds rows to a "
                       "document (use properties instead)");
    }
    return request;
}

Result<daemon::KgIngestRequest> parseGraphIngestPayloadText(std::string_view text) {
    auto payload = json::parse(text, nullptr, false);
    if (payload.is_discarded()) {
        return invalid("graph ingest payload is not valid JSON");
    }
    return parseGraphIngestPayload(payload);
}

std::vector<daemon::KgIngestRequest> chunkGraphIngestRequest(const daemon::KgIngestRequest& request,
                                                             std::size_t chunkRows) {
    chunkRows = std::max<std::size_t>(1, chunkRows);
    std::vector<daemon::KgIngestRequest> chunks;
    const auto fresh = [&]() {
        daemon::KgIngestRequest chunk;
        chunk.documentHash = request.documentHash;
        chunk.skipExistingNodes = request.skipExistingNodes;
        chunk.skipExistingEdges = request.skipExistingEdges;
        return chunk;
    };
    const auto append = [&](const auto& rows, auto member) {
        for (std::size_t offset = 0; offset < rows.size(); offset += chunkRows) {
            auto chunk = fresh();
            const auto end = std::min(rows.size(), offset + chunkRows);
            (chunk.*member)
                .assign(rows.begin() + static_cast<std::ptrdiff_t>(offset),
                        rows.begin() + static_cast<std::ptrdiff_t>(end));
            chunks.push_back(std::move(chunk));
        }
    };
    append(request.nodes, &daemon::KgIngestRequest::nodes);
    append(request.edges, &daemon::KgIngestRequest::edges);
    append(request.aliases, &daemon::KgIngestRequest::aliases);
    if (chunks.empty()) {
        chunks.push_back(fresh());
    }
    return chunks;
}

daemon::KgIngestResponse
mergeGraphIngestResponses(const std::vector<daemon::KgIngestResponse>& responses) {
    daemon::KgIngestResponse merged;
    merged.success = true;
    for (const auto& r : responses) {
        merged.nodesInserted += r.nodesInserted;
        merged.nodesUpdated += r.nodesUpdated;
        merged.nodesSkipped += r.nodesSkipped;
        merged.edgesInserted += r.edgesInserted;
        merged.edgesUpdated += r.edgesUpdated;
        merged.edgesSkipped += r.edgesSkipped;
        merged.aliasesInserted += r.aliasesInserted;
        merged.aliasesUpdated += r.aliasesUpdated;
        merged.nsConflicts.insert(merged.nsConflicts.end(), r.nsConflicts.begin(),
                                  r.nsConflicts.end());
        merged.aliasesSkipped += r.aliasesSkipped;
        merged.errors.insert(merged.errors.end(), r.errors.begin(), r.errors.end());
        merged.unresolvedEdges.insert(merged.unresolvedEdges.end(), r.unresolvedEdges.begin(),
                                      r.unresolvedEdges.end());
        merged.success = merged.success && r.success;
    }
    return merged;
}

json graphIngestResponseToJson(const daemon::KgIngestResponse& r) {
    json unresolved = json::array();
    for (const auto& edge : r.unresolvedEdges) {
        unresolved.push_back(json{{"src_node_key", edge.srcNodeKey},
                                  {"dst_node_key", edge.dstNodeKey},
                                  {"relation", edge.relation},
                                  {"src_missing", edge.srcMissing},
                                  {"dst_missing", edge.dstMissing}});
    }
    json out{{"action", "ingest"},
             {"success", r.success},
             {"nodes_inserted", r.nodesInserted},
             {"nodes_updated", r.nodesUpdated},
             {"nodes_skipped", r.nodesSkipped},
             {"edges_inserted", r.edgesInserted},
             {"edges_updated", r.edgesUpdated},
             {"edges_skipped", r.edgesSkipped},
             {"aliases_inserted", r.aliasesInserted},
             {"aliases_updated", r.aliasesUpdated},
             {"aliases_skipped", r.aliasesSkipped},
             {"unresolved_edges", std::move(unresolved)}};
    json conflicts = json::array();
    for (const auto& c : r.nsConflicts) {
        conflicts.push_back(json{{"kind", c.kind},
                                 {"node_key", c.nodeKey},
                                 {"src_node_key", c.srcNodeKey},
                                 {"dst_node_key", c.dstNodeKey},
                                 {"relation", c.relation},
                                 {"existing_ns", c.existingNs},
                                 {"requested_ns", c.requestedNs},
                                 {"reason", c.reason}});
    }
    out["ns_conflicts"] = std::move(conflicts);
    if (!r.errors.empty()) {
        out["errors"] = r.errors;
    }
    return out;
}

bool GraphIngestProgress::record(const Result<daemon::KgIngestResponse>& chunk) {
    if (!failure_.empty()) {
        return false;
    }
    if (!chunk) {
        failure_ = "chunk " + std::to_string(committed_ + 1) + "/" + std::to_string(total_) +
                   " failed: " + chunk.error().message;
        return false;
    }
    if (!chunk.value().success) {
        std::string why = chunk.value().errors.empty() ? std::string("not committed")
                                                       : chunk.value().errors.back();
        failure_ = "chunk " + std::to_string(committed_ + 1) + "/" + std::to_string(total_) +
                   " did not commit: " + why;
        return false;
    }
    committedChunks_.push_back(chunk.value());
    ++committed_;
    merged_ = mergeGraphIngestResponses(committedChunks_);
    return true;
}

json graphIngestProgressToJson(const GraphIngestProgress& progress) {
    auto out = graphIngestResponseToJson(progress.merged());
    out["success"] = progress.complete();
    out["chunks_committed"] = progress.chunksCommitted();
    out["chunks_total"] = progress.chunksTotal();
    if (!progress.failure().empty()) {
        out["error"] = progress.failure();
    }
    return out;
}

json graphRemoveResponseToJson(const daemon::KgRemoveResponse& r, std::string_view ns,
                               std::string_view relation) {
    json out{{"action", "remove"},
             {"success", !r.refused},
             {"ns", std::string(ns)},
             {"nodes_removed", r.nodesRemoved},
             {"edges_removed", r.edgesRemoved},
             {"foreign_edges_removed", r.foreignEdgesRemoved},
             {"refused", r.refused},
             {"dry_run", r.dryRun}};
    if (!relation.empty()) {
        out["relation"] = std::string(relation);
    }
    return out;
}

} // namespace yams::cli
