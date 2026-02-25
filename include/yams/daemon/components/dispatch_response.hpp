#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/metadata/knowledge_graph_store.h>

namespace yams::daemon::dispatch {

template <typename ResultType> inline ErrorResponse makeErrorResponse(const ResultType& result) {
    return ErrorResponse{result.error().code, result.error().message};
}

inline ErrorResponse makeErrorResponse(ErrorCode code, std::string message) {
    return ErrorResponse{code, std::move(message)};
}

inline ErrorResponse makeErrorResponse(const Error& err) {
    return ErrorResponse{err.code, err.message};
}

struct ListEntryMapper {
    template <typename ServiceDoc> static ListEntry fromServiceDoc(const ServiceDoc& doc) {
        ListEntry item;
        item.hash = doc.hash;
        item.path = doc.path;
        item.name = doc.name;
        item.fileName = doc.fileName;
        item.size = doc.size;
        item.mimeType = doc.mimeType;
        item.fileType = doc.fileType;
        item.extension = doc.extension;
        item.created = doc.created;
        item.modified = doc.modified;
        item.indexed = doc.indexed;
        if (doc.snippet) {
            item.snippet = doc.snippet.value();
        }
        item.tags = doc.tags;
        for (const auto& [key, value] : doc.metadata) {
            item.metadata[key] = value;
        }
        if (doc.changeType) {
            item.changeType = doc.changeType.value();
        }
        if (doc.changeTime) {
            item.changeTime = doc.changeTime.value();
        }
        item.relevanceScore = doc.relevanceScore;
        if (doc.matchReason) {
            item.matchReason = doc.matchReason.value();
        }
        item.extractionStatus = doc.extractionStatus;
        return item;
    }

    template <typename ServiceDoc>
    static std::vector<ListEntry> mapToListEntries(const std::vector<ServiceDoc>& docs) {
        std::vector<ListEntry> entries;
        entries.reserve(docs.size());
        for (const auto& doc : docs) {
            entries.push_back(fromServiceDoc(doc));
        }
        return entries;
    }
};

struct GetResponseMapper {
    template <typename ServiceDoc>
    static GetResponse fromServiceDoc(const ServiceDoc& doc, bool extract = false) {
        GetResponse response;
        response.hash = doc.hash;
        response.path = doc.path;
        response.name = doc.name;
        response.fileName = doc.fileName;
        response.size = doc.size;
        response.mimeType = doc.mimeType;
        response.fileType = doc.fileType;
        response.created = doc.created;
        response.modified = doc.modified;
        response.indexed = doc.indexed;

        if (extract && doc.extractedText.has_value() && !doc.extractedText.value().empty()) {
            response.content = doc.extractedText.value();
            response.hasContent = true;
        } else if (doc.content.has_value()) {
            response.content = doc.content.value();
            response.hasContent = true;
        } else {
            response.content.clear();
            response.hasContent = false;
        }

        response.compressed = doc.compressed;
        response.compressionAlgorithm = doc.compressionAlgorithm;
        response.compressionLevel = doc.compressionLevel;
        response.uncompressedSize = doc.uncompressedSize;
        response.compressedCrc32 = doc.compressedCrc32;
        response.uncompressedCrc32 = doc.uncompressedCrc32;
        response.compressionHeader = doc.compressionHeader;
        response.centroidWeight = doc.centroidWeight;
        response.centroidDims = doc.centroidDims;
        response.centroidPreview = doc.centroidPreview;

        for (const auto& [key, value] : doc.metadata) {
            response.metadata[key] = value;
        }

        return response;
    }
};

struct SearchResultMapper {
    template <typename ServiceItem>
    static SearchResult fromServiceItem(const ServiceItem& item, bool pathsOnly = false) {
        SearchResult resultItem;
        resultItem.id = std::to_string(item.id);
        resultItem.title = item.title;
        resultItem.path = item.path;
        resultItem.score = item.score;
        resultItem.snippet = item.snippet;

        if (!pathsOnly) {
            if constexpr (requires { item.metadata; }) {
                for (const auto& [key, value] : item.metadata) {
                    resultItem.metadata[key] = value;
                }
            }

            if (!item.hash.empty() && !resultItem.metadata.contains("hash")) {
                resultItem.metadata["hash"] = item.hash;
            }
            if (!item.path.empty() && !resultItem.metadata.contains("path")) {
                resultItem.metadata["path"] = item.path;
            }
            if (!item.title.empty() && !resultItem.metadata.contains("title")) {
                resultItem.metadata["title"] = item.title;
            }
        }

        return resultItem;
    }

    template <typename ServiceItem>
    static std::vector<SearchResult> mapToSearchResults(const std::vector<ServiceItem>& items,
                                                        size_t limit = 0) {
        std::vector<SearchResult> results;
        const size_t effectiveLimit = (limit > 0) ? limit : items.size();

        for (const auto& item : items) {
            if (results.size() >= effectiveLimit)
                break;
            results.push_back(fromServiceItem(item));
        }

        return results;
    }
};

struct GraphNodeMapper {
    template <typename SvcNode> static GraphNode fromServiceNode(const SvcNode& node) {
        GraphNode graphNode;
        graphNode.nodeId = node.nodeId;
        graphNode.nodeKey = node.nodeKey;
        graphNode.label = node.label.value_or("");
        graphNode.type = node.type.value_or("");
        return graphNode;
    }

    template <typename SvcNodeMetadata>
    static GraphNode fromServiceNodeMetadata(const SvcNodeMetadata& meta, int64_t distance = 0) {
        GraphNode graphNode;
        graphNode.nodeId = meta.node.nodeId;
        graphNode.nodeKey = meta.node.nodeKey;
        graphNode.label = meta.node.label.value_or("");
        graphNode.type = meta.node.type.value_or("");
        graphNode.documentHash = meta.documentHash.value_or("");
        graphNode.documentPath = meta.documentPath.value_or("");
        graphNode.snapshotId = meta.snapshotId.value_or("");
        graphNode.distance = distance;
        return graphNode;
    }
};

struct KGNodeMapper {
    static GraphNode fromKGNode(const yams::metadata::KGNode& node,
                                bool includeProperties = false) {
        GraphNode graphNode;
        graphNode.nodeId = node.id;
        graphNode.nodeKey = node.nodeKey;
        graphNode.label = node.label.value_or("");
        graphNode.type = node.type.value_or("");
        graphNode.distance = 0;
        if (includeProperties && node.properties) {
            graphNode.properties = node.properties.value();
        }
        return graphNode;
    }

    static std::vector<GraphNode> mapKGNodes(const std::vector<yams::metadata::KGNode>& nodes,
                                             bool includeProperties = false) {
        std::vector<GraphNode> result;
        result.reserve(nodes.size());
        for (const auto& node : nodes) {
            result.push_back(fromKGNode(node, includeProperties));
        }
        return result;
    }
};

struct GraphEdgeMapper {
    template <typename SvcEdge> static GraphEdge fromServiceEdge(const SvcEdge& edge) {
        GraphEdge graphEdge;
        graphEdge.edgeId = edge.edgeId;
        graphEdge.srcNodeId = edge.srcNodeId;
        graphEdge.dstNodeId = edge.dstNodeId;
        graphEdge.relation = edge.relation;
        graphEdge.weight = edge.weight;
        if (edge.properties.has_value()) {
            graphEdge.properties = edge.properties.value();
        }
        return graphEdge;
    }
};

struct GraphQueryResponseMapper {
    template <typename SvcResponse>
    static GraphQueryResponse fromServiceResponse(const SvcResponse& svcResp) {
        GraphQueryResponse resp;
        resp.kgAvailable = svcResp.kgAvailable;
        resp.warning = svcResp.warning.value_or("");
        resp.totalNodesFound = svcResp.totalNodesFound;
        resp.totalEdgesTraversed = svcResp.totalEdgesTraversed;
        resp.truncated = svcResp.truncated;
        resp.maxDepthReached = svcResp.maxDepthReached;

        resp.originNode.nodeId = svcResp.originNode.node.nodeId;
        resp.originNode.nodeKey = svcResp.originNode.node.nodeKey;
        resp.originNode.label = svcResp.originNode.node.label.value_or("");
        resp.originNode.type = svcResp.originNode.node.type.value_or("");
        resp.originNode.distance = 0;

        return resp;
    }

    template <typename SvcConnectedNode>
    static void mapConnectedNodesAndEdges(const SvcConnectedNode& cn, std::vector<GraphNode>& nodes,
                                          std::vector<GraphEdge>& edges,
                                          std::unordered_set<int64_t>& seenEdges) {
        GraphNode graphNode;
        graphNode.nodeId = cn.nodeMetadata.node.nodeId;
        graphNode.nodeKey = cn.nodeMetadata.node.nodeKey;
        graphNode.label = cn.nodeMetadata.node.label.value_or("");
        graphNode.type = cn.nodeMetadata.node.type.value_or("");
        graphNode.documentHash = cn.nodeMetadata.documentHash.value_or("");
        graphNode.documentPath = cn.nodeMetadata.documentPath.value_or("");
        graphNode.snapshotId = cn.nodeMetadata.snapshotId.value_or("");
        graphNode.distance = cn.distance;
        nodes.push_back(std::move(graphNode));

        for (const auto& edge : cn.connectingEdges) {
            if (edge.edgeId > 0) {
                if (!seenEdges.insert(edge.edgeId).second) {
                    continue;
                }
            }
            edges.push_back(GraphEdgeMapper::fromServiceEdge(edge));
        }
    }

    static GraphQueryResponse makeEmptyResponse(bool kgAvailable = true, std::string warning = "") {
        GraphQueryResponse resp;
        resp.kgAvailable = kgAvailable;
        resp.warning = std::move(warning);
        resp.originNode.nodeId = -1;
        resp.originNode.distance = 0;
        return resp;
    }

    static void setOriginNode(GraphQueryResponse& resp, int64_t nodeId, std::string nodeKey,
                              std::string label, std::string type = "query") {
        resp.originNode.nodeId = nodeId;
        resp.originNode.nodeKey = std::move(nodeKey);
        resp.originNode.label = std::move(label);
        resp.originNode.type = std::move(type);
        resp.originNode.distance = 0;
    }
};

inline ListResponse makeListResponse(uint64_t totalCount, std::vector<ListEntry>&& items) {
    ListResponse resp;
    resp.totalCount = totalCount;
    resp.items = std::move(items);
    return resp;
}

inline SearchResponse makeSearchResponse(uint64_t totalCount, std::chrono::milliseconds elapsed,
                                         std::vector<SearchResult>&& results,
                                         std::string traceId = {}) {
    SearchResponse resp;
    resp.totalCount = totalCount;
    resp.elapsed = elapsed;
    resp.results = std::move(results);
    resp.traceId = std::move(traceId);
    return resp;
}

} // namespace yams::daemon::dispatch
