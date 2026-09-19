// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/core/types.h>
#include <yams/memory_sync/key_policy.h>
#include <yams/memory_sync/version_vector.h>

namespace yams::memory_sync {

/// Store namespaces are stable protocol names, independent of local database IDs.
enum class MemoryStore { Document, Embedding, TopologyNode, TopologyEdge, ContentBlob };

inline std::string_view memoryStoreName(MemoryStore store) {
    switch (store) {
        case MemoryStore::Document:
            return "document";
        case MemoryStore::Embedding:
            return "embedding";
        case MemoryStore::TopologyNode:
            return "topology-node";
        case MemoryStore::TopologyEdge:
            return "topology-edge";
        case MemoryStore::ContentBlob:
            return "content-blob";
    }
    return "unknown";
}

/// Percent-encode a logical identity into one object-store path segment.
inline std::string escapeRecordKeySegment(std::string_view value) {
    return escapeKeySegment(value);
}

/// Metadata payload for a document. The content-addressed document bytes remain
/// in the existing blob store; this record carries portable metadata only.
struct DocumentRecord {
    std::string documentId;
    std::string name;
    std::string contentHash;
    std::map<std::string, std::string> metadata;

    std::string_view id() const noexcept { return documentId; }
    bool operator==(const DocumentRecord&) const = default;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(DocumentRecord, documentId, name, contentHash, metadata)
};

/// Lossless metadata value representation. `type` uses MetadataValueType's stable
/// ordinal; blob values are carried as JSON strings by the next adapter layer.
struct MetadataValueRecord {
    std::string value;
    std::int32_t type{0};

    bool operator==(const MetadataValueRecord&) const = default;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(MetadataValueRecord, value, type)
};

/// Complete portable input for MetadataRepository::batchInsertDocumentsWithMetadata.
/// Local numeric document IDs and derived path columns are intentionally excluded.
struct MetadataDocumentRecord {
    std::string documentId;
    std::string filePath;
    std::string fileName;
    std::string fileExtension;
    std::int64_t fileSize{0};
    std::string contentHash;
    std::string mimeType;
    std::int64_t createdTime{0};
    std::int64_t modifiedTime{0};
    std::int64_t indexedTime{0};
    bool contentExtracted = false;
    std::int32_t extractionStatus{0};
    std::string extractionError;
    std::int32_t repairStatus{0};
    std::int64_t repairAttemptedAt{0};
    std::int32_t repairAttempts{0};
    std::map<std::string, MetadataValueRecord> metadata;

    std::string_view id() const noexcept { return documentId; }
    bool operator==(const MetadataDocumentRecord&) const = default;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(MetadataDocumentRecord, documentId, filePath, fileName,
                                   fileExtension, fileSize, contentHash, mimeType, createdTime,
                                   modifiedTime, indexedTime, contentExtracted, extractionStatus,
                                   extractionError, repairStatus, repairAttemptedAt, repairAttempts,
                                   metadata)
};

/// Vector payload keyed by its stable chunk identity, not a local SQLite rowid.
/// Logical sync identity is (model, chunkId); `dimensions` is the vector length.
struct EmbeddingRecord {
    std::string chunkId;
    std::string documentId;
    std::string model;
    std::uint32_t dimensions{0};
    std::vector<float> values;
    std::string modelVersion;
    std::string content;
    std::uint64_t startOffset{0};
    std::uint64_t endOffset{0};
    std::map<std::string, std::string> metadata;
    std::uint32_t embeddingVersion{1};
    std::string contentHashAtEmbedding;
    std::int32_t level{0}; // EmbeddingLevel ordinal (CHUNK=0, DOCUMENT=1)
    std::vector<std::string> sourceChunkIds;
    std::string parentDocumentHash;
    std::vector<std::string> childDocumentHashes;

    std::string_view id() const noexcept { return chunkId; }
    bool operator==(const EmbeddingRecord&) const = default;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(EmbeddingRecord, chunkId, documentId, model, dimensions, values,
                                   modelVersion, content, startOffset, endOffset, metadata,
                                   embeddingVersion, contentHashAtEmbedding, level, sourceChunkIds,
                                   parentDocumentHash, childDocumentHashes)
};

/// Graph node payload keyed by YAMS's portable node key, never its local numeric ID.
struct TopologyNodeRecord {
    std::string nodeKey;
    std::string type;
    std::string label;
    std::map<std::string, std::string> properties;
    std::int64_t createdTime{0};
    std::int64_t updatedTime{0};
    std::string propertiesJson;
    bool hasCreatedTime = false;
    bool hasUpdatedTime = false;
    bool hasPropertiesJson = false;

    std::string_view id() const noexcept { return nodeKey; }
    bool operator==(const TopologyNodeRecord&) const = default;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(TopologyNodeRecord, nodeKey, type, label, properties,
                                   createdTime, updatedTime, propertiesJson, hasCreatedTime,
                                   hasUpdatedTime, hasPropertiesJson)
};

/// Graph edge payload expressed through portable node identities.
struct TopologyEdgeRecord {
    std::string sourceNodeKey;
    std::string relation;
    std::string targetNodeKey;
    double weight{0.0};
    std::int64_t createdTime{0};
    std::string propertiesJson;
    bool hasCreatedTime = false;
    bool hasPropertiesJson = false;

    std::string id() const {
        return escapeRecordKeySegment(sourceNodeKey) + "/" + escapeRecordKeySegment(relation) +
               "/" + escapeRecordKeySegment(targetNodeKey);
    }
    bool operator==(const TopologyEdgeRecord&) const = default;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(TopologyEdgeRecord, sourceNodeKey, relation, targetNodeKey,
                                   weight, createdTime, propertiesJson, hasCreatedTime,
                                   hasPropertiesJson)
};

/// Description of existing content-addressed bytes. Raw bytes remain under blob/<hash>.
struct ContentBlobRecord {
    std::string contentHash;
    std::string name;
    std::uint64_t sizeBytes{0};
    std::string mediaType;

    std::string_view id() const noexcept { return contentHash; }
    bool operator==(const ContentBlobRecord&) const = default;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ContentBlobRecord, contentHash, name, sizeBytes, mediaType)
};

/// Construct the logical key consumed by MemorySyncLoop. `record.entryHash` is
/// the common causal/LWW envelope's content-addressed payload reference.
inline Result<std::string> recordIndexKey(MemoryStore store, std::string_view id,
                                          const MemoryIndexRecord& record) {
    if (id.empty()) {
        return Error{ErrorCode::InvalidArgument, "memory sync record id must not be empty"};
    }
    if (!record.hasValidEntryHash()) {
        return Error{ErrorCode::InvalidArgument,
                     "memory sync record entryHash must be a SHA-256 digest"};
    }
    return "index/" + std::string(memoryStoreName(store)) + "/" + escapeRecordKeySegment(id) + "/" +
           record.entryHash;
}

} // namespace yams::memory_sync
