// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <cstring>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map> // IWYU pragma: keep
#include <vector>

#include <yams/core/types.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/memory_sync/records.h>
#include <yams/vector/vector_database.h>

namespace yams::vector {

/// Mirrors committed vector embeddings into the memory-sync `embedding/` namespace
/// and applies winning remote records into the local `VectorDatabase`. Logical sync
/// identity is (model, chunkId); `apply()` inserts or updates each winning record and
/// rebuilds the search index so a peer's vector query reflects the replicated rows.
class VectorSyncAdapter {
public:
    using RebuildCallback = std::function<Result<void>()>;
    using ContentExists = std::function<Result<bool>(std::string_view)>;

    VectorSyncAdapter(VectorDatabase& database, memory_sync::MemorySyncService& sync,
                      RebuildCallback rebuild = {}, bool* persistentRebuildDirty = nullptr,
                      ContentExists contentExists = {})
        : database_(database), sync_(sync), rebuild_(std::move(rebuild)),
          rebuildDirty_(persistentRebuildDirty ? persistentRebuildDirty : &localRebuildDirty_),
          contentExists_(std::move(contentExists)) {
        if (!rebuild_) {
            rebuild_ = [&database]() { return database.buildIndexChecked(); };
        }
    }

    void setRebuildCallback(RebuildCallback rebuild) { rebuild_ = std::move(rebuild); }

    /// Serialize a committed embedding and publish it under `embedding/<model>/<chunkId>`.
    Result<void> publish(const VectorRecord& record) {
        if (record.chunk_id.empty() || record.model_id.empty()) {
            return Error{ErrorCode::InvalidArgument,
                         "embedding sync requires non-empty chunk_id and model_id"};
        }
        const auto payload = toRecord(record);
        const std::string dump = nlohmann::json(payload).dump();
        std::vector<std::byte> bytes(dump.size());
        std::memcpy(bytes.data(), dump.data(), dump.size());
        auto published =
            sync_.publishIfChanged(embeddingKey(record.model_id, record.chunk_id), bytes);
        if (!published) {
            return published.error();
        }
        return {};
    }

    Result<void> publishDelete(std::string_view model, std::string_view chunkId) {
        if (model.empty() || chunkId.empty()) {
            return Error{ErrorCode::InvalidArgument,
                         "embedding deletion requires model and chunk identity"};
        }
        // The logical key carries model identity; the payload retains the legacy chunk-only
        // format. apply() validates their combination against the local row before deletion.
        return sync_.erase(embeddingKey(model, chunkId), std::string(chunkId));
    }

    /// Reconcile the sync backend and apply every winning embedding record to the local
    /// vector database. Existing chunks are updated in place; new chunks are inserted.
    /// Rebuilds the search index when any record was applied. Returns the number applied.
    Result<std::size_t> apply() {
        auto merged = sync_.syncOnce();
        if (!merged) {
            return merged.error();
        }

        const std::string prefix =
            std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::Embedding)) + "/";
        std::size_t applied = 0;
        struct PendingDeletion {
            std::string key;
            std::string chunkId;
        };
        std::vector<memory_sync::EmbeddingRecord> records;
        std::vector<PendingDeletion> deletions;
        std::unordered_map<std::string, std::string> modelsByChunk;
        for (const auto& [key, envelope] : merged.value()) {
            if (!key.starts_with(prefix)) {
                continue;
            }
            if (envelope.isTombstone()) {
                if (!tombstoneMatchesKey(key, envelope.tombstonePayload)) {
                    return Error{ErrorCode::InvalidData,
                                 "embedding tombstone identity does not match logical key"};
                }
                deletions.push_back(PendingDeletion{key, envelope.tombstonePayload});
                continue;
            }
            auto payload = sync_.readCached(key);
            if (!payload) {
                return payload.error();
            }
            memory_sync::EmbeddingRecord record;
            try {
                const std::string_view text(reinterpret_cast<const char*>(payload.value().data()),
                                            payload.value().size());
                record = nlohmann::json::parse(text).get<memory_sync::EmbeddingRecord>();
            } catch (const std::exception& e) {
                return Error{ErrorCode::InvalidData, e.what()};
            }

            if (record.chunkId.empty() || record.model.empty() ||
                record.values.size() != record.dimensions) {
                return Error{ErrorCode::InvalidData, "invalid replicated embedding record"};
            }
            if (embeddingKey(record.model, record.chunkId) != key) {
                return Error{ErrorCode::InvalidData,
                             "embedding record identity does not match logical key"};
            }
            if (contentExists_) {
                if (!memory_sync::isSha256Digest(record.documentId)) {
                    return Error{ErrorCode::InvalidData,
                                 "replicated embedding document identity is not a SHA-256 hash"};
                }
                auto exists = contentExists_(record.documentId);
                if (!exists) {
                    return exists.error();
                }
                if (!exists.value()) {
                    return Error{ErrorCode::NotFound,
                                 "replicated embedding content prerequisite is missing"};
                }
            }
            const auto [it, inserted] = modelsByChunk.emplace(record.chunkId, record.model);
            if (!inserted && it->second != record.model) {
                return Error{ErrorCode::NotSupported,
                             "vector database cannot represent multiple models for one chunk"};
            }
            records.push_back(std::move(record));
        }

        // Validate the complete winning batch before the first database mutation.
        for (const auto& deletion : deletions) {
            const auto existing = database_.getVector(deletion.chunkId);
            if (existing && embeddingKey(existing->model_id, deletion.chunkId) != deletion.key) {
                return Error{ErrorCode::InvalidData,
                             "embedding tombstone model does not match local vector identity"};
            }
        }
        for (const auto& record : records) {
            const auto existing = database_.getVector(record.chunkId);
            if (existing && existing->model_id != record.model) {
                return Error{ErrorCode::NotSupported,
                             "vector database cannot represent multiple models for one chunk"};
            }
        }

        for (const auto& deletion : deletions) {
            if (database_.getVector(deletion.chunkId)) {
                if (auto deleted = database_.deleteVectorChecked(deletion.chunkId); !deleted) {
                    return deleted.error();
                }
                ++applied;
                *rebuildDirty_ = true;
            }
        }
        for (const auto& record : records) {
            const auto rebuilt = toVectorRecord(record);
            const auto existing = database_.getVector(record.chunkId);
            if (existing && vectorMatches(*existing, rebuilt)) {
                continue;
            }
            auto written = existing ? database_.updateVectorChecked(record.chunkId, rebuilt)
                                    : database_.insertVectorChecked(rebuilt);
            if (!written) {
                return written.error();
            }
            ++applied;
            *rebuildDirty_ = true;
        }

        if (*rebuildDirty_) {
            if (auto rebuilt = rebuild_(); !rebuilt) {
                return rebuilt.error();
            }
            *rebuildDirty_ = false;
        }
        return applied;
    }

private:
    static bool vectorMatches(const VectorRecord& lhs, const VectorRecord& rhs) {
        return lhs.chunk_id == rhs.chunk_id && lhs.document_hash == rhs.document_hash &&
               lhs.model_id == rhs.model_id && lhs.model_version == rhs.model_version &&
               lhs.embedding == rhs.embedding && lhs.content == rhs.content &&
               lhs.start_offset == rhs.start_offset && lhs.end_offset == rhs.end_offset &&
               lhs.metadata == rhs.metadata && lhs.embedding_version == rhs.embedding_version &&
               lhs.content_hash_at_embedding == rhs.content_hash_at_embedding &&
               lhs.level == rhs.level && lhs.source_chunk_ids == rhs.source_chunk_ids &&
               lhs.parent_document_hash == rhs.parent_document_hash &&
               lhs.child_document_hashes == rhs.child_document_hashes;
    }

    static memory_sync::EmbeddingRecord toRecord(const VectorRecord& record) {
        memory_sync::EmbeddingRecord out;
        out.chunkId = record.chunk_id;
        out.documentId = record.document_hash;
        out.model = record.model_id;
        out.modelVersion = record.model_version;
        out.dimensions = static_cast<std::uint32_t>(record.embedding.size());
        out.values = record.embedding;
        out.content = record.content;
        out.startOffset = record.start_offset;
        out.endOffset = record.end_offset;
        out.metadata = record.metadata;
        out.embeddingVersion = record.embedding_version;
        out.contentHashAtEmbedding = record.content_hash_at_embedding;
        out.level = static_cast<std::int32_t>(record.level);
        out.sourceChunkIds = record.source_chunk_ids;
        out.parentDocumentHash = record.parent_document_hash;
        out.childDocumentHashes = record.child_document_hashes;
        return out;
    }

    static VectorRecord toVectorRecord(const memory_sync::EmbeddingRecord& record) {
        VectorRecord out;
        out.chunk_id = record.chunkId;
        out.document_hash = record.documentId;
        out.model_id = record.model;
        out.model_version = record.modelVersion;
        out.embedding = record.values;
        out.embedding_dim = record.dimensions;
        out.content = record.content;
        out.start_offset = record.startOffset;
        out.end_offset = record.endOffset;
        out.metadata = record.metadata;
        out.embedding_version = record.embeddingVersion;
        out.content_hash_at_embedding = record.contentHashAtEmbedding;
        out.level = static_cast<EmbeddingLevel>(record.level);
        out.source_chunk_ids = record.sourceChunkIds;
        out.parent_document_hash = record.parentDocumentHash;
        out.child_document_hashes = record.childDocumentHashes;
        return out;
    }

    static std::string embeddingKey(std::string_view model, std::string_view chunkId) {
        return std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::Embedding)) +
               "/" + memory_sync::escapeRecordKeySegment(model) + "/" +
               memory_sync::escapeRecordKeySegment(chunkId);
    }

    static bool tombstoneMatchesKey(std::string_view key, std::string_view chunkId) {
        const auto separator = key.rfind('/');
        return separator != std::string_view::npos &&
               key.substr(separator + 1) == memory_sync::escapeRecordKeySegment(chunkId);
    }

    VectorDatabase& database_;
    memory_sync::MemorySyncService& sync_;
    RebuildCallback rebuild_;
    bool localRebuildDirty_{false};
    bool* rebuildDirty_;
    ContentExists contentExists_;
};

} // namespace yams::vector
