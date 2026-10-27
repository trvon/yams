// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
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

    /// Whether a local embedding carries the identity a sync record needs. A row that fails this
    /// can never publish, however often it is retried (rows written before model ids were
    /// recorded have none).
    static Result<void> checkPublishable(const VectorRecord& record) {
        if (record.chunk_id.empty() || record.model_id.empty()) {
            return Error{ErrorCode::InvalidArgument,
                         "embedding sync requires non-empty chunk_id and model_id"};
        }
        return {};
    }

    /// Distinguish a record-local size rejection from an unrelated backend failure.
    /// Backfill uses this only after a failed publish, so healthy rows serialize once.
    static Result<void> checkPublishable(const VectorRecord& record, std::size_t maxValueBytes) {
        if (auto identity = checkPublishable(record); !identity) {
            return identity.error();
        }
        if (nlohmann::json(toRecord(record)).dump().size() > maxValueBytes) {
            return Error{ErrorCode::InvalidArgument,
                         "embedding sync value exceeds configured resident limit"};
        }
        return {};
    }

    /// Serialize a committed embedding and publish it under `embedding/<model>/<chunkId>`.
    Result<void> publish(const VectorRecord& record) {
        if (auto publishable = checkPublishable(record); !publishable) {
            return publishable.error();
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
    ///
    /// Embeddings are independent per chunk, so one bad record must not starve the rest: every
    /// winner is scanned, and each is applied, deferred, or skipped on its own.
    /// - A winner whose document content has not replicated locally yet is deferred and listed
    ///   in deferredKeys().
    /// - A winner that cannot apply (malformed payload, identity or dimension mismatch, a model
    ///   the local store cannot hold for its chunk, a failed store write or content probe) is
    ///   skipped, and the first such failure is kept in failure().
    /// Nothing marks a skipped winner applied, so it stays a winner in the replicated index and
    /// the next apply() retries it. Only a failure that ends the pass (reconciliation, the index
    /// rebuild) is returned as an error.
    Result<std::size_t> apply() {
        deferredKeys_.clear();
        failure_.reset();
        auto view = sync_.captureAdapterView();
        if (!view) {
            return view.error();
        }
        const auto recordFailure = [this](std::string_view key, const Error& error) {
            if (!failure_) {
                failure_ = Error{error.code, std::string(key) + ": " + error.message};
            }
        };

        const std::string prefix =
            std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::Embedding)) + "/";
        std::size_t applied = 0;
        const auto targetDimension = database_.getConfig().embedding_dim;
        std::shared_ptr<memory_sync::MemorySyncLoop::AdapterIndex> classification;
        // Classify every eligible model before ANY target mutation, including deletes. The
        // disposable point index holds identities only, never a corpus-sized payload plan.
        auto classified = sync_.visitAdapterWinners(
            view.value(), prefix,
            [&](const std::string& key,
                const memory_sync::MemoryIndexRecord& envelope) -> Result<void> {
                if (envelope.isTombstone()) {
                    if (!tombstoneMatchesKey(key, envelope.tombstonePayload)) {
                        recordFailure(key, Error{ErrorCode::InvalidData,
                                                 "embedding tombstone identity does not match "
                                                 "logical key"});
                    }
                    return {};
                }
                auto record = readRecord(view.value(), key);
                if (!record) {
                    recordFailure(key, record.error());
                    return {};
                }
                if (targetDimension != 0 && record.value().dimensions != targetDimension) {
                    recordFailure(key, Error{ErrorCode::InvalidArgument,
                                             "replicated embedding dimensions do not match "
                                             "vector database"});
                    return {};
                }
                if (contentExists_) {
                    auto exists = contentExists_(record.value().documentId);
                    if (!exists) {
                        recordFailure(key, exists.error());
                        return {};
                    }
                    if (!exists.value()) {
                        deferredKeys_.push_back(key);
                        return {};
                    }
                }
                if (!classification) {
                    auto created = sync_.createAdapterIndex(view.value());
                    if (!created) {
                        return created.error();
                    }
                    classification = std::move(created).value();
                }
                const auto groupKey = "chunk/" + record.value().chunkId;
                const auto model = "model:" + record.value().model;
                auto previous = classification->lookup(groupKey);
                if (!previous) {
                    return previous.error();
                }
                if (!previous.value()) {
                    if (auto stored = classification->put(groupKey, model); !stored) {
                        return stored.error();
                    }
                } else if (*previous.value() != "conflict") {
                    if (!previous.value()->starts_with("model:")) {
                        return Error{ErrorCode::InvalidData, "invalid embedding classification"};
                    }
                    if (*previous.value() != model) {
                        if (auto stored = classification->put(groupKey, "conflict"); !stored) {
                            return stored.error();
                        }
                    }
                }
                return classification->put("eligible/" + key, "1");
            });
        if (!classified) {
            return classified.error();
        }

        auto deleted = sync_.visitAdapterWinners(
            view.value(), prefix,
            [&](const std::string& key,
                const memory_sync::MemoryIndexRecord& envelope) -> Result<void> {
                if (!envelope.isTombstone() ||
                    !tombstoneMatchesKey(key, envelope.tombstonePayload)) {
                    return {};
                }
                const auto& chunkId = envelope.tombstonePayload;
                const auto existing = database_.getVector(chunkId);
                if (!existing) {
                    return {};
                }
                if (embeddingKey(existing->model_id, chunkId) != key) {
                    recordFailure(key, Error{ErrorCode::InvalidData,
                                             "embedding tombstone model does not match local "
                                             "vector identity"});
                    return {};
                }
                if (auto removed = database_.deleteVectorChecked(chunkId); !removed) {
                    recordFailure(key, removed.error());
                    return {};
                }
                ++applied;
                *rebuildDirty_ = true;
                return {};
            });
        if (!deleted) {
            return deleted.error();
        }
        if (classification) {
            auto written = sync_.visitAdapterWinners(
                view.value(), prefix,
                [&](const std::string& key,
                    const memory_sync::MemoryIndexRecord& envelope) -> Result<void> {
                    if (envelope.isTombstone()) {
                        return {};
                    }
                    auto eligible = classification->lookup("eligible/" + key);
                    if (!eligible) {
                        return eligible.error();
                    }
                    if (!eligible.value()) {
                        return {};
                    }
                    if (*eligible.value() != "1") {
                        return Error{ErrorCode::InvalidData, "invalid embedding eligibility"};
                    }
                    // Read and verify one exact historical payload at a time; never substitute
                    // a newer winner, and recheck current quarantine even after classification.
                    auto selected = readRecord(view.value(), key);
                    if (!selected) {
                        recordFailure(key, selected.error());
                        return {};
                    }
                    const auto& record = selected.value();
                    auto group = classification->lookup("chunk/" + record.chunkId);
                    if (!group) {
                        return group.error();
                    }
                    if (!group.value() || (*group.value() != "conflict" &&
                                           *group.value() != "model:" + record.model)) {
                        return Error{ErrorCode::InvalidData,
                                     "inconsistent embedding classification"};
                    }
                    const auto existing = database_.getVector(record.chunkId);
                    if (*group.value() == "conflict" ||
                        (existing && existing->model_id != record.model)) {
                        recordFailure(key, Error{ErrorCode::NotSupported,
                                                 "vector database cannot represent multiple "
                                                 "models for one chunk"});
                        return {};
                    }
                    const auto rebuilt = toVectorRecord(record);
                    if (existing && vectorMatches(*existing, rebuilt)) {
                        return {};
                    }
                    auto stored = existing ? database_.updateVectorChecked(record.chunkId, rebuilt)
                                           : database_.insertVectorChecked(rebuilt);
                    if (!stored) {
                        recordFailure(key, stored.error());
                        return {};
                    }
                    ++applied;
                    *rebuildDirty_ = true;
                    return {};
                });
            if (!written) {
                return written.error();
            }
        }

        if (*rebuildDirty_) {
            if (auto rebuilt = rebuild_(); !rebuilt) {
                return rebuilt.error();
            }
            *rebuildDirty_ = false;
        }
        return applied;
    }

    /// Logical keys the last apply() deferred because their content prerequisite is missing.
    const std::vector<std::string>& deferredKeys() const noexcept { return deferredKeys_; }

    /// First winner the last apply() skipped because it cannot apply, its message prefixed with
    /// the record's logical key; empty when every winner applied or was deferred.
    const std::optional<Error>& failure() const noexcept { return failure_; }

private:
    /// Read one winning embedding payload and validate it against its logical key.
    Result<memory_sync::EmbeddingRecord>
    readRecord(const memory_sync::MemorySyncService::AdapterView& view, const std::string& key) {
        auto payload = sync_.readAdapterPayload(view, key);
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
        if (contentExists_ && !memory_sync::isSha256Digest(record.documentId)) {
            return Error{ErrorCode::InvalidData,
                         "replicated embedding document identity is not a SHA-256 hash"};
        }
        return record;
    }

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
    std::vector<std::string> deferredKeys_;
    std::optional<Error> failure_;
};

} // namespace yams::vector
