// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <algorithm>
#include <cstring>
#include <functional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <yams/core/types.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/memory_sync/records.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

namespace yams::metadata {

/// Mirrors committed metadata documents into the memory-sync `document/` namespace
/// and applies winning remote records through the existing idempotent batch-insert
/// seam. Document identity is content-addressed (sha256Hash), so peers converge to
/// the same document row regardless of local numeric IDs or path-series state.
///
/// The adapter is deliberately narrow, mirroring the blob adapter: callers inject it
/// where the successful local `batchInsertDocumentsWithMetadata` already committed,
/// and apply() only consumes records the sync loop has already LWW-merged.
class MetadataSyncAdapter {
public:
    using ContentExists = std::function<Result<bool>(std::string_view)>;
    using AppliedObserver = std::function<void(const DocumentInfo&)>;

    MetadataSyncAdapter(MetadataRepository& repository, memory_sync::MemorySyncService& sync,
                        ContentExists contentExists = {}, AppliedObserver appliedObserver = {})
        : repository_(repository), sync_(sync), contentExists_(std::move(contentExists)),
          appliedObserver_(std::move(appliedObserver)) {}

    /// Serialize a committed document (+tags) and publish it under `document/<hash>`.
    Result<void> publish(const DocumentInfo& info,
                         const std::vector<std::pair<std::string, MetadataValue>>& tags) {
        if (!memory_sync::isSha256Digest(info.sha256Hash)) {
            return Error{ErrorCode::InvalidArgument,
                         "metadata sync requires a SHA-256 content-hash identity"};
        }
        const auto record = toRecord(info, tags);
        const std::string dump = nlohmann::json(record).dump();
        std::vector<std::byte> bytes(dump.size());
        std::memcpy(bytes.data(), dump.data(), dump.size());
        auto published = sync_.publishIfChanged(documentKey(record.contentHash), bytes);
        if (!published) {
            return published.error();
        }
        return {};
    }

    Result<void> publishDelete(std::string_view contentHash) {
        if (!memory_sync::isSha256Digest(contentHash)) {
            return Error{ErrorCode::InvalidArgument,
                         "metadata deletion requires a SHA-256 content hash"};
        }
        return sync_.erase(documentKey(contentHash), std::string(contentHash));
    }

    /// Reconcile the sync backend and apply every winning document record to the local
    /// repository through the idempotent batch-insert seam. Returns the number of
    /// document records applied (0 when the index carries none).
    Result<std::size_t> apply() {
        auto merged = sync_.syncOnce();
        if (!merged) {
            return merged.error();
        }

        const std::string prefix =
            std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::Document)) + "/";
        std::vector<BatchDocumentInsert> items;
        std::vector<std::int64_t> deletedIds;
        for (const auto& [key, envelope] : merged.value()) {
            if (!key.starts_with(prefix)) {
                continue;
            }
            if (envelope.isTombstone()) {
                if (!memory_sync::isSha256Digest(envelope.tombstonePayload) ||
                    documentKey(envelope.tombstonePayload) != key) {
                    return Error{ErrorCode::InvalidData,
                                 "metadata tombstone identity does not match logical key"};
                }
                const auto document = repository_.getDocumentByHash(envelope.tombstonePayload);
                if (!document) {
                    return document.error();
                }
                if (document.value() && envelope.origin == sync_.localNodeId()) {
                    // A local tombstone whose exact commitment crossed the point of no return can
                    // race a later local re-add. Preserve the present local row and publish it as
                    // the causally-later value instead of deleting the newer local state.
                    auto metadata = repository_.getAllMetadata(document.value()->id);
                    if (!metadata) {
                        return metadata.error();
                    }
                    std::vector<std::pair<std::string, MetadataValue>> tags;
                    tags.reserve(metadata.value().size());
                    for (const auto& [metadataKey, value] : metadata.value()) {
                        tags.emplace_back(metadataKey, value);
                    }
                    if (auto republished = publish(*document.value(), tags); !republished) {
                        return republished.error();
                    }
                } else if (document.value()) {
                    deletedIds.push_back(document.value()->id);
                }
                continue;
            }
            auto payload = sync_.readCached(key);
            if (!payload) {
                return payload.error();
            }
            memory_sync::MetadataDocumentRecord record;
            try {
                const std::string_view text(reinterpret_cast<const char*>(payload.value().data()),
                                            payload.value().size());
                record = nlohmann::json::parse(text).get<memory_sync::MetadataDocumentRecord>();
            } catch (const std::exception& e) {
                return Error{ErrorCode::InvalidData, e.what()};
            }
            const auto identity =
                record.contentHash.empty() ? record.documentId : record.contentHash;
            if (!memory_sync::isSha256Digest(identity) || documentKey(identity) != key ||
                (!record.documentId.empty() && !record.contentHash.empty() &&
                 record.documentId != record.contentHash)) {
                return Error{ErrorCode::InvalidData,
                             "metadata record identity does not match logical key"};
            }
            if (contentExists_) {
                auto exists = contentExists_(identity);
                if (!exists) {
                    return exists.error();
                }
                if (!exists.value()) {
                    return Error{ErrorCode::NotFound,
                                 "replicated metadata content prerequisite is missing"};
                }
            }
            items.push_back(toInsert(record));
        }

        std::size_t applied = 0;
        if (!deletedIds.empty()) {
            auto deleted = repository_.deleteDocumentsBatch(deletedIds);
            if (!deleted) {
                return deleted.error();
            }
            applied += deleted.value();
        }
        for (auto& item : items) {
            auto existing = repository_.getDocumentByHash(item.info.sha256Hash);
            if (!existing) {
                return existing.error();
            }
            if (!existing.value()) {
                std::vector<BatchDocumentInsert> insert{item};
                auto result = repository_.batchInsertDocumentsWithMetadata(insert);
                if (!result) {
                    return result.error();
                }
                ++applied;
                if (appliedObserver_) {
                    appliedObserver_(item.info);
                }
                continue;
            }

            item.info.id = existing.value()->id;
            auto currentMetadata = repository_.getAllMetadata(item.info.id);
            if (!currentMetadata) {
                return currentMetadata.error();
            }
            if (documentMatches(*existing.value(), item.info) &&
                metadataMatches(currentMetadata.value(), item.tags)) {
                continue;
            }

            // The selected winner is durable in memory-sync; a failed atomic replacement is
            // retryable on the next apply without exposing a mixed document/metadata winner.
            if (auto replaced = repository_.replaceDocumentAndMetadata(item.info, item.tags);
                !replaced) {
                return replaced.error();
            }
            ++applied;
        }
        return applied;
    }

private:
    static bool documentMatches(const DocumentInfo& lhs, const DocumentInfo& rhs) {
        return lhs.filePath == rhs.filePath && lhs.fileName == rhs.fileName &&
               lhs.fileExtension == rhs.fileExtension && lhs.fileSize == rhs.fileSize &&
               lhs.sha256Hash == rhs.sha256Hash && lhs.mimeType == rhs.mimeType &&
               lhs.createdTime == rhs.createdTime && lhs.modifiedTime == rhs.modifiedTime &&
               lhs.indexedTime == rhs.indexedTime;
    }

    static bool
    metadataMatches(const std::unordered_map<std::string, MetadataValue>& current,
                    const std::vector<std::pair<std::string, MetadataValue>>& expected) {
        if (current.size() != expected.size()) {
            return false;
        }
        return std::all_of(expected.begin(), expected.end(), [&](const auto& entry) {
            const auto it = current.find(entry.first);
            return it != current.end() && it->second.type == entry.second.type &&
                   it->second.value == entry.second.value;
        });
    }

    static memory_sync::MetadataDocumentRecord
    toRecord(const DocumentInfo& info,
             const std::vector<std::pair<std::string, MetadataValue>>& tags) {
        memory_sync::MetadataDocumentRecord record;
        record.documentId = info.sha256Hash;
        record.filePath = info.filePath;
        record.fileName = info.fileName;
        record.fileExtension = info.fileExtension;
        record.fileSize = info.fileSize;
        record.contentHash = info.sha256Hash;
        record.mimeType = info.mimeType;
        record.createdTime = info.createdTime.time_since_epoch().count();
        record.modifiedTime = info.modifiedTime.time_since_epoch().count();
        record.indexedTime = info.indexedTime.time_since_epoch().count();
        // Retain neutral legacy wire fields, not replica-local operational state.
        // Otherwise two peers with different extractor availability repeatedly
        // republish the same portable metadata with different payload hashes.
        for (const auto& [key, value] : tags) {
            record.metadata[key] = memory_sync::MetadataValueRecord{
                .value = value.value,
                .type = static_cast<std::int32_t>(value.type),
            };
        }
        return record;
    }

    static BatchDocumentInsert toInsert(const memory_sync::MetadataDocumentRecord& record) {
        BatchDocumentInsert item;
        item.info.sha256Hash = record.contentHash.empty() ? record.documentId : record.contentHash;
        item.info.filePath = record.filePath;
        item.info.fileName = record.fileName;
        item.info.fileExtension = record.fileExtension;
        item.info.fileSize = record.fileSize;
        item.info.mimeType = record.mimeType;
        item.info.setCreatedTime(record.createdTime);
        item.info.setModifiedTime(record.modifiedTime);
        item.info.setIndexedTime(record.indexedTime);
        // Sender readiness is not evidence of receiver-local content/FTS or repair
        // completion. Keep the wire fields readable for compatibility, but new
        // local documents must derive their own indexes. Existing local status is
        // preserved atomically by replaceDocumentAndMetadata.
        item.info.contentExtracted = false;
        item.info.extractionStatus = ExtractionStatus::Pending;
        item.info.repairStatus = RepairStatus::Pending;
        populatePathDerivedFields(item.info);
        for (const auto& [key, value] : record.metadata) {
            MetadataValue mv;
            mv.value = value.value;
            mv.type = static_cast<MetadataValueType>(value.type);
            item.tags.emplace_back(key, std::move(mv));
        }
        return item;
    }

    static std::string documentKey(std::string_view contentHash) {
        return std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::Document)) + "/" +
               memory_sync::escapeRecordKeySegment(contentHash);
    }

    MetadataRepository& repository_;
    memory_sync::MemorySyncService& sync_;
    ContentExists contentExists_;
    AppliedObserver appliedObserver_;
};

} // namespace yams::metadata
