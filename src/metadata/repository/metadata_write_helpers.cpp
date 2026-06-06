// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include "metadata_write_helpers.hpp"

#include <yams/metadata/database.h>
#include <yams/metadata/document_metadata.h>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "result_helpers.hpp"

namespace yams::metadata::repository {

bool isTagMetadataKey(std::string_view key) {
    return key == "tag" || key.starts_with("tag:");
}

namespace {
struct MetadataWriteKey {
    int64_t documentId{0};
    std::string key;

    bool operator==(const MetadataWriteKey& other) const noexcept {
        return documentId == other.documentId && key == other.key;
    }
};

struct MetadataWriteKeyHash {
    std::size_t operator()(const MetadataWriteKey& value) const noexcept {
        std::size_t seed = std::hash<int64_t>{}(value.documentId);
        seed ^=
            std::hash<std::string>{}(value.key) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        return seed;
    }
};

std::string buildMetadataUpsertSql(int rows) {
    std::string sql;
    sql.reserve(static_cast<std::size_t>(rows) * 20 + 200);
    sql += "INSERT INTO metadata (document_id, key, value, value_type) VALUES ";
    for (int i = 0; i < rows; ++i) {
        if (i > 0) {
            sql += ',';
        }
        sql += "(?, ?, ?, ?)";
    }
    sql += " ON CONFLICT(document_id, key) DO UPDATE SET value = excluded.value, "
           "value_type = excluded.value_type";
    return sql;
}
} // namespace

std::vector<MetadataWriteEntry>
deduplicateMetadataWrites(const std::vector<MetadataWriteEntry>& entries) {
    std::vector<MetadataWriteEntry> deduped;
    deduped.reserve(entries.size());

    std::unordered_map<MetadataWriteKey, std::size_t, MetadataWriteKeyHash> seen;
    seen.reserve(entries.size());
    for (const auto& entry : entries) {
        MetadataWriteKey key{std::get<0>(entry), std::get<1>(entry)};
        auto it = seen.find(key);
        if (it != seen.end()) {
            deduped[it->second] = entry;
        } else {
            seen.emplace(std::move(key), deduped.size());
            deduped.push_back(entry);
        }
    }
    return deduped;
}

Result<MetadataTagDelta>
calculateMetadataTagDeltaForUpsert(Database& db, const std::vector<MetadataWriteEntry>& entries) {
    std::unordered_map<int64_t, std::vector<std::string>> pendingTagKeysByDoc;
    for (const auto& [documentId, key, _value] : entries) {
        if (isTagMetadataKey(key)) {
            pendingTagKeysByDoc[documentId].push_back(key);
        }
    }

    MetadataTagDelta delta;
    if (pendingTagKeysByDoc.empty()) {
        return delta;
    }

    YAMS_TRY_UNWRAP(tagCountStmt, db.prepareCached("SELECT COUNT(*) FROM metadata WHERE "
                                                   "document_id = ? AND (key = 'tag' OR key "
                                                   "LIKE 'tag:%')"));
    YAMS_TRY_UNWRAP(keyExistsStmt, db.prepareCached("SELECT COUNT(*) FROM metadata WHERE "
                                                    "document_id = ? AND key = ?"));

    for (auto& [documentId, keys] : pendingTagKeysByDoc) {
        auto& tcStmt = *tagCountStmt;
        YAMS_TRY(tcStmt.reset());
        YAMS_TRY(tcStmt.bind(1, documentId));
        YAMS_TRY_UNWRAP(tagRow, tcStmt.step());
        const auto priorTagCount = tagRow ? tcStmt.getInt64(0) : 0;
        bool docWillGainFirstTag = priorTagCount == 0;

        std::unordered_set<std::string> seenKeys;
        seenKeys.reserve(keys.size());
        for (const auto& tagKey : keys) {
            if (!seenKeys.insert(tagKey).second) {
                continue;
            }

            auto& keStmt = *keyExistsStmt;
            YAMS_TRY(keStmt.reset());
            YAMS_TRY(keStmt.bind(1, documentId));
            YAMS_TRY(keStmt.bind(2, tagKey));
            YAMS_TRY_UNWRAP(keyRow, keStmt.step());
            const auto priorKeyCount = keyRow ? keStmt.getInt64(0) : 0;
            if (priorKeyCount == 0) {
                ++delta.tagCountDelta;
                if (docWillGainFirstTag) {
                    ++delta.docsWithTagsDelta;
                    docWillGainFirstTag = false;
                }
            }
        }
    }

    return delta;
}

Result<MetadataTagDelta> calculateMetadataTagDeltaForDelete(Database& db, int64_t documentId,
                                                            std::string_view key) {
    MetadataTagDelta delta;
    if (!isTagMetadataKey(key)) {
        return delta;
    }

    YAMS_TRY_UNWRAP(tagCountStmt, db.prepareCached("SELECT COUNT(*) FROM metadata WHERE "
                                                   "document_id = ? AND (key = 'tag' OR key "
                                                   "LIKE 'tag:%')"));
    YAMS_TRY(tagCountStmt->reset());
    YAMS_TRY(tagCountStmt->bind(1, documentId));
    YAMS_TRY_UNWRAP(tagRow, tagCountStmt->step());
    const auto priorTagCount = tagRow ? tagCountStmt->getInt64(0) : 0;

    YAMS_TRY_UNWRAP(keyExistsStmt, db.prepareCached("SELECT COUNT(*) FROM metadata WHERE "
                                                    "document_id = ? AND key = ?"));
    YAMS_TRY(keyExistsStmt->reset());
    YAMS_TRY(keyExistsStmt->bind(1, documentId));
    YAMS_TRY(keyExistsStmt->bind(2, key));
    YAMS_TRY_UNWRAP(keyRow, keyExistsStmt->step());
    const auto priorKeyCount = keyRow ? keyExistsStmt->getInt64(0) : 0;

    if (priorKeyCount > 0) {
        delta.tagCountDelta = -1;
        if (priorTagCount == 1) {
            delta.docsWithTagsDelta = -1;
        }
    }
    return delta;
}

Result<void> upsertMetadataWrites(Database& db, const std::vector<MetadataWriteEntry>& entries) {
    if (entries.empty()) {
        return {};
    }

    if (entries.size() == 1) {
        static const std::string singleRowSql = buildMetadataUpsertSql(1);
        YAMS_TRY_UNWRAP(stmt, db.prepareCached(singleRowSql));
        YAMS_TRY(stmt->reset());
        const auto& [documentId, key, value] = entries.front();
        YAMS_TRY(stmt->bind(1, documentId));
        YAMS_TRY(stmt->bind(2, key));
        YAMS_TRY(stmt->bind(3, value.value));
        YAMS_TRY(stmt->bind(4, MetadataValueTypeUtils::toStringView(value.type)));
        YAMS_TRY(stmt->execute());
        return {};
    }

    constexpr int kColumnsPerRow = 4;
    constexpr int kSqliteParamLimit = 999;
    constexpr int kMaxRowsPerChunk = kSqliteParamLimit / kColumnsPerRow;
    const std::string fullChunkSql = buildMetadataUpsertSql(kMaxRowsPerChunk);

    for (std::size_t offset = 0; offset < entries.size(); offset += kMaxRowsPerChunk) {
        const int rows = static_cast<int>(
            std::min(entries.size() - offset, static_cast<std::size_t>(kMaxRowsPerChunk)));

        if (rows == kMaxRowsPerChunk) {
            YAMS_TRY_UNWRAP(stmt, db.prepareCached(fullChunkSql));
            YAMS_TRY(stmt->reset());
            int bindIndex = 1;
            for (int i = 0; i < rows; ++i) {
                const auto& [documentId, key, value] =
                    entries[offset + static_cast<std::size_t>(i)];
                YAMS_TRY(stmt->bind(bindIndex++, documentId));
                YAMS_TRY(stmt->bind(bindIndex++, key));
                YAMS_TRY(stmt->bind(bindIndex++, value.value));
                YAMS_TRY(stmt->bind(bindIndex++, MetadataValueTypeUtils::toStringView(value.type)));
            }
            YAMS_TRY(stmt->execute());
        } else {
            const std::string tailSql = buildMetadataUpsertSql(rows);
            YAMS_TRY_UNWRAP(stmt, db.prepare(tailSql));
            int bindIndex = 1;
            for (int i = 0; i < rows; ++i) {
                const auto& [documentId, key, value] =
                    entries[offset + static_cast<std::size_t>(i)];
                YAMS_TRY(stmt.bind(bindIndex++, documentId));
                YAMS_TRY(stmt.bind(bindIndex++, key));
                YAMS_TRY(stmt.bind(bindIndex++, value.value));
                YAMS_TRY(stmt.bind(bindIndex++, MetadataValueTypeUtils::toStringView(value.type)));
            }
            YAMS_TRY(stmt.execute());
        }
    }

    return {};
}

Result<MetadataTagDelta>
upsertMetadataWritesWithTagDelta(Database& db, const std::vector<MetadataWriteEntry>& entries) {
    YAMS_TRY_UNWRAP(delta, calculateMetadataTagDeltaForUpsert(db, entries));
    YAMS_TRY(upsertMetadataWrites(db, entries));
    return delta;
}

} // namespace yams::metadata::repository
