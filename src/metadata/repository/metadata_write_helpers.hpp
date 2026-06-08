// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace yams {
template <typename T> class Result;
}

namespace yams::metadata {

class Database;
struct MetadataValue;

namespace repository {

struct MetadataTagDelta {
    int64_t tagCountDelta{0};
    int64_t docsWithTagsDelta{0};
};

using MetadataWriteEntry = std::tuple<int64_t, std::string, MetadataValue>;
using PendingTagKeysByDoc = std::unordered_map<int64_t, std::vector<std::string>>;

bool isTagMetadataKey(std::string_view key);
std::vector<MetadataWriteEntry>
deduplicateMetadataWrites(const std::vector<MetadataWriteEntry>& entries);
PendingTagKeysByDoc collectPendingTagKeysByDoc(const std::vector<MetadataWriteEntry>& entries);
Result<MetadataTagDelta>
calculateMetadataTagDeltaForUpsert(Database& db, const std::vector<MetadataWriteEntry>& entries);
Result<MetadataTagDelta>
calculateMetadataTagDeltaForUpsert(Database& db, const PendingTagKeysByDoc& pendingTagKeysByDoc);
Result<MetadataTagDelta> calculateMetadataTagDeltaForDelete(Database& db, int64_t documentId,
                                                            std::string_view key);
Result<void> upsertMetadataWrites(Database& db, const std::vector<MetadataWriteEntry>& entries);
Result<MetadataTagDelta>
upsertMetadataWritesWithTagDelta(Database& db, const std::vector<MetadataWriteEntry>& entries);
Result<MetadataTagDelta> upsertMetadataWritesWithTagDelta(
    Database& db, const std::vector<MetadataWriteEntry>& entries,
    const PendingTagKeysByDoc& pendingTagKeysByDoc);

} // namespace repository
} // namespace yams::metadata
