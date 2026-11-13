#pragma once

#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

#include <algorithm>
#include <cctype>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yams::common {

constexpr bool is_hex(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

constexpr bool is_valid_hash_prefix(std::string_view s) {
    if (s.size() < 8 || s.size() > 64)
        return false;
    for (char c : s) {
        if (!is_hex(c))
            return false;
    }
    return true;
}

// Resolve by exact path or name; on tie and flags, choose latest/oldest by indexedTime.
inline Result<std::string> resolve_name(yams::metadata::MetadataRepository& repo,
                                        const std::string& name, bool latest = false,
                                        bool oldest = false) {
    // Try exact path first
    auto exact = metadata::queryDocumentsByPattern(repo, name);
    if (exact && !exact.value().empty()) {
        if (exact.value().size() == 1)
            return exact.value()[0].sha256Hash;
        // Multiple: choose by time if flags provided
        auto docs = exact.value();
        if (latest || oldest) {
            std::sort(docs.begin(), docs.end(), [&](const auto& a, const auto& b) {
                return latest ? (a.indexedTime > b.indexedTime) : (a.indexedTime < b.indexedTime);
            });
            return docs.front().sha256Hash;
        }
    }
    // Try match by filename at any path
    auto docs2 = metadata::queryDocumentsByPattern(repo, "%/" + name);
    if (docs2 && !docs2.value().empty()) {
        auto docs = docs2.value();
        // Filter stricter equality on filename or path
        std::vector<yams::metadata::DocumentInfo> filtered;
        for (const auto& d : docs) {
            if (d.fileName == name || d.filePath == name)
                filtered.push_back(d);
        }
        if (filtered.empty())
            filtered = std::move(docs);
        if (filtered.size() == 1)
            return filtered[0].sha256Hash;
        if (latest || oldest) {
            std::sort(filtered.begin(), filtered.end(), [&](const auto& a, const auto& b) {
                return latest ? (a.indexedTime > b.indexedTime) : (a.indexedTime < b.indexedTime);
            });
            return filtered.front().sha256Hash;
        }
        return Error{ErrorCode::InvalidOperation, "Multiple documents with same name"};
    }
    // Fallback: keyword search then exact name match
    auto sr = repo.search(name, 50, 0);
    if (sr) {
        std::vector<yams::metadata::DocumentInfo> candidates;
        for (const auto& r : sr.value().results) {
            const auto& d = r.document;
            if (d.fileName == name || d.filePath == name)
                candidates.push_back(d);
        }
        if (!candidates.empty()) {
            if (candidates.size() == 1)
                return candidates[0].sha256Hash;
            if (latest || oldest) {
                std::sort(candidates.begin(), candidates.end(), [&](const auto& a, const auto& b) {
                    return latest ? (a.indexedTime > b.indexedTime)
                                  : (a.indexedTime < b.indexedTime);
                });
                return candidates.front().sha256Hash;
            }
            return Error{ErrorCode::InvalidOperation, "Multiple documents with same name"};
        }
    }
    return Error{ErrorCode::NotFound, "No document found for given name"};
}

} // namespace yams::common
