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

// Resolve partial or full hash to a full hash using MetadataRepository.
inline Result<std::string> resolve_hash_prefix(yams::metadata::MetadataRepository& repo,
                                               const std::string& prefix) {
    if (!is_valid_hash_prefix(prefix)) {
        return Error{ErrorCode::InvalidArgument, "Invalid hash prefix (need 8-64 hex chars)"};
    }
    // Fallback approach: scan paths (consistent with existing CLI patterns)
    auto docs = metadata::queryDocumentsByPattern(repo, "%");
    if (!docs) {
        return Error{docs.error().code, docs.error().message};
    }
    std::string lowerPrefix = prefix;
    std::transform(lowerPrefix.begin(), lowerPrefix.end(), lowerPrefix.begin(), ::tolower);
    std::vector<std::string> matches;
    for (const auto& d : docs.value()) {
        std::string h = d.sha256Hash;
        std::transform(h.begin(), h.end(), h.begin(), ::tolower);
        if (h.rfind(lowerPrefix, 0) == 0) {
            matches.push_back(d.sha256Hash);
        }
    }
    if (matches.empty())
        return Error{ErrorCode::NotFound, "No document with given hash prefix"};
    if (matches.size() > 1)
        return Error{ErrorCode::InvalidOperation, "Ambiguous hash prefix"};
    return matches.front();
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

// Resolve either hash(prefix) or name according to flags.
inline Result<std::string> resolve_hash_or_name(yams::metadata::MetadataRepository& repo,
                                                const std::string& hash, const std::string& name,
                                                bool latest = false, bool oldest = false) {
    if (!hash.empty()) {
        if (hash.size() == 64 && std::all_of(hash.begin(), hash.end(), is_hex)) {
            return hash; // full hash
        }
        return resolve_hash_prefix(repo, hash);
    }
    if (!name.empty())
        return resolve_name(repo, name, latest, oldest);
    return Error{ErrorCode::InvalidArgument, "No hash or name provided"};
}

} // namespace yams::common
