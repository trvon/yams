#pragma once

// Private to the storage library: the naming rules that map a CAS object file to its storage key.
// StorageEngine writes objects as <root>/<prefix>/<suffix>, so the key is the concatenation of
// the path components below the root. Anything else under the root is not an object.

#include <yams/core/types.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>

namespace yams::storage {

inline bool isHexHash(std::string_view hash) noexcept {
    return hash.size() == HASH_STRING_SIZE &&
           std::ranges::all_of(hash, [](unsigned char c) { return std::isxdigit(c) != 0; });
}

inline constexpr std::string_view kManifestKeySuffix = ".manifest";

inline bool isManifestKey(std::string_view key) noexcept {
    return key.ends_with(kManifestKeySuffix);
}

/// A content hash, or a content hash followed by ".manifest".
inline bool isValidStorageKey(std::string_view key) noexcept {
    if (isHexHash(key)) {
        return true;
    }
    if (!isManifestKey(key)) {
        return false;
    }
    return isHexHash(key.substr(0, key.size() - kManifestKeySuffix.size()));
}

/// The storage key named by an object file under objectsRoot; empty when the path is not below
/// objectsRoot. The result still needs isValidStorageKey().
inline std::string storageHashFromObjectPath(const std::filesystem::path& objectsRoot,
                                             const std::filesystem::path& objectPath) {
    std::error_code ec;
    const auto relative = std::filesystem::relative(objectPath, objectsRoot, ec);
    if (ec || relative.empty()) {
        return {};
    }

    std::string hash;
    for (const auto& part : relative) {
        hash += part.string();
    }
    return hash;
}

} // namespace yams::storage
