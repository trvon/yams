#pragma once

#include <cctype>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <set>
#include <string>
#include <string_view>

namespace yams::daemon::plugin_trust {

struct TrustStoreLoadResult {
    std::set<std::filesystem::path> entries;
    bool loaded{false};
    bool migrated{false};
};

inline constexpr std::string_view kTrustFileHeader = "# YAMS Plugin Trust List\n";
inline constexpr std::string_view kTrustFileBodyComment = "# One plugin path per line\n";

inline std::string_view trimAscii(std::string_view s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
        s.remove_prefix(1);
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
        s.remove_suffix(1);
    }
    return s;
}

inline bool hasUnsafeTrustPathChars(std::string_view s) {
    for (unsigned char ch : s) {
        if (ch == '\0' || ch == '\n' || ch == '\r') {
            return true;
        }
    }
    return false;
}

inline std::filesystem::path normalizePath(const std::filesystem::path& path) {
    std::error_code ec;
    auto canon = std::filesystem::weakly_canonical(path, ec);
    if (ec) {
        canon = path.lexically_normal();
    }
    return canon;
}

inline bool isPersistableTrustPath(const std::filesystem::path& path) {
    if (path.empty()) {
        return false;
    }

    const auto normalized = normalizePath(path);
    const auto serialized = normalized.string();
    return normalized.is_absolute() && !serialized.empty() && !hasUnsafeTrustPathChars(serialized);
}

// Parses a trust file body (one path per line).
// - Trims ASCII whitespace around each line.
// - Ignores empty lines.
// - Ignores comment lines whose first non-whitespace character is '#'.
// Returns raw paths as written; callers may canonicalize if desired.
inline std::set<std::filesystem::path> parseTrustList(std::string_view content) {
    std::set<std::filesystem::path> out;

    while (!content.empty()) {
        const size_t nl = content.find('\n');
        std::string_view line = (nl == std::string_view::npos) ? content : content.substr(0, nl);
        content = (nl == std::string_view::npos) ? std::string_view{} : content.substr(nl + 1);

        if (!line.empty() && line.back() == '\r') {
            line.remove_suffix(1);
        }

        line = trimAscii(line);
        if (line.empty() || line.front() == '#') {
            continue;
        }

        out.insert(std::filesystem::path(std::string(line)));
    }

    return out;
}

inline bool readTrustStore(const std::filesystem::path& trustFile,
                           std::set<std::filesystem::path>& entries) {
    std::ifstream in(trustFile);
    if (!in) {
        return false;
    }

    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    for (const auto& raw : parseTrustList(content)) {
        const auto normalized = normalizePath(raw);
        if (isPersistableTrustPath(normalized)) {
            entries.insert(normalized);
        }
    }
    return true;
}

inline TrustStoreLoadResult loadTrustStore(const std::filesystem::path& trustFile,
                                           const std::filesystem::path& canonicalTrustFile = {},
                                           const std::filesystem::path& legacyTrustFile = {}) {
    TrustStoreLoadResult result;
    if (trustFile.empty()) {
        return result;
    }

    if (readTrustStore(trustFile, result.entries)) {
        result.loaded = true;
        return result;
    }

    const auto trustNorm = normalizePath(trustFile);
    const auto canonicalNorm =
        canonicalTrustFile.empty() ? trustNorm : normalizePath(canonicalTrustFile);
    if (trustNorm != canonicalNorm || legacyTrustFile.empty()) {
        return result;
    }

    const auto legacyNorm = normalizePath(legacyTrustFile);
    if (legacyNorm == trustNorm) {
        return result;
    }

    std::error_code ec;
    if (std::filesystem::exists(legacyTrustFile, ec) && !ec &&
        readTrustStore(legacyTrustFile, result.entries)) {
        result.loaded = true;
        result.migrated = true;
    }

    return result;
}

inline void enforcePrivateTrustFilePermissions(const std::filesystem::path& path) {
#if !defined(_WIN32)
    std::error_code ec;
    std::filesystem::permissions(
        path, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace, ec);
#else
    (void)path;
#endif
}

// Trust roots are operational configuration, not secrets. Persist only canonical,
// absolute filesystem paths and lock the file to owner-only permissions.
inline bool writeTrustStore(const std::filesystem::path& trustFile,
                            const std::set<std::filesystem::path>& entries) {
    if (trustFile.empty()) {
        return false;
    }

    std::set<std::filesystem::path> normalizedEntries;
    for (const auto& entry : entries) {
        const auto normalized = normalizePath(entry);
        if (!isPersistableTrustPath(normalized)) {
            return false;
        }
        normalizedEntries.insert(normalized);
    }

    std::error_code ec;
    const auto parent = trustFile.parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent, ec);
        if (ec) {
            return false;
        }
    }

    auto tempPath = trustFile;
    tempPath += ".tmp";

    std::ofstream out(tempPath, std::ios::trunc);
    if (!out) {
        return false;
    }

    out << kTrustFileHeader;
    out << kTrustFileBodyComment;
    for (const auto& entry : normalizedEntries) {
        out << entry.string() << '\n';
    }
    out.close();
    if (!out) {
        std::error_code cleanupEc;
        std::filesystem::remove(tempPath, cleanupEc);
        return false;
    }

    enforcePrivateTrustFilePermissions(tempPath);

    ec.clear();
    std::filesystem::rename(tempPath, trustFile, ec);
#if defined(_WIN32)
    if (ec) {
        std::error_code removeEc;
        std::filesystem::remove(trustFile, removeEc);
        ec.clear();
        std::filesystem::rename(tempPath, trustFile, ec);
    }
#endif
    if (ec) {
        std::error_code cleanupEc;
        std::filesystem::remove(tempPath, cleanupEc);
        return false;
    }

    enforcePrivateTrustFilePermissions(trustFile);
    return true;
}

// Returns true iff `base` is a prefix of `candidate` in terms of filesystem path
// *components* (not a string prefix).
// This avoids bypasses like base='/trusted' trusting '/trusted_evil/...'.
// Note: Callers should normalize/canonicalize paths as appropriate for their trust model.
inline bool isPathWithin(const std::filesystem::path& base,
                         const std::filesystem::path& candidate) {
    if (base.empty()) {
        return false;
    }

    auto b = base.begin();
    auto c = candidate.begin();
    for (; b != base.end() && c != candidate.end(); ++b, ++c) {
        if (*b != *c) {
            return false;
        }
    }
    return b == base.end();
}

} // namespace yams::daemon::plugin_trust
