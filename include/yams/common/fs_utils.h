// yams/common/fs_utils.h - Consolidated filesystem utilities
#pragma once

#include <chrono>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace yams::common {

/**
 * macOS mounts /var and /tmp as symlinks into /private, so a document can be indexed under
 * either spelling. Everything that compares, displays, or expands stored paths goes through
 * this one table; the helpers are purely lexical and never touch the filesystem.
 */
namespace path_alias {

struct AliasPair {
    std::string_view shortForm;
    std::string_view privateForm;
};

inline constexpr AliasPair kAliases[] = {
    {"/var", "/private/var"},
    {"/tmp", "/private/tmp"},
};

/// True when `path` is exactly `root` or lives under it as a whole component.
[[nodiscard]] inline bool hasRoot(std::string_view path, std::string_view root) noexcept {
    return path.size() >= root.size() && path.compare(0, root.size(), root) == 0 &&
           (path.size() == root.size() || path[root.size()] == '/');
}

/// The other spelling of `path` (/var/x <-> /private/var/x), or empty when it has none.
[[nodiscard]] inline std::string otherSpelling(std::string_view path) {
    for (const auto& alias : kAliases) {
        if (hasRoot(path, alias.privateForm)) {
            return std::string(alias.shortForm) +
                   std::string(path.substr(alias.privateForm.size()));
        }
        if (hasRoot(path, alias.shortForm)) {
            return std::string(alias.privateForm) +
                   std::string(path.substr(alias.shortForm.size()));
        }
    }
    return {};
}

/// The /private spelling, used when comparing paths. Identity for anything else.
[[nodiscard]] inline std::string canonicalSpelling(std::string path) {
    for (const auto& alias : kAliases) {
        if (hasRoot(path, alias.shortForm)) {
            return std::string(alias.privateForm) + path.substr(alias.shortForm.size());
        }
    }
    return path;
}

/// The short spelling, used when showing a path back to the operator. Identity otherwise.
[[nodiscard]] inline std::string displaySpelling(std::string path) {
    for (const auto& alias : kAliases) {
        if (hasRoot(path, alias.privateForm)) {
            return std::string(alias.shortForm) + path.substr(alias.privateForm.size());
        }
    }
    return path;
}

} // namespace path_alias

/// Canonical (/private) spelling on macOS; identity on every other platform.
[[nodiscard]] inline std::string canonicalizeMacPathAlias(std::string path) {
#if defined(__APPLE__)
    return path_alias::canonicalSpelling(std::move(path));
#else
    return path;
#endif
}

/// Display (short) spelling on macOS; identity on every other platform.
[[nodiscard]] inline std::string displayMacPathAlias(std::string path) {
#if defined(__APPLE__)
    return path_alias::displaySpelling(std::move(path));
#else
    return path;
#endif
}

/// On macOS, append the other spelling of every pattern that has one (deduplicated), so a
/// pattern written as /var/... also matches documents stored as /private/var/... and vice
/// versa. No-op on every other platform.
inline void appendMacPathAliases(std::vector<std::string>& patterns) {
#if defined(__APPLE__)
    const std::size_t original = patterns.size();
    for (std::size_t i = 0; i < original; ++i) {
        std::string alias = path_alias::otherSpelling(patterns[i]);
        if (alias.empty())
            continue;
        bool seen = false;
        for (const auto& existing : patterns) {
            if (existing == alias) {
                seen = true;
                break;
            }
        }
        if (!seen)
            patterns.push_back(std::move(alias));
    }
#else
    (void)patterns;
#endif
}

/**
 * Ensure parent directories exist for the given path.
 *
 * @param path The path whose parent should exist
 * @return true if directories exist or were created, false on error
 *
 * @note Uses std::error_code for non-throwing operation.
 *       Silently succeeds if directories already exist.
 *       Callers who need error details can use the overload with std::error_code&.
 */
inline bool ensureDirectories(const std::filesystem::path& path) noexcept {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    return !ec;
}

/**
 * Ensure parent directories exist for the given path, returning error info.
 *
 * @param path The path whose parent should exist
 * @param[out] error Set to error code on failure
 * @return true if directories exist or were created
 */
inline bool ensureDirectories(const std::filesystem::path& path, std::error_code& error) noexcept {
    std::filesystem::create_directories(path, error);
    return !error;
}

/**
 * Normalize a filesystem path for consistent comparison.
 *
 * Converts backslashes to forward slashes and collapses consecutive slashes.
 *
 * @param path The path to normalize (any type convertible to string_view)
 * @return Normalized path string
 */
template <typename T>
[[nodiscard]] inline std::string normalizePath(T&& path)
requires std::constructible_from<std::string_view, T>
{
    std::string_view sv{std::forward<T>(path)};
    std::string out;
    out.reserve(sv.size());
    char prev = 0;
    for (char c : sv) {
        char d = (c == '\\') ? '/' : c;
        if (d == '/' && prev == '/') {
            continue; // collapse consecutive slashes
        }
        out.push_back(d);
        prev = d;
    }
    // Strip trailing slash (except for root "/")
    if (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    return out;
}

/**
 * Canonicalize a filesystem path for identity and containment comparisons.
 *
 * Resolves existing symlinks when possible (for example /var -> /private/var on macOS) and
 * otherwise falls back to lexical normalization.
 */
[[nodiscard]] inline std::string canonicalizePathForComparison(std::string_view path) {
    std::filesystem::path normalized = std::filesystem::path(path).lexically_normal();
    std::error_code error;
    auto canonical = std::filesystem::weakly_canonical(normalized, error);
    if (!error && !canonical.empty()) {
        normalized = std::move(canonical);
    }
    return normalized.generic_string();
}

/**
 * Create a temporary directory with a unique name.
 *
 * @param prefix Prefix for the directory name (default: "yams_")
 * @return Path to created directory, or empty path on failure
 */
[[nodiscard]] inline std::filesystem::path createTempDirectory(std::string_view prefix = "yams_") {
    std::error_code ec;
    auto tempPath = std::filesystem::temp_directory_path(ec);
    if (ec) {
        return {};
    }

    // Generate unique name using timestamp + random
    auto uniqueId = std::chrono::steady_clock::now().time_since_epoch().count();
    auto path = tempPath / (std::string{prefix} + std::to_string(uniqueId));

    std::filesystem::create_directory(path, ec);
    if (ec) {
        return {};
    }
    return path;
}

/**
 * Check if a path exists and is a directory.
 *
 * @param path The path to check
 * @return true if path exists and is a directory
 */
[[nodiscard]] inline bool isDirectory(const std::filesystem::path& path) noexcept {
    std::error_code ec;
    return std::filesystem::is_directory(path, ec) && !ec;
}

/**
 * Check if a path exists.
 *
 * @param path The path to check
 * @return true if path exists
 */
[[nodiscard]] inline bool exists(const std::filesystem::path& path) noexcept {
    std::error_code ec;
    return std::filesystem::exists(path, ec) && !ec;
}

/**
 * Find the nearest ancestor containing a .git directory or worktree marker.
 *
 * @return The repository root, or an empty path when no marker is found.
 */
[[nodiscard]] inline std::filesystem::path findGitRoot(const std::filesystem::path& start) {
    std::error_code error;
    auto current = std::filesystem::absolute(start, error);
    if (error) {
        current = start;
    }

    while (!current.empty()) {
        if (std::filesystem::exists(current / ".git", error)) {
            return current;
        }
        const auto parent = current.parent_path();
        if (parent == current) {
            break;
        }
        current = parent;
    }
    return {};
}

} // namespace yams::common
