// yams/common/fs_utils.h - Consolidated filesystem utilities
#pragma once

#include <chrono>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>

namespace yams::common {

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

} // namespace yams::common
