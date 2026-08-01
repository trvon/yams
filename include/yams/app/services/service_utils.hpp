#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>

#include <yams/common/time_utils.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/path_utils.h>

namespace yams::app::services {
/**
 * @brief Retry telemetry counters shared by metadata retry loops.
 */
struct MetadataTelemetry {
    std::atomic<std::uint64_t> operations{0};
    std::atomic<std::uint64_t> retries{0};
    std::atomic<std::uint64_t> transientFailures{0};
};

/**
 * @brief Whether an error is transient (safe to retry).
 *
 * Structured error codes plus message-level checks for the SQLite lock errors
 * that surface as InternalError.
 */
inline bool isTransientMetadataError(const Error& err) noexcept {
    switch (err.code) {
        case ErrorCode::NotInitialized:
        case ErrorCode::DatabaseError:
        case ErrorCode::ResourceBusy:
        case ErrorCode::OperationInProgress:
        case ErrorCode::Timeout:
            return true;
        case ErrorCode::InternalError: {
            const auto& msg = err.message;
            return msg.find("database is locked") != std::string::npos ||
                   msg.find("readonly") != std::string::npos ||
                   msg.find("busy") != std::string::npos;
        }
        default:
            return false;
    }
}

inline std::string normalizeExtension(const std::string& ext) {
    if (ext.empty())
        return {};
    if (ext[0] == '.')
        return ext;
    return "." + ext;
}

inline bool isTextMime(const std::string& mime) {
    if (mime.empty())
        return false;
    if (mime.substr(0, 5) == "text/")
        return true;
    return mime == "application/json" || mime == "application/xml" ||
           mime == "application/javascript" || mime == "application/x-yaml" ||
           mime == "application/yaml" || mime == "application/x-sh";
}

inline int64_t toEpochSeconds(const std::chrono::system_clock::time_point& tp) {
    return yams::common::timePointToEpochSeconds(tp);
}

inline const char* toFileType(const std::string& mime) {
    return isTextMime(mime) ? "text" : "binary";
}

inline std::string globToSqlLike(const std::string& glob) {
    if (glob.empty())
        return "%";
    std::string sql = glob;

    // Replace ** with a placeholder first, then handle single *
    // "**" in glob matches zero or more directories, map to single "%" in SQL LIKE
    std::string placeholder = "\x01";
    std::size_t pos = 0;
    while ((pos = sql.find("**", pos)) != std::string::npos) {
        sql.replace(pos, 2, placeholder);
        pos += placeholder.length();
    }
    // Replace single * with %
    std::replace(sql.begin(), sql.end(), '*', '%');
    // Restore ** placeholders to single %
    pos = 0;
    while ((pos = sql.find(placeholder, pos)) != std::string::npos) {
        sql.replace(pos, placeholder.length(), "%");
        pos += 1;
    }

    std::replace(sql.begin(), sql.end(), '?', '_');

    const bool is_absolute = !glob.empty() && std::filesystem::path(glob).is_absolute();
    const bool hasSlash =
        (sql.find('/') != std::string::npos) || (sql.find('\\') != std::string::npos);
    if (hasSlash && sql.front() != '%' && !is_absolute) {
        sql = "%" + sql;
    } else if (!sql.empty() && sql.front() != '%' && !hasSlash) {
        sql = "%/" + sql;
    }
    return sql;
}

} // namespace yams::app::services
