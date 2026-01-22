#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>

#include <yams/metadata/document_metadata.h>
#include <yams/metadata/path_utils.h>

namespace yams::app::services {

inline std::string normalizeExtension(const std::string& ext) {
    if (ext.empty())
        return {};
    if (ext[0] == '.')
        return ext;
    return "." + ext;
}

inline bool isTextMime(const std::string& mime) {
    return !mime.empty() && mime.substr(0, 5) == "text/";
}

inline int64_t toEpochSeconds(const std::chrono::system_clock::time_point& tp) {
    return std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();
}

inline void populatePathDerivedFields(metadata::DocumentInfo& info) {
    auto derived = metadata::computePathDerivedValues(info.filePath);
    info.filePath = derived.normalizedPath;
    info.pathPrefix = derived.pathPrefix;
    info.reversePath = derived.reversePath;
    info.pathHash = derived.pathHash;
    info.parentHash = derived.parentHash;
    info.pathDepth = derived.pathDepth;
}

inline const char* toFileType(const std::string& mime) {
    return isTextMime(mime) ? "text" : "binary";
}

inline std::string globToSqlLike(const std::string& glob) {
    if (glob.empty())
        return "%";
    std::string sql = glob;
    std::replace(sql.begin(), sql.end(), '*', '%');
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
