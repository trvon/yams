#pragma once

#include <cstddef>
#include <string>
#include <unordered_set>
#include <vector>

#include <yams/app/services/services.hpp>

namespace yams::app::services::path_projection {

inline std::size_t effectiveLimit(std::size_t requestedLimit, std::size_t fallbackSize) {
    return requestedLimit > 0 ? requestedLimit : fallbackSize;
}

inline std::string displayPath(const std::string& filePath, const std::string& fileName) {
    return !filePath.empty() ? filePath : fileName;
}

inline bool appendUniquePath(std::vector<std::string>& paths, std::unordered_set<std::string>& seen,
                             const std::string& path, std::size_t limit = 0) {
    if (path.empty()) {
        return false;
    }
    if (limit > 0 && paths.size() >= limit) {
        return false;
    }
    if (!seen.insert(path).second) {
        return false;
    }
    paths.push_back(path);
    return true;
}

inline void finalizePathsOnly(SearchResponse& resp) {
    resp.results.clear();
    resp.total = resp.paths.size();
}

template <typename Range, typename ProjectPath>
void assignPathsOnly(SearchResponse& resp, const Range& range, std::size_t requestedLimit,
                     ProjectPath projectPath) {
    resp.paths.clear();
    std::unordered_set<std::string> seen;
    const std::size_t limit = requestedLimit > 0 ? requestedLimit : 0;

    for (const auto& item : range) {
        if (limit > 0 && resp.paths.size() >= limit) {
            break;
        }
        appendUniquePath(resp.paths, seen, projectPath(item), limit);
    }

    finalizePathsOnly(resp);
}

inline void mergePathsOnly(SearchResponse& target, const SearchResponse& source,
                           std::size_t requestedLimit) {
    std::unordered_set<std::string> seen(target.paths.begin(), target.paths.end());
    const std::size_t limit = requestedLimit > 0 ? requestedLimit : 0;
    for (const auto& path : source.paths) {
        if (limit > 0 && target.paths.size() >= limit) {
            break;
        }
        appendUniquePath(target.paths, seen, path, limit);
    }
    finalizePathsOnly(target);
}

} // namespace yams::app::services::path_projection
