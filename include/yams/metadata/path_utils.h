#pragma once

#include <string>

namespace yams::metadata {

struct PathDerivedValues {
    std::string normalizedPath;
    std::string pathPrefix;
    std::string reversePath;
    std::string pathHash;
    std::string parentHash;
    int pathDepth{0};
};

/**
 * @brief Compute normalized path metadata used for fast lookup.
 */
PathDerivedValues computePathDerivedValues(const std::string& filePath);

} // namespace yams::metadata
