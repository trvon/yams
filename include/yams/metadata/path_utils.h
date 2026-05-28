#pragma once

#include <string>

#include <yams/metadata/document_metadata.h>

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

/**
 * @brief Populate DocumentInfo path-derived fields from filePath.
 *
 * Updates filePath to the normalized path and fills pathPrefix, reversePath, pathHash,
 * parentHash, and pathDepth using the same derivation as computePathDerivedValues().
 */
void populatePathDerivedFields(DocumentInfo& info);

} // namespace yams::metadata
