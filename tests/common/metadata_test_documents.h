// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#pragma once

#include <chrono>
#include <filesystem>
#include <string>

#include <yams/metadata/document_metadata.h>
#include <yams/metadata/path_utils.h>

namespace yams::test {

/// A fully extracted 100-byte ".txt" text/plain document at `filePath`, with path-derived
/// columns filled in. `fileName` defaults to the last path component.
inline metadata::DocumentInfo makeDocInfo(const std::string& filePath, const std::string& hash,
                                          const std::string& fileName = {}) {
    metadata::DocumentInfo info;
    info.filePath = filePath;
    info.fileName =
        fileName.empty() ? std::filesystem::path(filePath).filename().string() : fileName;
    info.fileExtension = ".txt";
    info.fileSize = 100;
    info.sha256Hash = hash;
    info.mimeType = "text/plain";
    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.createdTime = now;
    info.modifiedTime = now;
    info.indexedTime = now;
    info.contentExtracted = true;
    info.extractionStatus = metadata::ExtractionStatus::Success;
    auto derived = metadata::computePathDerivedValues(filePath);
    info.filePath = derived.normalizedPath;
    info.pathPrefix = derived.pathPrefix;
    info.reversePath = derived.reversePath;
    info.pathHash = derived.pathHash;
    info.parentHash = derived.parentHash;
    info.pathDepth = derived.pathDepth;
    return info;
}

} // namespace yams::test
