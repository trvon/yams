// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/cli/time_parser.h>
#include <yams/metadata/document_metadata.h>

#include <spdlog/spdlog.h>

#include <string>

namespace yams::cli {

/**
 * @brief Time filter values shared by commands that filter documents on
 * created/modified/indexed times. Empty strings mean "no filter".
 */
struct DocTimeFilters {
    std::string createdAfter;
    std::string createdBefore;
    std::string modifiedAfter;
    std::string modifiedBefore;
    std::string indexedAfter;
    std::string indexedBefore;
};

/**
 * @brief Returns false when @p doc does not match the time filters.
 *
 * Invalid filter strings are logged and treated as "no filter", matching the
 * historical per-command behavior (GetCommand/ListCommand applied identical
 * logic before this helper was extracted).
 */
inline bool applyDocTimeFilters(const metadata::DocumentInfo& doc, const DocTimeFilters& filters) {
    // Parse and apply created time filters
    if (!filters.createdAfter.empty()) {
        auto afterTime = TimeParser::parse(filters.createdAfter);
        if (!afterTime) {
            spdlog::warn("Invalid created-after time: {}", filters.createdAfter);
            return true; // Don't filter on invalid input
        }
        if (doc.createdTime < afterTime.value()) {
            return false;
        }
    }

    if (!filters.createdBefore.empty()) {
        auto beforeTime = TimeParser::parse(filters.createdBefore);
        if (!beforeTime) {
            spdlog::warn("Invalid created-before time: {}", filters.createdBefore);
            return true;
        }
        if (doc.createdTime > beforeTime.value()) {
            return false;
        }
    }

    // Parse and apply modified time filters
    if (!filters.modifiedAfter.empty()) {
        auto afterTime = TimeParser::parse(filters.modifiedAfter);
        if (!afterTime) {
            spdlog::warn("Invalid modified-after time: {}", filters.modifiedAfter);
            return true;
        }
        if (doc.modifiedTime < afterTime.value()) {
            return false;
        }
    }

    if (!filters.modifiedBefore.empty()) {
        auto beforeTime = TimeParser::parse(filters.modifiedBefore);
        if (!beforeTime) {
            spdlog::warn("Invalid modified-before time: {}", filters.modifiedBefore);
            return true;
        }
        if (doc.modifiedTime > beforeTime.value()) {
            return false;
        }
    }

    // Parse and apply indexed time filters
    if (!filters.indexedAfter.empty()) {
        auto afterTime = TimeParser::parse(filters.indexedAfter);
        if (!afterTime) {
            spdlog::warn("Invalid indexed-after time: {}", filters.indexedAfter);
            return true;
        }
        if (doc.indexedTime < afterTime.value()) {
            return false;
        }
    }

    if (!filters.indexedBefore.empty()) {
        auto beforeTime = TimeParser::parse(filters.indexedBefore);
        if (!beforeTime) {
            spdlog::warn("Invalid indexed-before time: {}", filters.indexedBefore);
            return true;
        }
        if (doc.indexedTime > beforeTime.value()) {
            return false;
        }
    }

    return true;
}

} // namespace yams::cli
