// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/metadata/document_metadata.h>

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
bool applyDocTimeFilters(const metadata::DocumentInfo& doc, const DocTimeFilters& filters);

} // namespace yams::cli
