// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/metadata/database.h>
#include <yams/metadata/document_metadata.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace yams::metadata::repository {

/// The path-tree leaf (`path_tree_nodes.full_path`) a document with this file path is linked
/// to: empty segments are dropped ("a//b/" -> "a/b") and a leading '/' is kept. Empty when the
/// path has no segments (such a document has no path-tree entry).
std::string pathTreeLeafPath(std::string_view filePath);

/// Create the path-tree nodes for a document's file path and link the document to each of
/// them, inside the caller's transaction. Idempotent: an existing link is not counted twice.
Result<void> upsertPathTreeForDocumentInTransaction(Database& db, const DocumentInfo& info,
                                                    int64_t documentId);

} // namespace yams::metadata::repository
