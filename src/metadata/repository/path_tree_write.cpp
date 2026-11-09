// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <string>
#include <string_view>
#include <vector>

#include "path_tree_write.hpp"
#include "result_helpers.hpp"

namespace yams::metadata::repository {

namespace {

std::vector<std::string> splitPathTreeSegments(std::string_view normalizedPath) {
    std::vector<std::string> segments;
    std::string segment;
    for (const char ch : normalizedPath) {
        if (ch == '/') {
            if (!segment.empty()) {
                segments.push_back(segment);
                segment.clear();
            }
        } else {
            segment.push_back(ch);
        }
    }
    if (!segment.empty()) {
        segments.push_back(segment);
    }
    return segments;
}

Result<void> bindPathTreeParentId(Statement& stmt, int index, int64_t parentId) {
    if (parentId == PathTreeNode::kNullParent) {
        return stmt.bind(index, nullptr);
    }
    return stmt.bind(index, parentId);
}

} // namespace

std::string pathTreeLeafPath(std::string_view filePath) {
    const auto segments = splitPathTreeSegments(filePath);
    if (segments.empty()) {
        return {};
    }
    // Built exactly as upsertPathTreeForDocumentInTransaction builds each node's full_path.
    std::string leaf = filePath.front() == '/' ? std::string("/") : std::string{};
    for (const auto& part : segments) {
        if (!leaf.empty() && leaf.back() != '/') {
            leaf.push_back('/');
        }
        leaf += part;
    }
    return leaf;
}

Result<void> upsertPathTreeForDocumentInTransaction(Database& db, const DocumentInfo& info,
                                                    int64_t documentId) {
    if (info.filePath.empty()) {
        return Result<void>();
    }

    const auto segments = splitPathTreeSegments(info.filePath);
    if (segments.empty()) {
        return Result<void>();
    }

    YAMS_TRY_UNWRAP(findStmtHolder, db.prepareCached("SELECT node_id FROM path_tree_nodes "
                                                     "WHERE parent_id IS ? AND path_segment = ?"));
    auto& findStmt = *findStmtHolder;
    YAMS_TRY_UNWRAP(insertStmtHolder,
                    db.prepareCached("INSERT OR IGNORE INTO path_tree_nodes "
                                     "(parent_id, path_segment, full_path) VALUES (?, ?, ?)"));
    auto& insertStmt = *insertStmtHolder;
    YAMS_TRY_UNWRAP(selectStmtHolder,
                    db.prepareCached("SELECT node_id FROM path_tree_nodes WHERE full_path = ?"));
    auto& selectStmt = *selectStmtHolder;
    YAMS_TRY_UNWRAP(assocStmtHolder,
                    db.prepareCached("INSERT OR IGNORE INTO path_tree_node_documents "
                                     "(node_id, document_id) VALUES (?, ?)"));
    auto& assocStmt = *assocStmtHolder;
    YAMS_TRY_UNWRAP(docCountStmtHolder,
                    db.prepareCached("UPDATE path_tree_nodes "
                                     "SET doc_count = doc_count + 1, last_updated = unixepoch() "
                                     "WHERE node_id = ?"));
    auto& docCountStmt = *docCountStmtHolder;

    int64_t parentNodeId = PathTreeNode::kNullParent;
    std::string currentPath =
        !info.filePath.empty() && info.filePath.front() == '/' ? std::string("/") : std::string{};

    for (const auto& part : segments) {
        if (!currentPath.empty() && currentPath.back() != '/') {
            currentPath.push_back('/');
        }
        currentPath += part;

        YAMS_TRY(findStmt.reset());
        YAMS_TRY(bindPathTreeParentId(findStmt, 1, parentNodeId));
        YAMS_TRY(findStmt.bind(2, part));
        YAMS_TRY_UNWRAP(foundNode, findStmt.step());

        int64_t nodeId = 0;
        if (foundNode) {
            nodeId = findStmt.getInt64(0);
        } else {
            YAMS_TRY(insertStmt.reset());
            YAMS_TRY(bindPathTreeParentId(insertStmt, 1, parentNodeId));
            YAMS_TRY(insertStmt.bind(2, part));
            YAMS_TRY(insertStmt.bind(3, currentPath));
            YAMS_TRY(insertStmt.execute());

            YAMS_TRY(selectStmt.reset());
            YAMS_TRY(selectStmt.bind(1, currentPath));
            YAMS_TRY_UNWRAP(selectedNode, selectStmt.step());
            if (!selectedNode) {
                return Error{ErrorCode::Unknown, "Failed to fetch inserted path tree node"};
            }
            nodeId = selectStmt.getInt64(0);
        }

        YAMS_TRY(assocStmt.reset());
        YAMS_TRY(assocStmt.bind(1, nodeId));
        YAMS_TRY(assocStmt.bind(2, documentId));
        YAMS_TRY(assocStmt.execute());
        if (db.changes() > 0) {
            YAMS_TRY(docCountStmt.reset());
            YAMS_TRY(docCountStmt.bind(1, nodeId));
            YAMS_TRY(docCountStmt.execute());
        }

        parentNodeId = nodeId;
    }

    return Result<void>();
}

} // namespace yams::metadata::repository
