// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <yams/core/assert.hpp>
#include <yams/metadata/metadata_repository.h>

#include "result_helpers.hpp"
#include "transaction_helpers.hpp"

namespace yams::metadata {

using repository::beginTransactionWithRetry;
using repository::commitOrRollback;
using repository::rollbackIgnoringErrors;
using repository::scope_exit;

namespace {
constexpr int64_t kPathTreeNullParent = PathTreeNode::kNullParent;

std::vector<float> blobToFloatVector(const std::vector<std::byte>& blob) {
    if (blob.empty() || (blob.size() % sizeof(float)) != 0)
        return {};
    std::vector<float> out(blob.size() / sizeof(float));
    std::memcpy(out.data(), blob.data(), blob.size());
    return out;
}

PathTreeNode mapPathTreeNodeRow(const Statement& stmt) {
    PathTreeNode node;
    node.id = stmt.getInt64(0);
    node.parentId = stmt.isNull(1) ? kPathTreeNullParent : stmt.getInt64(1);
    node.pathSegment = stmt.getString(2);
    node.fullPath = stmt.getString(3);
    node.docCount = stmt.getInt64(4);
    node.centroidWeight = stmt.getInt64(5);
    if (!stmt.isNull(6)) {
        node.centroid = blobToFloatVector(stmt.getBlob(6));
    }
    return node;
}

Result<void> bindParentId(Statement& stmt, int index, int64_t parentId) {
    if (parentId == kPathTreeNullParent) {
        return stmt.bind(index, nullptr);
    }
    return stmt.bind(index, parentId);
}

std::vector<std::string> splitPathSegments(const std::string& normalizedPath) {
    std::vector<std::string> segments;
    std::string segment;
    for (char ch : normalizedPath) {
        if (ch == '/') {
            if (!segment.empty()) {
                segments.push_back(segment);
                segment.clear();
            }
        } else {
            segment.push_back(ch);
        }
    }
    if (!segment.empty())
        segments.push_back(segment);
    return segments;
}

Result<std::optional<PathTreeNode>> findPathTreeNodeInDb(Database& db, int64_t parentId,
                                                         std::string_view pathSegment) {
    const bool parentIsNull = parentId == kPathTreeNullParent;
    const char* sql = parentIsNull
                          ? "SELECT node_id, parent_id, path_segment, full_path, doc_count, "
                            "centroid_weight, centroid FROM path_tree_nodes "
                            "WHERE parent_id IS NULL AND path_segment = ?"
                          : "SELECT node_id, parent_id, path_segment, full_path, doc_count, "
                            "centroid_weight, centroid FROM path_tree_nodes "
                            "WHERE parent_id = ? AND path_segment = ?";

    auto stmtResult = db.prepare(sql);
    if (!stmtResult)
        return stmtResult.error();

    auto stmt = std::move(stmtResult).value();
    int bindIndex = 1;

    if (!parentIsNull) {
        if (auto bindResult = stmt.bind(bindIndex++, parentId); !bindResult)
            return bindResult.error();
    }
    if (auto bindResult = stmt.bind(bindIndex, pathSegment); !bindResult)
        return bindResult.error();

    auto stepResult = stmt.step();
    if (!stepResult)
        return stepResult.error();
    if (!stepResult.value())
        return std::optional<PathTreeNode>{};

    return std::optional<PathTreeNode>{mapPathTreeNodeRow(stmt)};
}

Result<std::optional<PathTreeNode>> findPathTreeNodeByFullPathInDb(Database& db,
                                                                   std::string_view fullPath) {
    if (fullPath.empty())
        return std::optional<PathTreeNode>{};

    auto stmtResult = db.prepare("SELECT node_id, parent_id, path_segment, full_path, "
                                 "doc_count, centroid_weight, centroid "
                                 "FROM path_tree_nodes WHERE full_path = ?");
    if (!stmtResult)
        return stmtResult.error();

    auto stmt = std::move(stmtResult).value();
    if (auto bindRes = stmt.bind(1, fullPath); !bindRes)
        return bindRes.error();

    auto stepRes = stmt.step();
    if (!stepRes)
        return stepRes.error();
    if (!stepRes.value())
        return std::optional<PathTreeNode>{};

    return std::optional<PathTreeNode>{mapPathTreeNodeRow(stmt)};
}

} // namespace

Result<std::optional<PathTreeNode>>
MetadataRepository::findPathTreeNode(int64_t parentId, std::string_view pathSegment) {
    return executeReadQuery<std::optional<PathTreeNode>>(
        [&](Database& db) -> Result<std::optional<PathTreeNode>> {
            return findPathTreeNodeInDb(db, parentId, pathSegment);
        });
}

Result<PathTreeNode> MetadataRepository::insertPathTreeNode(int64_t parentId,
                                                            std::string_view pathSegment,
                                                            std::string_view fullPath) {
    return executeQuery<PathTreeNode>([&](Database& db) -> Result<PathTreeNode> {
        auto insertStmtResult = db.prepare("INSERT OR IGNORE INTO path_tree_nodes "
                                           "(parent_id, path_segment, full_path) VALUES (?, ?, ?)");
        if (!insertStmtResult)
            return insertStmtResult.error();

        auto insertStmt = std::move(insertStmtResult).value();
        if (auto bindParent = bindParentId(insertStmt, 1, parentId); !bindParent)
            return bindParent.error();
        if (auto bindSeg = insertStmt.bind(2, pathSegment); !bindSeg)
            return bindSeg.error();
        if (auto bindFull = insertStmt.bind(3, fullPath); !bindFull)
            return bindFull.error();

        if (auto execResult = insertStmt.execute(); !execResult)
            return execResult.error();

        auto selectStmtResult = db.prepare(
            "SELECT node_id, parent_id, path_segment, full_path, doc_count, centroid_weight "
            "FROM path_tree_nodes WHERE full_path = ?");
        if (!selectStmtResult)
            return selectStmtResult.error();

        auto selectStmt = std::move(selectStmtResult).value();
        if (auto bindPath = selectStmt.bind(1, fullPath); !bindPath)
            return bindPath.error();

        auto stepResult = selectStmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value())
            return Error{ErrorCode::Unknown, "Failed to fetch inserted path tree node"};

        return mapPathTreeNodeRow(selectStmt);
    });
}

Result<void> MetadataRepository::incrementPathTreeDocCount(int64_t nodeId, int64_t documentId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        // Use INSERT OR IGNORE to handle concurrent inserts atomically.
        // This avoids the race condition in check-then-insert pattern.
        auto insertStmtResult = db.prepare(
            "INSERT OR IGNORE INTO path_tree_node_documents (node_id, document_id) VALUES (?, ?)");
        if (!insertStmtResult)
            return insertStmtResult.error();
        auto insertStmt = std::move(insertStmtResult).value();
        if (auto bindNode = insertStmt.bind(1, nodeId); !bindNode)
            return bindNode.error();
        if (auto bindDoc = insertStmt.bind(2, documentId); !bindDoc)
            return bindDoc.error();
        if (auto execResult = insertStmt.execute(); !execResult)
            return execResult.error();

        // Only increment doc_count if a new row was actually inserted
        // (changes() returns 0 if INSERT was ignored due to conflict)
        if (db.changes() == 0) {
            return Result<void>(); // Already associated, nothing to update
        }

        auto updateStmtResult =
            db.prepare("UPDATE path_tree_nodes "
                       "SET doc_count = doc_count + 1, last_updated = unixepoch() "
                       "WHERE node_id = ?");
        if (!updateStmtResult)
            return updateStmtResult.error();
        auto updateStmt = std::move(updateStmtResult).value();
        if (auto bindNode = updateStmt.bind(1, nodeId); !bindNode)
            return bindNode.error();
        return updateStmt.execute();
    });
}

Result<void>
MetadataRepository::accumulatePathTreeCentroid(int64_t nodeId,
                                               std::span<const float> embeddingValues) {
    if (embeddingValues.empty())
        return Result<void>();

    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto selectStmtResult =
            db.prepare("SELECT centroid, centroid_weight FROM path_tree_nodes WHERE node_id = ?");
        if (!selectStmtResult)
            return selectStmtResult.error();

        auto selectStmt = std::move(selectStmtResult).value();
        if (auto bindNode = selectStmt.bind(1, nodeId); !bindNode)
            return bindNode.error();

        auto stepResult = selectStmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value())
            return Error{ErrorCode::NotFound, "Path tree node not found"};

        std::vector<float> centroid;
        centroid.reserve(embeddingValues.size());
        int64_t currentWeight = selectStmt.getInt64(1);

        auto existingBlob = selectStmt.isNull(0) ? std::vector<std::byte>() : selectStmt.getBlob(0);
        if (!existingBlob.empty() &&
            existingBlob.size() == embeddingValues.size() * sizeof(float) && currentWeight > 0) {
            centroid.resize(embeddingValues.size());
            std::memcpy(centroid.data(), existingBlob.data(), existingBlob.size());
        } else {
            centroid.assign(embeddingValues.begin(), embeddingValues.end());
            currentWeight = 0;
        }

        int64_t newWeight = currentWeight + 1;
        if (currentWeight > 0) {
            const double weightFactor = static_cast<double>(currentWeight);
            for (std::size_t i = 0; i < embeddingValues.size(); ++i) {
                double updated = (centroid[i] * weightFactor + embeddingValues[i]) /
                                 static_cast<double>(newWeight);
                centroid[i] = static_cast<float>(updated);
            }
        } else {
            centroid.assign(embeddingValues.begin(), embeddingValues.end());
        }

        auto updateStmtResult =
            db.prepare("UPDATE path_tree_nodes "
                       "SET centroid = ?, centroid_weight = ?, last_updated = unixepoch() "
                       "WHERE node_id = ?");
        if (!updateStmtResult)
            return updateStmtResult.error();

        auto updateStmt = std::move(updateStmtResult).value();

        std::span<const std::byte> blob(reinterpret_cast<const std::byte*>(centroid.data()),
                                        centroid.size() * sizeof(float));
        if (auto bindBlob = updateStmt.bind(1, blob); !bindBlob)
            return bindBlob.error();
        if (auto bindWeight = updateStmt.bind(2, newWeight); !bindWeight)
            return bindWeight.error();
        if (auto bindNode = updateStmt.bind(3, nodeId); !bindNode)
            return bindNode.error();

        return updateStmt.execute();
    });
}

Result<std::optional<PathTreeNode>>
MetadataRepository::findPathTreeNodeByFullPath(std::string_view fullPath) {
    return executeReadQuery<std::optional<PathTreeNode>>(
        [&](Database& db) -> Result<std::optional<PathTreeNode>> {
            return findPathTreeNodeByFullPathInDb(db, fullPath);
        });
}

// ---------------------------------------------------------------------------
// Repair helpers (concrete-class only)
// ---------------------------------------------------------------------------

Result<uint64_t> MetadataRepository::countDocsMissingPathTree() {
    return executeReadQuery<uint64_t>([&](Database& db) -> Result<uint64_t> {
        auto stmtResult = db.prepare("SELECT COUNT(*) FROM documents d "
                                     "LEFT JOIN path_tree_nodes p ON p.full_path = d.file_path "
                                     "WHERE d.file_path != '' AND p.node_id IS NULL");
        if (!stmtResult)
            return stmtResult.error();

        auto stmt = std::move(stmtResult).value();
        auto stepRes = stmt.step();
        if (!stepRes)
            return stepRes.error();
        if (!stepRes.value())
            return uint64_t(0);

        return static_cast<uint64_t>(stmt.getInt64(0));
    });
}

Result<std::vector<DocumentInfo>> MetadataRepository::findDocsMissingPathTree(int limit) {
    return executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            std::string sql = "SELECT ";
            sql += documentColumnList(false);
            sql += " FROM documents d "
                   "LEFT JOIN path_tree_nodes p ON p.full_path = d.file_path "
                   "WHERE d.file_path != '' AND p.node_id IS NULL";
            if (limit > 0) {
                sql += " LIMIT ?";
            }

            auto stmtResult = db.prepare(sql);
            if (!stmtResult)
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            if (limit > 0) {
                if (auto r = stmt.bind(1, static_cast<int64_t>(limit)); !r)
                    return r.error();
            }

            std::vector<DocumentInfo> docs;
            while (true) {
                auto stepRes = stmt.step();
                if (!stepRes)
                    return stepRes.error();
                if (!stepRes.value())
                    break;
                docs.push_back(mapDocumentRow(stmt));
            }
            return docs;
        });
}

Result<std::vector<PathTreeNode>>
MetadataRepository::listPathTreeChildren(std::string_view fullPath, std::size_t limit) {
    return executeReadQuery<std::vector<PathTreeNode>>(
        [&](Database& db) -> Result<std::vector<PathTreeNode>> {
            bool isRoot = fullPath.empty() || fullPath == "/";
            int64_t parentId = kPathTreeNullParent;

            if (!isRoot) {
                auto parentRes = findPathTreeNodeByFullPathInDb(db, fullPath);
                if (!parentRes)
                    return parentRes.error();
                const auto& parentOpt = parentRes.value();
                if (!parentOpt)
                    return std::vector<PathTreeNode>{};
                parentId = parentOpt->id;
            }

            std::string sql = "SELECT node_id, parent_id, path_segment, full_path, doc_count, "
                              "centroid_weight, centroid "
                              "FROM path_tree_nodes ";
            if (isRoot) {
                sql += "WHERE parent_id IS NULL ";
            } else {
                sql += "WHERE parent_id = ? ";
            }
            sql += "ORDER BY doc_count DESC, path_segment ASC ";
            if (limit > 0)
                sql += "LIMIT ?";

            auto stmtResult = db.prepare(sql);
            if (!stmtResult)
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            int bindIndex = 1;
            if (!isRoot) {
                if (auto bindParent = stmt.bind(bindIndex++, parentId); !bindParent)
                    return bindParent.error();
            }
            if (limit > 0) {
                if (auto bindLimit = stmt.bind(bindIndex, static_cast<int64_t>(limit)); !bindLimit)
                    return bindLimit.error();
            }

            std::vector<PathTreeNode> children;
            while (true) {
                auto step = stmt.step();
                if (!step)
                    return step.error();
                if (!step.value())
                    break;
                children.push_back(mapPathTreeNodeRow(stmt));
            }
            return children;
        });
}

Result<void> MetadataRepository::upsertPathTreeForDocument(const DocumentInfo& info,
                                                           int64_t documentId, bool isNewDocument,
                                                           std::span<const float> embeddingValues) {
    if (info.filePath.empty())
        return Result<void>();

    auto segments = splitPathSegments(info.filePath);
    if (segments.empty())
        return Result<void>();

    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto beginResult = beginTransactionWithRetry(db);
        if (!beginResult) {
            return beginResult.error();
        }

        bool committed = false;
        auto rollback = scope_exit([&] {
            if (!committed) {
                rollbackIgnoringErrors(db);
            }
        });

        const bool isAbsolute = !info.filePath.empty() && info.filePath.front() == '/';

        auto findStmtRes =
            db.prepare("SELECT node_id, parent_id, path_segment, full_path, doc_count, "
                       "centroid_weight, centroid FROM path_tree_nodes "
                       "WHERE parent_id IS ? AND path_segment = ?");
        if (!findStmtRes)
            return findStmtRes.error();
        auto findStmt = std::move(findStmtRes).value();

        auto insertStmtRes = db.prepare("INSERT OR IGNORE INTO path_tree_nodes "
                                        "(parent_id, path_segment, full_path) VALUES (?, ?, ?)");
        if (!insertStmtRes)
            return insertStmtRes.error();
        auto insertStmt = std::move(insertStmtRes).value();

        auto selectStmtRes = db.prepare("SELECT node_id, parent_id, path_segment, full_path, "
                                        "doc_count, centroid_weight, centroid "
                                        "FROM path_tree_nodes WHERE full_path = ?");
        if (!selectStmtRes)
            return selectStmtRes.error();
        auto selectStmt = std::move(selectStmtRes).value();

        auto assocInsertRes =
            db.prepare("INSERT OR IGNORE INTO path_tree_node_documents (node_id, document_id) "
                       "VALUES (?, ?)");
        if (!assocInsertRes)
            return assocInsertRes.error();
        auto assocStmt = std::move(assocInsertRes).value();

        auto docCountRes = db.prepare("UPDATE path_tree_nodes "
                                      "SET doc_count = doc_count + 1, last_updated = unixepoch() "
                                      "WHERE node_id = ?");
        if (!docCountRes)
            return docCountRes.error();
        auto docCountStmt = std::move(docCountRes).value();

        auto centroidSelectRes =
            db.prepare("SELECT centroid, centroid_weight FROM path_tree_nodes WHERE node_id = ?");
        if (!centroidSelectRes)
            return centroidSelectRes.error();
        auto centroidSelStmt = std::move(centroidSelectRes).value();

        auto centroidUpdateRes =
            db.prepare("UPDATE path_tree_nodes "
                       "SET centroid = ?, centroid_weight = ?, last_updated = unixepoch() "
                       "WHERE node_id = ?");
        if (!centroidUpdateRes)
            return centroidUpdateRes.error();
        auto centroidUpdStmt = std::move(centroidUpdateRes).value();

        int64_t parentNodeId = kPathTreeNullParent;
        std::string currentPath = isAbsolute ? std::string("/") : std::string{};

        for (const auto& part : segments) {
            if (!currentPath.empty() && currentPath.back() != '/')
                currentPath.push_back('/');
            currentPath += part;

            findStmt.reset();
            if (parentNodeId == kPathTreeNullParent) {
                if (auto b = findStmt.bind(1, nullptr); !b)
                    return b.error();
            } else {
                if (auto b = findStmt.bind(1, parentNodeId); !b)
                    return b.error();
            }
            if (auto b = findStmt.bind(2, part); !b)
                return b.error();

            auto stepRes = findStmt.step();
            if (!stepRes)
                return stepRes.error();

            int64_t nodeId = 0;
            bool found = stepRes.value();
            if (found) {
                nodeId = findStmt.getInt64(0);
            } else {
                insertStmt.reset();
                if (auto b = bindParentId(insertStmt, 1, parentNodeId); !b)
                    return b.error();
                if (auto b = insertStmt.bind(2, part); !b)
                    return b.error();
                if (auto b = insertStmt.bind(3, currentPath); !b)
                    return b.error();
                if (auto execRes = insertStmt.execute(); !execRes)
                    return execRes.error();

                selectStmt.reset();
                if (auto b = selectStmt.bind(1, currentPath); !b)
                    return b.error();
                auto selRes = selectStmt.step();
                if (!selRes)
                    return selRes.error();
                if (!selRes.value())
                    return Error{ErrorCode::Unknown, "Failed to fetch inserted path tree node"};
                nodeId = selectStmt.getInt64(0);
            }

            if (isNewDocument) {
                assocStmt.reset();
                if (auto b = assocStmt.bind(1, nodeId); !b)
                    return b.error();
                if (auto b = assocStmt.bind(2, documentId); !b)
                    return b.error();
                if (auto execRes = assocStmt.execute(); !execRes)
                    return execRes.error();

                if (db.changes() > 0) {
                    docCountStmt.reset();
                    if (auto b = docCountStmt.bind(1, nodeId); !b)
                        return b.error();
                    if (auto execRes = docCountStmt.execute(); !execRes)
                        return execRes.error();
                }
            }

            if (!embeddingValues.empty()) {
                centroidSelStmt.reset();
                if (auto b = centroidSelStmt.bind(1, nodeId); !b)
                    return b.error();
                auto cStepRes = centroidSelStmt.step();
                if (!cStepRes)
                    return cStepRes.error();
                if (!cStepRes.value())
                    return Error{ErrorCode::NotFound, "Path tree node not found"};

                int64_t currentWeight = centroidSelStmt.getInt64(1);
                bool hasBlob = !centroidSelStmt.isNull(0);

                std::size_t dimCount = embeddingValues.size();
                std::vector<float> centroid;
                centroid.reserve(dimCount);

                if (hasBlob && currentWeight > 0) {
                    auto blob = centroidSelStmt.getBlob(0);
                    if (blob.size() == dimCount * sizeof(float)) {
                        centroid.resize(dimCount);
                        std::memcpy(centroid.data(), blob.data(), blob.size());
                    } else {
                        centroid.assign(embeddingValues.begin(), embeddingValues.end());
                        currentWeight = 0;
                    }
                } else {
                    centroid.assign(embeddingValues.begin(), embeddingValues.end());
                    currentWeight = 0;
                }

                int64_t newWeight = currentWeight + 1;
                if (currentWeight > 0) {
                    const double weightFactor = static_cast<double>(currentWeight);
                    for (std::size_t i = 0; i < dimCount; ++i) {
                        double updated = (centroid[i] * weightFactor + embeddingValues[i]) /
                                         static_cast<double>(newWeight);
                        centroid[i] = static_cast<float>(updated);
                    }
                }

                centroidUpdStmt.reset();
                std::span<const std::byte> blob(reinterpret_cast<const std::byte*>(centroid.data()),
                                                centroid.size() * sizeof(float));
                if (auto b = centroidUpdStmt.bind(1, blob); !b)
                    return b.error();
                if (auto b = centroidUpdStmt.bind(2, newWeight); !b)
                    return b.error();
                if (auto b = centroidUpdStmt.bind(3, nodeId); !b)
                    return b.error();
                if (auto execRes = centroidUpdStmt.execute(); !execRes)
                    return execRes.error();
            }

            parentNodeId = nodeId;
        }

        YAMS_TRY(commitOrRollback(db));
        committed = true;
        return Result<void>();
    });
}

Result<void> MetadataRepository::removePathTreeForDocument(const DocumentInfo& info,
                                                           int64_t documentId,
                                                           std::span<const float> embeddingValues) {
    if (info.filePath.empty())
        return Result<void>();

    auto segments = splitPathSegments(info.filePath);
    if (segments.empty())
        return Result<void>();

    return executeQuery<void>([&](Database& db) -> Result<void> {
        auto beginResult = beginTransactionWithRetry(db);
        if (!beginResult) {
            return beginResult.error();
        }

        bool committed = false;
        auto rollback = scope_exit([&] {
            if (!committed) {
                rollbackIgnoringErrors(db);
            }
        });

        int64_t parentNodeId = kPathTreeNullParent;
        std::vector<int64_t> pathNodeIds;
        pathNodeIds.reserve(segments.size());

        for (const auto& part : segments) {
            auto nodeResult = findPathTreeNodeInDb(db, parentNodeId, part);
            if (!nodeResult)
                return nodeResult.error();

            if (!nodeResult.value()) {
                return Result<void>();
            }

            pathNodeIds.push_back(nodeResult.value()->id);
            parentNodeId = nodeResult.value()->id;
        }

        auto deleteAssocStmtResult = db.prepare(
            "DELETE FROM path_tree_node_documents WHERE node_id = ? AND document_id = ?");
        if (!deleteAssocStmtResult)
            return deleteAssocStmtResult.error();
        auto deleteAssocStmt = std::move(deleteAssocStmtResult).value();

        auto decrementStmtResult =
            db.prepare("UPDATE path_tree_nodes "
                       "SET doc_count = MAX(0, doc_count - 1), last_updated = unixepoch() "
                       "WHERE node_id = ?");
        if (!decrementStmtResult)
            return decrementStmtResult.error();
        auto decrementStmt = std::move(decrementStmtResult).value();

        auto selectCentroidStmtResult =
            db.prepare("SELECT centroid, centroid_weight FROM path_tree_nodes WHERE node_id = ?");
        if (!selectCentroidStmtResult)
            return selectCentroidStmtResult.error();
        auto selectCentroidStmt = std::move(selectCentroidStmtResult).value();

        auto clearCentroidStmtResult =
            db.prepare("UPDATE path_tree_nodes "
                       "SET centroid = NULL, centroid_weight = 0, last_updated = unixepoch() "
                       "WHERE node_id = ?");
        if (!clearCentroidStmtResult)
            return clearCentroidStmtResult.error();
        auto clearCentroidStmt = std::move(clearCentroidStmtResult).value();

        auto updateCentroidStmtResult =
            db.prepare("UPDATE path_tree_nodes "
                       "SET centroid = ?, centroid_weight = ?, last_updated = unixepoch() "
                       "WHERE node_id = ?");
        if (!updateCentroidStmtResult)
            return updateCentroidStmtResult.error();
        auto updateCentroidStmt = std::move(updateCentroidStmtResult).value();

        auto checkDeleteStmtResult =
            db.prepare("SELECT doc_count, "
                       "(SELECT COUNT(*) FROM path_tree_nodes WHERE parent_id = ?) as child_count "
                       "FROM path_tree_nodes WHERE node_id = ?");
        if (!checkDeleteStmtResult)
            return checkDeleteStmtResult.error();
        auto checkDeleteStmt = std::move(checkDeleteStmtResult).value();

        auto deleteNodeStmtResult = db.prepare("DELETE FROM path_tree_nodes WHERE node_id = ?");
        if (!deleteNodeStmtResult)
            return deleteNodeStmtResult.error();
        auto deleteNodeStmt = std::move(deleteNodeStmtResult).value();

        for (auto it = pathNodeIds.rbegin(); it != pathNodeIds.rend(); ++it) {
            const int64_t nodeId = *it;

            if (auto resetResult = deleteAssocStmt.reset(); !resetResult)
                return resetResult.error();
            if (auto bindNode = deleteAssocStmt.bind(1, nodeId); !bindNode)
                return bindNode.error();
            if (auto bindDoc = deleteAssocStmt.bind(2, documentId); !bindDoc)
                return bindDoc.error();
            if (auto execResult = deleteAssocStmt.execute(); !execResult)
                return execResult.error();

            const bool removedAssociation = db.changes() > 0;
            if (!removedAssociation) {
                return Error{ErrorCode::DatabaseError,
                             "path-tree: resolved node on a document path lost its document "
                             "association before removal"};
            }

            if (auto resetResult = decrementStmt.reset(); !resetResult)
                return resetResult.error();
            if (auto bindNode = decrementStmt.bind(1, nodeId); !bindNode)
                return bindNode.error();
            if (auto execResult = decrementStmt.execute(); !execResult)
                return execResult.error();

            if (embeddingValues.empty()) {
                continue;
            }

            if (auto resetResult = selectCentroidStmt.reset(); !resetResult)
                return resetResult.error();
            if (auto bindNode = selectCentroidStmt.bind(1, nodeId); !bindNode)
                return bindNode.error();

            auto stepRes = selectCentroidStmt.step();
            if (!stepRes)
                return stepRes.error();
            if (!stepRes.value()) {
                continue;
            }

            std::vector<float> centroid;
            const int64_t currentWeight = selectCentroidStmt.getInt64(1);
            if (!selectCentroidStmt.isNull(0)) {
                centroid = blobToFloatVector(selectCentroidStmt.getBlob(0));
            }

            if (currentWeight <= 1) {
                if (auto resetResult = clearCentroidStmt.reset(); !resetResult)
                    return resetResult.error();
                if (auto bindNode = clearCentroidStmt.bind(1, nodeId); !bindNode)
                    return bindNode.error();
                if (auto execResult = clearCentroidStmt.execute(); !execResult)
                    return execResult.error();
                continue;
            }

            YAMS_DCHECK(centroid.size() == embeddingValues.size(),
                        "path-tree: centroid dimension must match removed embedding dimension");
            if (centroid.size() != embeddingValues.size()) {
                continue;
            }

            const int64_t newWeight = currentWeight - 1;
            const double oldWeightFactor = static_cast<double>(currentWeight);
            const double newWeightFactor = static_cast<double>(newWeight);
            for (std::size_t i = 0; i < embeddingValues.size(); ++i) {
                double updated =
                    (centroid[i] * oldWeightFactor - embeddingValues[i]) / newWeightFactor;
                centroid[i] = static_cast<float>(updated);
            }

            if (auto resetResult = updateCentroidStmt.reset(); !resetResult)
                return resetResult.error();
            std::span<const std::byte> blob(reinterpret_cast<const std::byte*>(centroid.data()),
                                            centroid.size() * sizeof(float));
            if (auto bindBlob = updateCentroidStmt.bind(1, blob); !bindBlob)
                return bindBlob.error();
            if (auto bindWeight = updateCentroidStmt.bind(2, newWeight); !bindWeight)
                return bindWeight.error();
            if (auto bindNode = updateCentroidStmt.bind(3, nodeId); !bindNode)
                return bindNode.error();
            if (auto execResult = updateCentroidStmt.execute(); !execResult)
                return execResult.error();
        }

        for (size_t i = 0; i < pathNodeIds.size(); ++i) {
            const size_t reverseIdx = pathNodeIds.size() - 1 - i;
            const int64_t nodeId = pathNodeIds[reverseIdx];
            const bool isFirstLevel = (reverseIdx == 0);
            if (isFirstLevel) {
                continue;
            }

            if (auto resetResult = checkDeleteStmt.reset(); !resetResult)
                return resetResult.error();
            if (auto bind1 = checkDeleteStmt.bind(1, nodeId); !bind1)
                return bind1.error();
            if (auto bind2 = checkDeleteStmt.bind(2, nodeId); !bind2)
                return bind2.error();

            auto stepRes = checkDeleteStmt.step();
            if (!stepRes)
                return stepRes.error();
            if (!stepRes.value()) {
                continue;
            }

            const int64_t docCount = checkDeleteStmt.getInt64(0);
            const int64_t childCount = checkDeleteStmt.getInt64(1);
            if (docCount != 0 || childCount != 0) {
                continue;
            }

            if (auto resetResult = deleteNodeStmt.reset(); !resetResult)
                return resetResult.error();
            if (auto bindNode = deleteNodeStmt.bind(1, nodeId); !bindNode)
                return bindNode.error();
            if (auto execResult = deleteNodeStmt.execute(); !execResult)
                return execResult.error();
        }

        auto commitResult = db.execute("COMMIT");
        if (!commitResult) {
            return commitResult.error();
        }

        committed = true;
        return Result<void>();
    });
}

// -----------------------------------------------------------------------------
// Tree-based document queries (PBI-043 integration)
// -----------------------------------------------------------------------------

Result<std::vector<DocumentInfo>>
MetadataRepository::findDocumentsByPathTreePrefix(std::string_view pathPrefix,
                                                  bool includeSubdirectories, int limit) {
    DocumentQueryOptions opts;
    if (!pathPrefix.empty())
        opts.pathPrefix = std::string(pathPrefix);
    opts.includeSubdirectories = includeSubdirectories;
    opts.limit = limit;
    return queryDocuments(opts);
}

} // namespace yams::metadata
