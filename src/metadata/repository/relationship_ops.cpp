// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <cstdint>
#include <vector>

#include <yams/metadata/metadata_repository.h>

#include "crud_ops.hpp"

namespace yams::metadata {

// Relationship operations
Result<int64_t> MetadataRepository::insertRelationship(const DocumentRelationship& relationship) {
    return executeQuery<int64_t>([&](Database& db) -> Result<int64_t> {
        YAMS_TRY_UNWRAP(stmt, db.prepare(R"(
            INSERT OR IGNORE INTO document_relationships (
                parent_id, child_id, relationship_type, created_time
            ) VALUES (?, ?, ?, ?)
        )"));

        if (relationship.parentId > 0) {
            YAMS_TRY(stmt.bindAll(relationship.parentId, relationship.childId,
                                  relationship.getRelationshipTypeString(),
                                  relationship.createdTime));
        } else {
            YAMS_TRY(stmt.bind(1, nullptr));
            YAMS_TRY(stmt.bind(2, relationship.childId));
            YAMS_TRY(stmt.bind(3, relationship.getRelationshipTypeString()));
            YAMS_TRY(stmt.bind(4, relationship.createdTime));
        }

        YAMS_TRY(stmt.execute());
        if (db.changes() == 0) {
            // INSERT OR IGNORE skipped — row already exists. Return existing id.
            auto sel = db.prepare(
                "SELECT id FROM document_relationships "
                "WHERE (parent_id = ? OR parent_id IS NULL) "
                "AND child_id = ? AND relationship_type = ?");
            if (!sel) return sel.error();
            auto& s = sel.value();
            if (relationship.parentId != 0) {
                YAMS_TRY(s.bind(1, relationship.parentId));
            } else {
                YAMS_TRY(s.bind(1, nullptr));
            }
            YAMS_TRY(s.bind(2, relationship.childId));
            YAMS_TRY(s.bind(3, relationship.getRelationshipTypeString()));
            auto step = s.step();
            if (!step) return step.error();
            if (step.value()) {
                return s.getInt64(0);
            }
            return static_cast<int64_t>(0);
        }
        return db.lastInsertRowId();
    });
}

Result<std::vector<DocumentRelationship>> MetadataRepository::getRelationships(int64_t documentId) {
    return executeReadQuery<std::vector<DocumentRelationship>>(
        [&](Database& db) -> Result<std::vector<DocumentRelationship>> {
            repository::CrudOps<DocumentRelationship> ops;
            return ops.query(db, "parent_id = ? OR child_id = ?", documentId, documentId);
        });
}

Result<void> MetadataRepository::deleteRelationship(int64_t relationshipId) {
    return executeQuery<void>([&](Database& db) -> Result<void> {
        repository::CrudOps<DocumentRelationship> ops;
        return ops.deleteById(db, relationshipId);
    });
}
} // namespace yams::metadata
