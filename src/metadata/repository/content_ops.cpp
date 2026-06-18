// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <cstdint>
#include <optional>

#include <yams/common/utf8_utils.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>

#include "crud_ops.hpp"

namespace yams::metadata {

// Content operations
Result<void> MetadataRepository::insertContent(const DocumentContent& content) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::insertContent");
    YAMS_PLOT("metadata_repo::insert_content_bytes",
              static_cast<int64_t>(content.contentText.size()));
    return executeQuery<void>([&](Database& db) -> Result<void> {
        DocumentContent sanitized = content;
        sanitized.contentText = common::sanitizeUtf8(content.contentText);
        sanitized.contentLength = static_cast<int64_t>(sanitized.contentText.length());

        repository::CrudOps<DocumentContent> ops;
        return ops.upsertOnConflict(db, sanitized, "document_id");
    });
}

Result<std::optional<DocumentContent>> MetadataRepository::getContent(int64_t documentId) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::getContent");
    auto result = executeReadQuery<std::optional<DocumentContent>>(
        [&](Database& db) -> Result<std::optional<DocumentContent>> {
            repository::CrudOps<DocumentContent> ops;
            return ops.getById(db, documentId);
        });
    if (result && result.value().has_value()) {
        YAMS_PLOT("metadata_repo::get_content_bytes",
                  static_cast<int64_t>(result.value()->contentText.size()));
    }
    return result;
}

Result<void> MetadataRepository::updateContent(const DocumentContent& content) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::updateContent");
    YAMS_PLOT("metadata_repo::update_content_bytes",
              static_cast<int64_t>(content.contentText.size()));
    return executeQuery<void>([&](Database& db) -> Result<void> {
        DocumentContent sanitized = content;
        sanitized.contentText = common::sanitizeUtf8(content.contentText);
        sanitized.contentLength = static_cast<int64_t>(sanitized.contentText.length());

        repository::CrudOps<DocumentContent> ops;
        return ops.update(db, sanitized);
    });
}

Result<void> MetadataRepository::deleteContent(int64_t documentId) {
    YAMS_ZONE_SCOPED_N("MetadataRepo::deleteContent");
    return executeQuery<void>([&](Database& db) -> Result<void> {
        repository::CrudOps<DocumentContent> ops;
        return ops.deleteById(db, documentId);
    });
}
} // namespace yams::metadata
