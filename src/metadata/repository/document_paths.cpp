#include "document_paths.h"
#include "result_helpers.hpp"

#include <nlohmann/json.hpp>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include <chrono>
#include <filesystem>

namespace yams::metadata {
namespace repository {

std::string documentPathQueryCte(bool currentOnly) {
    // The leading columns intentionally shadow the content row's canonical path
    // columns. Consumers select explicit column names, never SELECT *.
    std::string sql = R"(
        WITH documents AS (
            SELECT h.file_path AS file_path, h.file_name AS file_name,
                   h.file_extension AS file_extension, h.modified_time AS modified_time,
                   h.indexed_time AS indexed_time, h.path_prefix AS path_prefix,
                   h.reverse_path AS reverse_path, h.path_hash AS path_hash,
                   h.parent_hash AS parent_hash, h.path_depth AS path_depth,
                   h.metadata_json AS location_metadata, 1 AS location_current, d.*
            FROM main.documents d JOIN document_path_heads h ON h.document_id = d.id
            WHERE h.deleted = 0
            UNION ALL
            SELECT d.file_path, d.file_name, d.file_extension, d.modified_time,
                   d.indexed_time, d.path_prefix, d.reverse_path, d.path_hash,
                   d.parent_hash, d.path_depth, NULL, 1, d.*
            FROM main.documents d
            WHERE NOT EXISTS (SELECT 1 FROM document_path_heads h WHERE h.file_path = d.file_path)
    )";
    if (currentOnly) {
        sql += R"(
            AND d.id = (SELECT candidate.id FROM main.documents candidate
                        WHERE candidate.path_hash = d.path_hash AND candidate.file_path = d.file_path
                        ORDER BY COALESCE((SELECT value FROM metadata WHERE document_id = candidate.id
                                           AND key = 'is_latest'), '0') IN ('1', 'true') DESC,
                                 COALESCE((SELECT CAST(value AS INTEGER) FROM metadata
                                           WHERE document_id = candidate.id AND key = 'version'), 0) DESC,
                                 candidate.indexed_time DESC, candidate.id DESC LIMIT 1)
        )";
    } else {
        // Preserve the legacy list/history view for canonical historical rows.
        // Retired paths stay retired; immutable snapshot graph history is kept.
        sql += R"(
            UNION ALL
            SELECT d.file_path, d.file_name, d.file_extension, d.modified_time,
                   d.indexed_time, d.path_prefix, d.reverse_path, d.path_hash,
                   d.parent_hash, d.path_depth, NULL, 0, d.*
            FROM main.documents d JOIN document_path_heads h ON h.file_path = d.file_path
            WHERE h.deleted = 0 AND h.document_id <> d.id
        )";
    }
    sql += ") ";
    return sql;
}

} // namespace repository

Result<void> MetadataRepository::recordDocumentPath(
    const DocumentInfo& info, int64_t documentId,
    const std::vector<std::pair<std::string, MetadataValue>>& metadata) {
    if (!hasDocumentPaths_)
        return Error{ErrorCode::InvalidState,
                     "Document path heads require the current metadata schema"};
    if (info.filePath.empty() || documentId <= 0)
        return Error{ErrorCode::InvalidArgument,
                     "A document path and valid content id are required"};
    const auto derived = computePathDerivedValues(info.filePath);
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        return db.transaction([&]() -> Result<void> {
            auto previous = db.prepare("SELECT document_id, revision, deleted FROM "
                                       "document_path_heads WHERE file_path = ?");
            if (!previous)
                return previous.error();
            YAMS_TRY(previous.value().bind(1, derived.normalizedPath));
            YAMS_TRY_UNWRAP(found, previous.value().step());
            int64_t revision = 1;
            if (found) {
                revision = previous.value().getInt64(1);
                if (previous.value().getInt64(0) != documentId || previous.value().getInt(2) != 0)
                    ++revision;
            } else {
                // Adopt a canonical legacy series without resetting its version.
                // An alias must not inherit another path's revision counter.
                auto legacy =
                    db.prepare("SELECT CAST(m.value AS INTEGER) FROM metadata m JOIN documents d "
                               "ON d.id = m.document_id WHERE d.id = ? AND d.file_path = ? AND "
                               "m.key = 'version'");
                if (!legacy)
                    return legacy.error();
                YAMS_TRY(legacy.value().bindAll(documentId, derived.normalizedPath));
                YAMS_TRY_UNWRAP(hasLegacyVersion, legacy.value().step());
                if (hasLegacyVersion)
                    revision = std::max<int64_t>(1, legacy.value().getInt64(0));
            }
            nlohmann::json values = nlohmann::json::object();
            for (const auto& [key, value] : metadata)
                values[key] = value.asString();
            values["series_key"] = derived.normalizedPath;
            values["version"] = std::to_string(revision);
            values["is_latest"] = "1";
            const auto encoded = values.dump();
            auto statement = db.prepare(R"(
                INSERT INTO document_path_heads
                    (file_path, document_id, file_name, file_extension, path_prefix, reverse_path,
                     path_hash, parent_hash, path_depth, revision, indexed_time, modified_time,
                     metadata_json, deleted)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(file_path) DO UPDATE SET document_id = excluded.document_id,
                    file_name = excluded.file_name, file_extension = excluded.file_extension,
                    path_prefix = excluded.path_prefix, reverse_path = excluded.reverse_path,
                    path_hash = excluded.path_hash, parent_hash = excluded.parent_hash,
                    path_depth = excluded.path_depth, revision = excluded.revision,
                    indexed_time = excluded.indexed_time, modified_time = excluded.modified_time,
                    metadata_json = excluded.metadata_json, deleted = 0
            )");
            if (!statement)
                return statement.error();
            YAMS_TRY(statement.value().bindAll(
                derived.normalizedPath, documentId,
                info.fileName.empty()
                    ? std::filesystem::path(derived.normalizedPath).filename().string()
                    : info.fileName,
                info.fileExtension, derived.pathPrefix, derived.reversePath, derived.pathHash,
                derived.parentHash, derived.pathDepth, revision,
                info.indexedTime.time_since_epoch().count(),
                info.modifiedTime.time_since_epoch().count(), encoded));
            return statement.value().execute();
        });
    });
    if (result)
        metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    return result;
}

Result<void> MetadataRepository::retireDocumentPath(const std::string& path) {
    auto document = findDocumentByExactPath(path);
    if (!document)
        return document.error();
    if (!document.value())
        return Error{ErrorCode::NotFound, "Document path not indexed: " + path};
    // One atomic upsert: never temporarily reactivate a path or overwrite a
    // concurrently advanced head while establishing a legacy-path tombstone.
    const auto derived = computePathDerivedValues(path);
    const auto& info = *document.value();
    auto result = executeQuery<void>([&](Database& db) -> Result<void> {
        auto statement = db.prepare(R"(
            INSERT INTO document_path_heads
                (file_path, document_id, file_name, file_extension, path_prefix, reverse_path,
                 path_hash, parent_hash, path_depth, revision, indexed_time, modified_time,
                 metadata_json, deleted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, '{}', 1)
            ON CONFLICT(file_path) DO UPDATE SET deleted = 1
        )");
        if (!statement)
            return statement.error();
        YAMS_TRY(statement.value().bindAll(
            derived.normalizedPath, info.id,
            info.fileName.empty()
                ? std::filesystem::path(derived.normalizedPath).filename().string()
                : info.fileName,
            info.fileExtension, derived.pathPrefix, derived.reversePath, derived.pathHash,
            derived.parentHash, derived.pathDepth, info.indexedTime.time_since_epoch().count(),
            info.modifiedTime.time_since_epoch().count()));
        return statement.value().execute();
    });
    if (result)
        metadataChangeCounter_.fetch_add(1, std::memory_order_release);
    return result;
}

Result<std::vector<DocumentInfo>>
MetadataRepository::getDocumentPathReferences(int64_t documentId) {
    if (!hasDocumentPaths_) {
        auto document = getDocument(documentId);
        if (!document)
            return document.error();
        std::vector<DocumentInfo> result;
        if (document.value())
            result.push_back(*document.value());
        return result;
    }
    return executeReadQuery<std::vector<DocumentInfo>>(
        [&](Database& db) -> Result<std::vector<DocumentInfo>> {
            auto statement = db.prepare(repository::documentPathQueryCte(true) + "SELECT " +
                                        documentColumnList(false) + " FROM documents WHERE id = ?");
            if (!statement)
                return statement.error();
            YAMS_TRY(statement.value().bind(1, documentId));
            std::vector<DocumentInfo> result;
            while (true) {
                YAMS_TRY_UNWRAP(row, statement.value().step());
                if (!row)
                    break;
                result.push_back(mapDocumentRow(statement.value()));
            }
            return result;
        });
}

Result<std::optional<std::unordered_map<std::string, MetadataValue>>>
MetadataRepository::getDocumentPathMetadata(const std::string& path, int64_t documentId) {
    using Metadata = std::unordered_map<std::string, MetadataValue>;
    if (!hasDocumentPaths_)
        return std::optional<Metadata>{};
    return executeReadQuery<std::optional<Metadata>>([&](Database& db)
                                                         -> Result<std::optional<Metadata>> {
        auto statement = db.prepare("SELECT metadata_json FROM document_path_heads WHERE file_path "
                                    "= ? AND document_id = ? AND deleted = 0");
        if (!statement)
            return statement.error();
        YAMS_TRY(
            statement.value().bindAll(computePathDerivedValues(path).normalizedPath, documentId));
        YAMS_TRY_UNWRAP(row, statement.value().step());
        if (!row)
            return std::optional<Metadata>{};
        const auto values = nlohmann::json::parse(statement.value().getString(0), nullptr, false);
        if (!values.is_object())
            return Error{ErrorCode::InvalidData, "Invalid per-path metadata for " + path};
        Metadata result;
        for (const auto& [key, value] : values.items()) {
            if (!value.is_string())
                return Error{ErrorCode::InvalidData, "Invalid per-path metadata value for " + path};
            result.emplace(key, MetadataValue(value.get<std::string>()));
        }
        return std::optional<Metadata>{std::move(result)};
    });
}

} // namespace yams::metadata
