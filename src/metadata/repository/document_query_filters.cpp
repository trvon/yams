#include <spdlog/spdlog.h>
#include <algorithm>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include "document_query_filters.hpp"
#include "search_query_helpers.hpp"

namespace yams::metadata::repository {

namespace {

struct CommonFilterSqlFlavor {
    std::string_view docPrefix;
    std::string_view outerDocumentIdExpr;
    std::string_view tagMetadataAlias;
    bool multiExtensionsUseInList{false};
    bool logExactPath{false};
};

std::string qcol(const CommonFilterSqlFlavor& flavor, std::string_view name) {
    std::string out;
    out.reserve(flavor.docPrefix.size() + name.size());
    out.append(flavor.docPrefix);
    out.append(name);
    return out;
}

void appendCommonDocumentFilters(const DocumentQueryOptions& options, bool joinFtsForContains,
                                 bool hasPathIndexing, std::vector<std::string>& conditions,
                                 std::vector<BindParam>& params,
                                 const CommonFilterSqlFlavor& flavor) {
    if (options.exactPath) {
        auto derived = computePathDerivedValues(*options.exactPath);
        const bool pathsDiffer = derived.normalizedPath != *options.exactPath;
        if (hasPathIndexing) {
            std::string clause = "(" + qcol(flavor, "path_hash") + " = ? OR " +
                                 qcol(flavor, "file_path") + " = ?";
            addTextParam(params, derived.pathHash);
            addTextParam(params, derived.normalizedPath);
            if (pathsDiffer) {
                clause += " OR " + qcol(flavor, "file_path") + " = ?";
                addTextParam(params, *options.exactPath);
            }
            clause += ')';
            conditions.emplace_back(std::move(clause));
        } else {
            if (pathsDiffer) {
                conditions.emplace_back("(" + qcol(flavor, "file_path") + " = ? OR " +
                                        qcol(flavor, "file_path") + " = ?)");
                addTextParam(params, derived.normalizedPath);
                addTextParam(params, *options.exactPath);
            } else {
                conditions.emplace_back(qcol(flavor, "file_path") + " = ?");
                addTextParam(params, derived.normalizedPath);
            }
        }
        if (flavor.logExactPath) {
            spdlog::info("[MetadataRepository] exactPath query path='{}' normalized='{}' hash={}",
                         *options.exactPath, derived.normalizedPath, derived.pathHash);
        }
    }

    if (options.pathPrefix && !options.pathPrefix->empty()) {
        const std::string& originalPrefix = *options.pathPrefix;
        bool treatAsDirectory = options.prefixIsDirectory;
        if (!treatAsDirectory) {
            char tail = originalPrefix.back();
            treatAsDirectory = (tail == '/' || tail == '\\');
        }

        auto derived = computePathDerivedValues(originalPrefix);
        std::string normalized = derived.normalizedPath;

        if (treatAsDirectory) {
            if (!normalized.empty() && normalized.back() == '/')
                normalized.pop_back();

            if (!normalized.empty()) {
                if (hasPathIndexing) {
                    std::string clause = "(" + qcol(flavor, "path_prefix") + " = ?";
                    addTextParam(params, normalized);
                    if (options.includeSubdirectories) {
                        clause += " OR " + qcol(flavor, "path_prefix") + " LIKE ?";
                        std::string likeValue = normalized;
                        likeValue.append("/%");
                        addTextParam(params, likeValue);
                    }
                    clause += ')';
                    conditions.emplace_back(std::move(clause));
                } else {
                    std::string likeValue = normalized;
                    likeValue.append("/%");
                    conditions.emplace_back(qcol(flavor, "file_path") + " LIKE ?");
                    addTextParam(params, likeValue);
                }
            } else if (!options.includeSubdirectories) {
                if (hasPathIndexing) {
                    conditions.emplace_back(qcol(flavor, "path_prefix") + " = ''");
                } else {
                    conditions.emplace_back(qcol(flavor, "file_path") + " NOT LIKE '%/%'");
                }
            }
        } else {
            std::string lower = normalized;
            std::string upper = normalized;
            upper.push_back(static_cast<char>(0xFF));
            conditions.emplace_back("(" + qcol(flavor, "file_path") + " >= ? AND " +
                                    qcol(flavor, "file_path") + " < ?)");
            addTextParam(params, lower);
            addTextParam(params, upper);
        }
    }

    if (options.containsFragment && !options.containsFragment->empty()) {
        std::string fragment = *options.containsFragment;
        std::replace(fragment.begin(), fragment.end(), '\\', '/');

        if (joinFtsForContains) {
            if (hasAdvancedFts5Operators(fragment)) {
                std::string sanitized = sanitizeFts5UserQuery(fragment, true);
                if (!sanitized.empty()) {
                    conditions.emplace_back("documents_path_fts MATCH ?");
                    addTextParam(params, sanitized);
                }
            } else {
                std::string ftsToken = fragment;
                auto slashPos = ftsToken.find_last_of('/');
                if (slashPos != std::string::npos)
                    ftsToken = ftsToken.substr(slashPos + 1);
                bool prefix = true;
                if (!ftsToken.empty() && ftsToken.back() == '*')
                    ftsToken.pop_back();
                ftsToken = stripPunctuation(std::move(ftsToken));
                if (!ftsToken.empty()) {
                    conditions.emplace_back("documents_path_fts MATCH ?");
                    addTextParam(params, renderFts5Token(ftsToken, prefix));
                }
            }
        }

        if (hasPathIndexing) {
            std::string reversed(fragment.rbegin(), fragment.rend());
            conditions.emplace_back(qcol(flavor, "reverse_path") + " LIKE ?");
            addTextParam(params, reversed + "%");
        } else {
            conditions.emplace_back(qcol(flavor, "file_path") + " LIKE ?");
            addTextParam(params, "%" + fragment + "%");
        }
    }

    if (options.likePattern && !options.likePattern->empty()) {
        conditions.emplace_back(qcol(flavor, "file_path") + " LIKE ?");
        addTextParam(params, *options.likePattern);
    }

    if (options.fileName && !options.fileName->empty()) {
        conditions.emplace_back(qcol(flavor, "file_name") + " = ?");
        addTextParam(params, *options.fileName);
    }

    if (options.extension && !options.extension->empty()) {
        conditions.emplace_back(qcol(flavor, "file_extension") + " = ?");
        addTextParam(params, *options.extension);
    }

    if (!options.extensions.empty()) {
        if (flavor.multiExtensionsUseInList) {
            std::string extList;
            extList.reserve(options.extensions.size() * 2);
            extList += '(';
            for (size_t i = 0; i < options.extensions.size(); ++i) {
                if (i > 0)
                    extList += ',';
                extList += '?';
            }
            extList += ')';
            conditions.emplace_back(qcol(flavor, "file_extension") + " IN " + extList);
            for (const auto& ext : options.extensions) {
                addTextParam(params, ext);
            }
        } else {
            std::vector<std::string> extConditions;
            extConditions.reserve(options.extensions.size());
            for (const auto& ext : options.extensions) {
                extConditions.emplace_back(qcol(flavor, "file_extension") + " = ?");
                addTextParam(params, ext);
            }
            std::string clause = "(";
            for (size_t i = 0; i < extConditions.size(); ++i) {
                if (i > 0)
                    clause += " OR ";
                clause += extConditions[i];
            }
            clause += ')';
            conditions.emplace_back(std::move(clause));
        }
    }

    if (options.mimeType && !options.mimeType->empty()) {
        conditions.emplace_back(qcol(flavor, "mime_type") + " = ?");
        addTextParam(params, *options.mimeType);
    }

    if (options.textOnly) {
        conditions.emplace_back(qcol(flavor, "mime_type") + " LIKE 'text/%'");
    } else if (options.binaryOnly) {
        conditions.emplace_back(qcol(flavor, "mime_type") + " NOT LIKE 'text/%'");
    }

    if (options.modifiedAfter) {
        conditions.emplace_back(qcol(flavor, "modified_time") + " >= ?");
        addIntParam(params, *options.modifiedAfter);
    }

    if (options.createdAfter) {
        conditions.emplace_back(qcol(flavor, "created_time") + " >= ?");
        addIntParam(params, *options.createdAfter);
    }

    if (options.createdBefore) {
        conditions.emplace_back(qcol(flavor, "created_time") + " <= ?");
        addIntParam(params, *options.createdBefore);
    }

    if (options.modifiedBefore) {
        conditions.emplace_back(qcol(flavor, "modified_time") + " <= ?");
        addIntParam(params, *options.modifiedBefore);
    }

    if (options.indexedAfter) {
        conditions.emplace_back(qcol(flavor, "indexed_time") + " >= ?");
        addIntParam(params, *options.indexedAfter);
    }

    if (options.indexedBefore) {
        conditions.emplace_back(qcol(flavor, "indexed_time") + " <= ?");
        addIntParam(params, *options.indexedBefore);
    }

    if (options.changedSince) {
        conditions.emplace_back("(" + qcol(flavor, "modified_time") + " >= ? OR " +
                                qcol(flavor, "created_time") + " >= ? OR " +
                                qcol(flavor, "indexed_time") + " >= ?)");
        addIntParam(params, *options.changedSince);
        addIntParam(params, *options.changedSince);
        addIntParam(params, *options.changedSince);
    }

    for (const auto& tag : options.tags) {
        conditions.emplace_back(
            "EXISTS (SELECT 1 FROM metadata " + std::string(flavor.tagMetadataAlias) +
            " WHERE " + std::string(flavor.tagMetadataAlias) + ".document_id = " +
            std::string(flavor.outerDocumentIdExpr) + " AND " +
            std::string(flavor.tagMetadataAlias) + ".key = ? AND " +
            std::string(flavor.tagMetadataAlias) + ".value = ?)");
        addTextParam(params, std::string("tag:") + tag);
        addTextParam(params, tag);
    }
}

} // namespace

void addTextParam(std::vector<BindParam>& params, std::string value) {
    params.push_back(BindParam{BindParam::Type::Text, std::move(value), 0});
}

void addIntParam(std::vector<BindParam>& params, int64_t value) {
    params.push_back(BindParam{BindParam::Type::Int, {}, value});
}

void appendDocumentQueryFilters(const DocumentQueryOptions& options, bool joinFtsForContains,
                                bool hasPathIndexing, std::vector<std::string>& conditions,
                                std::vector<BindParam>& params, bool logExactPath) {
    appendCommonDocumentFilters(options, joinFtsForContains, hasPathIndexing, conditions, params,
                                {.docPrefix = "",
                                 .outerDocumentIdExpr = "documents.id",
                                 .tagMetadataAlias = "m",
                                 .multiExtensionsUseInList = false,
                                 .logExactPath = logExactPath});

    for (const auto& [key, value] : options.metadataFilters) {
        conditions.emplace_back(
            "EXISTS (SELECT 1 FROM metadata m WHERE m.document_id = documents.id "
            "AND m.key = ? AND m.value = ?)");
        addTextParam(params, key);
        addTextParam(params, value);
    }

    if (!options.extractionStatuses.empty()) {
        if (options.extractionStatuses.size() == 1) {
            conditions.emplace_back("extraction_status = ?");
            addTextParam(params, ExtractionStatusUtils::toString(options.extractionStatuses[0]));
        } else {
            std::string clause = "(";
            for (size_t i = 0; i < options.extractionStatuses.size(); ++i) {
                if (i > 0)
                    clause += " OR ";
                clause += "extraction_status = ?";
                addTextParam(params,
                             ExtractionStatusUtils::toString(options.extractionStatuses[i]));
            }
            clause += ')';
            conditions.emplace_back(std::move(clause));
        }
    }

    if (!options.repairStatuses.empty()) {
        if (options.repairStatuses.size() == 1) {
            conditions.emplace_back("repair_status = ?");
            addTextParam(params, RepairStatusUtils::toString(options.repairStatuses[0]));
        } else {
            std::string clause = "(";
            for (size_t i = 0; i < options.repairStatuses.size(); ++i) {
                if (i > 0)
                    clause += " OR ";
                clause += "repair_status = ?";
                addTextParam(params, RepairStatusUtils::toString(options.repairStatuses[i]));
            }
            clause += ')';
            conditions.emplace_back(std::move(clause));
        }
    }

    if (options.maxRepairAttempts > 0) {
        conditions.emplace_back("repair_attempts < ?");
        addIntParam(params, options.maxRepairAttempts);
    }

    if (options.onlyMissingContent) {
        conditions.emplace_back("NOT EXISTS (SELECT 1 FROM document_content c "
                                "WHERE c.document_id = documents.id)");
    }

    if (options.stalledBefore) {
        conditions.emplace_back("COALESCE(NULLIF(indexed_time, 0), modified_time) < ?");
        addIntParam(params, *options.stalledBefore);
    }

    if (options.repairAttemptedBefore) {
        conditions.emplace_back("repair_attempted_at < ?");
        addIntParam(params, *options.repairAttemptedBefore);
    }
}

void appendMetadataValueCountDocumentFilters(const DocumentQueryOptions& options,
                                             bool joinFtsForContains, bool hasPathIndexing,
                                             std::vector<std::string>& conditions,
                                             std::vector<BindParam>& params) {
    appendCommonDocumentFilters(options, joinFtsForContains, hasPathIndexing, conditions, params,
                                {.docPrefix = "d.",
                                 .outerDocumentIdExpr = "d.id",
                                 .tagMetadataAlias = "tm",
                                 .multiExtensionsUseInList = true,
                                 .logExactPath = false});
}

} // namespace yams::metadata::repository
