// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_tree_repair.h>

#include <spdlog/spdlog.h>

#include <exception>
#include <span>

namespace yams::metadata {

Result<PathTreeRepairStats> repairMissingPathTree(MetadataRepository& repo,
                                                  const PathTreeRepairOptions& options,
                                                  const PathTreeRepairProgress& progress) {
    if (options.batchSize == 0) {
        return Error{ErrorCode::InvalidArgument, "path tree repair batch size must be positive"};
    }

    PathTreeRepairStats stats;
    stats.lastDocumentId = options.startAfterId;

    while (options.maxBatches == 0 || stats.batches < options.maxBatches) {
        if (options.shouldContinue && !options.shouldContinue()) {
            break;
        }

        auto page = repo.scanDocsMissingPathTree(stats.lastDocumentId, options.batchSize);
        if (!page) {
            spdlog::warn("Path tree repair: scan after document {} failed: {}",
                         stats.lastDocumentId, page.error().message);
            return page.error();
        }

        const auto& window = page.value();
        if (window.scanned == 0) {
            stats.finished = true;
            break;
        }

        for (const auto& doc : window.missing) {
            try {
                auto treeRes = repo.upsertPathTreeForDocument(doc, doc.id, true /* isNewDocument */,
                                                              std::span<const float>());
                if (treeRes) {
                    ++stats.nodesCreated;
                    spdlog::debug("Path tree repair: created entry for '{}'", doc.filePath);
                } else {
                    ++stats.errors;
                    spdlog::warn("Path tree repair: failed for '{}': {}", doc.filePath,
                                 treeRes.error().message);
                }
            } catch (const std::exception& e) {
                ++stats.errors;
                spdlog::warn("Path tree repair: exception for '{}': {}", doc.filePath, e.what());
            }
        }

        stats.documentsScanned += window.scanned;
        stats.documentsMissing += window.missing.size();
        stats.lastDocumentId = window.lastScannedId;
        ++stats.batches;

        if (progress) {
            progress(stats);
        }

        // A short window means the table ended inside it; skip the empty follow-up query.
        if (window.scanned < options.batchSize) {
            stats.finished = true;
            break;
        }
    }

    return stats;
}

} // namespace yams::metadata
