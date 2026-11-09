// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_tree_repair.h>

#include <spdlog/spdlog.h>

namespace yams::metadata {

PathTreeRepairStats repairMissingPathTree(MetadataRepository& repo,
                                          const PathTreeRepairOptions& options,
                                          const PathTreeRepairProgress& progress) {
    PathTreeRepairStats stats;
    stats.lastDocumentId = options.startAfterId;
    if (options.batchSize == 0) {
        stats.error =
            Error{ErrorCode::InvalidArgument, "path tree repair batch size must be positive"};
        return stats;
    }

    while (options.maxBatches == 0 || stats.batches < options.maxBatches) {
        if (options.shouldContinue && !options.shouldContinue()) {
            break;
        }

        auto batch = repo.repairPathTreeWindow(stats.lastDocumentId, options.batchSize);
        if (!batch) {
            spdlog::warn("Path tree repair: batch after document {} failed: {}",
                         stats.lastDocumentId, batch.error().message);
            stats.error = batch.error();
            break;
        }

        const auto& window = batch.value();
        if (window.scanned == 0) {
            stats.finished = true;
            break;
        }

        stats.documentsScanned += window.scanned;
        stats.documentsMissing += window.missingIds.size();
        stats.nodesCreated += window.repaired;
        stats.errors += window.failed;
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
