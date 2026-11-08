// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>

#include <cstddef>
#include <cstdint>
#include <functional>

namespace yams::metadata {

class MetadataRepository;

/// Options for one pass (or one slice of a pass) of path-tree repair.
struct PathTreeRepairOptions {
    /// Documents examined per batch. Bounds the work done between two cooperation checks.
    std::size_t batchSize{500};
    /// Resume cursor: only documents with id > startAfterId are examined (0: from the start).
    std::int64_t startAfterId{0};
    /// Stop after this many batches even if documents remain (0: no limit). A caller that
    /// does one batch per scheduling tick passes 1 and resumes from `lastDocumentId`.
    std::size_t maxBatches{0};
    /// Checked before every batch; returning false stops the run with `finished == false` so
    /// the caller can resume later. Empty means always continue.
    std::function<bool()> shouldContinue;
};

/// Outcome of a path-tree repair run.
struct PathTreeRepairStats {
    std::uint64_t documentsScanned{0}; ///< Documents examined.
    std::uint64_t documentsMissing{0}; ///< Examined documents that had no path-tree node.
    std::uint64_t nodesCreated{0};     ///< Missing documents now in the path tree.
    std::uint64_t errors{0};           ///< Missing documents whose upsert failed.
    std::uint64_t batches{0};          ///< Batches processed.
    /// Cursor: the largest document id examined. Pass it as `startAfterId` to resume.
    std::int64_t lastDocumentId{0};
    /// True when the scan reached the end of the documents table.
    bool finished{false};
};

/// Called after every batch with the running totals.
using PathTreeRepairProgress = std::function<void(const PathTreeRepairStats&)>;

/// Find documents that have a file path but no path-tree node and upsert their path-tree
/// entries. Documents are visited in id order, `batchSize` at a time, starting after
/// `options.startAfterId`. Documents that already have a node are left untouched. A document
/// whose upsert fails is counted in `errors` and skipped, so a pass always terminates; a later
/// pass retries it.
[[nodiscard]] Result<PathTreeRepairStats>
repairMissingPathTree(MetadataRepository& repo, const PathTreeRepairOptions& options = {},
                      const PathTreeRepairProgress& progress = {});

} // namespace yams::metadata
