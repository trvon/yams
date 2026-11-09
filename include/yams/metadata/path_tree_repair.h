// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>

namespace yams::metadata {

class MetadataRepository;

/// Options for one pass (or one slice of a pass) of path-tree repair.
struct PathTreeRepairOptions {
    /// Documents examined per batch. Each batch commits in one transaction and bounds the work
    /// done between two cooperation checks.
    std::size_t batchSize{500};
    /// Resume cursor: only documents with id > startAfterId are examined (0: from the start).
    std::int64_t startAfterId{0};
    /// Stop after this many batches even if documents remain (0: no limit). A caller that
    /// does one batch per scheduling tick passes 1 and resumes from `lastDocumentId`.
    std::size_t maxBatches{0};
    /// Checked before every batch; returning false stops the run with `finished == false` so
    /// the caller can resume later (or report a cancellation). Empty means always continue.
    std::function<bool()> shouldContinue;
    /// Checked before every document upsert inside a batch (cancel, shutdown). Returning true
    /// commits the documents done so far and stops with `finished == false`; the cursor then
    /// resumes at the first document not handled. Empty means never abort.
    std::function<bool()> shouldAbort;
};

/// Outcome of a path-tree repair run. Batches committed before a failure are reported even
/// when `error` is set.
struct PathTreeRepairStats {
    std::uint64_t documentsScanned{0}; ///< Documents examined.
    std::uint64_t documentsMissing{0}; ///< Examined documents not linked into the path tree.
    std::uint64_t nodesCreated{0};     ///< Missing documents now linked into the path tree.
    std::uint64_t errors{0};           ///< Missing documents whose upsert failed.
    std::uint64_t batches{0};          ///< Batches committed.
    /// Cursor: the largest document id examined in a committed batch. Pass it as
    /// `startAfterId` to resume.
    std::int64_t lastDocumentId{0};
    /// True when the scan reached the end of the documents table.
    bool finished{false};
    /// Set when a batch could not be scanned or committed (that batch is rolled back) or the
    /// options were invalid. The run stops there; everything above describes the work done.
    std::optional<Error> error;
};

/// Called after every committed batch with the running totals.
using PathTreeRepairProgress = std::function<void(const PathTreeRepairStats&)>;

/// Link every document that is missing from the path tree (see
/// MetadataRepository::PathTreeRepairWindow for the rule) into it. Documents are visited in id
/// order, `batchSize` at a time, starting after `options.startAfterId`; each batch is one
/// transaction. Documents already linked are left untouched. A document whose upsert fails is
/// counted in `errors` and skipped, so a pass always terminates; a later pass retries it.
[[nodiscard]] PathTreeRepairStats
repairMissingPathTree(MetadataRepository& repo, const PathTreeRepairOptions& options = {},
                      const PathTreeRepairProgress& progress = {});

} // namespace yams::metadata
