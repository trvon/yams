// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::daemon {

/// Fills the grep trigram index from the bytes grep scans. Each pass takes a batch of documents
/// that are not indexed at their current content hash (smallest first) and indexes them. The
/// same sweep covers the first backfill and documents ingested later; a document waiting its
/// turn is simply scanned by grep, so the index never has to be complete to be correct.
class GrepTrigramIndexer {
public:
    struct Options {
        std::size_t batchSize = 64;
        /// Grep does not scan larger documents, so they are not worth indexing.
        std::int64_t maxDocumentBytes = 100LL * 1024 * 1024;
        /// The bytes grep scans for a content hash (the content store's retrieveBytes).
        std::function<Result<std::vector<std::byte>>(const std::string& sha256Hash)> fetchBytes;
        /// Checked before every document; false ends the pass early.
        std::function<bool()> admit;
        std::function<bool()> stopRequested;
    };

    struct PassResult {
        std::size_t indexed = 0;
        std::size_t failed = 0;
        /// The batch was full, so more documents may be waiting.
        bool morePending = false;
        /// Admission or a stop request ended the pass before the batch was done.
        bool interrupted = false;
    };

    GrepTrigramIndexer(std::shared_ptr<metadata::IMetadataRepository> repository, Options options);

    /// Index one batch. Documents whose bytes cannot be read or indexed are skipped for the rest
    /// of this indexer's lifetime, so a broken blob cannot keep the sweep busy.
    PassResult runPass();

    [[nodiscard]] bool available() const;

private:
    std::shared_ptr<metadata::IMetadataRepository> repository_;
    Options options_;
    std::unordered_set<std::int64_t> skipped_;
};

} // namespace yams::daemon
