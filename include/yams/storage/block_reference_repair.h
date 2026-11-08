#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>

namespace yams::storage {

/// Same busy timeout as the storage layer's refs.db connections (storage::Database).
inline constexpr std::chrono::milliseconds kBlockRefsRepairBusyTimeout{15000};

/// Result of reconciling refs.db block_references rows with CAS object files
struct BlockRefsRepairResult {
    uint64_t blocksScanned{0};
    uint64_t blocksUpdated{0};
    uint64_t blocksSkipped{0};
    uint64_t errors{0};
    uint64_t compressedBlocks{0};       ///< Blocks with compression headers
    uint64_t uncompressedBlocks{0};     ///< Blocks without compression
    uint64_t totalDiskBytes{0};         ///< Sum of on-disk file sizes
    uint64_t totalUncompressedBytes{0}; ///< Sum of uncompressed sizes
    uint64_t entriesIgnored{0}; ///< Non-object names, and shards/files that vanished mid-scan
};

struct BlockRefsRepairOptions {
    bool dryRun{false}; ///< Only report what would be updated
    std::chrono::milliseconds busyTimeout{kBlockRefsRepairBusyTimeout};
    /// Scanned objects per batch. Each batch is one write transaction, and shouldContinue is
    /// checked between batches.
    uint64_t batchSize{1000};
    std::function<void(uint64_t current, uint64_t total)> progress;
    /// Returning false stops the repair after committing the current batch; the call then
    /// returns ErrorCode::OperationCancelled.
    std::function<bool()> shouldContinue;
};

/// Scan CAS files and update block_references with correct sizes from disk and headers, keeping
/// the ref_statistics counters consistent with the refs.db schema triggers.
/// @param objectsPath Path to the CAS objects directory
/// @param refsDbPath Path to the refs.db database
[[nodiscard]] Result<BlockRefsRepairResult>
repairBlockReferences(const std::filesystem::path& objectsPath,
                      const std::filesystem::path& refsDbPath,
                      const BlockRefsRepairOptions& options);

/// Convenience overload with the default batch size and busy timeout.
[[nodiscard]] Result<BlockRefsRepairResult>
repairBlockReferences(const std::filesystem::path& objectsPath,
                      const std::filesystem::path& refsDbPath, bool dryRun = false,
                      std::function<void(uint64_t current, uint64_t total)> progress = nullptr,
                      std::function<bool()> shouldContinue = nullptr);

} // namespace yams::storage
