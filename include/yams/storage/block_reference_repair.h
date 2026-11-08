#pragma once

#include <yams/core/types.h>

#include <cstdint>
#include <filesystem>
#include <functional>

namespace yams::storage {

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
};

/// Scan CAS files and update block_references with correct sizes from disk and headers,
/// then refresh the ref_statistics totals.
/// @param objectsPath Path to the CAS objects directory
/// @param refsDbPath Path to the refs.db database
/// @param dryRun If true, only report what would be updated
/// @param progress Optional progress callback
[[nodiscard]] Result<BlockRefsRepairResult>
repairBlockReferences(const std::filesystem::path& objectsPath,
                      const std::filesystem::path& refsDbPath, bool dryRun = false,
                      std::function<void(uint64_t current, uint64_t total)> progress = nullptr);

} // namespace yams::storage
