#pragma once

#include <yams/core/types.h>
#include <yams/storage/storage_engine.h>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::metadata {
class MetadataRepository;
struct DocumentInfo;
} // namespace yams::metadata

namespace yams::integrity {

enum class RepairStrategy { FromBackup, FromP2P, FromParity, FromManifest };

struct RepairManagerConfig {
    std::function<Result<std::vector<std::byte>>(const std::string&)> backupFetcher;
    std::function<Result<std::vector<std::byte>>(const std::string&)> p2pFetcher;
    std::function<Result<std::vector<std::byte>>(const std::string&)> parityReconstructor;
    std::function<Result<std::vector<std::byte>>(const std::string&)> manifestReconstructor;
    std::vector<RepairStrategy> defaultOrder = {RepairStrategy::FromBackup, RepairStrategy::FromP2P,
                                                RepairStrategy::FromParity,
                                                RepairStrategy::FromManifest};
};

/// Configuration for prune operations
struct PruneConfig {
    std::vector<std::string> categories; ///< Prune categories (build-artifacts, logs, etc.)
    std::vector<std::string> extensions; ///< Specific file extensions
    std::chrono::seconds minAge{0};      ///< Minimum file age to prune
    int64_t minSize{0};                  ///< Minimum file size to prune
    int64_t maxSize{std::numeric_limits<int64_t>::max()}; ///< Maximum file size to prune
    bool dryRun{true};                                    ///< Preview mode (don't actually delete)
};

/// Result of prune operation
struct PruneResult {
    uint64_t filesDeleted{0};
    uint64_t filesFailed{0};
    uint64_t totalBytesFreed{0};
    std::unordered_map<std::string, uint64_t> categoryCounts; ///< Count by category
    std::unordered_map<std::string, uint64_t> categorySizes;  ///< Bytes by category
    std::vector<std::string> deletedPaths;
    std::vector<std::string> failedPaths;
};

/// Candidate file for pruning
struct PruneCandidate {
    std::string hash;
    std::string path;
    std::string category;
    int64_t fileSize{0};
    std::chrono::system_clock::time_point modifiedTime;
};

class RepairManager {
public:
    /// Constructor for repair-only operations (prune will fail without metadata repo)
    RepairManager(storage::IStorageEngine& storage, RepairManagerConfig config = {});

    /// Constructor with metadata repository for prune/repair operations
    RepairManager(storage::IStorageEngine& storage, metadata::MetadataRepository& repo,
                  RepairManagerConfig config = {});

    RepairManager(const RepairManager&) = delete;
    RepairManager& operator=(const RepairManager&) = delete;

    // Repair operations
    [[nodiscard]] bool attemptRepair(const std::string& blockHash,
                                     const std::vector<RepairStrategy>& order = {});

    [[nodiscard]] bool canRepair(const std::string& blockHash) const;

    // Prune operations
    [[nodiscard]] Result<PruneResult>
    pruneFiles(const PruneConfig& config,
               std::function<void(uint64_t current, uint64_t total)> progress = nullptr);

    /// Query metadata for prune candidates (static, doesn't need storage engine)
    [[nodiscard]] static std::vector<PruneCandidate>
    queryCandidatesForPrune(metadata::MetadataRepository& repo, const PruneConfig& config);

private:
    [[nodiscard]] bool storeIfValid(const std::string& blockHash,
                                    const Result<std::vector<std::byte>>& fetchResult) const;

    // Prune helpers using modern C++
    template <typename Predicate>
    [[nodiscard]] auto filterDocuments(std::span<const metadata::DocumentInfo> docs,
                                       Predicate&& pred) const;

    [[nodiscard]] bool matchesCategory(const metadata::DocumentInfo& doc,
                                       std::span<const std::string> categories) const noexcept;

    [[nodiscard]] bool meetsAgeCriteria(const metadata::DocumentInfo& doc,
                                        std::chrono::seconds minAge) const noexcept;

    [[nodiscard]] bool meetsSizeCriteria(const metadata::DocumentInfo& doc, int64_t minSize,
                                         int64_t maxSize) const noexcept;

private:
    storage::IStorageEngine& storage_;
    RepairManagerConfig config_;
    metadata::MetadataRepository* repo_{nullptr}; ///< Optional for prune operations
};

std::shared_ptr<RepairManager> makeRepairManager(storage::IStorageEngine& storage,
                                                 RepairManagerConfig config = {});

} // namespace yams::integrity
