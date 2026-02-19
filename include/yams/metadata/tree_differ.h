#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <yams/core/types.h>
#include <yams/metadata/tree_builder.h>

namespace yams::metadata {

/**
 * @brief Types of changes detected between tree snapshots
 */
enum class ChangeType {
    Added,    ///< File was added in target snapshot
    Deleted,  ///< File was deleted (present in base, not in target)
    Modified, ///< File content changed (same path, different hash)
    Renamed,  ///< File moved to different path (same hash)
    Moved     ///< Directory moved (children maintain relative paths)
};

/**
 * @brief Record of a single change between snapshots
 */
struct TreeChange {
    ChangeType type = ChangeType::Added;
    std::string oldPath;      ///< Path in base snapshot (empty for Added)
    std::string newPath;      ///< Path in target snapshot (empty for Deleted)
    std::string oldHash;      ///< Content hash in base (empty for Added)
    std::string newHash;      ///< Content hash in target (empty for Deleted)
    uint32_t mode = 0;        ///< File mode (POSIX permissions)
    bool isDirectory = false; ///< True if this is a directory change
    int64_t size = -1;        ///< File size (-1 for directories)

    TreeChange() = default;
    TreeChange(ChangeType t, std::string op, std::string np, std::string oh, std::string nh,
               uint32_t m, bool isDir, int64_t sz)
        : type(t), oldPath(std::move(op)), newPath(std::move(np)), oldHash(std::move(oh)),
          newHash(std::move(nh)), mode(m), isDirectory(isDir), size(sz) {}
};

/**
 * @brief Options for diff computation
 */
struct DiffOptions {
    bool detectRenames = true;                ///< Enable rename/move detection
    bool compareSubtrees = true;              ///< Use O(log n) subtree hash comparison
    bool includeUnchanged = false;            ///< Include unchanged entries in output
    std::vector<std::string> excludePatterns; ///< Glob patterns to exclude

    DiffOptions() = default;
};

/**
 * @brief Result of tree diff computation
 */
struct DiffResult {
    std::vector<TreeChange> changes; ///< List of all changes
    int64_t filesAdded = 0;          ///< Count of added files
    int64_t filesDeleted = 0;        ///< Count of deleted files
    int64_t filesModified = 0;       ///< Count of modified files
    int64_t filesRenamed = 0;        ///< Count of renamed/moved files
    int64_t dirsAdded = 0;           ///< Count of added directories
    int64_t dirsDeleted = 0;         ///< Count of deleted directories

    DiffResult() = default;
};

/**
 * @brief Computes differences between tree snapshots
 *
 * TreeDiffer implements efficient tree comparison using:
 * - O(log n) subtree hash comparison for unchanged subtrees
 * - Hash-based rename detection for moved files
 * - Myers diff algorithm for entry-level comparison
 *
 * Example usage:
 * @code
 *   TreeDiffer differ;
 *   auto result = differ.computeDiff(baseTree, targetTree);
 *   if (result) {
 *       for (const auto& change : result->changes) {
 *           // Process change...
 *       }
 *   }
 * @endcode
 */
class TreeDiffer {
public:
    TreeDiffer() = default;
    ~TreeDiffer() = default;

    // Non-copyable, movable
    TreeDiffer(const TreeDiffer&) = delete;
    TreeDiffer& operator=(const TreeDiffer&) = delete;
    TreeDiffer(TreeDiffer&&) = default;
    TreeDiffer& operator=(TreeDiffer&&) = default;

    /**
     * @brief Compute differences between two tree snapshots
     *
     * @param baseTree Base snapshot (old state)
     * @param targetTree Target snapshot (new state)
     * @param options Diff computation options
     * @return Result containing list of changes and summary statistics
     *
     * Time complexity: O(n) where n is total entries across both trees.
     * Subtree hash comparison reduces to O(log n) for unchanged subtrees.
     */
    Result<DiffResult> computeDiff(const TreeNode& baseTree, const TreeNode& targetTree,
                                   const DiffOptions& options = DiffOptions());

    /**
     * @brief Compute diff from serialized tree data
     *
     * @param baseData Serialized base tree
     * @param targetData Serialized target tree
     * @param options Diff computation options
     * @return Result containing diff
     */
    Result<DiffResult> computeDiffFromData(std::span<const uint8_t> baseData,
                                           std::span<const uint8_t> targetData,
                                           const DiffOptions& options = DiffOptions());

private:
    /**
     * @brief Recursively compare two tree nodes
     */
    void diffTrees(const TreeNode& baseTree, const TreeNode& targetTree,
                   const std::string& basePath, DiffResult& result, const DiffOptions& options);

    /**
     * @brief Detect renames by matching hashes
     */
    void detectRenames(DiffResult& result) const;

    /**
     * @brief Check if path matches any exclude pattern
     */
    bool shouldExclude(const std::string& path, const std::vector<std::string>& patterns) const;
};

/**
 * @brief Convert ChangeType to string for display
 */
constexpr const char* changeTypeToString(ChangeType type) {
    switch (type) {
        case ChangeType::Added:
            return "Added";
        case ChangeType::Deleted:
            return "Deleted";
        case ChangeType::Modified:
            return "Modified";
        case ChangeType::Renamed:
            return "Renamed";
        case ChangeType::Moved:
            return "Moved";
        default:
            return "Unknown";
    }
}

} // namespace yams::metadata
