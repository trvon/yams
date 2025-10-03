#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <yams/core/types.h>

// Forward declarations
namespace yams::storage {
class IStorageEngine;
}

namespace yams::metadata {

/**
 * @brief Represents a single entry in a tree node (file or subdirectory)
 *
 * Similar to Git's tree entry format:
 * - mode: file permissions (e.g., 0100644 for regular file, 040000 for directory)
 * - name: filename or directory name
 * - hash: SHA-256 hash of content (for files) or subtree (for directories)
 */
struct TreeEntry {
    uint32_t mode{0};        // File mode/permissions
    std::string name;        // Entry name (filename or dirname)
    std::string hash;        // SHA-256 hash (hex-encoded, 64 chars)
    bool isDirectory{false}; // True if this entry is a directory
    int64_t size{0};         // Size in bytes (0 for directories)

    // Ordering for canonical tree representation (lexicographic by name)
    bool operator<(const TreeEntry& other) const { return name < other.name; }
};

/**
 * @brief Represents a Merkle tree node (directory snapshot)
 *
 * A TreeNode contains:
 * - Sorted list of entries (files + subdirectories)
 * - Computed hash (SHA-256 of canonical representation)
 *
 * This enables:
 * - Content-addressed storage (same tree = same hash)
 * - Efficient diff computation (compare subtree hashes)
 * - Deduplication (shared subtrees stored once)
 */
class TreeNode {
public:
    TreeNode() = default;
    explicit TreeNode(std::vector<TreeEntry> entries);

    /**
     * @brief Add an entry to this tree node
     * @param entry The entry to add (file or directory)
     * @note Entries are kept sorted by name for canonical representation
     */
    void addEntry(TreeEntry entry);

    /**
     * @brief Get all entries in this tree node
     * @return Sorted vector of entries
     */
    const std::vector<TreeEntry>& entries() const { return entries_; }

    /**
     * @brief Compute the SHA-256 hash of this tree node
     * @return 64-character hex-encoded hash
     *
     * Hash computation:
     * 1. Sort entries by name (canonical order)
     * 2. Serialize each entry: "<mode> <name>\0<hash>"
     * 3. Concatenate all serialized entries
     * 4. Compute SHA-256 of concatenated bytes
     */
    std::string computeHash() const;

    /**
     * @brief Serialize tree node to Git-compatible format
     * @return Byte vector suitable for CAS storage
     *
     * Format (per entry):
     * - ASCII mode (e.g., "100644" or "40000")
     * - Space
     * - Filename (UTF-8)
     * - Null byte
     * - SHA-256 hash (32 bytes, binary not hex)
     */
    std::vector<uint8_t> serialize() const;

    /**
     * @brief Deserialize tree node from Git-compatible format
     * @param data Serialized tree data
     * @return TreeNode instance or error
     */
    static Result<TreeNode> deserialize(std::span<const uint8_t> data);

    /**
     * @brief Get total number of entries
     */
    size_t size() const { return entries_.size(); }

    /**
     * @brief Check if tree is empty
     */
    bool empty() const { return entries_.empty(); }

private:
    std::vector<TreeEntry> entries_;
    mutable std::optional<std::string> cachedHash_;
};

/**
 * @brief Builds Merkle tree from filesystem directory
 *
 * TreeBuilder recursively walks a directory and constructs a Merkle tree:
 * - Files → hash of content
 * - Directories → hash of subtree (recursively built)
 *
 * This enables:
 * - Efficient snapshot creation
 * - Fast diff computation (subtree hash comparison)
 * - Content deduplication
 *
 * Usage:
 * ```cpp
 * TreeBuilder builder(storageEngine, hashFunc);
 * auto result = builder.buildFromDirectory("/path/to/dir");
 * if (result) {
 *     std::string rootTreeHash = *result;
 *     // Store rootTreeHash in metadata repository
 * }
 * ```
 */
class TreeBuilder {
public:
    /**
     * @brief Construct tree builder
     * @param storageEngine CAS storage for storing tree objects
     */
    explicit TreeBuilder(std::shared_ptr<::yams::storage::IStorageEngine> storageEngine);

    /**
     * @brief Build Merkle tree from directory
     * @param directoryPath Path to directory root
     * @param excludePatterns Glob patterns to exclude (e.g., ".git", "node_modules")
     * @return Root tree hash or error
     *
     * Process:
     * 1. Recursively walk directory structure
     * 2. Compute hash for each file (SHA-256 of content)
     * 3. Build tree nodes bottom-up (leaves → root)
     * 4. Store each tree node in CAS
     * 5. Return root tree hash
     *
     * Complexity: O(n) where n = number of files + directories
     * Memory: O(d) where d = maximum directory depth (stack-based recursion)
     */
    Result<std::string> buildFromDirectory(std::string_view directoryPath,
                                           const std::vector<std::string>& excludePatterns = {});

    /**
     * @brief Build tree node from explicit entries (for testing)
     * @param entries Vector of tree entries
     * @return Tree hash or error
     */
    Result<std::string> buildFromEntries(const std::vector<TreeEntry>& entries);

    /**
     * @brief Retrieve tree node from CAS by hash
     * @param treeHash SHA-256 hash of tree node
     * @return TreeNode instance or error
     */
    Result<TreeNode> getTree(std::string_view treeHash);

    /**
     * @brief Check if tree exists in CAS
     * @param treeHash SHA-256 hash of tree node
     * @return True if tree exists
     */
    Result<bool> hasTree(std::string_view treeHash);

private:
    /**
     * @brief Recursively build tree from directory
     * @param dirPath Current directory path
     * @param excludePatterns Patterns to exclude
     * @return Tree hash or error
     */
    Result<std::string> buildTreeRecursive(const std::string& dirPath,
                                           const std::vector<std::string>& excludePatterns);

    /**
     * @brief Store tree node in CAS
     * @param node Tree node to store
     * @return Tree hash or error
     */
    Result<std::string> storeTree(const TreeNode& node);

    /**
     * @brief Check if path matches any exclude pattern
     * @param path Path to check
     * @param patterns Glob patterns
     * @return True if path should be excluded
     */
    bool shouldExclude(std::string_view path, const std::vector<std::string>& patterns) const;

    std::shared_ptr<::yams::storage::IStorageEngine> storageEngine_;
};

} // namespace yams::metadata
