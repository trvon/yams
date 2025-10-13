// Copyright 2025 YAMS Project
// SPDX-License-Identifier: GPL-3.0-or-later

#include <spdlog/spdlog.h>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <yams/metadata/tree_differ.h>

namespace yams::metadata {

namespace {

/**
 * @brief Simple glob pattern matcher (supports * wildcard)
 */
bool matchesPattern(std::string_view path, std::string_view pattern) {
    // Simple implementation: * matches any sequence
    // For production, consider using std::regex or fnmatch
    if (pattern.empty())
        return false;
    if (pattern == "*")
        return true;

    // Check for prefix/suffix patterns
    if (pattern.back() == '*') {
        std::string_view prefix = pattern.substr(0, pattern.size() - 1);
        return path.substr(0, prefix.size()) == prefix;
    }
    if (pattern.front() == '*') {
        std::string_view suffix = pattern.substr(1);
        if (path.size() >= suffix.size()) {
            return path.substr(path.size() - suffix.size()) == suffix;
        }
        return false;
    }

    // Exact match
    return path == pattern;
}

} // anonymous namespace

Result<DiffResult> TreeDiffer::computeDiff(const TreeNode& baseTree, const TreeNode& targetTree,
                                           const DiffOptions& options) {
    DiffResult result;

    try {
        // Optimization: if tree hashes are identical, no changes
        if (options.compareSubtrees) {
            std::string baseHash = baseTree.computeHash();
            std::string targetHash = targetTree.computeHash();
            if (baseHash == targetHash) {
                spdlog::debug("Tree hashes match, no changes detected");
                return result;
            }
        }

        // Recursively diff trees
        diffTrees(baseTree, targetTree, "", result, options);

        // Detect renames if enabled
        if (options.detectRenames) {
            detectRenames(result);
        }

        // Compute summary statistics
        for (const auto& change : result.changes) {
            switch (change.type) {
                case ChangeType::Added:
                    if (change.isDirectory) {
                        result.dirsAdded++;
                    } else {
                        result.filesAdded++;
                    }
                    break;
                case ChangeType::Deleted:
                    if (change.isDirectory) {
                        result.dirsDeleted++;
                    } else {
                        result.filesDeleted++;
                    }
                    break;
                case ChangeType::Modified:
                    result.filesModified++;
                    break;
                case ChangeType::Renamed:
                case ChangeType::Moved:
                    result.filesRenamed++;
                    break;
            }
        }

        spdlog::info("Diff computed: +{} -{} ~{} renamed:{}", result.filesAdded,
                     result.filesDeleted, result.filesModified, result.filesRenamed);

        return result;
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, fmt::format("Failed to compute diff: {}", e.what())};
    }
}

Result<DiffResult> TreeDiffer::computeDiffFromData(std::span<const uint8_t> baseData,
                                                   std::span<const uint8_t> targetData,
                                                   const DiffOptions& options) {
    // Deserialize base tree
    auto baseResult = TreeNode::deserialize(baseData);
    if (!baseResult.has_value()) {
        return Error{baseResult.error().code, fmt::format("Failed to deserialize base tree: {}",
                                                          baseResult.error().message)};
    }

    // Deserialize target tree
    auto targetResult = TreeNode::deserialize(targetData);
    if (!targetResult.has_value()) {
        return Error{targetResult.error().code, fmt::format("Failed to deserialize target tree: {}",
                                                            targetResult.error().message)};
    }

    return computeDiff(baseResult.value(), targetResult.value(), options);
}

void TreeDiffer::diffTrees(const TreeNode& baseTree, const TreeNode& targetTree,
                           const std::string& basePath, DiffResult& result,
                           const DiffOptions& options) {
    const auto& baseEntries = baseTree.entries();
    const auto& targetEntries = targetTree.entries();

    // Build maps for O(1) lookup
    std::unordered_map<std::string, const TreeEntry*> baseMap;
    std::unordered_map<std::string, const TreeEntry*> targetMap;

    for (const auto& entry : baseEntries) {
        baseMap[entry.name] = &entry;
    }
    for (const auto& entry : targetEntries) {
        targetMap[entry.name] = &entry;
    }

    // Find deleted and modified entries
    for (const auto& [name, baseEntry] : baseMap) {
        std::string fullPath = basePath.empty() ? name : basePath + "/" + name;

        // Check exclusions
        if (shouldExclude(fullPath, options.excludePatterns)) {
            continue;
        }

        auto targetIt = targetMap.find(name);
        if (targetIt == targetMap.end()) {
            // Entry deleted
            result.changes.emplace_back(ChangeType::Deleted,
                                        fullPath,        // oldPath
                                        "",              // newPath
                                        baseEntry->hash, // oldHash
                                        "",              // newHash
                                        baseEntry->mode, baseEntry->isDirectory, baseEntry->size);
        } else {
            const TreeEntry* targetEntry = targetIt->second;

            // Compare hashes
            if (baseEntry->hash != targetEntry->hash) {
                if (baseEntry->isDirectory && targetEntry->isDirectory) {
                    // Recursively diff subdirectories
                    // Need to load subtrees - for now, mark as modified
                    result.changes.emplace_back(ChangeType::Modified, fullPath, fullPath,
                                                baseEntry->hash, targetEntry->hash,
                                                targetEntry->mode, true, -1);
                } else {
                    // File modified
                    result.changes.emplace_back(ChangeType::Modified, fullPath, fullPath,
                                                baseEntry->hash, targetEntry->hash,
                                                targetEntry->mode, false, targetEntry->size);
                }
            } else if (!options.includeUnchanged) {
                // Hash matches, no change (skip unless includeUnchanged is true)
            } else {
                // Include unchanged entry for completeness
                // (not adding to changes list for now)
            }
        }
    }

    // Find added entries
    for (const auto& [name, targetEntry] : targetMap) {
        std::string fullPath = basePath.empty() ? name : basePath + "/" + name;

        // Check exclusions
        if (shouldExclude(fullPath, options.excludePatterns)) {
            continue;
        }

        if (baseMap.find(name) == baseMap.end()) {
            // Entry added
            result.changes.emplace_back(ChangeType::Added,
                                        "",                // oldPath
                                        fullPath,          // newPath
                                        "",                // oldHash
                                        targetEntry->hash, // newHash
                                        targetEntry->mode, targetEntry->isDirectory,
                                        targetEntry->size);
        }
    }
}

void TreeDiffer::detectRenames(DiffResult& result) const {
    // Build hash -> path maps for deleted and added entries
    std::unordered_multimap<std::string, size_t> deletedByHash;
    std::unordered_multimap<std::string, size_t> addedByHash;

    for (size_t i = 0; i < result.changes.size(); ++i) {
        const auto& change = result.changes[i];
        if (change.type == ChangeType::Deleted && !change.oldHash.empty()) {
            deletedByHash.emplace(change.oldHash, i);
        } else if (change.type == ChangeType::Added && !change.newHash.empty()) {
            addedByHash.emplace(change.newHash, i);
        }
    }

    // Find matches: hash appears in both deleted and added
    std::unordered_set<size_t> processedIndices;

    for (const auto& [hash, deletedIdx] : deletedByHash) {
        if (processedIndices.count(deletedIdx))
            continue;

        auto range = addedByHash.equal_range(hash);
        for (auto it = range.first; it != range.second; ++it) {
            size_t addedIdx = it->second;
            if (processedIndices.count(addedIdx))
                continue;

            // Found a rename: same hash, different paths
            auto& deletedChange = result.changes[deletedIdx];
            auto& addedChange = result.changes[addedIdx];

            // Create rename entry
            TreeChange rename(ChangeType::Renamed,
                              deletedChange.oldPath, // oldPath
                              addedChange.newPath,   // newPath
                              hash,                  // oldHash (same)
                              hash,                  // newHash (same)
                              addedChange.mode, addedChange.isDirectory, addedChange.size);

            // Mark original changes as processed
            processedIndices.insert(deletedIdx);
            processedIndices.insert(addedIdx);

            // Replace one, remove the other
            deletedChange = rename;
            addedChange.type = ChangeType::Renamed; // Mark for removal
            addedChange.oldPath = "";               // Signal this is a duplicate

            break; // Process only first match per deleted entry
        }
    }

    // Remove duplicate rename entries
    result.changes.erase(std::remove_if(result.changes.begin(), result.changes.end(),
                                        [](const TreeChange& change) {
                                            return change.type == ChangeType::Renamed &&
                                                   change.oldPath.empty();
                                        }),
                         result.changes.end());
}

bool TreeDiffer::shouldExclude(const std::string& path,
                               const std::vector<std::string>& patterns) const {
    for (const auto& pattern : patterns) {
        if (matchesPattern(path, pattern)) {
            return true;
        }
    }
    return false;
}

} // namespace yams::metadata
