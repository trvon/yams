#include <sqlite3.h>
#include <spdlog/spdlog.h>
#include <yams/api/content_store.h>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/tree_differ.h>

#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif

#include <nlohmann/json.hpp>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <vector>

namespace yams::cli {

using json = nlohmann::json;

class DiffCommand : public ICommand {
public:
    std::string getName() const override { return "diff"; }

    std::string getDescription() const override {
        return "Compare two snapshots and show file changes";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("diff", getDescription());

        // Positional arguments for snapshot IDs
        cmd->add_option("snapshotA", snapshotA_, "First snapshot ID (older)")->required();
        cmd->add_option("snapshotB", snapshotB_, "Second snapshot ID (newer)")->required();

        // Output format options
        cmd->add_option("--format", format_, "Output format: tree (default), flat, json")
            ->default_val("tree")
            ->check(CLI::IsMember({"tree", "flat", "json"}));

        // Legacy flag for flat diff (convenience shortcut)
        cmd->add_flag("--flat-diff", flatDiff_,
                      "Use flat diff format (legacy mode, equivalent to --format flat)");

        // Filtering options
        cmd->add_option("--include", includePatterns_,
                        "Include only files matching patterns (comma-separated globs, e.g., "
                        "*.cpp,*.h)");
        cmd->add_option("--exclude", excludePatterns_,
                        "Exclude files matching patterns (comma-separated globs, e.g., "
                        "*.log,build/**)");
        cmd->add_option("--type", changeType_,
                        "Filter by change type: added, deleted, modified, renamed")
            ->check(CLI::IsMember({"added", "deleted", "modified", "renamed"}));

        // Display options
        cmd->add_flag("--stats", statsOnly_, "Show only summary statistics");
        cmd->add_flag("--no-renames", noRenames_, "Disable rename detection");
        cmd->add_flag("-v,--verbose", verbose_, "Show detailed information including hashes");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Diff command failed: {}", result.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        try {
            // Ensure storage is initialized
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return Error{ErrorCode::NotInitialized,
                             "YAMS not initialized. Run 'yams init' first."};
            }

            auto metadataRepo = cli_->getMetadataRepository();
            auto contentStore = cli_->getContentStore();

            if (!metadataRepo || !contentStore) {
                return Error{ErrorCode::NotInitialized,
                             "Failed to access metadata repository or content store"};
            }

            // Retrieve snapshot metadata
            auto snapshotAMeta = getSnapshotMetadata(metadataRepo, snapshotA_);
            if (!snapshotAMeta) {
                return Error{ErrorCode::NotFound,
                             yamsfmt::format("Snapshot '{}' not found", snapshotA_)};
            }

            auto snapshotBMeta = getSnapshotMetadata(metadataRepo, snapshotB_);
            if (!snapshotBMeta) {
                return Error{ErrorCode::NotFound,
                             yamsfmt::format("Snapshot '{}' not found", snapshotB_)};
            }

            // Load tree roots from metadata
            auto treeRootA = snapshotAMeta->tree_root_hash;
            auto treeRootB = snapshotBMeta->tree_root_hash;

            if (treeRootA.empty() || treeRootB.empty()) {
                return Error{
                    ErrorCode::InvalidArgument,
                    "One or both snapshots are missing tree metadata. Trees may not have been "
                    "built during snapshot creation. Consider re-creating snapshots with 'yams "
                    "add --snapshot-id'."};
            }

            // Load tree nodes from storage
            auto treeA = loadTree(contentStore, treeRootA);
            if (!treeA) {
                return Error{ErrorCode::NotFound,
                             yamsfmt::format("Failed to load tree for snapshot '{}': {}",
                                             snapshotA_, treeA.error().message)};
            }

            auto treeB = loadTree(contentStore, treeRootB);
            if (!treeB) {
                return Error{ErrorCode::NotFound,
                             yamsfmt::format("Failed to load tree for snapshot '{}': {}",
                                             snapshotB_, treeB.error().message)};
            }

            // Configure diff options
            metadata::DiffOptions options;
            options.detectRenames = !noRenames_;
            options.excludePatterns = parsePatterns(excludePatterns_);
            // includePatterns are applied in post-processing since DiffOptions doesn't have them

            // Compute diff
            metadata::TreeDiffer differ;
            auto diffResult = differ.computeDiff(*treeA.value(), *treeB.value(), options);

            if (!diffResult) {
                return Error{ErrorCode::Unknown, yamsfmt::format("Failed to compute diff: {}",
                                                                 diffResult.error().message)};
            }

            // Apply include patterns if specified
            auto filteredChanges = applyIncludeFilter(diffResult.value().changes, includePatterns_);

            // Apply change type filter if specified
            if (!changeType_.empty()) {
                filteredChanges = applyTypeFilter(filteredChanges, changeType_);
            }

            // Handle --flat-diff flag (overrides --format)
            if (flatDiff_) {
                format_ = "flat";
            }

            // Output results
            if (format_ == "json") {
                outputJson(filteredChanges, *snapshotAMeta, *snapshotBMeta);
            } else if (statsOnly_) {
                outputStats(filteredChanges, snapshotA_, snapshotB_);
            } else if (format_ == "flat") {
                outputFlat(filteredChanges, snapshotA_, snapshotB_);
            } else {
                // Default: tree format
                outputTree(filteredChanges, snapshotA_, snapshotB_);
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    std::string snapshotA_;
    std::string snapshotB_;
    std::string format_ = "tree";
    std::string includePatterns_;
    std::string excludePatterns_;
    std::string changeType_;
    bool statsOnly_ = false;
    bool noRenames_ = false;
    bool verbose_ = false;
    bool flatDiff_ = false;

    struct SnapshotMetadata {
        std::string snapshot_id;
        std::string label;
        std::string collection;
        std::string tree_root_hash;
        int64_t created_at = 0;
    };

    std::optional<SnapshotMetadata>
    getSnapshotMetadata([[maybe_unused]] std::shared_ptr<metadata::MetadataRepository> repo,
                        const std::string& snapshotId) {
        // Query metadata repository for snapshot by opening the database directly
        try {
            auto dbPath = cli_->getDataPath() / "yams.db";
            sqlite3* db = nullptr;

            if (sqlite3_open(dbPath.string().c_str(), &db) != SQLITE_OK || !db) {
                spdlog::error("Failed to open database: {}", db ? sqlite3_errmsg(db) : "null db");
                if (db)
                    sqlite3_close(db);
                return std::nullopt;
            }

            sqlite3_stmt* stmt = nullptr;

            const char* sql = "SELECT snapshot_id, snapshot_label, tree_root_hash, created_at "
                              "FROM tree_snapshots WHERE snapshot_id = ?";

            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
                spdlog::error("Failed to prepare snapshot query: {}", sqlite3_errmsg(db));
                sqlite3_close(db);
                return std::nullopt;
            }

            sqlite3_bind_text(stmt, 1, snapshotId.c_str(), -1, SQLITE_TRANSIENT);

            SnapshotMetadata meta;
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                meta.snapshot_id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                if (sqlite3_column_type(stmt, 1) != SQLITE_NULL) {
                    meta.label = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                }
                // No collection column in tree_snapshots schema
                if (sqlite3_column_type(stmt, 2) != SQLITE_NULL) {
                    meta.tree_root_hash =
                        reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
                }
                meta.created_at = sqlite3_column_int64(stmt, 3);

                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return meta;
            }

            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return std::nullopt;

        } catch (const std::exception& e) {
            spdlog::error("Exception querying snapshot metadata: {}", e.what());
            return std::nullopt;
        }
    }

    Result<std::shared_ptr<metadata::TreeNode>>
    loadTree(std::shared_ptr<api::IContentStore> contentStore, const std::string& hexHash) {
        try {
            // Retrieve serialized tree from content store using retrieveBytes
            auto serializedResult = contentStore->retrieveBytes(hexHash);

            if (!serializedResult) {
                return Error{ErrorCode::NotFound,
                             yamsfmt::format("Tree object not found in storage: {}", hexHash)};
            }

            // Convert std::byte vector to uint8_t vector
            auto serializedData = serializedResult.value();
            std::vector<uint8_t> data;
            data.reserve(serializedData.size());
            for (auto b : serializedData) {
                data.push_back(static_cast<uint8_t>(b));
            }

            // Deserialize tree node
            auto node = metadata::TreeNode::deserialize(data);
            if (!node) {
                return Error{ErrorCode::Unknown, yamsfmt::format("Failed to deserialize tree: {}",
                                                                 node.error().message)};
            }

            // Return as shared_ptr
            return std::make_shared<metadata::TreeNode>(std::move(node.value()));

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown,
                         yamsfmt::format("Exception loading tree: {}", e.what())};
        }
    }

    std::vector<std::string> parsePatterns(const std::string& patterns) {
        if (patterns.empty())
            return {};

        std::vector<std::string> result;
        std::istringstream ss(patterns);
        std::string pattern;

        while (std::getline(ss, pattern, ',')) {
            yams::config::trim(pattern);
            if (!pattern.empty()) {
                result.push_back(pattern);
            }
        }

        return result;
    }

    std::vector<metadata::TreeChange>
    applyIncludeFilter(const std::vector<metadata::TreeChange>& changes,
                       const std::string& includePatterns) {
        if (includePatterns.empty())
            return changes;

        auto patterns = parsePatterns(includePatterns);
        std::vector<metadata::TreeChange> filtered;

        for (const auto& change : changes) {
            bool matches = false;
            // Check against the appropriate path based on change type
            const std::string& pathToCheck =
                (change.type == metadata::ChangeType::Deleted) ? change.oldPath : change.newPath;

            for (const auto& pattern : patterns) {
                if (matchesPattern(pathToCheck, pattern)) {
                    matches = true;
                    break;
                }
            }
            if (matches) {
                filtered.push_back(change);
            }
        }

        return filtered;
    }

    std::vector<metadata::TreeChange>
    applyTypeFilter(const std::vector<metadata::TreeChange>& changes, const std::string& typeName) {
        metadata::ChangeType targetType;

        if (typeName == "added") {
            targetType = metadata::ChangeType::Added;
        } else if (typeName == "deleted") {
            targetType = metadata::ChangeType::Deleted;
        } else if (typeName == "modified") {
            targetType = metadata::ChangeType::Modified;
        } else if (typeName == "renamed") {
            targetType = metadata::ChangeType::Renamed;
        } else {
            return changes; // Invalid type, return all
        }

        std::vector<metadata::TreeChange> filtered;
        for (const auto& change : changes) {
            if (change.type == targetType) {
                filtered.push_back(change);
            }
        }

        return filtered;
    }

    bool matchesPattern(const std::string& path, const std::string& pattern) {
        // Simple glob matching: support * and ?
        size_t pi = 0, si = 0;
        size_t starIdx = std::string::npos, matchIdx = 0;

        while (si < path.size()) {
            if (pi < pattern.size() && (pattern[pi] == '?' || pattern[pi] == path[si])) {
                ++pi;
                ++si;
            } else if (pi < pattern.size() && pattern[pi] == '*') {
                starIdx = pi;
                matchIdx = si;
                ++pi;
            } else if (starIdx != std::string::npos) {
                pi = starIdx + 1;
                ++matchIdx;
                si = matchIdx;
            } else {
                return false;
            }
        }

        while (pi < pattern.size() && pattern[pi] == '*') {
            ++pi;
        }

        return pi == pattern.size();
    }

    void outputTree(const std::vector<metadata::TreeChange>& changes, const std::string& snapA,
                    const std::string& snapB) {
        std::cout << yamsfmt::format("Comparing {} → {}\n\n", snapA, snapB);

        // Group by change type
        std::vector<metadata::TreeChange> added, deleted, modified, renamed;

        for (const auto& change : changes) {
            switch (change.type) {
                case metadata::ChangeType::Added:
                    added.push_back(change);
                    break;
                case metadata::ChangeType::Deleted:
                    deleted.push_back(change);
                    break;
                case metadata::ChangeType::Modified:
                    modified.push_back(change);
                    break;
                case metadata::ChangeType::Renamed:
                case metadata::ChangeType::Moved:
                    renamed.push_back(change);
                    break;
            }
        }

        // Output each group
        if (!added.empty()) {
            std::cout << yamsfmt::format("Added ({} files):\n", added.size());
            for (const auto& ch : added) {
                std::cout << "  + " << ch.newPath;
                if (verbose_ && !ch.newHash.empty()) {
                    std::cout << "  (hash: " << ch.newHash << ")";
                }
                std::cout << "\n";
            }
            std::cout << "\n";
        }

        if (!deleted.empty()) {
            std::cout << yamsfmt::format("Deleted ({} files):\n", deleted.size());
            for (const auto& ch : deleted) {
                std::cout << "  - " << ch.oldPath;
                if (verbose_ && !ch.oldHash.empty()) {
                    std::cout << "  (hash: " << ch.oldHash << ")";
                }
                std::cout << "\n";
            }
            std::cout << "\n";
        }

        if (!modified.empty()) {
            std::cout << yamsfmt::format("Modified ({} files):\n", modified.size());
            for (const auto& ch : modified) {
                std::cout << "  ~ " << ch.newPath;
                if (verbose_) {
                    std::cout << yamsfmt::format("  ({} → {})", ch.oldHash.substr(0, 8),
                                                 ch.newHash.substr(0, 8));
                }
                std::cout << "\n";
            }
            std::cout << "\n";
        }

        if (!renamed.empty()) {
            std::cout << yamsfmt::format("Renamed ({} files):\n", renamed.size());
            for (const auto& ch : renamed) {
                std::cout << "  → " << ch.oldPath << " → " << ch.newPath;
                if (verbose_ && !ch.newHash.empty()) {
                    std::cout << "  (hash: " << ch.newHash << ")";
                }
                std::cout << "\n";
            }
            std::cout << "\n";
        }

        // Summary
        std::cout << "Summary:\n";
        std::cout << yamsfmt::format("  +{} files added\n", added.size());
        std::cout << yamsfmt::format("  -{} files deleted\n", deleted.size());
        std::cout << yamsfmt::format("  ~{} files modified\n", modified.size());
        std::cout << yamsfmt::format("  →{} files renamed\n", renamed.size());
        std::cout << yamsfmt::format("  Total: {} changes\n", changes.size());
    }

    void outputFlat(const std::vector<metadata::TreeChange>& changes, const std::string& snapA,
                    const std::string& snapB) {
        std::cout << yamsfmt::format("Diff: {} → {}\n\n", snapA, snapB);

        for (const auto& change : changes) {
            std::string typeStr;
            std::string displayPath;

            switch (change.type) {
                case metadata::ChangeType::Added:
                    typeStr = "A";
                    displayPath = change.newPath;
                    break;
                case metadata::ChangeType::Deleted:
                    typeStr = "D";
                    displayPath = change.oldPath;
                    break;
                case metadata::ChangeType::Modified:
                    typeStr = "M";
                    displayPath = change.newPath;
                    break;
                case metadata::ChangeType::Renamed:
                case metadata::ChangeType::Moved:
                    typeStr = "R";
                    displayPath = change.newPath;
                    break;
            }

            std::cout << typeStr << " " << displayPath;

            if (change.type == metadata::ChangeType::Renamed ||
                change.type == metadata::ChangeType::Moved) {
                std::cout << " (from " << change.oldPath << ")";
            }

            if (verbose_) {
                if (!change.oldHash.empty()) {
                    std::cout << " [old: " << change.oldHash.substr(0, 8) << "]";
                }
                if (!change.newHash.empty()) {
                    std::cout << " [new: " << change.newHash.substr(0, 8) << "]";
                }
            }

            std::cout << "\n";
        }

        std::cout << yamsfmt::format("\nTotal changes: {}\n", changes.size());
    }

    void outputStats(const std::vector<metadata::TreeChange>& changes, const std::string& snapA,
                     const std::string& snapB) {
        size_t added = 0, deleted = 0, modified = 0, renamed = 0;

        for (const auto& change : changes) {
            switch (change.type) {
                case metadata::ChangeType::Added:
                    ++added;
                    break;
                case metadata::ChangeType::Deleted:
                    ++deleted;
                    break;
                case metadata::ChangeType::Modified:
                    ++modified;
                    break;
                case metadata::ChangeType::Renamed:
                case metadata::ChangeType::Moved:
                    ++renamed;
                    break;
            }
        }

        std::cout << yamsfmt::format("Snapshot Comparison: {} → {}\n", snapA, snapB);
        std::cout << yamsfmt::format("  Files added:    {}\n", added);
        std::cout << yamsfmt::format("  Files deleted:  {}\n", deleted);
        std::cout << yamsfmt::format("  Files modified: {}\n", modified);
        std::cout << yamsfmt::format("  Files renamed:  {}\n", renamed);
        std::cout << yamsfmt::format("  Total changes:  {}\n", changes.size());
    }

    void outputJson(const std::vector<metadata::TreeChange>& changes, const SnapshotMetadata& snapA,
                    const SnapshotMetadata& snapB) {
        json output;
        output["snapshotA"] = {{"id", snapA.snapshot_id},
                               {"label", snapA.label},
                               {"collection", snapA.collection},
                               {"tree_root", snapA.tree_root_hash}};

        output["snapshotB"] = {{"id", snapB.snapshot_id},
                               {"label", snapB.label},
                               {"collection", snapB.collection},
                               {"tree_root", snapB.tree_root_hash}};

        json changesArray = json::array();
        for (const auto& change : changes) {
            json changeObj;
            switch (change.type) {
                case metadata::ChangeType::Added:
                    changeObj["type"] = "added";
                    break;
                case metadata::ChangeType::Deleted:
                    changeObj["type"] = "deleted";
                    break;
                case metadata::ChangeType::Modified:
                    changeObj["type"] = "modified";
                    break;
                case metadata::ChangeType::Renamed:
                    changeObj["type"] = "renamed";
                    break;
                case metadata::ChangeType::Moved:
                    changeObj["type"] = "moved";
                    break;
            }

            changeObj["path"] = change.newPath;

            if (!change.oldPath.empty() && change.oldPath != change.newPath) {
                changeObj["oldPath"] = change.oldPath;
            }
            if (!change.oldHash.empty()) {
                changeObj["oldHash"] = change.oldHash;
            }
            if (!change.newHash.empty()) {
                changeObj["newHash"] = change.newHash;
            }

            changesArray.push_back(changeObj);
        }

        output["changes"] = changesArray;

        // Statistics
        size_t added = 0, deleted = 0, modified = 0, renamed = 0;
        for (const auto& change : changes) {
            switch (change.type) {
                case metadata::ChangeType::Added:
                    ++added;
                    break;
                case metadata::ChangeType::Deleted:
                    ++deleted;
                    break;
                case metadata::ChangeType::Modified:
                    ++modified;
                    break;
                case metadata::ChangeType::Renamed:
                case metadata::ChangeType::Moved:
                    ++renamed;
                    break;
            }
        }

        output["summary"] = {{"added", added},
                             {"deleted", deleted},
                             {"modified", modified},
                             {"renamed", renamed},
                             {"total", changes.size()}};

        std::cout << output.dump(2) << std::endl;
    }
};

// Factory function
std::unique_ptr<ICommand> createDiffCommand() {
    return std::make_unique<DiffCommand>();
}

} // namespace yams::cli
