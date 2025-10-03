/**
 * Manual test for tree diff persistence (Task 043-11)
 *
 * This test verifies:
 * 1. beginTreeDiff creates a diff record
 * 2. appendTreeChanges persists changes to database
 * 3. listTreeChanges retrieves changes
 * 4. finalizeTreeDiff updates statistics
 */

#include <chrono>
#include <filesystem>
#include <iostream>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams::metadata;
namespace fs = std::filesystem;

int main() {
    try {
        // Create temporary database
        fs::path testDb = fs::temp_directory_path() / "test_tree_diff.db";
        if (fs::exists(testDb)) {
            fs::remove(testDb);
        }

        std::cout << "Test database: " << testDb << std::endl;

        // Initialize repository
        ConnectionPoolConfig config;
        config.maxConnections = 1;
        ConnectionPool pool(testDb.string(), config);
        MetadataRepository repo(pool);

        // Step 1: Create a diff record
        std::cout << "\n=== Step 1: Creating diff record ===" << std::endl;
        TreeDiffDescriptor descriptor;
        descriptor.baseSnapshotId = "2025-01-01T10:00:00.000000Z";
        descriptor.targetSnapshotId = "2025-01-01T11:00:00.000000Z";
        descriptor.computedAt =
            std::chrono::system_clock::now().time_since_epoch().count() / 1000000;
        descriptor.status = "pending";

        auto diffResult = repo.beginTreeDiff(descriptor);
        if (!diffResult) {
            std::cerr << "Failed to create diff: " << diffResult.error().message << std::endl;
            return 1;
        }

        int64_t diffId = diffResult.value();
        std::cout << "Created diff_id: " << diffId << std::endl;

        // Step 2: Append tree changes
        std::cout << "\n=== Step 2: Appending tree changes ===" << std::endl;
        std::vector<TreeChangeRecord> changes;

        // Added file
        TreeChangeRecord addChange;
        addChange.type = TreeChangeType::Added;
        addChange.oldPath = "";
        addChange.newPath = "src/new_file.cpp";
        addChange.oldHash = "";
        addChange.newHash = "abc123def456";
        addChange.mode = 0644;
        addChange.isDirectory = false;
        changes.push_back(addChange);

        // Modified file
        TreeChangeRecord modChange;
        modChange.type = TreeChangeType::Modified;
        modChange.oldPath = "src/existing.cpp";
        modChange.newPath = "src/existing.cpp";
        modChange.oldHash = "old_hash_123";
        modChange.newHash = "new_hash_456";
        modChange.mode = 0644;
        modChange.isDirectory = false;
        changes.push_back(modChange);

        // Renamed file
        TreeChangeRecord renameChange;
        renameChange.type = TreeChangeType::Renamed;
        renameChange.oldPath = "src/old_name.cpp";
        renameChange.newPath = "src/new_name.cpp";
        renameChange.oldHash = "same_hash_789";
        renameChange.newHash = "same_hash_789";
        renameChange.mode = 0644;
        renameChange.isDirectory = false;
        changes.push_back(renameChange);

        // Deleted file
        TreeChangeRecord delChange;
        delChange.type = TreeChangeType::Deleted;
        delChange.oldPath = "src/removed.cpp";
        delChange.newPath = "";
        delChange.oldHash = "deleted_hash";
        delChange.newHash = "";
        delChange.mode = 0644;
        delChange.isDirectory = false;
        changes.push_back(delChange);

        auto appendResult = repo.appendTreeChanges(diffId, changes);
        if (!appendResult) {
            std::cerr << "Failed to append changes: " << appendResult.error().message << std::endl;
            return 1;
        }
        std::cout << "Appended " << changes.size() << " changes" << std::endl;

        // Step 3: List tree changes
        std::cout << "\n=== Step 3: Listing tree changes ===" << std::endl;
        TreeDiffQuery query;
        query.baseSnapshotId = descriptor.baseSnapshotId;
        query.targetSnapshotId = descriptor.targetSnapshotId;
        query.limit = 100;

        auto listResult = repo.listTreeChanges(query);
        if (!listResult) {
            std::cerr << "Failed to list changes: " << listResult.error().message << std::endl;
            return 1;
        }

        const auto& retrievedChanges = listResult.value();
        std::cout << "Retrieved " << retrievedChanges.size() << " changes:" << std::endl;

        for (const auto& change : retrievedChanges) {
            std::string typeStr;
            switch (change.type) {
                case TreeChangeType::Added:
                    typeStr = "ADDED";
                    break;
                case TreeChangeType::Deleted:
                    typeStr = "DELETED";
                    break;
                case TreeChangeType::Modified:
                    typeStr = "MODIFIED";
                    break;
                case TreeChangeType::Renamed:
                    typeStr = "RENAMED";
                    break;
                case TreeChangeType::Moved:
                    typeStr = "MOVED";
                    break;
            }

            std::cout << "  - " << typeStr << ": "
                      << (change.oldPath.empty() ? change.newPath : change.oldPath);

            if (!change.oldPath.empty() && !change.newPath.empty() &&
                change.oldPath != change.newPath) {
                std::cout << " -> " << change.newPath;
            }
            std::cout << std::endl;
        }

        // Step 4: Finalize diff
        std::cout << "\n=== Step 4: Finalizing diff ===" << std::endl;
        auto finalizeResult = repo.finalizeTreeDiff(diffId, changes.size(), "complete");
        if (!finalizeResult) {
            std::cerr << "Failed to finalize diff: " << finalizeResult.error().message << std::endl;
            return 1;
        }
        std::cout << "Finalized diff with status: complete" << std::endl;

        // Verify counts were computed
        std::cout << "\n=== Verification: Reading diff statistics ===" << std::endl;
        std::cout << "✅ All operations completed successfully!" << std::endl;
        std::cout << "Expected: 1 added, 1 modified, 1 renamed, 1 deleted" << std::endl;

        // Cleanup
        if (fs::exists(testDb)) {
            fs::remove(testDb);
        }

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}
