#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <gtest/gtest.h>
#include <yams/metadata/tree_builder.h>
#include <yams/storage/storage_engine.h>

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::metadata;
using namespace yams::storage;

// Mock storage engine for testing
class MockStorageEngine : public IStorageEngine {
public:
    Result<void> store(std::string_view hash, std::span<const std::byte> data) override {
        std::vector<uint8_t> uint8Data(data.size());
        std::memcpy(uint8Data.data(), data.data(), data.size());
        storage_[std::string(hash)] = std::move(uint8Data);
        return {};
    }

    Result<std::vector<std::byte>> retrieve(std::string_view hash) const override {
        auto it = storage_.find(std::string(hash));
        if (it == storage_.end()) {
            return Error{ErrorCode::NotFound, "Hash not found"};
        }
        std::vector<std::byte> byteData(it->second.size());
        std::memcpy(byteData.data(), it->second.data(), it->second.size());
        return byteData;
    }

    Result<bool> exists(std::string_view hash) const noexcept override {
        return storage_.find(std::string(hash)) != storage_.end();
    }

    Result<void> remove(std::string_view hash) override {
        storage_.erase(std::string(hash));
        return {};
    }

    std::future<Result<void>> storeAsync(std::string_view hash,
                                         std::span<const std::byte> data) override {
        std::promise<Result<void>> promise;
        promise.set_value(store(hash, data));
        return promise.get_future();
    }

    std::future<Result<std::vector<std::byte>>>
    retrieveAsync(std::string_view hash) const override {
        std::promise<Result<std::vector<std::byte>>> promise;
        promise.set_value(retrieve(hash));
        return promise.get_future();
    }

    std::vector<Result<void>>
    storeBatch(const std::vector<std::pair<std::string, std::vector<std::byte>>>& items) override {
        std::vector<Result<void>> results;
        results.reserve(items.size());
        for (const auto& [hash, data] : items) {
            std::span<const std::byte> span(data.data(), data.size());
            results.push_back(store(hash, span));
        }
        return results;
    }

    storage::StorageStats getStats() const noexcept override { return storage::StorageStats{}; }

    Result<uint64_t> getStorageSize() const override {
        uint64_t total = 0;
        for (const auto& [_, data] : storage_) {
            total += data.size();
        }
        return total;
    }

    void clear() { storage_.clear(); }

private:
    mutable std::unordered_map<std::string, std::vector<uint8_t>> storage_;
};

class TreeBuilderTest : public ::testing::Test {
protected:
    void SetUp() override {
        storage_ = std::make_shared<MockStorageEngine>();
        builder_ = std::make_unique<TreeBuilder>(storage_);

        // Create temp directory
        tempDir_ = fs::temp_directory_path() / "yams_tree_builder_test";
        fs::create_directories(tempDir_);
    }

    void TearDown() override {
        // Clean up temp directory
        if (fs::exists(tempDir_)) {
            fs::remove_all(tempDir_);
        }
        storage_->clear();
    }

    void createFile(const fs::path& path, const std::string& content) {
        fs::create_directories(path.parent_path());
        std::ofstream file(path);
        file << content;
    }

    std::shared_ptr<MockStorageEngine> storage_;
    std::unique_ptr<TreeBuilder> builder_;
    fs::path tempDir_;
};

// -----------------------------------------------------------------------------
// TreeNode Tests
// -----------------------------------------------------------------------------

TEST_F(TreeBuilderTest, TreeNodeEmpty) {
    TreeNode node;
    EXPECT_TRUE(node.empty());
    EXPECT_EQ(node.size(), size_t(0));
}

TEST_F(TreeBuilderTest, TreeNodeAddEntry) {
    TreeNode node;

    TreeEntry entry1;
    entry1.name = "file1.txt";
    entry1.hash = "a" + std::string(63, '0'); // 64-char hex hash
    entry1.mode = 0100644;

    node.addEntry(entry1);
    EXPECT_EQ(node.size(), size_t(1));
    EXPECT_FALSE(node.empty());
}

TEST_F(TreeBuilderTest, TreeNodeEntriesSorted) {
    TreeNode node;

    TreeEntry entry1, entry2, entry3;
    entry1.name = "zebra.txt";
    entry1.hash = "a" + std::string(63, '0');
    entry2.name = "alpha.txt";
    entry2.hash = "b" + std::string(63, '0');
    entry3.name = "middle.txt";
    entry3.hash = "c" + std::string(63, '0');

    // Add in non-sorted order
    node.addEntry(entry1);
    node.addEntry(entry2);
    node.addEntry(entry3);

    // Verify sorted order
    const auto& entries = node.entries();
    ASSERT_EQ(entries.size(), size_t(3));
    EXPECT_EQ(entries[0].name, "alpha.txt");
    EXPECT_EQ(entries[1].name, "middle.txt");
    EXPECT_EQ(entries[2].name, "zebra.txt");
}

TEST_F(TreeBuilderTest, TreeNodeComputeHash) {
    TreeNode node;

    TreeEntry entry;
    entry.name = "test.txt";
    entry.hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // SHA-256 of
                                                                                     // empty string
    entry.mode = 0100644;

    node.addEntry(entry);

    std::string hash = node.computeHash();
    EXPECT_EQ(hash.size(), size_t(64));    // SHA-256 hex = 64 chars
    EXPECT_NE(hash, std::string(64, '0')); // Should not be all zeros
}

TEST_F(TreeBuilderTest, TreeNodeComputeHashCached) {
    TreeNode node;

    TreeEntry entry;
    entry.name = "test.txt";
    entry.hash = std::string(64, 'a');
    entry.mode = 0100644;

    node.addEntry(entry);

    std::string hash1 = node.computeHash();
    std::string hash2 = node.computeHash();

    // Should return same hash (cached)
    EXPECT_EQ(hash1, hash2);
}

TEST_F(TreeBuilderTest, TreeNodeSerializeDeserialize) {
    TreeNode node;

    TreeEntry entry1, entry2;
    entry1.name = "file1.txt";
    entry1.hash = "a" + std::string(63, '0');
    entry1.mode = 0100644;
    entry1.isDirectory = false;

    entry2.name = "subdir";
    entry2.hash = "b" + std::string(63, '0');
    entry2.mode = 040000;
    entry2.isDirectory = true;

    node.addEntry(entry1);
    node.addEntry(entry2);

    // Serialize
    auto serialized = node.serialize();
    EXPECT_GT(serialized.size(), size_t(0));

    // Deserialize
    auto result = TreeNode::deserialize(serialized);
    ASSERT_TRUE(result.has_value());

    const auto& deserialized = result.value();
    EXPECT_EQ(deserialized.size(), size_t(2));

    const auto& entries = deserialized.entries();
    EXPECT_EQ(entries[0].name, "file1.txt");
    EXPECT_EQ(entries[0].hash, entry1.hash);
    EXPECT_EQ(entries[0].mode, entry1.mode);

    EXPECT_EQ(entries[1].name, "subdir");
    EXPECT_EQ(entries[1].hash, entry2.hash);
    EXPECT_EQ(entries[1].mode, entry2.mode);
}

// -----------------------------------------------------------------------------
// TreeBuilder Tests
// -----------------------------------------------------------------------------

TEST_F(TreeBuilderTest, BuildFromEntriesEmpty) {
    std::vector<TreeEntry> entries;

    auto result = builder_->buildFromEntries(entries);
    ASSERT_TRUE(result.has_value());

    std::string treeHash = result.value();
    EXPECT_EQ(treeHash.size(), 64);

    // Verify stored in CAS
    auto exists = storage_->exists(treeHash);
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value());
}

TEST_F(TreeBuilderTest, BuildFromEntriesSingle) {
    std::vector<TreeEntry> entries;

    TreeEntry entry;
    entry.name = "test.txt";
    entry.hash = std::string(64, 'a');
    entry.mode = 0100644;

    entries.push_back(entry);

    auto result = builder_->buildFromEntries(entries);
    ASSERT_TRUE(result.has_value());

    std::string treeHash = result.value();
    EXPECT_EQ(treeHash.size(), 64);

    // Retrieve and verify
    auto treeResult = builder_->getTree(treeHash);
    ASSERT_TRUE(treeResult.has_value());

    const auto& tree = treeResult.value();
    EXPECT_EQ(tree.size(), 1);
    EXPECT_EQ(tree.entries()[0].name, "test.txt");
}

TEST_F(TreeBuilderTest, BuildFromDirectoryEmpty) {
    // Empty directory
    auto result = builder_->buildFromDirectory(tempDir_.string());
    ASSERT_TRUE(result.has_value());

    std::string treeHash = result.value();
    EXPECT_EQ(treeHash.size(), 64);
}

TEST_F(TreeBuilderTest, BuildFromDirectorySingleFile) {
    // Create single file
    createFile(tempDir_ / "test.txt", "hello world");

    auto result = builder_->buildFromDirectory(tempDir_.string());
    ASSERT_TRUE(result.has_value());

    std::string treeHash = result.value();

    // Retrieve tree
    auto treeResult = builder_->getTree(treeHash);
    ASSERT_TRUE(treeResult.has_value());

    const auto& tree = treeResult.value();
    EXPECT_EQ(tree.size(), 1);
    EXPECT_EQ(tree.entries()[0].name, "test.txt");
    EXPECT_FALSE(tree.entries()[0].isDirectory);
}

TEST_F(TreeBuilderTest, BuildFromDirectoryMultipleFiles) {
    // Create multiple files
    createFile(tempDir_ / "file1.txt", "content1");
    createFile(tempDir_ / "file2.txt", "content2");
    createFile(tempDir_ / "file3.txt", "content3");

    auto result = builder_->buildFromDirectory(tempDir_.string());
    ASSERT_TRUE(result.has_value());

    std::string treeHash = result.value();

    // Retrieve tree
    auto treeResult = builder_->getTree(treeHash);
    ASSERT_TRUE(treeResult.has_value());

    const auto& tree = treeResult.value();
    EXPECT_EQ(tree.size(), 3);

    // Entries should be sorted
    EXPECT_EQ(tree.entries()[0].name, "file1.txt");
    EXPECT_EQ(tree.entries()[1].name, "file2.txt");
    EXPECT_EQ(tree.entries()[2].name, "file3.txt");
}

TEST_F(TreeBuilderTest, BuildFromDirectoryWithSubdirectories) {
    // Create nested structure
    createFile(tempDir_ / "root.txt", "root");
    createFile(tempDir_ / "subdir1" / "file1.txt", "content1");
    createFile(tempDir_ / "subdir2" / "file2.txt", "content2");
    createFile(tempDir_ / "subdir1" / "nested" / "deep.txt", "deep");

    auto result = builder_->buildFromDirectory(tempDir_.string());
    ASSERT_TRUE(result.has_value());

    std::string rootTreeHash = result.value();

    // Retrieve root tree
    auto rootTreeResult = builder_->getTree(rootTreeHash);
    ASSERT_TRUE(rootTreeResult.has_value());

    const auto& rootTree = rootTreeResult.value();
    EXPECT_EQ(rootTree.size(), 3); // root.txt, subdir1, subdir2

    // Find subdir1 entry
    const TreeEntry* subdir1Entry = nullptr;
    for (const auto& entry : rootTree.entries()) {
        if (entry.name == "subdir1") {
            subdir1Entry = &entry;
            break;
        }
    }
    ASSERT_NE(subdir1Entry, nullptr);
    EXPECT_TRUE(subdir1Entry->isDirectory);

    // Retrieve subdir1 tree
    auto subdir1TreeResult = builder_->getTree(subdir1Entry->hash);
    ASSERT_TRUE(subdir1TreeResult.has_value());

    const auto& subdir1Tree = subdir1TreeResult.value();
    EXPECT_EQ(subdir1Tree.size(), 2); // file1.txt, nested
}

TEST_F(TreeBuilderTest, BuildFromDirectoryExcludePatterns) {
    // Create files
    createFile(tempDir_ / "include.txt", "include");
    createFile(tempDir_ / "exclude.log", "exclude");
    createFile(tempDir_ / ".git" / "config", "git");

    std::vector<std::string> excludePatterns = {".git", ".log"};

    auto result = builder_->buildFromDirectory(tempDir_.string(), excludePatterns);
    ASSERT_TRUE(result.has_value());

    std::string treeHash = result.value();

    // Retrieve tree
    auto treeResult = builder_->getTree(treeHash);
    ASSERT_TRUE(treeResult.has_value());

    const auto& tree = treeResult.value();

    // Should only have files not matching exclude patterns
    bool hasExcludeLog = false;
    bool hasGitDir = false;
    for (const auto& entry : tree.entries()) {
        if (entry.name == "exclude.log")
            hasExcludeLog = true;
        if (entry.name == ".git")
            hasGitDir = true;
    }

    EXPECT_FALSE(hasExcludeLog);
    EXPECT_FALSE(hasGitDir);
}

TEST_F(TreeBuilderTest, BuildFromDirectoryGlobExclusions) {
    // Layout:
    //  build/tmp/a.o        (excluded by build/*)
    //  src/foo.tmp          (excluded by **/*.tmp)
    //  src/foo.txt          (kept)
    //  dist/keep.txt        (kept)
    fs::create_directories(tempDir_ / "build" / "tmp");
    fs::create_directories(tempDir_ / "src");
    fs::create_directories(tempDir_ / "dist");
    createFile(tempDir_ / "build" / "tmp" / "a.o", "obj");
    createFile(tempDir_ / "src" / "foo.tmp", "tmp");
    createFile(tempDir_ / "src" / "foo.txt", "ok");
    createFile(tempDir_ / "dist" / "keep.txt", "ok");

    std::vector<std::string> excludePatterns = {"build/*", "**/*.tmp"};

    auto result = builder_->buildFromDirectory(tempDir_.string(), excludePatterns);
    ASSERT_TRUE(result.has_value());

    const auto rootTreeHash = result.value();
    auto rootTree = builder_->getTree(rootTreeHash);
    ASSERT_TRUE(rootTree.has_value());

    // Collect names in root tree for quick checks
    std::vector<std::string> names;
    for (const auto& e : rootTree.value().entries())
        names.push_back(e.name);

    // Excluded directory 'build' should not appear
    EXPECT_TRUE(std::find(names.begin(), names.end(), "build") == names.end());

    // 'src' directory exists and should not contain foo.tmp
    const TreeEntry* srcEntry = nullptr;
    for (const auto& e : rootTree.value().entries())
        if (e.name == "src")
            srcEntry = &e;
    ASSERT_NE(srcEntry, nullptr);
    auto srcTree = builder_->getTree(srcEntry->hash);
    ASSERT_TRUE(srcTree.has_value());
    bool hasTmp = false, hasTxt = false;
    for (const auto& e : srcTree.value().entries()) {
        if (e.name == "foo.tmp")
            hasTmp = true;
        if (e.name == "foo.txt")
            hasTxt = true;
    }
    EXPECT_FALSE(hasTmp);
    EXPECT_TRUE(hasTxt);
}

TEST_F(TreeBuilderTest, BuildFromDirectoryDeduplication) {
    // Create identical files (should deduplicate in CAS)
    createFile(tempDir_ / "file1.txt", "same content");
    createFile(tempDir_ / "file2.txt", "same content");

    auto result = builder_->buildFromDirectory(tempDir_.string());
    ASSERT_TRUE(result.has_value());

    std::string treeHash = result.value();

    // Retrieve tree
    auto treeResult = builder_->getTree(treeHash);
    ASSERT_TRUE(treeResult.has_value());

    const auto& tree = treeResult.value();
    EXPECT_EQ(tree.size(), 2);

    // Both files should have same hash (deduplicated)
    EXPECT_EQ(tree.entries()[0].hash, tree.entries()[1].hash);
}

TEST_F(TreeBuilderTest, BuildFromDirectoryNotFound) {
    auto result = builder_->buildFromDirectory("/nonexistent/path");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::NotFound);
}

TEST_F(TreeBuilderTest, BuildFromDirectoryNotADirectory) {
    // Create a file, not a directory
    createFile(tempDir_ / "file.txt", "content");

    auto result = builder_->buildFromDirectory((tempDir_ / "file.txt").string());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
}

TEST_F(TreeBuilderTest, HasTreeExisting) {
    std::vector<TreeEntry> entries;
    TreeEntry entry;
    entry.name = "test.txt";
    entry.hash = std::string(64, 'a');
    entry.mode = 0100644;
    entries.push_back(entry);

    auto buildResult = builder_->buildFromEntries(entries);
    ASSERT_TRUE(buildResult.has_value());

    std::string treeHash = buildResult.value();

    auto hasResult = builder_->hasTree(treeHash);
    ASSERT_TRUE(hasResult.has_value());
    EXPECT_TRUE(hasResult.value());
}

TEST_F(TreeBuilderTest, HasTreeNonExisting) {
    std::string fakeHash = std::string(64, 'f');

    auto hasResult = builder_->hasTree(fakeHash);
    ASSERT_TRUE(hasResult.has_value());
    EXPECT_FALSE(hasResult.value());
}

TEST_F(TreeBuilderTest, GetTreeNonExisting) {
    std::string fakeHash = std::string(64, 'f');

    auto result = builder_->getTree(fakeHash);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::NotFound);
}
