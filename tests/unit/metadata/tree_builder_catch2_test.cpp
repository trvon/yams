// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/tree_builder.h>
#include <yams/storage/storage_engine.h>

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::metadata;
using namespace yams::storage;

namespace {

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

    Result<IStorageEngine::RawObject> retrieveRaw(std::string_view hash) const override {
        auto it = storage_.find(std::string(hash));
        if (it == storage_.end()) {
            return Error{ErrorCode::NotFound, "Hash not found"};
        }
        IStorageEngine::RawObject obj;
        obj.data.resize(it->second.size());
        std::memcpy(obj.data.data(), it->second.data(), it->second.size());
        obj.header = std::nullopt;
        return obj;
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

    std::future<Result<IStorageEngine::RawObject>>
    retrieveRawAsync(std::string_view hash) const override {
        std::promise<Result<IStorageEngine::RawObject>> promise;
        promise.set_value(retrieveRaw(hash));
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

    Result<uint64_t> getBlockSize(std::string_view hash) const override {
        auto it = storage_.find(std::string(hash));
        if (it == storage_.end()) {
            return Error{ErrorCode::NotFound, "Hash not found"};
        }
        return static_cast<uint64_t>(it->second.size());
    }

    void clear() { storage_.clear(); }

private:
    mutable std::unordered_map<std::string, std::vector<uint8_t>> storage_;
};

fs::path tempTestDir(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? fs::path(t) : fs::temp_directory_path();
    std::error_code ec;
    fs::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return base / (std::string(prefix) + std::to_string(ts));
}

struct TreeBuilderFixture {
    TreeBuilderFixture() {
        storage_ = std::make_shared<MockStorageEngine>();
        builder_ = std::make_unique<TreeBuilder>(storage_);
        tempDir_ = tempTestDir("yams_tree_builder_catch2_test_");
        fs::create_directories(tempDir_);
    }

    ~TreeBuilderFixture() {
        std::error_code ec;
        if (fs::exists(tempDir_)) {
            fs::remove_all(tempDir_, ec);
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

} // namespace

// -----------------------------------------------------------------------------
// TreeNode Tests
// -----------------------------------------------------------------------------

TEST_CASE("TreeNode: empty node", "[unit][metadata][tree_builder]") {
    TreeNode node;
    CHECK(node.empty());
    CHECK(node.size() == 0);
}

TEST_CASE("TreeNode: add entry", "[unit][metadata][tree_builder]") {
    TreeNode node;

    TreeEntry entry1;
    entry1.name = "file1.txt";
    entry1.hash = "a" + std::string(63, '0'); // 64-char hex hash
    entry1.mode = 0100644;

    node.addEntry(entry1);
    CHECK(node.size() == 1);
    CHECK_FALSE(node.empty());
}

TEST_CASE("TreeNode: entries are sorted", "[unit][metadata][tree_builder]") {
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
    REQUIRE(entries.size() == 3);
    CHECK(entries[0].name == "alpha.txt");
    CHECK(entries[1].name == "middle.txt");
    CHECK(entries[2].name == "zebra.txt");
}

TEST_CASE("TreeNode: compute hash", "[unit][metadata][tree_builder]") {
    TreeNode node;

    TreeEntry entry;
    entry.name = "test.txt";
    entry.hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    entry.mode = 0100644;

    node.addEntry(entry);

    std::string hash = node.computeHash();
    CHECK(hash.size() == 64);            // SHA-256 hex = 64 chars
    CHECK(hash != std::string(64, '0')); // Should not be all zeros
}

TEST_CASE("TreeNode: compute hash is cached", "[unit][metadata][tree_builder]") {
    TreeNode node;

    TreeEntry entry;
    entry.name = "test.txt";
    entry.hash = std::string(64, 'a');
    entry.mode = 0100644;

    node.addEntry(entry);

    std::string hash1 = node.computeHash();
    std::string hash2 = node.computeHash();

    // Should return same hash (cached)
    CHECK(hash1 == hash2);
}

TEST_CASE("TreeNode: serialize and deserialize", "[unit][metadata][tree_builder]") {
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
    REQUIRE(serialized.size() > 0);

    // Deserialize
    auto result = TreeNode::deserialize(serialized);
    REQUIRE(result.has_value());

    const auto& deserialized = result.value();
    REQUIRE(deserialized.size() == 2);

    const auto& entries = deserialized.entries();
    CHECK(entries[0].name == "file1.txt");
    CHECK(entries[0].hash == entry1.hash);
    CHECK(entries[0].mode == entry1.mode);

    CHECK(entries[1].name == "subdir");
    CHECK(entries[1].hash == entry2.hash);
    CHECK(entries[1].mode == entry2.mode);
}

// -----------------------------------------------------------------------------
// TreeBuilder Tests
// -----------------------------------------------------------------------------

TEST_CASE("TreeBuilder: build from empty entries", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    std::vector<TreeEntry> entries;

    auto result = fix.builder_->buildFromEntries(entries);
    REQUIRE(result.has_value());

    std::string treeHash = result.value();
    CHECK(treeHash.size() == 64);

    // Verify stored in CAS
    auto exists = fix.storage_->exists(treeHash);
    REQUIRE(exists.has_value());
    CHECK(exists.value());
}

TEST_CASE("TreeBuilder: build from single entry", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    std::vector<TreeEntry> entries;

    TreeEntry entry;
    entry.name = "test.txt";
    entry.hash = std::string(64, 'a');
    entry.mode = 0100644;

    entries.push_back(entry);

    auto result = fix.builder_->buildFromEntries(entries);
    REQUIRE(result.has_value());

    std::string treeHash = result.value();
    CHECK(treeHash.size() == 64);

    // Retrieve and verify
    auto treeResult = fix.builder_->getTree(treeHash);
    REQUIRE(treeResult.has_value());

    const auto& tree = treeResult.value();
    CHECK(tree.size() == 1);
    CHECK(tree.entries()[0].name == "test.txt");
}

TEST_CASE("TreeBuilder: build from empty directory", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;

    auto result = fix.builder_->buildFromDirectory(fix.tempDir_.string());
    REQUIRE(result.has_value());

    std::string treeHash = result.value();
    CHECK(treeHash.size() == 64);
}

TEST_CASE("TreeBuilder: build from directory with single file", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    fix.createFile(fix.tempDir_ / "test.txt", "hello world");

    auto result = fix.builder_->buildFromDirectory(fix.tempDir_.string());
    REQUIRE(result.has_value());

    std::string treeHash = result.value();

    // Retrieve tree
    auto treeResult = fix.builder_->getTree(treeHash);
    REQUIRE(treeResult.has_value());

    const auto& tree = treeResult.value();
    CHECK(tree.size() == 1);
    CHECK(tree.entries()[0].name == "test.txt");
    CHECK_FALSE(tree.entries()[0].isDirectory);
}

TEST_CASE("TreeBuilder: build from directory with multiple files",
          "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    fix.createFile(fix.tempDir_ / "file1.txt", "content1");
    fix.createFile(fix.tempDir_ / "file2.txt", "content2");
    fix.createFile(fix.tempDir_ / "file3.txt", "content3");

    auto result = fix.builder_->buildFromDirectory(fix.tempDir_.string());
    REQUIRE(result.has_value());

    std::string treeHash = result.value();

    // Retrieve tree
    auto treeResult = fix.builder_->getTree(treeHash);
    REQUIRE(treeResult.has_value());

    const auto& tree = treeResult.value();
    CHECK(tree.size() == 3);

    // Entries should be sorted
    CHECK(tree.entries()[0].name == "file1.txt");
    CHECK(tree.entries()[1].name == "file2.txt");
    CHECK(tree.entries()[2].name == "file3.txt");
}

TEST_CASE("TreeBuilder: build from directory with subdirectories",
          "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    fix.createFile(fix.tempDir_ / "root.txt", "root");
    fix.createFile(fix.tempDir_ / "subdir1" / "file1.txt", "content1");
    fix.createFile(fix.tempDir_ / "subdir2" / "file2.txt", "content2");
    fix.createFile(fix.tempDir_ / "subdir1" / "nested" / "deep.txt", "deep");

    auto result = fix.builder_->buildFromDirectory(fix.tempDir_.string());
    REQUIRE(result.has_value());

    std::string rootTreeHash = result.value();

    // Retrieve root tree
    auto rootTreeResult = fix.builder_->getTree(rootTreeHash);
    REQUIRE(rootTreeResult.has_value());

    const auto& rootTree = rootTreeResult.value();
    CHECK(rootTree.size() == 3); // root.txt, subdir1, subdir2

    // Find subdir1 entry
    const TreeEntry* subdir1Entry = nullptr;
    for (const auto& entry : rootTree.entries()) {
        if (entry.name == "subdir1") {
            subdir1Entry = &entry;
            break;
        }
    }
    REQUIRE(subdir1Entry != nullptr);
    CHECK(subdir1Entry->isDirectory);

    // Retrieve subdir1 tree
    auto subdir1TreeResult = fix.builder_->getTree(subdir1Entry->hash);
    REQUIRE(subdir1TreeResult.has_value());

    const auto& subdir1Tree = subdir1TreeResult.value();
    CHECK(subdir1Tree.size() == 2); // file1.txt, nested
}

TEST_CASE("TreeBuilder: build with exclude patterns", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    fix.createFile(fix.tempDir_ / "include.txt", "include");
    fix.createFile(fix.tempDir_ / "exclude.log", "exclude");
    fix.createFile(fix.tempDir_ / ".git" / "config", "git");

    std::vector<std::string> excludePatterns = {".git", ".log"};

    auto result = fix.builder_->buildFromDirectory(fix.tempDir_.string(), excludePatterns);
    REQUIRE(result.has_value());

    std::string treeHash = result.value();

    // Retrieve tree
    auto treeResult = fix.builder_->getTree(treeHash);
    REQUIRE(treeResult.has_value());

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

    CHECK_FALSE(hasExcludeLog);
    CHECK_FALSE(hasGitDir);
}

TEST_CASE("TreeBuilder: build with glob exclusions", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    fs::create_directories(fix.tempDir_ / "build" / "tmp");
    fs::create_directories(fix.tempDir_ / "src");
    fs::create_directories(fix.tempDir_ / "dist");
    fix.createFile(fix.tempDir_ / "build" / "tmp" / "a.o", "obj");
    fix.createFile(fix.tempDir_ / "src" / "foo.tmp", "tmp");
    fix.createFile(fix.tempDir_ / "src" / "foo.txt", "ok");
    fix.createFile(fix.tempDir_ / "dist" / "keep.txt", "ok");

    std::vector<std::string> excludePatterns = {"build/*", "**/*.tmp"};

    auto result = fix.builder_->buildFromDirectory(fix.tempDir_.string(), excludePatterns);
    REQUIRE(result.has_value());

    const auto rootTreeHash = result.value();
    auto rootTree = fix.builder_->getTree(rootTreeHash);
    REQUIRE(rootTree.has_value());

    // Collect names in root tree for quick checks
    std::vector<std::string> names;
    for (const auto& e : rootTree.value().entries())
        names.push_back(e.name);

    // Excluded directory 'build' should not appear
    CHECK(std::find(names.begin(), names.end(), "build") == names.end());

    // 'src' directory exists and should not contain foo.tmp
    const TreeEntry* srcEntry = nullptr;
    for (const auto& e : rootTree.value().entries())
        if (e.name == "src")
            srcEntry = &e;
    REQUIRE(srcEntry != nullptr);
    auto srcTree = fix.builder_->getTree(srcEntry->hash);
    REQUIRE(srcTree.has_value());
    bool hasTmp = false, hasTxt = false;
    for (const auto& e : srcTree.value().entries()) {
        if (e.name == "foo.tmp")
            hasTmp = true;
        if (e.name == "foo.txt")
            hasTxt = true;
    }
    CHECK_FALSE(hasTmp);
    CHECK(hasTxt);
}

TEST_CASE("TreeBuilder: build with deduplication", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    fix.createFile(fix.tempDir_ / "file1.txt", "same content");
    fix.createFile(fix.tempDir_ / "file2.txt", "same content");

    auto result = fix.builder_->buildFromDirectory(fix.tempDir_.string());
    REQUIRE(result.has_value());

    std::string treeHash = result.value();

    // Retrieve tree
    auto treeResult = fix.builder_->getTree(treeHash);
    REQUIRE(treeResult.has_value());

    const auto& tree = treeResult.value();
    CHECK(tree.size() == 2);

    // Both files should have same hash (deduplicated)
    CHECK(tree.entries()[0].hash == tree.entries()[1].hash);
}

TEST_CASE("TreeBuilder: build from nonexistent directory fails", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;

    auto result = fix.builder_->buildFromDirectory("/nonexistent/path");
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::NotFound);
}

TEST_CASE("TreeBuilder: build from file (not directory) fails", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    fix.createFile(fix.tempDir_ / "file.txt", "content");

    auto result = fix.builder_->buildFromDirectory((fix.tempDir_ / "file.txt").string());
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::InvalidArgument);
}

TEST_CASE("TreeBuilder: hasTree for existing tree", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    std::vector<TreeEntry> entries;
    TreeEntry entry;
    entry.name = "test.txt";
    entry.hash = std::string(64, 'a');
    entry.mode = 0100644;
    entries.push_back(entry);

    auto buildResult = fix.builder_->buildFromEntries(entries);
    REQUIRE(buildResult.has_value());

    std::string treeHash = buildResult.value();

    auto hasResult = fix.builder_->hasTree(treeHash);
    REQUIRE(hasResult.has_value());
    CHECK(hasResult.value());
}

TEST_CASE("TreeBuilder: hasTree for nonexistent tree", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    std::string fakeHash = std::string(64, 'f');

    auto hasResult = fix.builder_->hasTree(fakeHash);
    REQUIRE(hasResult.has_value());
    CHECK_FALSE(hasResult.value());
}

TEST_CASE("TreeBuilder: getTree for nonexistent tree fails", "[unit][metadata][tree_builder]") {
    TreeBuilderFixture fix;
    std::string fakeHash = std::string(64, 'f');

    auto result = fix.builder_->getTree(fakeHash);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::NotFound);
}
