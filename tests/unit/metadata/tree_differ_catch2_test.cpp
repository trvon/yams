// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/tree_builder.h>
#include <yams/metadata/tree_differ.h>

namespace fs = std::filesystem;
using namespace yams::metadata;

namespace {

TreeNode createEmptyTree() { return TreeNode(); }

TreeNode createSimpleTree() {
    TreeNode tree;
    TreeEntry entry;
    entry.mode = 0100644;
    entry.name = "file1.txt";
    entry.hash = "0000000000000000000000000000000000000000000000000000000000000001";
    entry.isDirectory = false;
    entry.size = 100;
    tree.addEntry(entry);
    return tree;
}

TreeNode createTreeWithMultipleFiles() {
    TreeNode tree;

    TreeEntry f1;
    f1.mode = 0100644;
    f1.name = "a.txt";
    f1.hash = "000000000000000000000000000000000000000000000000000000000000000a";
    f1.isDirectory = false;
    f1.size = 50;
    tree.addEntry(f1);

    TreeEntry f2;
    f2.mode = 0100644;
    f2.name = "b.txt";
    f2.hash = "000000000000000000000000000000000000000000000000000000000000000b";
    f2.isDirectory = false;
    f2.size = 75;
    tree.addEntry(f2);

    TreeEntry f3;
    f3.mode = 0100644;
    f3.name = "c.txt";
    f3.hash = "000000000000000000000000000000000000000000000000000000000000000c";
    f3.isDirectory = false;
    f3.size = 120;
    tree.addEntry(f3);

    return tree;
}

} // namespace

TEST_CASE("TreeDiffer: empty trees produce no changes", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base = createEmptyTree();
    TreeNode target = createEmptyTree();

    auto result = differ.computeDiff(base, target);
    REQUIRE(result.has_value());

    CHECK(result.value().changes.size() == 0);
    CHECK(result.value().filesAdded == 0);
    CHECK(result.value().filesDeleted == 0);
    CHECK(result.value().filesModified == 0);
}

TEST_CASE("TreeDiffer: identical trees produce no changes", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode tree = createSimpleTree();

    auto result = differ.computeDiff(tree, tree);
    REQUIRE(result.has_value());

    CHECK(result.value().changes.size() == 0);
    CHECK(result.value().filesAdded == 0);
    CHECK(result.value().filesDeleted == 0);
}

TEST_CASE("TreeDiffer: file added", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base = createEmptyTree();
    TreeNode target = createSimpleTree();

    auto result = differ.computeDiff(base, target);
    REQUIRE(result.has_value());

    CHECK(result.value().changes.size() == 1);
    CHECK(result.value().filesAdded == 1);
    CHECK(result.value().filesDeleted == 0);

    const auto& change = result.value().changes[0];
    CHECK(change.type == ChangeType::Added);
    CHECK(change.newPath == "file1.txt");
    CHECK(change.newHash == "0000000000000000000000000000000000000000000000000000000000000001");
    CHECK_FALSE(change.isDirectory);
}

TEST_CASE("TreeDiffer: file deleted", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base = createSimpleTree();
    TreeNode target = createEmptyTree();

    auto result = differ.computeDiff(base, target);
    REQUIRE(result.has_value());

    CHECK(result.value().changes.size() == 1);
    CHECK(result.value().filesAdded == 0);
    CHECK(result.value().filesDeleted == 1);

    const auto& change = result.value().changes[0];
    CHECK(change.type == ChangeType::Deleted);
    CHECK(change.oldPath == "file1.txt");
    CHECK(change.oldHash == "0000000000000000000000000000000000000000000000000000000000000001");
}

TEST_CASE("TreeDiffer: file modified", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base = createSimpleTree();

    // Create target with modified file hash
    TreeNode target;
    TreeEntry modified;
    modified.mode = 0100644;
    modified.name = "file1.txt";
    modified.hash = "0000000000000000000000000000000000000000000000000000000000000011";
    modified.isDirectory = false;
    modified.size = 200;
    target.addEntry(modified);

    auto result = differ.computeDiff(base, target);
    REQUIRE(result.has_value());

    CHECK(result.value().changes.size() == 1);
    CHECK(result.value().filesModified == 1);

    const auto& change = result.value().changes[0];
    CHECK(change.type == ChangeType::Modified);
    CHECK(change.oldPath == "file1.txt");
    CHECK(change.newPath == "file1.txt");
    CHECK(change.oldHash == "0000000000000000000000000000000000000000000000000000000000000001");
    CHECK(change.newHash == "0000000000000000000000000000000000000000000000000000000000000011");
}

TEST_CASE("TreeDiffer: file renamed with rename detection enabled", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base = createSimpleTree();
    TreeNode target;

    // Same file, different name
    TreeEntry renamed;
    renamed.mode = 0100644;
    renamed.name = "file1_renamed.txt";
    renamed.hash = "0000000000000000000000000000000000000000000000000000000000000001"; // Same hash!
    renamed.isDirectory = false;
    renamed.size = 100;
    target.addEntry(renamed);

    DiffOptions options;
    options.detectRenames = true;

    auto result = differ.computeDiff(base, target, options);
    REQUIRE(result.has_value());

    CHECK(result.value().changes.size() == 1);
    CHECK(result.value().filesRenamed == 1);

    const auto& change = result.value().changes[0];
    CHECK(change.type == ChangeType::Renamed);
    CHECK(change.oldPath == "file1.txt");
    CHECK(change.newPath == "file1_renamed.txt");
    CHECK(change.oldHash == "0000000000000000000000000000000000000000000000000000000000000001");
    CHECK(change.newHash == "0000000000000000000000000000000000000000000000000000000000000001");
}

TEST_CASE("TreeDiffer: rename detection disabled shows delete+add", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base = createSimpleTree();
    TreeNode target;

    TreeEntry renamed;
    renamed.mode = 0100644;
    renamed.name = "file1_renamed.txt";
    renamed.hash = "0000000000000000000000000000000000000000000000000000000000000001";
    renamed.isDirectory = false;
    renamed.size = 100;
    target.addEntry(renamed);

    DiffOptions options;
    options.detectRenames = false;

    auto result = differ.computeDiff(base, target, options);
    REQUIRE(result.has_value());

    // Should see a delete and an add
    CHECK(result.value().changes.size() == 2);
    CHECK(result.value().filesDeleted == 1);
    CHECK(result.value().filesAdded == 1);
}

TEST_CASE("TreeDiffer: multiple changes", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base = createTreeWithMultipleFiles();
    TreeNode target;

    // Keep a.txt (unchanged)
    TreeEntry a;
    a.mode = 0100644;
    a.name = "a.txt";
    a.hash = "000000000000000000000000000000000000000000000000000000000000000a";
    a.isDirectory = false;
    a.size = 50;
    target.addEntry(a);

    // Modify b.txt
    TreeEntry b;
    b.mode = 0100644;
    b.name = "b.txt";
    b.hash = "00000000000000000000000000000000000000000000000000000000000000bb";
    b.isDirectory = false;
    b.size = 80;
    target.addEntry(b);

    // Delete c.txt (not in target)

    // Add d.txt
    TreeEntry d;
    d.mode = 0100644;
    d.name = "d.txt";
    d.hash = "000000000000000000000000000000000000000000000000000000000000000d";
    d.isDirectory = false;
    d.size = 90;
    target.addEntry(d);

    auto result = differ.computeDiff(base, target);
    REQUIRE(result.has_value());

    // Should have: 1 modified, 1 deleted, 1 added
    CHECK(result.value().changes.size() == 3);
    CHECK(result.value().filesAdded == 1);
    CHECK(result.value().filesDeleted == 1);
    CHECK(result.value().filesModified == 1);
}

TEST_CASE("TreeDiffer: exclude patterns", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base;
    TreeNode target;

    // Add files to target
    TreeEntry f1;
    f1.mode = 0100644;
    f1.name = "file.txt";
    f1.hash = "0000000000000000000000000000000000000000000000000000000000000001";
    f1.isDirectory = false;
    f1.size = 100;
    target.addEntry(f1);

    TreeEntry f2;
    f2.mode = 0100644;
    f2.name = "file.log";
    f2.hash = "0000000000000000000000000000000000000000000000000000000000000002";
    f2.isDirectory = false;
    f2.size = 50;
    target.addEntry(f2);

    DiffOptions options;
    options.excludePatterns = {"*.log"};

    auto result = differ.computeDiff(base, target, options);
    REQUIRE(result.has_value());

    // Should only see file.txt added (file.log excluded)
    CHECK(result.value().changes.size() == 1);
    CHECK(result.value().changes[0].newPath == "file.txt");
}

TEST_CASE("TreeDiffer: directory changes", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base;
    TreeNode target;

    // Add directory to target
    TreeEntry dir;
    dir.mode = 040755;
    dir.name = "subdir";
    dir.hash = "0000000000000000000000000000000000000000000000000000000000d00001";
    dir.isDirectory = true;
    dir.size = -1;
    target.addEntry(dir);

    auto result = differ.computeDiff(base, target);
    REQUIRE(result.has_value());

    CHECK(result.value().changes.size() == 1);
    CHECK(result.value().dirsAdded == 1);
    CHECK(result.value().changes[0].isDirectory);
}

TEST_CASE("TreeDiffer: diff from serialized data", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base = createSimpleTree();
    TreeNode target = createEmptyTree();

    // Serialize trees
    std::vector<uint8_t> baseData = base.serialize();
    std::vector<uint8_t> targetData = target.serialize();

    auto result = differ.computeDiffFromData(baseData, targetData);
    REQUIRE(result.has_value());

    CHECK(result.value().changes.size() == 1);
    CHECK(result.value().filesDeleted == 1);
}

TEST_CASE("TreeDiffer: large diff with many files", "[unit][metadata][tree_differ]") {
    TreeDiffer differ;
    TreeNode base;
    TreeNode target;

    // Add 100 files to base
    for (int i = 0; i < 100; ++i) {
        TreeEntry entry;
        entry.mode = 0100644;
        entry.name = "file" + std::to_string(i) + ".txt";
        // Create proper 64-char hash with index encoded in last 2 digits
        char hashBuf[65];
        snprintf(hashBuf, sizeof(hashBuf), "%062d%02d", 0, i);
        entry.hash = hashBuf;
        entry.isDirectory = false;
        entry.size = i * 10;
        base.addEntry(entry);
    }

    // Add 100 different files to target
    for (int i = 0; i < 100; ++i) {
        TreeEntry entry;
        entry.mode = 0100644;
        entry.name = "newfile" + std::to_string(i) + ".txt";
        // Create proper 64-char hash with different prefix
        char hashBuf[65];
        snprintf(hashBuf, sizeof(hashBuf), "ff%060d%02d", 0, i);
        entry.hash = hashBuf;
        entry.isDirectory = false;
        entry.size = i * 15;
        target.addEntry(entry);
    }

    auto result = differ.computeDiff(base, target);
    REQUIRE(result.has_value());

    CHECK(result.value().changes.size() == 200);
    CHECK(result.value().filesDeleted == 100);
    CHECK(result.value().filesAdded == 100);
}

TEST_CASE("TreeDiffer: changeTypeToString", "[unit][metadata][tree_differ]") {
    CHECK(std::string(changeTypeToString(ChangeType::Added)) == "Added");
    CHECK(std::string(changeTypeToString(ChangeType::Deleted)) == "Deleted");
    CHECK(std::string(changeTypeToString(ChangeType::Modified)) == "Modified");
    CHECK(std::string(changeTypeToString(ChangeType::Renamed)) == "Renamed");
    CHECK(std::string(changeTypeToString(ChangeType::Moved)) == "Moved");
}
