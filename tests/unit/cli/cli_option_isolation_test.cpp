/**
 * Tests for CLI11 option isolation - ensuring option descriptions
 * do not leak into option values.
 *
 * These tests verify that:
 * 1. Positional argument descriptions stay separate from values
 * 2. Named options with descriptions don't pollute values
 * 3. Default values are correctly applied (not descriptions)
 * 4. Multi-value positional args remain isolated from descriptions
 *
 * This test file was created after discovering a bug where CLI11 option
 * descriptions were leaking into config.toml values (e.g., data_dir
 * contained "[Deprecated] Single path to file/directory...").
 */

#include <CLI/CLI.hpp>
#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

namespace {

// ============================================================================
// Positional argument isolation tests
// ============================================================================

TEST(CLIOptionIsolationTest, PositionalDescriptionDoesNotLeakIntoValue) {
    CLI::App app("Test");

    std::string targetPath;
    app.add_option("path", targetPath,
                   "[Deprecated] Single path to file/directory (use '-' for stdin)");

    // Simulate: yams add /some/path
    std::vector<const char*> args = {"test", "/some/path"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    // The value should be the actual path, not the description
    EXPECT_EQ(targetPath, "/some/path");
    EXPECT_NE(targetPath, "[Deprecated] Single path to file/directory (use '-' for stdin)");
}

TEST(CLIOptionIsolationTest, PositionalWithoutArgumentRemainsEmpty) {
    CLI::App app("Test");

    std::string targetPath;
    app.add_option("path", targetPath,
                   "[Deprecated] Single path to file/directory (use '-' for stdin)");

    // Simulate: yams add (no path argument)
    std::vector<const char*> args = {"test"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    // Value should remain empty (default), not contain description
    EXPECT_TRUE(targetPath.empty());
}

TEST(CLIOptionIsolationTest, MultiplePositionalsRemainIsolated) {
    CLI::App app("Test");

    std::vector<std::string> targetPaths;
    std::string legacyPath;

    app.add_option("paths", targetPaths,
                   "File or directory paths to add (use '-' for stdin). Accepts multiple.");
    app.add_option("path", legacyPath,
                   "[Deprecated] Single path to file/directory (use '-' for stdin)");

    // Simulate: yams add file1.txt file2.txt
    std::vector<const char*> args = {"test", "file1.txt", "file2.txt"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    // Verify paths contain actual values, not descriptions
    EXPECT_FALSE(targetPaths.empty());
    for (const auto& path : targetPaths) {
        EXPECT_TRUE(path.find("[Deprecated]") == std::string::npos);
        EXPECT_TRUE(path.find("File or directory") == std::string::npos);
    }
}

// ============================================================================
// Named option isolation tests
// ============================================================================

TEST(CLIOptionIsolationTest, NamedOptionDescriptionDoesNotLeakIntoValue) {
    CLI::App app("Test");

    std::string dataDir;
    app.add_option("--data-dir", dataDir, "Data directory for storage");

    // Simulate: yams --data-dir /custom/path
    std::vector<const char*> args = {"test", "--data-dir", "/custom/path"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    EXPECT_EQ(dataDir, "/custom/path");
    EXPECT_NE(dataDir, "Data directory for storage");
}

TEST(CLIOptionIsolationTest, NamedOptionWithDefaultValDoesNotContainDescription) {
    CLI::App app("Test");

    std::string dataDir;
    app.add_option("--data-dir", dataDir, "Data directory for storage")
        ->default_val("/default/path");

    // Simulate: yams (no --data-dir argument)
    std::vector<const char*> args = {"test"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    EXPECT_EQ(dataDir, "/default/path");
    EXPECT_NE(dataDir, "Data directory for storage");
}

TEST(CLIOptionIsolationTest, NamedOptionWithoutArgumentUsesDefault) {
    CLI::App app("Test");

    std::string name;
    app.add_option("-n,--name", name, "Name for the document (especially useful for stdin)")
        ->default_val("");

    // Simulate: yams add (no --name argument)
    std::vector<const char*> args = {"test"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    // Should be empty default, not the description
    EXPECT_TRUE(name.empty());
    EXPECT_NE(name, "Name for the document (especially useful for stdin)");
}

// ============================================================================
// Subcommand isolation tests
// ============================================================================

TEST(CLIOptionIsolationTest, SubcommandOptionDescriptionsRemainIsolated) {
    CLI::App app("Test");

    std::string dataDir;
    app.add_option("--data-dir", dataDir, "Data directory for storage")->default_val("/default");

    auto* addCmd = app.add_subcommand("add", "Add document(s) to the store");

    std::string targetPath;
    addCmd->add_option("path", targetPath,
                       "[Deprecated] Single path to file/directory (use '-' for stdin)");

    std::string docName;
    addCmd->add_option("-n,--name", docName, "Name for the document (especially useful for stdin)");

    // Simulate: yams add /some/file --name myfile
    std::vector<const char*> args = {"test", "add", "/some/file", "--name", "myfile"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    EXPECT_EQ(targetPath, "/some/file");
    EXPECT_EQ(docName, "myfile");
    EXPECT_EQ(dataDir, "/default");

    // None should contain descriptions
    EXPECT_TRUE(targetPath.find("[Deprecated]") == std::string::npos);
    EXPECT_TRUE(docName.find("Name for the document") == std::string::npos);
    EXPECT_TRUE(dataDir.find("Data directory") == std::string::npos);
}

// ============================================================================
// Edge cases - the actual bug scenario
// ============================================================================

TEST(CLIOptionIsolationTest, PathDoesNotBecomeDescriptionWhenMissing) {
    // This replicates the exact structure from add_command.cpp
    CLI::App app("YAMS");

    std::string dataDir;
    app.add_option("--data-dir,--storage", dataDir, "Data directory for storage")
        ->default_val("/home/user/.local/share/yams");

    auto* addCmd = app.add_subcommand("add", "Add document(s) or directory to the content store");

    std::vector<std::string> targetPaths;
    std::string legacyTargetPath;

    addCmd->add_option("paths", targetPaths,
                       "File or directory paths to add (use '-' for stdin). Accepts multiple.");
    addCmd->add_option("path", legacyTargetPath,
                       "[Deprecated] Single path to file/directory (use '-' for stdin)");

    // Simulate: yams add (no path provided - this is where the bug occurred)
    std::vector<const char*> args = {"yams", "add"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    // CRITICAL: Values should be empty/default, NOT descriptions
    EXPECT_TRUE(targetPaths.empty());
    EXPECT_TRUE(legacyTargetPath.empty());
    EXPECT_EQ(dataDir, "/home/user/.local/share/yams");

    // None should contain help text
    EXPECT_TRUE(legacyTargetPath.find("[Deprecated]") == std::string::npos);
    EXPECT_TRUE(dataDir.find("Data directory") == std::string::npos);
}

TEST(CLIOptionIsolationTest, GlobalOptionNotPollutedBySubcommandDescriptions) {
    CLI::App app("YAMS");
    app.prefix_command(); // As used in yams_cli.cpp

    std::string dataDir;
    app.add_option("--data-dir", dataDir, "Data directory for storage")
        ->default_val("/default/data");

    auto* addCmd = app.add_subcommand("add", "Add documents");

    std::string targetPath;
    addCmd->add_option("path", targetPath,
                       "[Deprecated] Single path to file/directory (use '-' for stdin)");

    // Simulate: yams --data-dir /custom add somefile.txt
    std::vector<const char*> args = {"yams", "--data-dir", "/custom", "add", "somefile.txt"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    EXPECT_EQ(dataDir, "/custom");
    EXPECT_EQ(targetPath, "somefile.txt");

    // Verify no cross-contamination
    EXPECT_TRUE(dataDir.find("[Deprecated]") == std::string::npos);
    EXPECT_TRUE(targetPath.find("Data directory") == std::string::npos);
}

// ============================================================================
// Value validation - ensure descriptions are never valid file paths
// ============================================================================

TEST(CLIOptionIsolationTest, DescriptionStringsAreNotValidPaths) {
    // These are the actual description strings from add_command.cpp
    // If they appear as values, they would create invalid directories

    std::vector<std::string> descriptions = {
        "[Deprecated] Single path to file/directory (use '-' for stdin)",
        "File or directory paths to add (use '-' for stdin). Accepts multiple.",
        "Name for the document (especially useful for stdin)", "Data directory for storage"};

    for (const auto& desc : descriptions) {
        // On Windows, paths with brackets are technically valid but highly unusual
        // On Unix, they're valid but still wrong for a data directory
        // The key test is: if these appear as paths, something went wrong

        std::filesystem::path p(desc);

        // These should NOT be reasonable path names
        EXPECT_TRUE(desc.find('[') != std::string::npos || desc.find('(') != std::string::npos ||
                    desc.length() > 60)
            << "Description '" << desc << "' looks too path-like, may indicate a bug";
    }
}

TEST(CLIOptionIsolationTest, EmptyValueDoesNotBecomeDescription) {
    CLI::App app("Test");

    std::string value;
    auto* opt = app.add_option("--option", value, "This is the option description");

    // Parse with no arguments
    std::vector<const char*> args = {"test"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    // Value should remain empty
    EXPECT_TRUE(value.empty());

    // Verify the option's description is separate from its value
    EXPECT_EQ(opt->get_description(), "This is the option description");
    EXPECT_NE(value, opt->get_description());
}

// ============================================================================
// Multiple values with same description pattern
// ============================================================================

TEST(CLIOptionIsolationTest, MultipleOptionsWithSimilarDescriptions) {
    CLI::App app("Test");

    std::string queryPath;
    std::string namePath;
    std::string deletePath;

    app.add_option("--query", queryPath, "Read query from file path (use '-' to read from STDIN)");
    app.add_option("--name", namePath, "Name of the document to retrieve (explicit flag form)");
    app.add_option("--delete", deletePath, "Delete multiple documents by names (comma-separated)");

    // Parse with one option specified
    std::vector<const char*> args = {"test", "--query", "search.txt"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    EXPECT_EQ(queryPath, "search.txt");
    EXPECT_TRUE(namePath.empty());
    EXPECT_TRUE(deletePath.empty());

    // Ensure descriptions didn't leak into any value
    EXPECT_TRUE(queryPath.find("Read query from") == std::string::npos);
    EXPECT_TRUE(namePath.find("Name of the document") == std::string::npos);
    EXPECT_TRUE(deletePath.find("Delete multiple") == std::string::npos);
}

} // namespace
