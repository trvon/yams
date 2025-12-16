// CLI Option Isolation tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for CLI11 option isolation - ensuring option descriptions
// do not leak into option values.

#include <catch2/catch_test_macros.hpp>

#include <CLI/CLI.hpp>

#include <filesystem>
#include <string>
#include <vector>

// ============================================================================
// Positional argument isolation tests
// ============================================================================

TEST_CASE("CLIOptionIsolation - Positional description does not leak into value", "[cli][option_isolation][catch2]") {
    CLI::App app("Test");

    std::string targetPath;
    app.add_option("path", targetPath,
                   "[Deprecated] Single path to file/directory (use '-' for stdin)");

#ifdef _WIN32
    std::vector<const char*> args = {"test", "C:\\some\\path"};
    const char* expectedPath = "C:\\some\\path";
#else
    std::vector<const char*> args = {"test", "/some/path"};
    const char* expectedPath = "/some/path";
#endif
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    CHECK(targetPath == expectedPath);
    CHECK(targetPath != "[Deprecated] Single path to file/directory (use '-' for stdin)");
}

TEST_CASE("CLIOptionIsolation - Positional without argument remains empty", "[cli][option_isolation][catch2]") {
    CLI::App app("Test");

    std::string targetPath;
    app.add_option("path", targetPath,
                   "[Deprecated] Single path to file/directory (use '-' for stdin)");

    std::vector<const char*> args = {"test"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    CHECK(targetPath.empty());
}

TEST_CASE("CLIOptionIsolation - Multiple positionals remain isolated", "[cli][option_isolation][catch2]") {
    CLI::App app("Test");

    std::vector<std::string> targetPaths;
    std::string legacyPath;

    app.add_option("paths", targetPaths,
                   "File or directory paths to add (use '-' for stdin). Accepts multiple.");
    app.add_option("path", legacyPath,
                   "[Deprecated] Single path to file/directory (use '-' for stdin)");

    std::vector<const char*> args = {"test", "file1.txt", "file2.txt"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    REQUIRE_FALSE(targetPaths.empty());
    for (const auto& path : targetPaths) {
        CHECK(path.find("[Deprecated]") == std::string::npos);
        CHECK(path.find("File or directory") == std::string::npos);
    }
}

// ============================================================================
// Named option isolation tests
// ============================================================================

TEST_CASE("CLIOptionIsolation - Named option description does not leak into value", "[cli][option_isolation][catch2]") {
    CLI::App app("Test");

    std::string dataDir;
    app.add_option("--data-dir", dataDir, "Data directory for storage");

    std::vector<const char*> args = {"test", "--data-dir", "/custom/path"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    CHECK(dataDir == "/custom/path");
    CHECK(dataDir != "Data directory for storage");
}

TEST_CASE("CLIOptionIsolation - Named option with default val does not contain description", "[cli][option_isolation][catch2]") {
    CLI::App app("Test");

    std::string dataDir;
    app.add_option("--data-dir", dataDir, "Data directory for storage")
        ->default_val("/default/path");

    std::vector<const char*> args = {"test"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    CHECK(dataDir == "/default/path");
    CHECK(dataDir != "Data directory for storage");
}

TEST_CASE("CLIOptionIsolation - Named option without argument uses default", "[cli][option_isolation][catch2]") {
    CLI::App app("Test");

    std::string name;
    app.add_option("-n,--name", name, "Name for the document (especially useful for stdin)")
        ->default_val("");

    std::vector<const char*> args = {"test"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    CHECK(name.empty());
    CHECK(name != "Name for the document (especially useful for stdin)");
}

// ============================================================================
// Subcommand isolation tests
// ============================================================================

TEST_CASE("CLIOptionIsolation - Subcommand option descriptions remain isolated", "[cli][option_isolation][catch2]") {
    CLI::App app("Test");

    std::string dataDir;
    app.add_option("--data-dir", dataDir, "Data directory for storage")->default_val("/default");

    auto* addCmd = app.add_subcommand("add", "Add document(s) to the store");

    std::string targetPath;
    addCmd->add_option("path", targetPath,
                       "[Deprecated] Single path to file/directory (use '-' for stdin)");

    std::string docName;
    addCmd->add_option("-n,--name", docName, "Name for the document (especially useful for stdin)");

#ifdef _WIN32
    std::vector<const char*> args = {"test", "add", "C:\\some\\file", "--name", "myfile"};
    const char* expectedPath = "C:\\some\\file";
#else
    std::vector<const char*> args = {"test", "add", "/some/file", "--name", "myfile"};
    const char* expectedPath = "/some/file";
#endif
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    CHECK(targetPath == expectedPath);
    CHECK(docName == "myfile");
    CHECK(dataDir == "/default");

    CHECK(targetPath.find("[Deprecated]") == std::string::npos);
    CHECK(docName.find("Name for the document") == std::string::npos);
    CHECK(dataDir.find("Data directory") == std::string::npos);
}

// ============================================================================
// Edge cases - the actual bug scenario
// ============================================================================

TEST_CASE("CLIOptionIsolation - Path does not become description when missing", "[cli][option_isolation][catch2]") {
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

    std::vector<const char*> args = {"yams", "add"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    CHECK(targetPaths.empty());
    CHECK(legacyTargetPath.empty());
    CHECK(dataDir == "/home/user/.local/share/yams");

    CHECK(legacyTargetPath.find("[Deprecated]") == std::string::npos);
    CHECK(dataDir.find("Data directory") == std::string::npos);
}

TEST_CASE("CLIOptionIsolation - Global option not polluted by subcommand descriptions", "[cli][option_isolation][catch2]") {
    CLI::App app("YAMS");
    app.prefix_command();

    std::string dataDir;
    app.add_option("--data-dir", dataDir, "Data directory for storage")
        ->default_val("/default/data");

    auto* addCmd = app.add_subcommand("add", "Add documents");

    std::string targetPath;
    addCmd->add_option("path", targetPath,
                       "[Deprecated] Single path to file/directory (use '-' for stdin)");

    std::vector<const char*> args = {"yams", "--data-dir", "/custom", "add", "somefile.txt"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    CHECK(dataDir == "/custom");
    CHECK(targetPath == "somefile.txt");

    CHECK(dataDir.find("[Deprecated]") == std::string::npos);
    CHECK(targetPath.find("Data directory") == std::string::npos);
}

// ============================================================================
// Value validation - ensure descriptions are never valid file paths
// ============================================================================

TEST_CASE("CLIOptionIsolation - Description strings are not valid paths", "[cli][option_isolation][catch2]") {
    std::vector<std::string> descriptions = {
        "[Deprecated] Single path to file/directory (use '-' for stdin)",
        "File or directory paths to add (use '-' for stdin). Accepts multiple.",
        "Name for the document (especially useful for stdin)", 
        "Data directory for storage"
    };

    for (const auto& desc : descriptions) {
        INFO("Checking description: " << desc);
        
        std::filesystem::path p(desc);

        bool hasSpecialChars =
            desc.find('[') != std::string::npos || desc.find('(') != std::string::npos;
        bool isLongDescription = desc.length() > 60;
        bool containsMultipleSpaces = std::count(desc.begin(), desc.end(), ' ') >= 2;
        bool hasDescriptionWords = desc.find("directory") != std::string::npos ||
                                   desc.find("file") != std::string::npos ||
                                   desc.find("path") != std::string::npos;

        CHECK((hasSpecialChars || isLongDescription || containsMultipleSpaces || hasDescriptionWords));
    }
}

TEST_CASE("CLIOptionIsolation - Empty value does not become description", "[cli][option_isolation][catch2]") {
    CLI::App app("Test");

    std::string value;
    auto* opt = app.add_option("--option", value, "This is the option description");

    std::vector<const char*> args = {"test"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    CHECK(value.empty());
    CHECK(opt->get_description() == "This is the option description");
    CHECK(value != opt->get_description());
}

// ============================================================================
// Multiple values with same description pattern
// ============================================================================

TEST_CASE("CLIOptionIsolation - Multiple options with similar descriptions", "[cli][option_isolation][catch2]") {
    CLI::App app("Test");

    std::string queryPath;
    std::string namePath;
    std::string deletePath;

    app.add_option("--query", queryPath, "Read query from file path (use '-' to read from STDIN)");
    app.add_option("--name", namePath, "Name of the document to retrieve (explicit flag form)");
    app.add_option("--delete", deletePath, "Delete multiple documents by names (comma-separated)");

    std::vector<const char*> args = {"test", "--query", "search.txt"};
    app.parse(static_cast<int>(args.size()), const_cast<char**>(args.data()));

    CHECK(queryPath == "search.txt");
    CHECK(namePath.empty());
    CHECK(deletePath.empty());

    CHECK(queryPath.find("Read query from") == std::string::npos);
    CHECK(namePath.find("Name of the document") == std::string::npos);
    CHECK(deletePath.find("Delete multiple") == std::string::npos);
}
