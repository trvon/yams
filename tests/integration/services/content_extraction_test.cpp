/**
 * @file content_extraction_test.cpp
 * @brief Integration tests for content extraction and text indexing across file types
 *
 * This test suite validates:
 * - File type detection (MIME type and extension mapping)
 * - Text extraction from various file formats (.md, .cpp, .py, .js, .json, .toml, .hpp, .h)
 * - Content availability in list/grep commands
 * - FTS5 searchability of extracted text
 * - End-to-end pipeline: add -> extract -> index -> search
 *
 * Tests cover the full ingestion pipeline to catch issues where unit tests pass
 * but real-world content extraction fails.
 */

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <gtest/gtest.h>

#include "../daemon/test_async_helpers.h"
#include "../daemon/test_daemon_harness.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/detection/file_type_detector.h>

// Redefine SKIP_DAEMON_TEST_ON_WINDOWS for gtest (harness header uses Catch2's SKIP)
#ifdef _WIN32
#undef SKIP_DAEMON_TEST_ON_WINDOWS
#define SKIP_DAEMON_TEST_ON_WINDOWS()                                                              \
    GTEST_SKIP() << "Daemon IPC tests unstable on Windows - see windows-daemon-ipc-plan.md"
#endif

namespace fs = std::filesystem;
using namespace std::chrono_literals;

/**
 * @brief Test harness for content extraction integration tests
 */
class ContentExtractionIT : public ::testing::Test {
protected:
    std::unique_ptr<yams::test::DaemonHarness> harness_;
    std::unique_ptr<yams::daemon::DaemonClient> client_;
    fs::path testFilesDir_;

    void SetUp() override {
        // Skip on Windows - daemon IPC tests are unstable there
        SKIP_DAEMON_TEST_ON_WINDOWS();
        
        // Start daemon
        harness_ = std::make_unique<yams::test::DaemonHarness>();
        ASSERT_TRUE(harness_->start(5s)) << "Failed to start daemon";

        // Create test files directory
        testFilesDir_ = harness_->dataDir().parent_path() / "content_test_files";
        fs::create_directories(testFilesDir_);

        // Create client
        yams::daemon::ClientConfig cfg;
        cfg.socketPath = harness_->socketPath();
        cfg.autoStart = false;
        client_ = std::make_unique<yams::daemon::DaemonClient>(cfg);
    }

    void TearDown() override {
        client_.reset();
        harness_.reset();
        // Cleanup test files
        if (fs::exists(testFilesDir_)) {
            std::error_code ec;
            fs::remove_all(testFilesDir_, ec);
        }
    }

    /**
     * @brief Create a test file with given content and extension
     */
    fs::path createTestFile(const std::string& name, const std::string& content) {
        auto path = testFilesDir_ / name;
        std::ofstream file(path);
        EXPECT_TRUE(file.is_open()) << "Failed to create test file: " << path;
        file << content;
        file.close();
        return path;
    }

    /**
     * @brief Add a file through the daemon and return its hash
     */
    std::string addFile(const fs::path& path) {
        yams::app::services::DocumentIngestionService ing;
        yams::app::services::AddOptions opts;
        opts.socketPath = harness_->socketPath();
        opts.path = path.string();
        opts.noEmbeddings = true; // Faster tests
        opts.explicitDataDir = harness_->dataDir();

        auto result = ing.addViaDaemon(opts);
        EXPECT_TRUE(result) << "Failed to add file: " << result.error().message;
        if (!result) {
            return "";
        }
        return result.value().hash;
    }

    /**
     * @brief List documents and find one by name
     */
    std::optional<yams::daemon::ListEntry> findDocument(const std::string& name) {
        yams::daemon::ListRequest req;
        req.limit = 1000;
        req.showSnippets = true;
        req.snippetLength = 200;

        auto result = yams::cli::run_sync(client_->list(req), 3s);
        if (!result) {
            ADD_FAILURE() << "List request failed: " << result.error().message;
            return std::nullopt;
        }

        for (const auto& entry : result.value().items) {
            if (entry.name == name || entry.name.find(name) != std::string::npos) {
                return entry;
            }
        }
        return std::nullopt;
    }

    /**
     * @brief Search for content using grep
     */
    bool grepFindsContent(const std::string& pattern, size_t minMatches = 1,
                          std::chrono::milliseconds timeout = 3s) {
        auto deadline = std::chrono::steady_clock::now() + timeout;

        yams::daemon::GrepRequest req;
        req.pattern = pattern;
        req.pathsOnly = false;

        while (std::chrono::steady_clock::now() < deadline) {
            auto result = yams::cli::run_sync(client_->grep(req), 1s);

            if (result && result.value().matches.size() >= minMatches) {
                return true;
            }

            std::this_thread::sleep_for(100ms);
        }

        return false;
    }

    /**
     * @brief Search for content using hybrid search
     */
    bool searchFindsContent(const std::string& query, size_t minMatches = 1,
                            std::chrono::milliseconds timeout = 3s) {
        auto deadline = std::chrono::steady_clock::now() + timeout;

        yams::daemon::SearchRequest req;
        req.query = query;
        req.limit = 20;
        req.searchType = "keyword"; // Keyword search only for faster tests

        while (std::chrono::steady_clock::now() < deadline) {
            auto result = yams::cli::run_sync(client_->search(req), 1s);

            if (result && result.value().results.size() >= minMatches) {
                return true;
            }

            std::this_thread::sleep_for(100ms);
        }

        return false;
    }
};

/**
 * @brief Test: Markdown file content extraction and indexing
 */
TEST_F(ContentExtractionIT, MarkdownFileExtraction) {
    const std::string content = R"(# Test Document

This is a **markdown** document with various elements.

## Features
- Bullet points
- Multiple sections
- Code blocks

```cpp
int main() { return 0; }
```

Unique marker: MARKDOWN_EXTRACTION_TEST_12345
)";

    auto path = createTestFile("test_doc.md", content);
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    // Allow brief time for FTS5 commit
    std::this_thread::sleep_for(500ms);

    // Verify content in list output
    auto doc = findDocument("test_doc.md");
    ASSERT_TRUE(doc.has_value()) << "Document not found in list output";
    EXPECT_FALSE(doc->snippet.empty()) << "Snippet is empty";
    EXPECT_NE(doc->snippet, "[Content not available]") << "Content not extracted for markdown file";
    EXPECT_NE(doc->snippet, "[No text content]") << "Content marked as empty";

    // Verify FTS5 searchability
    EXPECT_TRUE(grepFindsContent("MARKDOWN_EXTRACTION_TEST"))
        << "Grep did not find markdown content";
    EXPECT_TRUE(searchFindsContent("markdown document")) << "Search did not find markdown content";
}

/**
 * @brief Test: C++ source file extraction (.cpp)
 */
TEST_F(ContentExtractionIT, CppSourceFileExtraction) {
    const std::string content = R"(#include <iostream>
#include <string>

namespace yams::test {

/**
 * @brief Test function for content extraction
 */
void testFunction() {
    std::cout << "CPP_EXTRACTION_MARKER_98765" << std::endl;
}

class TestClass {
public:
    void method() {
        // Method implementation
    }
};

} // namespace yams::test
)";

    auto path = createTestFile("test_source.cpp", content);
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    std::this_thread::sleep_for(500ms);

    auto doc = findDocument("test_source.cpp");
    ASSERT_TRUE(doc.has_value());
    EXPECT_FALSE(doc->snippet.empty());
    EXPECT_NE(doc->snippet, "[Content not available]") << "Content not extracted for C++ file";

    EXPECT_TRUE(grepFindsContent("CPP_EXTRACTION_MARKER")) << "Grep did not find C++ content";
    EXPECT_TRUE(searchFindsContent("testFunction")) << "Search did not find C++ content";
}

/**
 * @brief Test: C++ header file extraction (.hpp)
 */
TEST_F(ContentExtractionIT, CppHeaderFileExtraction) {
    const std::string content = R"(#pragma once

#include <memory>
#include <string>

namespace yams::extraction {

/**
 * @brief Header test class
 * Unique marker: HPP_HEADER_MARKER_11223
 */
class HeaderTestClass {
public:
    virtual ~HeaderTestClass() = default;
    virtual void process() = 0;
};

} // namespace yams::extraction
)";

    auto path = createTestFile("test_header.hpp", content);
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    std::this_thread::sleep_for(500ms);

    auto doc = findDocument("test_header.hpp");
    ASSERT_TRUE(doc.has_value());
    EXPECT_NE(doc->snippet, "[Content not available]") << "Content not extracted for HPP file";

    EXPECT_TRUE(grepFindsContent("HPP_HEADER_MARKER")) << "Grep did not find HPP content";
}

/**
 * @brief Test: Python file extraction
 */
TEST_F(ContentExtractionIT, PythonFileExtraction) {
    const std::string content = R"(#!/usr/bin/env python3
"""
Python test module
Marker: PYTHON_EXTRACTION_MARKER_44556
"""

def test_function():
    """Test function for extraction"""
    print("Hello from Python")
    return True

class TestClass:
    def __init__(self):
        self.value = 42
)";

    auto path = createTestFile("test_script.py", content);
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    std::this_thread::sleep_for(500ms);

    auto doc = findDocument("test_script.py");
    ASSERT_TRUE(doc.has_value());
    EXPECT_NE(doc->snippet, "[Content not available]") << "Content not extracted for Python file";

    EXPECT_TRUE(grepFindsContent("PYTHON_EXTRACTION_MARKER")) << "Grep did not find Python content";
}

/**
 * @brief Test: JavaScript file extraction
 */
TEST_F(ContentExtractionIT, JavaScriptFileExtraction) {
    const std::string content = R"(// JavaScript test file
// Marker: JAVASCRIPT_EXTRACTION_MARKER_77889

const testFunction = () => {
    console.log("JavaScript test");
    return true;
};

class TestClass {
    constructor() {
        this.value = 42;
    }
    
    method() {
        console.log("Method called");
    }
}

export { testFunction, TestClass };
)";

    auto path = createTestFile("test_module.js", content);
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    std::this_thread::sleep_for(500ms);

    auto doc = findDocument("test_module.js");
    ASSERT_TRUE(doc.has_value());
    EXPECT_NE(doc->snippet, "[Content not available]")
        << "Content not extracted for JavaScript file";

    EXPECT_TRUE(grepFindsContent("JAVASCRIPT_EXTRACTION_MARKER"))
        << "Grep did not find JavaScript content";
}

/**
 * @brief Test: JSON file extraction
 */
TEST_F(ContentExtractionIT, JsonFileExtraction) {
    const std::string content = R"({
    "name": "test-package",
    "version": "1.0.0",
    "description": "JSON_EXTRACTION_MARKER_33221",
    "dependencies": {
        "test-lib": "^2.0.0"
    },
    "scripts": {
        "test": "jest"
    }
}
)";

    auto path = createTestFile("package.json", content);
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    std::this_thread::sleep_for(500ms);

    auto doc = findDocument("package.json");
    ASSERT_TRUE(doc.has_value());
    EXPECT_NE(doc->snippet, "[Content not available]") << "Content not extracted for JSON file";

    EXPECT_TRUE(grepFindsContent("JSON_EXTRACTION_MARKER")) << "Grep did not find JSON content";
}

/**
 * @brief Test: TOML file extraction
 */
TEST_F(ContentExtractionIT, TomlFileExtraction) {
    const std::string content = R"([package]
name = "test-project"
version = "0.1.0"
description = "TOML_EXTRACTION_MARKER_55443"
authors = ["Test Author"]

[dependencies]
serde = "1.0"
tokio = { version = "1.0", features = ["full"] }

[dev-dependencies]
criterion = "0.5"
)";

    auto path = createTestFile("Cargo.toml", content);
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    std::this_thread::sleep_for(500ms);

    auto doc = findDocument("Cargo.toml");
    ASSERT_TRUE(doc.has_value());
    EXPECT_NE(doc->snippet, "[Content not available]") << "Content not extracted for TOML file";

    EXPECT_TRUE(grepFindsContent("TOML_EXTRACTION_MARKER")) << "Grep did not find TOML content";
}

/**
 * @brief Test: YAML file extraction
 */
TEST_F(ContentExtractionIT, YamlFileExtraction) {
    const std::string content = R"(name: test-workflow
on:
  push:
    branches: [ main ]
    
jobs:
  build:
    runs-on: ubuntu-latest
    # YAML_EXTRACTION_MARKER_66778
    steps:
      - uses: actions/checkout@v3
      - name: Build
        run: make test
)";

    auto path = createTestFile("workflow.yml", content);
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    std::this_thread::sleep_for(500ms);

    auto doc = findDocument("workflow.yml");
    ASSERT_TRUE(doc.has_value());
    EXPECT_NE(doc->snippet, "[Content not available]") << "Content not extracted for YAML file";

    EXPECT_TRUE(grepFindsContent("YAML_EXTRACTION_MARKER")) << "Grep did not find YAML content";
}

/**
 * @brief Test: Multiple file types in batch
 * Validates that bulk indexing correctly handles diverse file types
 */
TEST_F(ContentExtractionIT, MultipleFileTypesBatch) {
    // Create various file types
    std::vector<fs::path> paths;
    paths.push_back(createTestFile("batch1.md", "# Batch test BATCH_MD_MARKER"));
    paths.push_back(createTestFile("batch2.cpp", "// Batch C++ BATCH_CPP_MARKER\nint main() {}"));
    paths.push_back(createTestFile("batch3.py", "# Batch Python BATCH_PY_MARKER\nprint('test')"));
    paths.push_back(createTestFile("batch4.json", R"({"marker": "BATCH_JSON_MARKER"})"));
    paths.push_back(createTestFile("batch5.h", "// Batch header BATCH_H_MARKER\n#pragma once"));

    // Add all files
    std::vector<std::string> hashes;
    for (const auto& path : paths) {
        std::string hash = addFile(path);
        ASSERT_FALSE(hash.empty()) << "Failed to add: " << path;
        hashes.push_back(hash);
    }

    std::this_thread::sleep_for(1s); // Allow time for batch indexing

    // Verify all files are searchable
    EXPECT_TRUE(grepFindsContent("BATCH_MD_MARKER")) << "Markdown not indexed";
    EXPECT_TRUE(grepFindsContent("BATCH_CPP_MARKER")) << "C++ not indexed";
    EXPECT_TRUE(grepFindsContent("BATCH_PY_MARKER")) << "Python not indexed";
    EXPECT_TRUE(grepFindsContent("BATCH_JSON_MARKER")) << "JSON not indexed";
    EXPECT_TRUE(grepFindsContent("BATCH_H_MARKER")) << "Header not indexed";

    // Verify all files have content in list
    for (size_t i = 0; i < paths.size(); ++i) {
        auto doc = findDocument(paths[i].filename().string());
        ASSERT_TRUE(doc.has_value()) << "Document not found: " << paths[i].filename();
        EXPECT_NE(doc->snippet, "[Content not available]")
            << "Content missing for: " << paths[i].filename();
    }
}

/**
 * @brief Test: File type detection correctness
 * Validates that FileTypeDetector correctly identifies MIME types
 */
TEST_F(ContentExtractionIT, FileTypeDetection) {
    auto& detector = yams::detection::FileTypeDetector::instance();

    // Test extension-based detection
    EXPECT_EQ(detector.getMimeTypeFromExtension(".md"), "text/markdown");
    EXPECT_EQ(detector.getMimeTypeFromExtension(".cpp"), "text/x-c++src");
    EXPECT_EQ(detector.getMimeTypeFromExtension(".hpp"), "text/x-c++hdr");
    EXPECT_EQ(detector.getMimeTypeFromExtension(".py"), "text/x-python");
    EXPECT_EQ(detector.getMimeTypeFromExtension(".js"), "text/javascript");
    EXPECT_EQ(detector.getMimeTypeFromExtension(".json"), "application/json");
    EXPECT_EQ(detector.getMimeTypeFromExtension(".toml"), "application/toml");
    EXPECT_EQ(detector.getMimeTypeFromExtension(".yml"), "text/x-yaml");

    // Test text MIME type identification
    EXPECT_TRUE(detector.isTextMimeType("text/markdown"));
    EXPECT_TRUE(detector.isTextMimeType("text/x-c++src"));
    EXPECT_TRUE(detector.isTextMimeType("text/x-python"));
    EXPECT_TRUE(detector.isTextMimeType("text/javascript"));
    EXPECT_TRUE(detector.isTextMimeType("application/json"));
    EXPECT_TRUE(detector.isTextMimeType("application/toml"));
    EXPECT_TRUE(detector.isTextMimeType("text/x-yaml"));

    EXPECT_FALSE(detector.isTextMimeType("image/jpeg"));
    EXPECT_FALSE(detector.isTextMimeType("application/octet-stream"));
}

/**
 * @brief Test: Empty file handling
 */
TEST_F(ContentExtractionIT, EmptyFileHandling) {
    auto path = createTestFile("empty.txt", "");
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    std::this_thread::sleep_for(500ms);

    auto doc = findDocument("empty.txt");
    ASSERT_TRUE(doc.has_value());
    // Empty files should not show "Content not available"
    EXPECT_NE(doc->snippet, "[Content not available]");
}

/**
 * @brief Test: Large text file extraction
 * Validates that large files are properly extracted (up to configured limits)
 */
TEST_F(ContentExtractionIT, LargeFileExtraction) {
    // Create a ~100KB text file
    std::string content;
    content.reserve(100000);
    for (int i = 0; i < 1000; ++i) {
        content += "Line " + std::to_string(i) +
                   ": This is a test line with content LARGE_FILE_MARKER " + std::to_string(i) +
                   "\n";
    }

    auto path = createTestFile("large.txt", content);
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    std::this_thread::sleep_for(1s);

    auto doc = findDocument("large.txt");
    ASSERT_TRUE(doc.has_value());
    EXPECT_NE(doc->snippet, "[Content not available]") << "Large file content not extracted";

    EXPECT_TRUE(grepFindsContent("LARGE_FILE_MARKER")) << "Large file not searchable";
}

/**
 * @brief Test: TypeScript file extraction
 */
TEST_F(ContentExtractionIT, TypeScriptFileExtraction) {
    const std::string content = R"(// TypeScript test file
// Marker: TYPESCRIPT_EXTRACTION_MARKER_88990

interface TestInterface {
    value: number;
    method(): void;
}

class TestClass implements TestInterface {
    value: number = 42;
    
    method(): void {
        console.log("TypeScript method");
    }
}

export { TestInterface, TestClass };
)";

    auto path = createTestFile("test_module.ts", content);
    std::string hash = addFile(path);
    ASSERT_FALSE(hash.empty());

    std::this_thread::sleep_for(500ms);

    auto doc = findDocument("test_module.ts");
    ASSERT_TRUE(doc.has_value());
    EXPECT_NE(doc->snippet, "[Content not available]")
        << "Content not extracted for TypeScript file";

    EXPECT_TRUE(grepFindsContent("TYPESCRIPT_EXTRACTION_MARKER"))
        << "Grep did not find TypeScript content";
}
