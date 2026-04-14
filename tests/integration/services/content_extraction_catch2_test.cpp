#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../daemon/test_daemon_harness.h"
#include "../../common/test_helpers_catch2.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/detection/file_type_detector.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

#ifdef _WIN32
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN()                                                          \
    SKIP("Windows daemon shutdown hangs - see windows-daemon-ipc-plan.md")
#else
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN() ((void)0)
#endif

namespace {

using yams::test::ScopedEnvVar;

class ContentExtractionFixture {
public:
    ContentExtractionFixture() {
        SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();

        yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
        yams::daemon::GlobalIOContext::reset();

        envGuards_.push_back(std::make_unique<ScopedEnvVar>("YAMS_DB_DUAL_POOL",
                                                            std::make_optional<std::string>("0")));
        envGuards_.push_back(std::make_unique<ScopedEnvVar>("YAMS_DB_POOL_MIN",
                                                            std::make_optional<std::string>("1")));
        envGuards_.push_back(std::make_unique<ScopedEnvVar>("YAMS_DB_POOL_MAX",
                                                            std::make_optional<std::string>("4")));

        yams::test::DaemonHarnessOptions options;
        options.isolateState = true;
        options.skipSocketVerificationOnReady = true;
        harness_ = std::make_unique<yams::test::DaemonHarness>(options);
        REQUIRE(harness_->start(10s));

        testFilesDir_ = harness_->dataDir().parent_path() / "content_test_files";
        fs::create_directories(testFilesDir_);

        yams::daemon::ClientConfig cfg;
        cfg.socketPath = harness_->socketPath();
        cfg.autoStart = false;
        client_ = std::make_unique<yams::daemon::DaemonClient>(cfg);
    }

    ~ContentExtractionFixture() {
        client_.reset();
        harness_.reset();
        if (fs::exists(testFilesDir_)) {
            std::error_code ec;
            fs::remove_all(testFilesDir_, ec);
        }
        envGuards_.clear();
        yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
        yams::daemon::GlobalIOContext::reset();
    }

    fs::path createTestFile(const std::string& name, const std::string& content) {
        auto path = testFilesDir_ / name;
        std::ofstream file(path);
        REQUIRE(file.is_open());
        file << content;
        file.close();
        return path;
    }

    std::string addFile(const fs::path& path) {
        yams::app::services::DocumentIngestionService ing;
        yams::app::services::AddOptions opts;
        opts.socketPath = harness_->socketPath();
        opts.path = path.string();
        opts.noEmbeddings = true;
        opts.explicitDataDir = harness_->dataDir();
        opts.waitForProcessing = true;
        opts.waitTimeoutSeconds = 10;

        auto result = ing.addViaDaemon(opts);
        REQUIRE(result);
        return result.value().hash;
    }

    std::optional<yams::daemon::GetResponse> getDocument(const std::string& name,
                                                         bool requireContent = true,
                                                         std::chrono::milliseconds timeout = 10s) {
        auto deadline = std::chrono::steady_clock::now() + timeout;

        yams::daemon::GetRequest req;
        req.name = name;
        req.byName = true;
        req.metadataOnly = false;

        while (std::chrono::steady_clock::now() < deadline) {
            auto result = yams::cli::run_sync(client_->get(req), 3s);
            if (result && !result.value().name.empty()) {
                if (!requireContent ||
                    (result.value().hasContent && !result.value().content.empty())) {
                    return result.value();
                }
            }
            std::this_thread::sleep_for(100ms);
        }
        return std::nullopt;
    }

    bool grepFindsContent(const std::string& pattern, size_t minMatches = 1,
                          std::chrono::milliseconds timeout = 8s) {
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

    bool searchFindsContent(const std::string& query, size_t minMatches = 1,
                            std::chrono::milliseconds timeout = 8s) {
        auto deadline = std::chrono::steady_clock::now() + timeout;

        yams::daemon::SearchRequest req;
        req.query = query;
        req.limit = 20;
        req.searchType = "keyword";

        while (std::chrono::steady_clock::now() < deadline) {
            auto result = yams::cli::run_sync(client_->search(req), 1s);
            if (result && result.value().results.size() >= minMatches) {
                return true;
            }
            std::this_thread::sleep_for(100ms);
        }

        return false;
    }

private:
    std::unique_ptr<yams::test::DaemonHarness> harness_;
    std::unique_ptr<yams::daemon::DaemonClient> client_;
    fs::path testFilesDir_;
    std::vector<std::unique_ptr<ScopedEnvVar>> envGuards_;
};

} // namespace

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: markdown file extraction",
                 "[catch2][integration][services][content]") {
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
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(500ms);

    auto doc = getDocument("test_doc.md");
    REQUIRE(doc.has_value());
    CHECK(doc->content.find("MARKDOWN_EXTRACTION_TEST_12345") != std::string::npos);
    CHECK(grepFindsContent("MARKDOWN_EXTRACTION_TEST"));
    CHECK(searchFindsContent("markdown document"));
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: cpp source extraction",
                 "[catch2][integration][services][content]") {
    const std::string content = R"(#include <iostream>
#include <string>

namespace yams::test {

void testFunction() {
    std::cout << "CPP_EXTRACTION_MARKER_98765" << std::endl;
}

class TestClass {
public:
    void method() {
    }
};

} // namespace yams::test
)";

    auto path = createTestFile("test_source.cpp", content);
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(500ms);

    auto doc = getDocument("test_source.cpp");
    REQUIRE(doc.has_value());
    CHECK(doc->content.find("CPP_EXTRACTION_MARKER_98765") != std::string::npos);
    CHECK(grepFindsContent("CPP_EXTRACTION_MARKER"));
    CHECK(searchFindsContent("testFunction"));
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: cpp header extraction",
                 "[catch2][integration][services][content]") {
    const std::string content = R"(#pragma once

#include <memory>
#include <string>

namespace yams::extraction {

class HeaderTestClass {
public:
    // Unique marker: HPP_HEADER_MARKER_11223
    virtual ~HeaderTestClass() = default;
    virtual void process() = 0;
};

} // namespace yams::extraction
)";

    auto path = createTestFile("test_header.hpp", content);
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(500ms);

    auto doc = getDocument("test_header.hpp");
    REQUIRE(doc.has_value());
    CHECK(doc->content.find("HPP_HEADER_MARKER_11223") != std::string::npos);
    CHECK(grepFindsContent("HPP_HEADER_MARKER"));
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: python extraction",
                 "[catch2][integration][services][content]") {
    const std::string content = R"(#!/usr/bin/env python3
"""
Python test module
Marker: PYTHON_EXTRACTION_MARKER_44556
"""

def test_function():
    print("Hello from Python")
    return True
)";

    auto path = createTestFile("test_script.py", content);
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(500ms);

    auto doc = getDocument("test_script.py");
    REQUIRE(doc.has_value());
    CHECK(doc->content.find("PYTHON_EXTRACTION_MARKER_44556") != std::string::npos);
    CHECK(grepFindsContent("PYTHON_EXTRACTION_MARKER"));
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: javascript extraction",
                 "[catch2][integration][services][content]") {
    const std::string content = R"(// JavaScript test file
// Marker: JAVASCRIPT_EXTRACTION_MARKER_77889

const testFunction = () => {
    console.log("JavaScript test");
    return true;
};
)";

    auto path = createTestFile("test_module.js", content);
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(500ms);

    auto doc = getDocument("test_module.js");
    REQUIRE(doc.has_value());
    CHECK(doc->content.find("JAVASCRIPT_EXTRACTION_MARKER_77889") != std::string::npos);
    CHECK(grepFindsContent("JAVASCRIPT_EXTRACTION_MARKER"));
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: json extraction",
                 "[catch2][integration][services][content]") {
    const std::string content = R"({
    "name": "test-package",
    "description": "JSON_EXTRACTION_MARKER_33221"
}
)";

    auto path = createTestFile("package.json", content);
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(500ms);

    auto doc = getDocument("package.json");
    REQUIRE(doc.has_value());
    CHECK(doc->content.find("JSON_EXTRACTION_MARKER_33221") != std::string::npos);
    CHECK(grepFindsContent("JSON_EXTRACTION_MARKER"));
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: toml extraction",
                 "[catch2][integration][services][content]") {
    const std::string content = R"([package]
name = "test-project"
description = "TOML_EXTRACTION_MARKER_55443"
)";

    auto path = createTestFile("Cargo.toml", content);
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(500ms);

    auto doc = getDocument("Cargo.toml");
    REQUIRE(doc.has_value());
    CHECK(doc->content.find("TOML_EXTRACTION_MARKER_55443") != std::string::npos);
    CHECK(grepFindsContent("TOML_EXTRACTION_MARKER"));
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: yaml extraction",
                 "[catch2][integration][services][content]") {
    const std::string content = R"(name: test-workflow
jobs:
  build:
    # YAML_EXTRACTION_MARKER_66778
    runs-on: ubuntu-latest
)";

    auto path = createTestFile("workflow.yml", content);
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(500ms);

    auto doc = getDocument("workflow.yml");
    REQUIRE(doc.has_value());
    CHECK(doc->content.find("YAML_EXTRACTION_MARKER_66778") != std::string::npos);
    CHECK(grepFindsContent("YAML_EXTRACTION_MARKER"));
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: multiple file types batch",
                 "[catch2][integration][services][content]") {
    std::vector<fs::path> paths;
    paths.push_back(createTestFile("batch1.md", "# Batch test BATCH_MD_MARKER"));
    paths.push_back(createTestFile("batch2.cpp", "// Batch C++ BATCH_CPP_MARKER\nint main() {}"));
    paths.push_back(createTestFile("batch3.py", "# Batch Python BATCH_PY_MARKER\nprint('test')"));
    paths.push_back(createTestFile("batch4.json", R"({"marker": "BATCH_JSON_MARKER"})"));
    paths.push_back(createTestFile("batch5.h", "// Batch header BATCH_H_MARKER\n#pragma once"));

    for (const auto& path : paths) {
        auto hash = addFile(path);
        REQUIRE_FALSE(hash.empty());
    }

    std::this_thread::sleep_for(1s);

    CHECK(grepFindsContent("BATCH_MD_MARKER"));
    CHECK(grepFindsContent("BATCH_CPP_MARKER"));
    CHECK(grepFindsContent("BATCH_PY_MARKER"));
    CHECK(grepFindsContent("BATCH_JSON_MARKER"));
    CHECK(grepFindsContent("BATCH_H_MARKER"));

    for (const auto& path : paths) {
        auto doc = getDocument(path.filename().string());
        REQUIRE(doc.has_value());
        CHECK_FALSE(doc->content.empty());
    }
}

TEST_CASE("ContentExtraction: file type detection",
          "[catch2][integration][services][content][detector]") {
    auto& detector = yams::detection::FileTypeDetector::instance();

    CHECK(detector.getMimeTypeFromExtension(".md") == "text/markdown");
    CHECK(detector.getMimeTypeFromExtension(".cpp") == "text/x-c++");
    CHECK(detector.getMimeTypeFromExtension(".hpp") == "text/x-c++");
    CHECK(detector.getMimeTypeFromExtension(".py") == "text/x-python");
    CHECK(detector.getMimeTypeFromExtension(".js") == "application/javascript");
    CHECK(detector.getMimeTypeFromExtension(".json") == "application/json");
    CHECK(detector.getMimeTypeFromExtension(".yml") == "application/x-yaml");

    CHECK(detector.isTextMimeType("text/markdown"));
    CHECK(detector.isTextMimeType("text/x-c++"));
    CHECK(detector.isTextMimeType("text/x-python"));
    CHECK(detector.isTextMimeType("application/javascript"));
    CHECK(detector.isTextMimeType("application/json"));
    CHECK(detector.isTextMimeType("application/x-yaml"));

    CHECK_FALSE(detector.isTextMimeType("image/jpeg"));
    CHECK_FALSE(detector.isTextMimeType("application/octet-stream"));
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: empty file handling",
                 "[catch2][integration][services][content]") {
    auto path = createTestFile("empty.txt", "");
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(500ms);

    auto doc = getDocument("empty.txt", false);
    REQUIRE(doc.has_value());
    CHECK(doc->content.empty());
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: large file extraction",
                 "[catch2][integration][services][content]") {
    std::string content;
    content.reserve(100000);
    for (int i = 0; i < 1000; ++i) {
        content += "Line " + std::to_string(i) +
                   ": This is a test line with content LARGE_FILE_MARKER " + std::to_string(i) +
                   "\n";
    }

    auto path = createTestFile("large.txt", content);
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(1s);

    auto doc = getDocument("large.txt");
    REQUIRE(doc.has_value());
    CHECK(doc->content.find("LARGE_FILE_MARKER") != std::string::npos);
    CHECK(grepFindsContent("LARGE_FILE_MARKER"));
}

TEST_CASE_METHOD(ContentExtractionFixture, "ContentExtraction: typescript extraction",
                 "[catch2][integration][services][content]") {
    const std::string content = R"(// TypeScript test file
// Marker: TYPESCRIPT_EXTRACTION_MARKER_88990

interface TestInterface {
    value: number;
}

class TestClass implements TestInterface {
    value: number = 42;
}
)";

    auto path = createTestFile("test_module.ts", content);
    auto hash = addFile(path);
    REQUIRE_FALSE(hash.empty());
    std::this_thread::sleep_for(500ms);

    auto doc = getDocument("test_module.ts");
    REQUIRE(doc.has_value());
    CHECK(doc->content.find("TYPESCRIPT_EXTRACTION_MARKER_88990") != std::string::npos);
    CHECK(grepFindsContent("TYPESCRIPT_EXTRACTION_MARKER"));
}
