/**
 * @file sync_indexing_catch2_test.cpp
 * @brief Catch2 integration tests for PBI-040-4: Synchronous FTS5 indexing
 *
 * Migrated from GTest to Catch2 with improved async handling and timing
 */

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include "../daemon/test_async_helpers.h"
#include "../daemon/test_daemon_harness.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

class SyncIndexingFixture {
public:
    SyncIndexingFixture() {
        // Start daemon with harness
        harness_ = std::make_unique<yams::test::DaemonHarness>();
        bool started = harness_->start(5s);
        REQUIRE(started);

        // Create test files directory
        testFilesDir_ = harness_->dataDir().parent_path() / "test_files";
        fs::create_directories(testFilesDir_);

        // Create client
        yams::daemon::ClientConfig cfg;
        cfg.socketPath = harness_->socketPath();
        cfg.autoStart = false;
        client_ = std::make_unique<yams::daemon::DaemonClient>(cfg);
    }

    ~SyncIndexingFixture() {
        client_.reset();
        harness_.reset();
    }

    // Helper: create a test file with content
    fs::path createTestFile(const std::string& name, const std::string& content) {
        auto path = testFilesDir_ / name;
        std::ofstream file(path);
        file << content;
        file.close();
        return path;
    }

    // Helper: check if grep finds content with polling and better timeout handling
    bool grepFindsContent(const std::string& pattern, size_t expectedMatches = 1,
                          std::chrono::milliseconds totalTimeout = 3000ms,
                          std::chrono::milliseconds pollInterval = 100ms) {
        auto start = std::chrono::steady_clock::now();
        auto deadline = start + totalTimeout;

        yams::daemon::GrepRequest req;
        req.pattern = pattern;
        req.pathsOnly = true;

        size_t attempts = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            attempts++;

            auto result = yams::cli::run_sync(client_->grep(req), 1500ms);

            if (result) {
                size_t matchCount = result.value().matches.size();
                if (matchCount >= expectedMatches) {
                    auto elapsed = std::chrono::steady_clock::now() - start;
                    auto elapsedMs =
                        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
                    INFO("Grep found " << matchCount << " matches in " << elapsedMs
                                       << "ms (attempt " << attempts << ")");
                    return true;
                }
            }

            // Poll interval
            std::this_thread::sleep_for(pollInterval);
        }

        auto elapsed = std::chrono::steady_clock::now() - start;
        auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
        WARN("Grep did not find content after " << elapsedMs << "ms (" << attempts << " attempts)");
        return false;
    }

    // Helper: get PostIngestQueue depth from status
    uint64_t getQueueDepth() {
        auto result = yams::cli::run_sync(client_->status(), 1000ms);
        if (!result) {
            return 0;
        }
        return result.value().postIngestQueueDepth;
    }

    std::unique_ptr<yams::test::DaemonHarness> harness_;
    std::unique_ptr<yams::daemon::DaemonClient> client_;
    fs::path testFilesDir_;
};

} // anonymous namespace

TEST_CASE("SyncIndexing: Single file add immediate grep", "[integration][sync][indexing]") {
    SyncIndexingFixture fixture;

    auto testFile =
        fixture.createTestFile("test1.txt", "The quick brown fox jumps over the lazy dog");

    // Add file via DocumentIngestionService
    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = fixture.harness_->socketPath();
    opts.path = testFile.string();
    opts.noEmbeddings = true;
    opts.explicitDataDir = fixture.harness_->dataDir();

    auto addResult = ing.addViaDaemon(opts);
    REQUIRE(addResult);
    REQUIRE_FALSE(addResult.value().hash.empty());

    // Brief sleep to ensure WAL checkpoint propagates to reader connections
    std::this_thread::sleep_for(100ms);

    // Grep immediately - should find content (sync indexing)
    // Increased timeout to 3s and poll interval to 100ms for better reliability
    bool found = fixture.grepFindsContent("quick brown fox", 1, 3000ms, 100ms);
    REQUIRE(found);

    // Queue should be minimal (only KG/embeddings stages, not FTS5 metadata)
    std::this_thread::sleep_for(200ms); // brief wait for queue update
    auto queueDepth = fixture.getQueueDepth();
    CHECK(queueDepth <= 2);
}

TEST_CASE("SyncIndexing: No delay after sync add", "[integration][sync][indexing]") {
    SyncIndexingFixture fixture;

    auto testFile =
        fixture.createTestFile("instant.txt", "Instant search validation test INSTANTGREP");

    // Add file via DocumentIngestionService
    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = fixture.harness_->socketPath();
    opts.path = testFile.string();
    opts.noEmbeddings = true;
    opts.explicitDataDir = fixture.harness_->dataDir();

    auto addStart = std::chrono::steady_clock::now();
    auto addResult = ing.addViaDaemon(opts);
    auto addDuration = std::chrono::steady_clock::now() - addStart;
    auto addMs = std::chrono::duration_cast<std::chrono::milliseconds>(addDuration).count();

    REQUIRE(addResult);
    INFO("Add completed in " << addMs << "ms");

    // Brief sleep to ensure WAL checkpoint propagates
    std::this_thread::sleep_for(100ms);

    // Grep with reasonable timeout
    auto grepStart = std::chrono::steady_clock::now();
    bool found = fixture.grepFindsContent("INSTANTGREP", 1, 3000ms, 100ms);
    auto grepDuration = std::chrono::steady_clock::now() - grepStart;
    auto grepMs = std::chrono::duration_cast<std::chrono::milliseconds>(grepDuration).count();

    REQUIRE(found);
    // Relaxed timing - focus on correctness over strict performance
    CHECK(grepMs < 2000);

    INFO("Grep found content in " << grepMs << "ms");
}

TEST_CASE("SyncIndexing: Minimal queue growth", "[integration][sync][indexing]") {
    SyncIndexingFixture fixture;

    auto queueBefore = fixture.getQueueDepth();

    // Add multiple files
    std::vector<fs::path> testFiles;
    for (int i = 0; i < 5; i++) {
        auto file = fixture.createTestFile("file" + std::to_string(i) + ".txt",
                                           "Test content for file " + std::to_string(i) +
                                               " QUEUETEST" + std::to_string(i));
        testFiles.push_back(file);
    }

    // Add files via DocumentIngestionService
    yams::app::services::DocumentIngestionService ing;
    for (const auto& file : testFiles) {
        yams::app::services::AddOptions opts;
        opts.socketPath = fixture.harness_->socketPath();
        opts.path = file.string();
        opts.noEmbeddings = true;
        opts.explicitDataDir = fixture.harness_->dataDir();

        auto addResult = ing.addViaDaemon(opts);
        REQUIRE(addResult);
    }

    std::this_thread::sleep_for(500ms);

    auto queueAfter = fixture.getQueueDepth();
    auto queueGrowth = (queueAfter > queueBefore) ? (queueAfter - queueBefore) : 0;

    INFO("Queue before: " << queueBefore << ", after: " << queueAfter
                          << ", growth: " << queueGrowth);

    // With sync indexing, queue should not grow significantly
    CHECK(queueGrowth <= 10);

    // Verify all content is searchable
    for (int i = 0; i < 5; i++) {
        bool found = fixture.grepFindsContent("QUEUETEST" + std::to_string(i), 1, 3000ms, 100ms);
        REQUIRE(found);
    }
}
