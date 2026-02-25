/**
 * @file sync_indexing_test.cpp
 * @brief Integration tests for PBI-040-4: Synchronous FTS5 indexing for small adds
 *
 * This test validates that small document adds (≤10 files) perform FTS5 indexing
 * synchronously, making grep results immediately available without the 5-10s
 * async indexing delay.
 */

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <gtest/gtest.h>

#include "../daemon/test_async_helpers.h"
#include "../daemon/test_daemon_harness.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

// Redefine SKIP_DAEMON_TEST_ON_WINDOWS for gtest (harness header uses Catch2's SKIP)
#ifdef _WIN32
#undef SKIP_DAEMON_TEST_ON_WINDOWS
#define SKIP_DAEMON_TEST_ON_WINDOWS()                                                              \
    GTEST_SKIP() << "Daemon IPC tests unstable on Windows - see windows-daemon-ipc-plan.md"
#endif

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class SyncIndexingIT : public ::testing::Test {
protected:
    std::unique_ptr<yams::test::DaemonHarness> harness_;
    std::unique_ptr<yams::daemon::DaemonClient> client_;
    fs::path testFilesDir_;

    void SetUp() override {
        // Skip on Windows - daemon IPC tests are unstable there
        SKIP_DAEMON_TEST_ON_WINDOWS();

        // Start daemon with harness
        harness_ = std::make_unique<yams::test::DaemonHarness>();
        ASSERT_TRUE(harness_->start(5s)) << "Failed to start daemon";

        // Create test files directory
        testFilesDir_ = harness_->dataDir().parent_path() / "test_files";
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
    }

    // Helper: create a test file with content
    fs::path createTestFile(const std::string& name, const std::string& content) {
        auto path = testFilesDir_ / name;
        std::ofstream file(path);
        file << content;
        file.close();
        return path;
    }

    // Helper: check if grep finds content (with brief retry for FTS5 commit latency)
    // Even with sync indexing, SQLite FTS5 commits may take a few milliseconds
    bool grepFindsContent(const std::string& pattern, size_t expectedMatches = 1,
                          std::chrono::milliseconds totalTimeout = 2000ms) {
        auto start = std::chrono::steady_clock::now();
        auto deadline = start + totalTimeout;

        yams::daemon::GrepRequest req;
        req.pattern = pattern;
        req.pathsOnly = true;

        while (std::chrono::steady_clock::now() < deadline) {
            auto result = yams::cli::run_sync(client_->grep(req), 1000ms);

            if (result) {
                size_t matchCount = result.value().matches.size();
                if (matchCount >= expectedMatches) {
                    auto elapsed = std::chrono::steady_clock::now() - start;
                    auto elapsedMs =
                        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
                    std::cout << "Grep found " << matchCount << " matches in " << elapsedMs << "ms"
                              << std::endl;
                    return true;
                }
            }

            // Brief retry delay (FTS5 commit latency)
            std::this_thread::sleep_for(50ms);
        }

        std::cerr << "Grep did not find content after "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(totalTimeout).count()
                  << "ms" << std::endl;
        return false;
    }

    // Helper: get PostIngestQueue depth from status
    uint64_t getQueueDepth() {
        auto result = yams::cli::run_sync(client_->status(), 2000ms);
        if (!result) {
            return 0;
        }
        return result.value().postIngestQueueDepth;
    }
};

/**
 * Test: Single file add + immediate grep
 * Expected: FTS5 indexing is synchronous, grep finds content immediately
 */
TEST_F(SyncIndexingIT, SingleFileAddImmediateGrep) {
    // Create test file with unique content
    auto testFile = createTestFile("test1.txt", "The quick brown fox jumps over the lazy dog");

    // Add file via DocumentIngestionService
    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = harness_->socketPath();
    opts.path = testFile.string();
    opts.noEmbeddings = true;
    opts.explicitDataDir = harness_->dataDir();

    auto addResult = ing.addViaDaemon(opts);
    ASSERT_TRUE(addResult) << addResult.error().message;
    ASSERT_FALSE(addResult.value().hash.empty());

    // Grep immediately - should find content (sync indexing)
    EXPECT_TRUE(grepFindsContent("quick brown fox", 1))
        << "Sync indexing failed: grep did not find content immediately after single file add";

    // Queue should be minimal (only KG/embeddings stages, not FTS5 metadata)
    std::this_thread::sleep_for(200ms); // brief wait for queue update
    auto queueDepth = getQueueDepth();
    EXPECT_LE(queueDepth, 2u) << "Queue depth too high after sync add: " << queueDepth;
}

/**
 * Test: Content immediately searchable after sync add
 * Expected: No delay, results available within milliseconds
 */
TEST_F(SyncIndexingIT, NoDelayAfterSyncAdd) {
    auto testFile = createTestFile("instant.txt", "Instant search validation test INSTANTGREP");

    // Add file via DocumentIngestionService
    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = harness_->socketPath();
    opts.path = testFile.string();
    opts.noEmbeddings = true;
    opts.explicitDataDir = harness_->dataDir();

    auto addStart = std::chrono::steady_clock::now();
    auto addResult = ing.addViaDaemon(opts);
    auto addDuration = std::chrono::steady_clock::now() - addStart;
    auto addMs = std::chrono::duration_cast<std::chrono::milliseconds>(addDuration).count();

    ASSERT_TRUE(addResult) << addResult.error().message;
    std::cout << "Add completed in " << addMs << "ms" << std::endl;

    // Grep immediately (no sleep/wait)
    auto grepStart = std::chrono::steady_clock::now();
    bool found = grepFindsContent("INSTANTGREP", 1);
    auto grepDuration = std::chrono::steady_clock::now() - grepStart;
    auto grepMs = std::chrono::duration_cast<std::chrono::milliseconds>(grepDuration).count();

    EXPECT_TRUE(found) << "Content not found immediately";
    EXPECT_LT(grepMs, 1000) << "Grep took too long: " << grepMs << "ms (expected < 1000ms)";

    std::cout << "Grep found content in " << grepMs << "ms (sync indexing working)" << std::endl;
}

/**
 * Test: Verify queue depth behavior with sync indexing
 * Expected: Queue growth is minimal since FTS5 is done synchronously
 */
TEST_F(SyncIndexingIT, MinimalQueueGrowthWithSyncAdd) {
    auto queueBefore = getQueueDepth();

    auto testFile = createTestFile("logtest.txt", "Log validation test SYNCLOG");

    // Add file
    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = harness_->socketPath();
    opts.path = testFile.string();
    opts.noEmbeddings = true;
    opts.explicitDataDir = harness_->dataDir();

    auto addResult = ing.addViaDaemon(opts);
    ASSERT_TRUE(addResult) << addResult.error().message;

    // Wait a bit for async KG/embedding stages
    std::this_thread::sleep_for(200ms);

    auto queueAfter = getQueueDepth();

    // Queue growth should be minimal (only KG/embeddings, not FTS5 metadata)
    // With sync indexing, we skip the metadata stage in the queue
    uint64_t queueGrowth = (queueAfter > queueBefore) ? (queueAfter - queueBefore) : 0;
    EXPECT_LE(queueGrowth, 2u) << "Queue grew too much (" << queueGrowth
                               << "), suggesting async path was used instead of sync";

    // Content should be immediately searchable
    EXPECT_TRUE(grepFindsContent("SYNCLOG", 1));
}
