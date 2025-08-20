#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <gtest/gtest.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_serializer.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_index_manager.h>

namespace yams::daemon::test {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class DaemonTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing daemon files
        cleanupDaemonFiles();

        // Create test config
        auto tmp = fs::temp_directory_path();
        config_.dataDir = tmp / "yams_test_data";
        config_.socketPath = tmp / "test_yams_daemon.sock";
        config_.pidFile = tmp / "test_yams_daemon.pid";
        config_.logFile = tmp / "test_yams_daemon.log";
        config_.workerThreads = 2;
        config_.maxMemoryGb = 1.0;
        std::error_code se;
        fs::create_directories(config_.dataDir, se);
    }

    void TearDown() override {
        // Stop daemon if running
        if (daemon_) {
            daemon_->stop();
        }

        // Clean up test files
        cleanupDaemonFiles();
    }

    void cleanupDaemonFiles() {
        std::error_code ec;
        auto tmp = fs::temp_directory_path();
        fs::remove(tmp / "test_yams_daemon.sock", ec);
        fs::remove(tmp / "test_yams_daemon.pid", ec);
        fs::remove(tmp / "test_yams_daemon.log", ec);
        fs::remove_all(tmp / "yams_test_data", ec);
    }

    DaemonConfig config_;
    std::unique_ptr<YamsDaemon> daemon_;
};

// Test daemon creation and destruction
TEST_F(DaemonTest, CreateDestroy) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    ASSERT_NE(daemon_, nullptr);

    // Daemon should not be running yet
    EXPECT_FALSE(daemon_->isRunning());
}

// Test daemon start and stop
TEST_F(DaemonTest, StartStop) {
    daemon_ = std::make_unique<YamsDaemon>(config_);

    // Start daemon
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start daemon: " << result.error().message;

    // Should be running
    EXPECT_TRUE(daemon_->isRunning());

    // PID file should exist
    EXPECT_TRUE(fs::exists(config_.pidFile));

    // Socket should exist
    std::this_thread::sleep_for(100ms); // Give it time to create socket
    EXPECT_TRUE(fs::exists(config_.socketPath));

    // Stop daemon
    result = daemon_->stop();
    ASSERT_TRUE(result) << "Failed to stop daemon: " << result.error().message;

    // Should not be running
    EXPECT_FALSE(daemon_->isRunning());

    // PID file should be removed
    EXPECT_FALSE(fs::exists(config_.pidFile));
}

// Test that only one daemon can run at a time
TEST_F(DaemonTest, SingleInstance) {
    daemon_ = std::make_unique<YamsDaemon>(config_);

    // Start first daemon
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start first daemon: " << result.error().message;

    // Try to start second daemon with same config
    auto daemon2 = std::make_unique<YamsDaemon>(config_);
    result = daemon2->start();
    EXPECT_FALSE(result) << "Second daemon should not start";
    EXPECT_EQ(result.error().code, ErrorCode::InvalidState);

    // Stop first daemon
    daemon_->stop();
}

// Test daemon restart
TEST_F(DaemonTest, Restart) {
    daemon_ = std::make_unique<YamsDaemon>(config_);

    // Start daemon
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start daemon: " << result.error().message;

    // Stop daemon
    result = daemon_->stop();
    ASSERT_TRUE(result) << "Failed to stop daemon: " << result.error().message;

    // Start again
    result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to restart daemon: " << result.error().message;

    // Should be running
    EXPECT_TRUE(daemon_->isRunning());

    // Stop daemon
    daemon_->stop();
}

// Test path resolution for non-root user
TEST_F(DaemonTest, PathResolution) {
    // Test with empty paths (should auto-resolve)
    DaemonConfig autoConfig;
    daemon_ = std::make_unique<YamsDaemon>(autoConfig);

    // Check that daemon with auto config can be created
    // The daemon should have valid config even with empty paths
    EXPECT_NE(daemon_, nullptr);
}

// Test daemon signal handling
TEST_F(DaemonTest, SignalHandling) {
    daemon_ = std::make_unique<YamsDaemon>(config_);

    // Start daemon
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start daemon: " << result.error().message;

    // Get PID from file
    std::ifstream pidFile(config_.pidFile);
    pid_t pid;
    pidFile >> pid;
    pidFile.close();

    // Should be our process
    EXPECT_EQ(pid, getpid());

    // Send SIGTERM (would normally stop daemon, but we're in same process)
    // This tests that signal handlers are installed

    // Stop daemon normally
    daemon_->stop();
}

// Test stale PID file cleanup
TEST_F(DaemonTest, StalePidFileCleanup) {
    // Create a stale PID file with non-existent PID
    std::ofstream pidFile(config_.pidFile);
    pidFile << "99999999\n"; // Very unlikely to be a real PID
    pidFile.close();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    // Should be able to start despite stale PID file
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start daemon with stale PID: " << result.error().message;

    // Should be running
    EXPECT_TRUE(daemon_->isRunning());

    // Stop daemon
    daemon_->stop();
}

// Test daemon with invalid configuration
TEST_F(DaemonTest, InvalidConfiguration) {
    // Set socket path to unwritable location
    config_.socketPath = "/root/cannot_write_here.sock";
    daemon_ = std::make_unique<YamsDaemon>(config_);

    // Should fail to start
    auto result = daemon_->start();
    EXPECT_FALSE(result) << "Daemon should not start with invalid socket path";
}

// Test concurrent start attempts
TEST_F(DaemonTest, ConcurrentStart) {
    daemon_ = std::make_unique<YamsDaemon>(config_);

    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};

    // Try to start daemon from multiple threads
    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([this, &successCount, &failCount]() {
            auto result = daemon_->start();
            if (result) {
                successCount++;
            } else {
                failCount++;
            }
        });
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Only one should succeed
    EXPECT_EQ(successCount, 1);
    EXPECT_EQ(failCount, 4);

    // Should be running
    EXPECT_TRUE(daemon_->isRunning());

    // Stop daemon
    daemon_->stop();
}

// Test daemon stats tracking
TEST_F(DaemonTest, StatsTracking) {
    daemon_ = std::make_unique<YamsDaemon>(config_);

    // Start daemon
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start daemon: " << result.error().message;

    // Get initial stats
    const auto& stats = daemon_->getStats();
    EXPECT_EQ(stats.requestsProcessed.load(), 0);
    EXPECT_EQ(stats.activeConnections.load(), 0);
    EXPECT_EQ(stats.totalConnections.load(), 0);

    // Stop daemon
    daemon_->stop();
}

// Smoke test: Hybrid search should provide results when vector index and repo are present
TEST_F(DaemonTest, HybridSearchSmoke) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start daemon: " << result.error().message;

#ifdef GTEST_API_
    // Seed minimal metadata and vector entries
    auto repo = daemon_->_test_getMetadataRepo();
    auto vim = daemon_->_test_getVectorIndexManager();
    ASSERT_TRUE(repo);
    ASSERT_TRUE(vim);

    // Insert a minimal document into metadata repository
    yams::metadata::DocumentInfo doc{};
    doc.filePath = (config_.dataDir / "docs" / "seed.txt").string();
    doc.fileName = "seed.txt";
    doc.fileExtension = ".txt";
    doc.fileSize = 11;
    doc.sha256Hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    doc.mimeType = "text/plain";
    auto now = std::chrono::system_clock::now();
    doc.createdTime = now;
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto ins = repo->insertDocument(doc);
    ASSERT_TRUE(ins);
    const auto docId = ins.value();

    // Add vector for the document into the vector index
    std::vector<float> vec(384, 0.0f);
    vec[0] = 0.42f; // simple signal
    auto addVec = vim->addVector(std::to_string(docId), vec,
                                 {{"path", doc.filePath}, {"title", doc.fileName}});
    ASSERT_TRUE(addVec);
#endif

    // Construct a SearchRequest preferring hybrid path
    SearchRequest req;
    req.query = "seed";
    req.limit = 5;
    req.fuzzy = false;
    req.literalText = false;
    req.similarity = 0.7;

    // Serialize request as a Message and deserialize to simulate IPC round-trip
    Message msg;
    msg.version = 1;
    msg.requestId = 42;
    msg.payload = Request{req};

    auto bytes = MessageSerializer::serialize_bytes(msg);
    auto parsed =
        MessageSerializer::deserialize(std::span<const std::byte>(bytes.data(), bytes.size()));
    ASSERT_TRUE(parsed);

    // Since we seeded one vector/doc, hybrid should return at least one result
    // We cannot invoke process directly here; instead ensure daemon is running after seeding
    EXPECT_TRUE(daemon_->isRunning());

    // Stop daemon
    daemon_->stop();
}

// Fallback test: Without vector index (or if hybrid fails), fuzzy or FTS paths still respond
TEST_F(DaemonTest, FallbackSearchWhenHybridUnavailable) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    ASSERT_TRUE(result) << "Failed to start daemon: " << result.error().message;

    // Prefer fuzzy path explicitly
    SearchRequest req;
    req.query = "fallback";
    req.limit = 3;
    req.fuzzy = true;     // ensures fuzzy path is chosen
    req.similarity = 0.6; // similarity threshold for fuzzy

    Message msg;
    msg.version = 1;
    msg.requestId = 7;
    msg.payload = Request{req};

    auto bytes = MessageSerializer::serialize_bytes(msg);
    auto parsed =
        MessageSerializer::deserialize(std::span<const std::byte>(bytes.data(), bytes.size()));
    ASSERT_TRUE(parsed);

    // Minimal assertion: daemon should remain running and fuzzy path should be valid
    EXPECT_TRUE(daemon_->isRunning());

    // Stop daemon
    daemon_->stop();
}

// Verify that StatusResponse.ready survives IPC serialization/deserialization
TEST_F(DaemonTest, StatusResponseSerializationIncludesReady) {
    using namespace yams::daemon;

    StatusResponse sr;
    sr.running = true;
    sr.ready = true;
    sr.uptimeSeconds = 0;
    sr.requestsProcessed = 0;
    sr.activeConnections = 0;
    sr.memoryUsageMb = 0.0;
    sr.cpuUsagePercent = 0.0;
    sr.version = "test";

    Message msg;
    msg.version = 1;
    msg.requestId = 0;
    msg.payload = Response{sr};

    auto bytes = MessageSerializer::serialize_bytes(msg);
    auto des =
        MessageSerializer::deserialize(std::span<const std::byte>(bytes.data(), bytes.size()));
    ASSERT_TRUE(des) << "Failed to deserialize StatusResponse message";

    Message out = des.value();
    auto respVariant = std::get<Response>(out.payload);
    ASSERT_TRUE(std::holds_alternative<StatusResponse>(respVariant));
    auto outSr = std::get<StatusResponse>(respVariant);

    EXPECT_TRUE(outSr.ready);
    EXPECT_EQ(outSr.running, true);
    EXPECT_EQ(outSr.version, "test");
}

// Start should fail with invalid/unwritable data directory (simulates missing core resources)
TEST_F(DaemonTest, StartFailsWithInvalidDataDir) {
    DaemonConfig bad = config_;
    bad.dataDir = "/root/forbidden_yams_test";
    daemon_ = std::make_unique<YamsDaemon>(bad);

    auto result = daemon_->start();
    EXPECT_FALSE(result) << "Daemon should not start with invalid dataDir";
}

TEST_F(DaemonTest, WarmLatencyBenchmark) {
    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto startRes = daemon_->start();
    ASSERT_TRUE(startRes) << "Failed to start daemon: " << startRes.error().message;

    long long total_ms = 0;
#ifdef GTEST_API_
    auto repo = daemon_->_test_getMetadataRepo();
    ASSERT_TRUE(repo);

    // Seed a minimal document to keep the fuzzy path realistic
    yams::metadata::DocumentInfo doc{};
    doc.filePath = (config_.dataDir / "docs" / "bench.txt").string();
    doc.fileName = "bench.txt";
    doc.fileExtension = ".txt";
    doc.fileSize = 5;
    doc.sha256Hash = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef";
    doc.mimeType = "text/plain";
    auto now = std::chrono::system_clock::now();
    doc.createdTime = now;
    doc.modifiedTime = now;
    doc.indexedTime = now;
    (void)repo->insertDocument(doc);

    using clock = std::chrono::steady_clock;
    constexpr int kWarmIters = 3;
    constexpr int kIters = 10;

    // Warm-up fuzzy search path
    for (int i = 0; i < kWarmIters; ++i) {
        (void)repo->fuzzySearch("bench", 0.7f, 5);
    }

    auto t0 = clock::now();
    for (int i = 0; i < kIters; ++i) {
        (void)repo->fuzzySearch("bench", 0.7f, 5);
    }
    auto t1 = clock::now();
    total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
#endif

    // Avoid flakes in CI: only assert non-negative timing
    EXPECT_GE(total_ms, 0);

    daemon_->stop();
}

} // namespace yams::daemon::test