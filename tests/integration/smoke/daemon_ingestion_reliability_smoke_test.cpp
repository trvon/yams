#include <chrono>
#include <filesystem>
#include <future>
#include <thread>
#include "common/daemon_preflight.h"
#include <gtest/gtest.h>
// Local-socket bind probe for sandboxed environments
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>

#include <yams/app/services/document_ingestion_service.h>
#include <yams/daemon/daemon.h>

#include "common/fixture_manager.h"
#include "common/test_data_generator.h"

using namespace std::chrono_literals;
namespace fs = std::filesystem;

// This smoke test exercises transient IPC unavailability during daemon startup
// and validates the retriable ingestion path does not surface I/O errors.
class DaemonIngestionReliabilitySmoke : public ::testing::Test {
protected:
    fs::path root_;
    fs::path storageDir_;
    fs::path runtimeRoot_;
    std::unique_ptr<yams::test::FixtureManager> fixtures_;

    void SetUp() override {
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        root_ = fs::temp_directory_path() / ("yams_daemon_ingest_smoke_" + unique);
        storageDir_ = root_ / "storage";
        runtimeRoot_ = root_ / "runtime";
        fs::create_directories(storageDir_);
        fs::create_directories(runtimeRoot_);

        fixtures_ = std::make_unique<yams::test::FixtureManager>(root_ / "fixtures");

        // Standardize daemon test environment (no external daemon kills; unique paths)
        yams::tests::harnesses::DaemonPreflight::ensure_environment({
            .runtime_dir = runtimeRoot_,
            .socket_name_prefix = "yams-daemon-smoke-",
            .kill_others = false,
        });
    }

    void TearDown() override {
        fixtures_.reset();
        // Best-effort cleanup of runtime paths
        yams::tests::harnesses::DaemonPreflight::post_test_cleanup(runtimeRoot_);
        std::error_code ec;
        fs::remove_all(root_, ec);
    }
};

TEST_F(DaemonIngestionReliabilitySmoke, IngestRetriesUntilDaemonReady) {
    // Skip if AF_UNIX bind is forbidden
    auto canBind = []() {
        try {
            boost::asio::io_context io;
            boost::asio::local::stream_protocol::acceptor acc(io);
            auto path = fs::path("/tmp") /
                        (std::string("yams-ingest-probe-") + std::to_string(::getpid()) + ".sock");
            std::error_code ec;
            fs::remove(path, ec);
            boost::system::error_code bec;
            acc.open(boost::asio::local::stream_protocol::endpoint(path.string()).protocol(), bec);
            if (bec)
                return false;
            acc.bind(boost::asio::local::stream_protocol::endpoint(path.string()), bec);
            if (bec)
                return false;
            acc.close();
            fs::remove(path, ec);
            return true;
        } catch (...) {
            return false;
        }
    }();
    if (!canBind) {
        GTEST_SKIP() << "Skipping smoke: environment forbids AF_UNIX bind (sandbox).";
    }
    ASSERT_TRUE(fixtures_) << "Fixture manager not initialized";

    yams::test::TestDataGenerator generator(4242);
    auto documentFixture = fixtures_->createTextFixture(
        "hello.txt", generator.generateTextDocument(256, "daemon"), {"daemon", "ingest", "smoke"});
    const fs::path& src = documentFixture.path;

    // Compose add options pointing at a socket that does not exist yet
    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    // Use preflight-provided socket path when available, otherwise fall back
    if (const char* s = std::getenv("YAMS_SOCKET_PATH")) {
        opts.socketPath = s;
    } else {
        opts.socketPath = runtimeRoot_ / "yams-daemon.sock";
    }
    opts.path = src.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    opts.explicitDataDir = storageDir_;
    // Enable retries/backoff to survive transient startup window
    opts.retries = 5;
    opts.backoffMs = 100;
    opts.timeoutMs = 1500; // per-attempt timeout
    opts.verify = true;

    // Kick off ingestion on a background thread BEFORE the daemon is started
    std::promise<yams::Result<yams::daemon::AddDocumentResponse>> prom;
    auto fut = prom.get_future();
    std::thread worker(
        [&ing, &opts, p = std::move(prom)]() mutable { p.set_value(ing.addViaDaemon(opts)); });

    // Give the client a head start to encounter an initial connection failure
    std::this_thread::sleep_for(150ms);

    // DaemonPreflight already configured YAMS_DAEMON_KILL_OTHERS=0

    // Now start the daemon bound to our test socket/data dir
    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = storageDir_;
    cfg.socketPath = *opts.socketPath;
    cfg.pidFile = root_ / "daemon.pid";
    cfg.logFile = root_ / "daemon.log";
    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    ASSERT_TRUE(started) << started.error().message;

    // Start runLoop in background thread - REQUIRED for daemon to process requests
    std::thread runLoopThread([&daemon]() { daemon.runLoop(); });

    // Wait for daemon to reach Ready state before allowing client requests to complete
    // Without this, the client might connect before services are fully initialized
    auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline) {
        auto lifecycle = daemon.getLifecycle().snapshot();
        if (lifecycle.state == yams::daemon::LifecycleState::Ready) {
            break;
        }
        std::this_thread::sleep_for(100ms);
    }

    // Wait for result (bounded)
    ASSERT_EQ(fut.wait_for(5s), std::future_status::ready);
    auto addRes = fut.get();

    // Join worker before assertions complete
    worker.join();

    ASSERT_TRUE(addRes) << (addRes ? "" : addRes.error().message);
    EXPECT_FALSE(addRes.value().hash.empty());

    // Shutdown daemon cleanly
    daemon.stop();
    runLoopThread.join();
}
