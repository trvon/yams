#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>
#include "common/fixture_manager.h"
#include <gtest/gtest.h>

#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
// Direct client for status polling
#include <yams/daemon/client/daemon_client.h>
// Minimal boost.asio includes for a tiny status helper
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/system_executor.hpp>
#include <boost/system/error_code.hpp>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class ServicesRetrievalIngestionIT : public ::testing::Test {
protected:
    fs::path testRoot_;
    fs::path storageDir_;
    fs::path xdgRuntimeDir_;
    fs::path socketPath_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;
    std::unique_ptr<yams::test::FixtureManager> fixtures_;
    static bool canBindUnixSocketHere() {
        try {
            boost::asio::io_context io;
            boost::asio::local::stream_protocol::acceptor acc(io);
            auto path = std::filesystem::path("/tmp") /
                        (std::string("yams-svc-probe-") + std::to_string(::getpid()) + ".sock");
            std::error_code ec;
            std::filesystem::remove(path, ec);
            boost::system::error_code bec;
            acc.open(boost::asio::local::stream_protocol::endpoint(path.string()).protocol(), bec);
            if (bec)
                return false;
            acc.bind(boost::asio::local::stream_protocol::endpoint(path.string()), bec);
            if (bec)
                return false;
            acc.close();
            std::filesystem::remove(path, ec);
            return true;
        } catch (...) {
            return false;
        }
    }

    void SetUp() override {
        if (!canBindUnixSocketHere()) {
            GTEST_SKIP() << "Skipping services IT: environment forbids AF_UNIX bind (sandbox).";
        }
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        testRoot_ = fs::temp_directory_path() / ("yams_services_it_" + unique);
        storageDir_ = testRoot_ / "storage";
        xdgRuntimeDir_ = testRoot_ / "xdg";
        fs::create_directories(storageDir_);
        fs::create_directories(xdgRuntimeDir_);
        fixtures_ = std::make_unique<yams::test::FixtureManager>(testRoot_ / "fixtures");

        // Ensure test daemon never kills any existing user/system daemon
#if defined(_WIN32)
        _putenv_s("YAMS_DAEMON_KILL_OTHERS", "0");
#else
        setenv("YAMS_DAEMON_KILL_OTHERS", "0", 1);
#endif

        // Use short /tmp path for AF_UNIX sun_path compatibility and sandbox friendliness
        socketPath_ = fs::path("/tmp") / ("yams-svc-" + unique + ".sock");

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = storageDir_;
        cfg.socketPath = socketPath_;
        cfg.pidFile = testRoot_ / "daemon.pid";
        cfg.logFile = testRoot_ / "daemon.log";

        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);
        auto started = daemon_->start();
        ASSERT_TRUE(started) << started.error().message;
        std::this_thread::sleep_for(200ms);
    }

    void TearDown() override {
        if (daemon_)
            daemon_->stop();
        fixtures_.reset();
        // Best-effort removal of test socket and pid file
        std::error_code ec2;
        std::filesystem::remove(socketPath_, ec2);
        std::filesystem::remove(testRoot_ / "daemon.pid", ec2);
        std::error_code ec;
        fs::remove_all(testRoot_, ec);
    }
};

// Helper: wait until post-ingest queue drains (queued==0 && inflight==0) or timeout
static void
waitForPostIngestQuiescent(const std::filesystem::path& socket,
                           const std::filesystem::path& dataDir, std::chrono::milliseconds timeout,
                           std::chrono::milliseconds poll = std::chrono::milliseconds(50)) {
    yams::daemon::ClientConfig cfg;
    cfg.socketPath = socket;
    cfg.dataDir = dataDir;
    cfg.requestTimeout = std::chrono::milliseconds(1500);

    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        // Run a single status call "synchronously" using a promise. To avoid
        // use-after-free if the coroutine outlives this scope on timeout,
        // heap-allocate the client and capture it by value.
        auto client = std::make_shared<yams::daemon::DaemonClient>(cfg);
        std::promise<yams::Result<yams::daemon::StatusResponse>> prom;
        auto fut = prom.get_future();
        boost::asio::co_spawn(
            boost::asio::system_executor{},
            [client, p = std::move(prom)]() mutable -> boost::asio::awaitable<void> {
                auto r = co_await client->status();
                p.set_value(std::move(r));
                co_return;
            },
            boost::asio::detached);
        if (fut.wait_for(std::chrono::milliseconds(1500)) == std::future_status::ready) {
            auto res = fut.get();
            if (res) {
                const auto& s = res.value();
                auto getU64 = [&](const char* key) -> std::uint64_t {
                    auto it = s.requestCounts.find(key);
                    return (it == s.requestCounts.end()) ? 0ull
                                                         : static_cast<std::uint64_t>(it->second);
                };
                auto queued = getU64("post_ingest_queued");
                auto inflight = getU64("post_ingest_inflight");
                if (queued == 0 && inflight == 0)
                    return; // drained
            }
        }
        std::this_thread::sleep_for(poll);
    }
}

TEST_F(ServicesRetrievalIngestionIT, AddViaDaemonAndListGetGrep) {
    using yams::app::services::DocumentIngestionService;
    using yams::app::services::RetrievalOptions;
    using yams::app::services::RetrievalService;

    // 1) Create a small file to add
    auto doc = fixtures_->createTextFixture("hello/hello.txt", "hello yams services",
                                            {"services", "smoke"});
    fs::path filePath = doc.path;

    // 2) Add via DocumentIngestionService (daemon-first)
    DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    // Ensure we target this test's daemon socket
    opts.socketPath = socketPath_;
    opts.path = filePath.string();
    opts.recursive = false;
    opts.noEmbeddings = true; // avoid model work in IT
    // Point explicitly to storage used by daemon
    opts.explicitDataDir = storageDir_;
    auto addRes = ing.addViaDaemon(opts);
    ASSERT_TRUE(addRes) << (addRes ? "" : addRes.error().message);
    ASSERT_FALSE(addRes.value().hash.empty());

    // Ensure deferred extraction/indexing completes before assertions
    waitForPostIngestQuiescent(socketPath_, storageDir_, std::chrono::milliseconds(5000));

    // 3) List via RetrievalService (with a short bounded wait for visibility)
    RetrievalService rsvc;
    RetrievalOptions ropts;
    // Pin to test daemon socket and storage
    ropts.socketPath = socketPath_;
    ropts.explicitDataDir = storageDir_;
    yams::daemon::ListRequest lreq;
    lreq.limit = 100;
    bool found = false;
    for (int attempt = 0; attempt < 60 && !found; ++attempt) { // up to ~3s
        auto lres = rsvc.list(lreq, ropts);
        ASSERT_TRUE(lres) << (lres ? "" : lres.error().message);
        found = false;
        for (const auto& e : lres.value().items) {
            if (e.name == "hello.txt") {
                found = true;
                break;
            }
        }
        if (!found)
            std::this_thread::sleep_for(50ms);
    }
    EXPECT_TRUE(found);

    // 4) Get by hash via RetrievalService
    yams::daemon::GetRequest greq;
    greq.hash = addRes.value().hash;
    greq.metadataOnly = false;
    auto gres = rsvc.get(greq, ropts);
    ASSERT_TRUE(gres) << (gres ? "" : gres.error().message);
    EXPECT_EQ(gres.value().name, "hello.txt");

    // 5) Grep for a term
    yams::daemon::GrepRequest gpreq;
    gpreq.pattern = "hello";
    gpreq.pathsOnly = true;
    // Grep may require deferred extraction/indexing to complete; allow a brief wait
    {
        bool grepReady = false;
        for (int attempt = 0; attempt < 60 && !grepReady; ++attempt) { // up to ~3s
            auto gpres = rsvc.grep(gpreq, ropts);
            ASSERT_TRUE(gpres) << (gpres ? "" : gpres.error().message);
            grepReady = !gpres.value().matches.empty();
            if (!grepReady)
                std::this_thread::sleep_for(50ms);
        }
    }

    // 6) Name-smart get
    auto gname = rsvc.getByNameSmart("hello.txt", /*oldest*/ false, /*includeContent*/ true,
                                     /*useSession*/ false, /*sessionName*/ std::string{}, ropts,
                                     /*resolver*/ {});
    ASSERT_TRUE(gname) << (gname ? "" : gname.error().message);
    EXPECT_EQ(gname.value().name, "hello.txt");

    // 7) Chunked buffer get with cap (allow generous timeout to avoid flakiness)
    yams::daemon::GetInitRequest ginit;
    ginit.hash = addRes.value().hash;
    ropts.requestTimeoutMs = 30000; // default
    auto chunked = rsvc.getChunkedBuffer(ginit, /*capBytes*/ 8, ropts);
    ASSERT_TRUE(chunked) << (chunked ? "" : chunked.error().message);
    EXPECT_LE(chunked.value().content.size(), 8u);
}

TEST_F(ServicesRetrievalIngestionIT, AddDirectoryWithPatternsAndTags) {
    using yams::app::services::DocumentIngestionService;
    using yams::app::services::RetrievalOptions;
    using yams::app::services::RetrievalService;

    // Create a small directory tree
    fs::path dirRoot = testRoot_ / "ingest" / "dirA" / "dirB";
    fs::create_directories(dirRoot);
    // Files: 2 included by patterns, 1 excluded
    {
        std::ofstream(dirRoot / "keep.cpp") << "int main() { return 0; }\n";
        std::ofstream(dirRoot / "keep.md") << "# Title\nhello pattern tags\n";
        std::ofstream(dirRoot / "skip.bin") << std::string("\x00\x01\x02", 3);
    }

    // Add recursively with include/exclude patterns and tags/metadata/collection
    DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    // Use the same socket path as the daemon started in SetUp()
    opts.socketPath = socketPath_;
    auto keepCpp = fixtures_->createTextFixture(
        "ingest/dirA/dirB/keep.cpp", "int main() { return 0; }\n", {"code", "cpp", "ingest"});
    auto keepMd = fixtures_->createTextFixture(
        "ingest/dirA/dirB/keep.md", "# Title\nhello pattern tags\n", {"docs", "md", "ingest"});
    std::vector<std::byte> skipData = {std::byte{0x00}, std::byte{0x01}, std::byte{0x02}};
    auto skipBin =
        fixtures_->createBinaryFixture("ingest/dirA/dirB/skip.bin", skipData, {"bin", "ingest"});

    (void)keepCpp;
    (void)keepMd;
    (void)skipBin;

    opts.path = (fixtures_->root() / "ingest").string();
    opts.recursive = true;
    opts.includePatterns = {"*.cpp", "*.md"};
    opts.excludePatterns = {"*.bin"};
    opts.tags = {"code", "working", "kg", "kg:node:test", "topic:pbi-002", "component:cli"};
    opts.metadata = {{"pbi", "002"}, {"system", "yams"}};
    opts.collection = "workspace";
    opts.noEmbeddings = true; // keep this IT fast and deterministic
    opts.explicitDataDir = storageDir_;
    auto addRes = ing.addViaDaemon(opts);
    ASSERT_TRUE(addRes) << (addRes ? "" : addRes.error().message);
    // When recursive, documentsAdded should be >= 2 (keep.cpp, keep.md)
    EXPECT_GE(addRes.value().documentsAdded, static_cast<size_t>(2));

    // Wait for background post-ingest to finish so grep sees content
    waitForPostIngestQuiescent(socketPath_, storageDir_, std::chrono::milliseconds(7000));

    // List entries under our path; ensure tags are present and excluded file absent
    RetrievalService rsvc;
    RetrievalOptions ropts;
    ropts.socketPath = socketPath_;
    ropts.explicitDataDir = storageDir_;

    yams::daemon::ListRequest lreq;
    lreq.limit = 50;
    // Filter to our directory by pattern
    lreq.namePattern = (fixtures_->root() / "ingest" / "**").string();
    lreq.showTags = true;
    lreq.showMetadata = true;
    bool sawCpp = false, sawMd = false, sawBin = false;
    for (int attempt = 0; attempt < 100; ++attempt) { // up to ~5s
        sawCpp = sawMd = sawBin = false;
        auto lres = rsvc.list(lreq, ropts);
        ASSERT_TRUE(lres) << (lres ? "" : lres.error().message);
        for (const auto& e : lres.value().items) {
            if (e.name == "keep.cpp") {
                sawCpp = true;
                // Verify tags carried through
                EXPECT_NE(std::find(e.tags.begin(), e.tags.end(), std::string("code")),
                          e.tags.end());
                EXPECT_NE(std::find(e.tags.begin(), e.tags.end(), std::string("working")),
                          e.tags.end());
            } else if (e.name == "keep.md") {
                sawMd = true;
                EXPECT_NE(std::find(e.tags.begin(), e.tags.end(), std::string("kg")), e.tags.end());
                EXPECT_NE(std::find(e.tags.begin(), e.tags.end(), std::string("kg:node:test")),
                          e.tags.end());
            } else if (e.name == "skip.bin") {
                sawBin = true;
            }
            // Common metadata expectations
            auto it = e.metadata.find("pbi");
            if (it != e.metadata.end()) {
                EXPECT_EQ(it->second, "002");
            }
        }
        if (sawCpp && sawMd && !sawBin)
            break;
        std::this_thread::sleep_for(50ms);
    }
    EXPECT_TRUE(sawCpp);
    EXPECT_TRUE(sawMd);
    EXPECT_FALSE(sawBin);

    // Grep should find content only in included files
    yams::daemon::GrepRequest gpreq;
    gpreq.pattern = "hello";
    gpreq.pathsOnly = true;
    // Use fixtures root, matching the path passed to ingestion so grep scopes correctly
    gpreq.includePatterns = {(fixtures_->root() / "ingest" / "**").string()};
    // Grep can lag slightly behind post-ingest extraction; wait briefly for match
    {
        bool grepFoundMd = false;
        for (int attempt = 0; attempt < 100 && !grepFoundMd; ++attempt) { // up to ~5s
            auto gpres = rsvc.grep(gpreq, ropts);
            ASSERT_TRUE(gpres) << (gpres ? "" : gpres.error().message);
            for (const auto& m : gpres.value().matches) {
                if (m.file.find("keep.md") != std::string::npos) {
                    grepFoundMd = true;
                    break;
                }
            }
            if (!grepFoundMd)
                std::this_thread::sleep_for(50ms);
        }
        EXPECT_TRUE(grepFoundMd);
    }
}
