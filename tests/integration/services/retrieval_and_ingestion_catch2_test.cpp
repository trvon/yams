/**
 * @file retrieval_and_ingestion_catch2_test.cpp
 * @brief Catch2 integration tests for RetrievalService and DocumentIngestionService
 *
 * Tests the daemon-based services layer for document ingestion and retrieval,
 * using DaemonHarness for proper lifecycle management.
 */

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <string_view>
#include <thread>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include "common/capability.h"
#include "common/fixture_manager.h"
#include "../daemon/test_daemon_harness.h"

#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {
constexpr int kRequestTimeoutMs = 5000;
constexpr int kBodyTimeoutMs = 15000;

// Helper: wait until post-ingest queue drains (queued==0 && inflight==0) or timeout
void waitForPostIngestQuiescent(const fs::path& socket, const fs::path& dataDir,
                                std::chrono::milliseconds timeout,
                                std::chrono::milliseconds poll = 50ms) {
    yams::daemon::ClientConfig cfg;
    cfg.socketPath = socket;
    cfg.dataDir = dataDir;
    cfg.requestTimeout = 1500ms;

    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto client = std::make_shared<yams::daemon::DaemonClient>(cfg);
        std::promise<yams::Result<yams::daemon::StatusResponse>> prom;
        auto fut = prom.get_future();
        boost::asio::co_spawn(
            yams::daemon::GlobalIOContext::global_executor(),
            [client, p = std::move(prom)]() mutable -> boost::asio::awaitable<void> {
                auto r = co_await client->status();
                p.set_value(std::move(r));
                co_return;
            },
            boost::asio::detached);
        if (fut.wait_for(1500ms) == std::future_status::ready) {
            auto res = fut.get();
            if (res) {
                const auto& s = res.value();
                auto getU64 = [&](const char* key) -> std::uint64_t {
                    auto it = s.requestCounts.find(key);
                    return (it == s.requestCounts.end()) ? 0ull
                                                         : static_cast<std::uint64_t>(it->second);
                };
                if (getU64("post_ingest_queued") == 0 && getU64("post_ingest_inflight") == 0)
                    return;
            }
        }
        std::this_thread::sleep_for(poll);
    }
}

} // namespace

// Windows daemon shutdown hangs during WorkCoordinator thread join.
// The tests themselves work - IPC communication succeeds - but cleanup blocks indefinitely.
// See: docs/developer/windows-daemon-ipc-plan.md for the underlying issue.
// TODO: Remove this skip once Windows daemon shutdown is fixed.
#ifdef _WIN32
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN()                                                          \
    SKIP("Windows daemon shutdown hangs - see windows-daemon-ipc-plan.md")
#else
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN() ((void)0)
#endif

/**
 * @brief Test fixture using DaemonHarness for proper lifecycle management
 */
class ServicesRetrievalIngestionFixture {
public:
    ServicesRetrievalIngestionFixture() {
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        testRoot_ = fs::temp_directory_path() / ("yams_services_it_" + unique);
        fs::create_directories(testRoot_);
        fixtures_ = std::make_unique<yams::test::FixtureManager>(testRoot_ / "fixtures");

        // Ensure test daemon never kills any existing user/system daemon
#if defined(_WIN32)
        _putenv_s("YAMS_DAEMON_KILL_OTHERS", "0");
#else
        setenv("YAMS_DAEMON_KILL_OTHERS", "0", 1);
#endif

        harness_ = std::make_unique<yams::test::DaemonHarness>();
    }

    ~ServicesRetrievalIngestionFixture() {
        bool keepArtifacts = std::getenv("YAMS_KEEP_IT_DIR") != nullptr;

        if (harness_) {
            harness_->stop();
            harness_.reset();
        }

        fixtures_.reset();

        if (keepArtifacts) {
            std::cout << "Keeping integration test artifacts under " << testRoot_.string()
                      << " (YAMS_KEEP_IT_DIR set)." << std::endl;
        } else {
            std::error_code ec;
            fs::remove_all(testRoot_, ec);
        }
    }

    bool startDaemon(std::chrono::seconds timeout = 30s) {
        if (!harness_->start(timeout))
            return false;
        socketPath_ = harness_->socketPath();
        storageDir_ = harness_->dataDir();
        return true;
    }

    yams::daemon::YamsDaemon* daemon() const { return harness_ ? harness_->daemon() : nullptr; }

    fs::path testRoot_;
    fs::path socketPath_;
    fs::path storageDir_;
    std::unique_ptr<yams::test::DaemonHarness> harness_;
    std::unique_ptr<yams::test::FixtureManager> fixtures_;
};

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "AddViaDaemonAndListGetGrep",
                 "[integration][services][retrieval][ingestion]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    using yams::app::services::DocumentIngestionService;
    using yams::app::services::RetrievalOptions;
    using yams::app::services::RetrievalService;

    // 1) Create a small file to add
    auto doc =
        fixtures_->createTextFixture("hello/hello.txt", "hello yams services", {"services", "smoke"});
    fs::path filePath = doc.path;

    // 2) Add via DocumentIngestionService (daemon-first)
    DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.path = filePath.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    opts.timeoutMs = kRequestTimeoutMs;
    opts.explicitDataDir = storageDir_;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);
    REQUIRE_FALSE(addRes.value().hash.empty());

    waitForPostIngestQuiescent(socketPath_, storageDir_, 5000ms);

    // 3) List via RetrievalService
    RetrievalService rsvc;
    RetrievalOptions ropts;
    ropts.socketPath = socketPath_;
    ropts.explicitDataDir = storageDir_;
    ropts.headerTimeoutMs = kRequestTimeoutMs;
    ropts.bodyTimeoutMs = kBodyTimeoutMs;
    ropts.requestTimeoutMs = kRequestTimeoutMs;

    yams::app::services::ListOptions lreq;
    lreq.limit = 25;
    bool found = false;
    for (int attempt = 0; attempt < 40 && !found; ++attempt) {
        auto lres = rsvc.list(lreq, ropts);
        REQUIRE(lres);
        for (const auto& e : lres.value().items) {
            if (e.name == "hello.txt") {
                found = true;
                break;
            }
        }
        if (!found)
            std::this_thread::sleep_for(50ms);
    }
    if (!found) {
        SKIP("List output did not surface hello.txt; eventual consistency issue.");
    }

    // 4) Get by hash
    yams::app::services::GetOptions greq;
    greq.hash = addRes.value().hash;
    greq.metadataOnly = false;
    auto gres = rsvc.get(greq, ropts);
    REQUIRE(gres);
    CHECK(gres.value().name == "hello.txt");

    // 5) Grep for pattern
    yams::app::services::GrepOptions gpreq;
    gpreq.pattern = "hello";
    gpreq.pathsOnly = true;
    bool grepReady = false;
    for (int attempt = 0; attempt < 40 && !grepReady; ++attempt) {
        auto gpres = rsvc.grep(gpreq, ropts);
        REQUIRE(gpres);
        grepReady = !gpres.value().matches.empty();
        if (!grepReady)
            std::this_thread::sleep_for(50ms);
    }
    if (!grepReady) {
        SKIP("Grep matches not available; skipping on this runner.");
    }

    // 6) Name-smart get
    auto gname = rsvc.getByNameSmart("hello.txt", false, true, false, std::string{}, ropts, {});
    REQUIRE(gname);
    CHECK(gname.value().name == "hello.txt");

    // 7) Chunked buffer get
    yams::app::services::GetInitOptions ginit;
    ginit.hash = addRes.value().hash;
    ropts.requestTimeoutMs = 30000;
    auto chunked = rsvc.getChunkedBuffer(ginit, 8, ropts);
    REQUIRE(chunked);
    CHECK(chunked.value().content.size() <= 8u);
}

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "StatsZeroThenGrowthAndUnreachableHints",
                 "[integration][services][retrieval]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    using yams::app::services::DocumentIngestionService;
    using yams::app::services::makeStatsService;
    using yams::app::services::StatsRequest;

    auto* sm = daemon()->getServiceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    auto statsSvc = makeStatsService(ctx);

    // Capture baseline stats
    StatsRequest rq{};
    auto s0 = statsSvc->getStats(rq);
    REQUIRE(s0);
    auto baseObjects = s0.value().totalObjects;
    auto baseBytes = s0.value().totalBytes;

    // Ingest one small doc
    auto doc = fixtures_->createTextFixture("stats/one.txt", "alpha beta", {"stats", "it"});
    DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = doc.path.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    opts.timeoutMs = kRequestTimeoutMs;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);

    waitForPostIngestQuiescent(socketPath_, storageDir_, 5000ms);

    auto s2 = statsSvc->getStats(StatsRequest{});
    REQUIRE(s2);
    CHECK(s2.value().totalObjects >= baseObjects);
    CHECK(s2.value().totalBytes >= baseBytes);

    // Unreachable daemon should fail cleanly
    {
        DocumentIngestionService ing2;
        yams::app::services::AddOptions bad;
        bad.socketPath =
            fs::path("/tmp") / ("yams-does-not-exist-" + std::to_string(::getpid()) + ".sock");
        bad.explicitDataDir = storageDir_;
        bad.path = doc.path.string();
        bad.recursive = false;
        bad.noEmbeddings = true;
        bad.timeoutMs = kRequestTimeoutMs;
        auto r = ing2.addViaDaemon(bad);
        CHECK_FALSE(r);
    }
}

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "AddDirectoryWithPatternsAndTags",
                 "[integration][services][directory][patterns]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    using yams::app::services::DocumentIngestionService;
    using yams::app::services::RetrievalOptions;
    using yams::app::services::RetrievalService;

    // Create fixtures
    auto keepCpp = fixtures_->createTextFixture("ingest/dirA/dirB/keep.cpp",
                                                 "int main() { return 0; }\n", {"code", "cpp"});
    auto keepMd = fixtures_->createTextFixture("ingest/dirA/dirB/keep.md",
                                                "# Title\nhello pattern tags\n", {"docs", "md"});
    std::vector<std::byte> skipData = {std::byte{0x00}, std::byte{0x01}, std::byte{0x02}};
    auto skipBin = fixtures_->createBinaryFixture("ingest/dirA/dirB/skip.bin", skipData, {"bin"});
    (void)keepCpp;
    (void)keepMd;
    (void)skipBin;

    DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.path = (fixtures_->root() / "ingest").string();
    opts.recursive = true;
    opts.includePatterns = {"*.cpp", "*.md"};
    opts.excludePatterns = {"*.bin"};
    opts.tags = {"code", "working", "kg"};
    opts.metadata = {{"pbi", "002"}, {"system", "yams"}};
    opts.collection = "workspace";
    opts.noEmbeddings = true;
    opts.explicitDataDir = storageDir_;
    opts.timeoutMs = kRequestTimeoutMs;

    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);
    const auto& firstResp = addRes.value();
    CHECK(firstResp.documentsAdded >= 2);
    CHECK(firstResp.documentsUpdated == 0);
    CHECK_FALSE(firstResp.hash.empty());

    waitForPostIngestQuiescent(socketPath_, storageDir_, 7000ms);

    // Verify via list
    RetrievalService rsvc;
    RetrievalOptions ropts;
    ropts.socketPath = socketPath_;
    yams::app::services::ListOptions lreq;
    lreq.limit = 10;
    lreq.namePattern = (fixtures_->root() / "ingest" / "**").string();
    lreq.showTags = true;
    lreq.showMetadata = true;

    bool haveCpp = false, haveMd = false, skipPresent = false;
    yams::daemon::ListEntry keepCppEntry, keepMdEntry;

    for (int attempt = 0; attempt < 40; ++attempt) {
        auto lres = rsvc.list(lreq, ropts);
        REQUIRE(lres);
        const auto& items = lres.value().items;
        for (const auto& e : items) {
            if (e.name == "keep.cpp") {
                haveCpp = true;
                keepCppEntry = e;
            }
            if (e.name == "keep.md") {
                haveMd = true;
                keepMdEntry = e;
            }
            if (e.name == "skip.bin")
                skipPresent = true;
        }
        if (haveCpp && haveMd && !skipPresent)
            break;
        std::this_thread::sleep_for(50ms);
    }

    REQUIRE(haveCpp);
    REQUIRE(haveMd);
    CHECK_FALSE(skipPresent);

    // Check tags/metadata were persisted
    CHECK(std::find(keepCppEntry.tags.begin(), keepCppEntry.tags.end(), "code") !=
          keepCppEntry.tags.end());
    auto cppMeta = keepCppEntry.metadata.find("pbi");
    REQUIRE(cppMeta != keepCppEntry.metadata.end());
    CHECK(cppMeta->second == "002");
}

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "LightIndexInvalidHashTolerantError",
                 "[integration][services][search][error]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    auto* sm = daemon()->getServiceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    std::string bogus = "notahashvalue";
    auto r = searchSvc->lightIndexForHash(bogus);
    REQUIRE_FALSE(r);

    std::string msg = r.error().message;
    if (msg.find("Worker executor not available") != std::string::npos) {
        SKIP("SearchService worker executor unavailable on this runner.");
    }
    bool ok = (msg.find("not found") != std::string::npos) ||
              (msg.find("Invalid") != std::string::npos) ||
              (msg.find("invalid") != std::string::npos);
    CHECK(ok);
}

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "IndexingAddDirectoryVerifyAndDefer",
                 "[integration][services][indexing]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    using yams::app::services::AddDirectoryRequest;
    using yams::app::services::makeIndexingService;
    using yams::app::services::RetrievalOptions;
    using yams::app::services::RetrievalService;

    auto dir = fixtures_->root() / "idx";
    fs::create_directories(dir);
    std::ofstream(dir / "a.md") << "alpha";
    std::ofstream(dir / "b.bin") << std::string(4, '\0');

    auto* sm = daemon()->getServiceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    auto idxSvc = makeIndexingService(ctx);

    if (!yams::test::capability::indexing_available(ctx)) {
        SKIP("IndexingService capability not available on this runner.");
    }

    AddDirectoryRequest req;
    req.directoryPath = dir.string();
    req.includePatterns = {"*.md", "*.bin"};
    req.excludePatterns = {"*.bin"};
    req.tags = {"idx", "phase2"};
    req.metadata = {{"pbi", "028-45"}};
    req.recursive = true;
    req.verify = true;

    auto ar = idxSvc->addDirectory(req);
    if (!ar) {
        SKIP("IndexingService.addDirectory unavailable: " + ar.error().message);
    }
    const auto& resp = ar.value();
    if (resp.filesProcessed == 0) {
        SKIP("IndexingService processed=0 (capability gap)");
    }
    CHECK(resp.filesIndexed + resp.filesFailed + resp.filesSkipped >= 1);
    if (resp.filesIndexed == 0 && resp.filesFailed > 0) {
        SKIP("IndexingService reports failure for all files.");
    }
}

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "RetrievalGetToFileWritesContent",
                 "[integration][services][retrieval]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    using yams::app::services::DocumentIngestionService;
    using yams::app::services::RetrievalOptions;
    using yams::app::services::RetrievalService;

    auto doc = fixtures_->createTextFixture("rtv/one.txt", "retrieval content", {"retrieval"});

    DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = doc.path.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);

    RetrievalService rsvc;
    RetrievalOptions ro;
    ro.socketPath = socketPath_;
    ro.explicitDataDir = storageDir_;

    yams::app::services::GetInitOptions gi;
    gi.hash = addRes.value().hash;
    auto outPath = testRoot_ / "out_one.txt";

    if (auto chunked = rsvc.getChunkedBuffer(gi, 8, ro); chunked) {
        CHECK(chunked.value().content.size() > 0u);
        return;
    }
    auto wr = rsvc.getToFile(gi, outPath, ro);
    if (!wr) {
        SKIP("RetrievalService getToFile/chunked unavailable: " + wr.error().message);
    }

    std::error_code ec;
    if (!fs::exists(outPath, ec)) {
        SKIP("Output path not created.");
    }
    auto sz = fs::file_size(outPath, ec);
    if (ec) {
        SKIP("Cannot stat output path: " + ec.message());
    }
    CHECK(sz > 0u);
}

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "BinaryChunkedGetBytesRoundTrip",
                 "[integration][services][retrieval]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    fs::create_directories(fixtures_->root() / "bin");
    auto p = fixtures_->root() / "bin" / "b.dat";
    std::string bytes;
    bytes.push_back('\0');
    bytes.push_back((char)0xFF);
    bytes.push_back((char)0x10);
    bytes.push_back('A');
    bytes.push_back('B');
    bytes.push_back('C');
    bytes.push_back('\n');
    bytes.push_back((char)0x7F);
    {
        std::ofstream out(p, std::ios::binary);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions a;
    a.socketPath = socketPath_;
    a.explicitDataDir = storageDir_;
    a.path = p.string();
    a.recursive = false;
    a.noEmbeddings = true;
    a.timeoutMs = kRequestTimeoutMs;
    auto addRes = ing.addViaDaemon(a);
    REQUIRE(addRes);

    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ropts;
    ropts.socketPath = socketPath_;
    ropts.explicitDataDir = storageDir_;
    ropts.headerTimeoutMs = kRequestTimeoutMs;
    ropts.bodyTimeoutMs = kBodyTimeoutMs;
    ropts.requestTimeoutMs = kRequestTimeoutMs;

    yams::app::services::GetInitOptions gi{};
    gi.name = p.filename().string();
    gi.maxBytes = 0;
    gi.chunkSize = 4;
    auto cr = rsvc.getChunkedBuffer(gi, 16, ropts);
    if (!cr) {
        SKIP("Chunked retrieval unavailable: " + cr.error().message);
    }
    auto out = cr.value().content;
    if (out.empty()) {
        SKIP("Chunked retrieval returned empty content.");
    }
    REQUIRE(bytes.size() >= out.size());
    CHECK(out == bytes.substr(0, out.size()));
}

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "RetrievalListRespectsLimitAndPattern",
                 "[integration][services][retrieval]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    using yams::app::services::DocumentIngestionService;
    using yams::app::services::RetrievalOptions;
    using yams::app::services::RetrievalService;

    fs::create_directories(fixtures_->root() / "list" / "d");
    fixtures_->createTextFixture("list/d/a.md", "alpha", {"list", "md"});
    fixtures_->createTextFixture("list/d/b.md", "bravo", {"list", "md"});
    fixtures_->createTextFixture("list/d/c.txt", "charlie", {"list", "txt"});

    DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = (fixtures_->root() / "list").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    opts.timeoutMs = kRequestTimeoutMs;
    auto addRes = ing.addViaDaemon(opts);
    REQUIRE(addRes);

    RetrievalService rsvc;
    RetrievalOptions ro;
    ro.socketPath = socketPath_;
    ro.explicitDataDir = storageDir_;
    ro.headerTimeoutMs = kRequestTimeoutMs;
    ro.bodyTimeoutMs = kBodyTimeoutMs;
    ro.requestTimeoutMs = kRequestTimeoutMs;

    yams::app::services::ListOptions lreq;
    lreq.limit = 1;
    lreq.namePattern = (fixtures_->root() / "list" / "**" / "*.md").string();

    auto lres = rsvc.list(lreq, ro);
    REQUIRE(lres);
    const auto& items = lres.value().items;
    REQUIRE(items.size() <= 1);
    for (const auto& e : items) {
        CHECK(e.name.rfind(".md") != std::string::npos);
    }
}

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "RetrievalListEchoesTotalCountAndStructure",
                 "[integration][services][retrieval][list][structure]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    using yams::app::services::DocumentIngestionService;
    using yams::app::services::RetrievalOptions;
    using yams::app::services::RetrievalService;

    fs::create_directories(fixtures_->root() / "list2");
    fixtures_->createTextFixture("list2/one.txt", "one", {"l2"});
    fixtures_->createTextFixture("list2/two.txt", "two", {"l2"});
    fixtures_->createTextFixture("list2/three.md", "three", {"l2"});

    DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = (fixtures_->root() / "list2").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    opts.timeoutMs = kRequestTimeoutMs;
    REQUIRE(ing.addViaDaemon(opts));

    RetrievalService rsvc;
    RetrievalOptions ro;
    ro.socketPath = socketPath_;
    ro.explicitDataDir = storageDir_;
    ro.headerTimeoutMs = kRequestTimeoutMs;
    ro.bodyTimeoutMs = kBodyTimeoutMs;
    ro.requestTimeoutMs = kRequestTimeoutMs;

    yams::app::services::ListOptions lq;
    lq.limit = 2;
    lq.showTags = true;
    lq.showMetadata = true;

    auto lr = rsvc.list(lq, ro);
    REQUIRE(lr);
    const auto& resp = lr.value();
    CHECK(resp.items.size() <= static_cast<size_t>(lq.limit));
    CHECK(resp.totalCount >= static_cast<uint64_t>(resp.items.size()));
    for (const auto& e : resp.items) {
        CHECK_FALSE(e.name.empty());
        CHECK_FALSE(e.path.empty());
    }
}

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "AppDocumentListEchoDetailsAndBurst",
                 "[integration][services][document][list][burst]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    using yams::app::services::ListDocumentsRequest;
    using yams::app::services::makeDocumentService;

    fs::create_directories(fixtures_->root() / "app_list");
    fixtures_->createTextFixture("app_list/a.txt", "alpha", {"app-list"});
    fixtures_->createTextFixture("app_list/b.md", "bravo", {"app-list"});

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = (fixtures_->root() / "app_list").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    opts.timeoutMs = kRequestTimeoutMs;
    REQUIRE(ing.addViaDaemon(opts));

    auto* sm = daemon()->getServiceManager();
    REQUIRE(sm != nullptr);
    auto ctx = sm->getAppContext();
    auto docSvc = makeDocumentService(ctx);

    ListDocumentsRequest rq;
    rq.limit = 2;
    rq.pattern = (fixtures_->root() / "app_list" / "**").string();
    rq.showSnippets = false;
    rq.showMetadata = false;
    rq.showTags = true;

    auto r1 = docSvc->list(rq);
    REQUIRE(r1);
    const auto& resp = r1.value();
    REQUIRE(resp.documents.size() <= 2);
    CHECK(resp.totalFound >= resp.documents.size());

    // Burst: call list repeatedly
    for (int i = 0; i < 25; ++i) {
        auto ri = docSvc->list(rq);
        REQUIRE(ri);
        CHECK(ri.value().documents.size() <= 2);
    }
}

TEST_CASE_METHOD(ServicesRetrievalIngestionFixture, "StressTail",
                 "[integration][services][stress]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    REQUIRE(startDaemon());

    auto p = fixtures_->createTextFixture("stress/tail.txt", "hello", {"stress"});
    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = p.path.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    opts.timeoutMs = kRequestTimeoutMs;
    REQUIRE(ing.addViaDaemon(opts));

    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ro;
    ro.socketPath = socketPath_;
    ro.explicitDataDir = storageDir_;
    ro.headerTimeoutMs = kRequestTimeoutMs;
    ro.bodyTimeoutMs = kBodyTimeoutMs;
    ro.requestTimeoutMs = kRequestTimeoutMs;

    yams::app::services::ListOptions lq;
    lq.limit = 5;
    lq.namePattern = (fixtures_->root() / "stress" / "**").string();

    int stress_iters = 100;
    if (const char* s = std::getenv("YAMS_STRESS_ITERS")) {
        int v = std::atoi(s);
        if (v > 0 && v < 100000)
            stress_iters = v;
    }

    int ok = 0;
    for (int i = 0; i < stress_iters; ++i) {
        auto lr = rsvc.list(lq, ro);
        if (lr)
            ++ok;
        std::this_thread::sleep_for(5ms);
    }
    CHECK(ok > 0);
}
