#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <gtest/gtest.h>

#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class ServicesRetrievalIngestionIT : public ::testing::Test {
protected:
    fs::path testRoot_;
    fs::path storageDir_;
    fs::path xdgRuntimeDir_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;

    void SetUp() override {
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        testRoot_ = fs::temp_directory_path() / ("yams_services_it_" + unique);
        storageDir_ = testRoot_ / "storage";
        xdgRuntimeDir_ = testRoot_ / "xdg";
        fs::create_directories(storageDir_);
        fs::create_directories(xdgRuntimeDir_);

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = storageDir_;
        cfg.socketPath = xdgRuntimeDir_ / "yams-daemon.sock";
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
        std::error_code ec;
        fs::remove_all(testRoot_, ec);
    }
};

TEST_F(ServicesRetrievalIngestionIT, AddViaDaemonAndListGetGrep) {
    using yams::app::services::DocumentIngestionService;
    using yams::app::services::RetrievalOptions;
    using yams::app::services::RetrievalService;

    // 1) Create a small file to add
    fs::path filePath = testRoot_ / "hello.txt";
    {
        std::ofstream f(filePath);
        f << "hello yams services";
        f.close();
    }

    // 2) Add via DocumentIngestionService (daemon-first)
    DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    // Ensure we target this test's daemon socket
    opts.socketPath = xdgRuntimeDir_ / "yams-daemon.sock";
    opts.path = filePath.string();
    opts.recursive = false;
    opts.noEmbeddings = true; // avoid model work in IT
    // Point explicitly to storage used by daemon
    opts.explicitDataDir = storageDir_;
    auto addRes = ing.addViaDaemon(opts);
    ASSERT_TRUE(addRes) << (addRes ? "" : addRes.error().message);
    ASSERT_FALSE(addRes.value().hash.empty());

    // 3) List via RetrievalService
    RetrievalService rsvc;
    RetrievalOptions ropts;
    // Pin to test daemon socket and storage
    ropts.socketPath = xdgRuntimeDir_ / "yams-daemon.sock";
    ropts.explicitDataDir = storageDir_;
    yams::daemon::ListRequest lreq;
    lreq.limit = 100;
    auto lres = rsvc.list(lreq, ropts);
    ASSERT_TRUE(lres) << (lres ? "" : lres.error().message);
    bool found = false;
    for (const auto& e : lres.value().items) {
        if (e.name == "hello.txt") {
            found = true;
            break;
        }
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
    auto gpres = rsvc.grep(gpreq, ropts);
    ASSERT_TRUE(gpres) << (gpres ? "" : gpres.error().message);

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
    opts.socketPath = xdgRuntimeDir_ / "yams-daemon.sock";
    opts.path = (testRoot_ / "ingest").string();
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

    // List entries under our path; ensure tags are present and excluded file absent
    RetrievalService rsvc;
    RetrievalOptions ropts;
    ropts.socketPath = xdgRuntimeDir_ / "yams-daemon.sock";
    ropts.explicitDataDir = storageDir_;

    yams::daemon::ListRequest lreq;
    lreq.limit = 50;
    // Filter to our directory by pattern
    lreq.namePattern = (testRoot_ / "ingest" / "**").string();
    lreq.showTags = true;
    lreq.showMetadata = true;
    auto lres = rsvc.list(lreq, ropts);
    ASSERT_TRUE(lres) << (lres ? "" : lres.error().message);

    bool sawCpp = false, sawMd = false, sawBin = false;
    for (const auto& e : lres.value().items) {
        if (e.name == "keep.cpp") {
            sawCpp = true;
            // Verify tags carried through
            EXPECT_NE(std::find(e.tags.begin(), e.tags.end(), std::string("code")), e.tags.end());
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
    EXPECT_TRUE(sawCpp);
    EXPECT_TRUE(sawMd);
    EXPECT_FALSE(sawBin);

    // Grep should find content only in included files
    yams::daemon::GrepRequest gpreq;
    gpreq.pattern = "hello";
    gpreq.pathsOnly = true;
    gpreq.includePatterns = {(testRoot_ / "ingest" / "**").string()};
    auto gpres = rsvc.grep(gpreq, ropts);
    ASSERT_TRUE(gpres) << (gpres ? "" : gpres.error().message);
    // At least the markdown file should match
    bool grepFoundMd = false;
    for (const auto& m : gpres.value().matches) {
        if (m.file.find("keep.md") != std::string::npos) {
            grepFoundMd = true;
            break;
        }
    }
    EXPECT_TRUE(grepFoundMd);
}
