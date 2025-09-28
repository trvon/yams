#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>

#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/metadata_repository.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

class GrepServiceExpectationsIT : public ::testing::Test {
protected:
    fs::path root_;
    fs::path storageDir_;
    fs::path socketPath_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;

    static bool canBindUnixSocketHere();

    void SetUp() override {
        if (!canBindUnixSocketHere()) {
            GTEST_SKIP() << "Skipping: AF_UNIX not available in this environment.";
        }
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        root_ = fs::temp_directory_path() / ("yams_grep_it_" + unique);
        storageDir_ = root_ / "storage";
        fs::create_directories(storageDir_);
        socketPath_ = fs::path("/tmp") / ("yams-grep-" + unique + ".sock");

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = storageDir_;
        cfg.socketPath = socketPath_;
        cfg.pidFile = root_ / "daemon.pid";
        cfg.logFile = root_ / "daemon.log";
        cfg.enableModelProvider = true;
        cfg.useMockModelProvider = true;
        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);
        auto started = daemon_->start();
        ASSERT_TRUE(started) << started.error().message;
        std::this_thread::sleep_for(150ms);
    }

    void TearDown() override {
        if (daemon_)
            daemon_->stop();
        std::error_code ec;
        fs::remove_all(root_, ec);
    }
};

bool GrepServiceExpectationsIT::canBindUnixSocketHere() {
    try {
        boost::asio::io_context io;
        boost::asio::local::stream_protocol::acceptor acceptor(io);
        auto path = std::filesystem::path("/tmp") /
                    (std::string("yams-grep-probe-") + std::to_string(::getpid()) + ".sock");
        std::error_code ec;
        std::filesystem::remove(path, ec);
        boost::system::error_code bec;
        acceptor.open(boost::asio::local::stream_protocol::endpoint(path.string()).protocol(), bec);
        if (bec)
            return false;
        acceptor.bind(boost::asio::local::stream_protocol::endpoint(path.string()), bec);
        if (bec)
            return false;
        acceptor.close();
        std::filesystem::remove(path, ec);
        return true;
    } catch (...) {
        return false;
    }
}

// A) Basic regex-only pathsOnly with include filter
TEST_F(GrepServiceExpectationsIT, RegexOnlyPathsOnlyWithInclude) {
    fs::create_directories(root_ / "ingest");
    auto fHello = (root_ / "ingest" / "hello.txt");
    auto fSkip = (root_ / "ingest" / "skip.bin");
    std::ofstream(fHello) << "hello grep service";
    std::ofstream(fSkip) << std::string(5, '\0');

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    // Ingest text file individually to capture hash
    opts.path = fHello.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    auto add1 = ing.addViaDaemon(opts);
    ASSERT_TRUE(add1) << (add1 ? "" : add1.error().message);
    // Ingest binary (no effect on grep)
    opts.path = fSkip.string();
    auto add2 = ing.addViaDaemon(opts);
    ASSERT_TRUE(add2);

    auto* sm = daemon_->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);
    // Ensure metadata/FTS is warm for hello.txt
    auto searchSvc = yams::app::services::makeSearchService(ctx);
    (void)searchSvc->lightIndexForHash(add1.value().hash);
    // Poll metadata repo until the file appears (bounded)
    {
        bool visible = false;
        for (int i = 0; i < 40 && !visible; ++i) {
            auto allDocs = ctx.metadataRepo->findDocumentsByPath("%");
            if (allDocs) {
                for (const auto& d : allDocs.value()) {
                    if (d.filePath.find("hello.txt") != std::string::npos) {
                        visible = true;
                        break;
                    }
                }
            }
            if (!visible)
                std::this_thread::sleep_for(50ms);
        }
        ASSERT_TRUE(visible);
    }

    yams::app::services::GrepRequest rq;
    rq.pattern = "hello";
    rq.regexOnly = true;
    rq.pathsOnly = true;
    rq.paths = {(root_ / "ingest").string()};

    yams::Result<yams::app::services::GrepResponse> res =
        yams::Error{yams::ErrorCode::Unknown, "init"};
    bool ready = false;
    for (int i = 0; i < 60 && !ready; ++i) {
        res = grepSvc->grep(rq);
        ASSERT_TRUE(res) << (res ? "" : res.error().message);
        if (!res.value().pathsOnly.empty())
            ready = true;
        else
            std::this_thread::sleep_for(50ms);
    }
    const auto& r = res.value();
    // Expect at least one path ending with hello.txt
    bool hasHello = false;
    for (const auto& p : r.pathsOnly) {
        if (p.find("hello.txt") != std::string::npos)
            hasHello = true;
        EXPECT_EQ(p.find("skip.bin"), std::string::npos);
    }
    EXPECT_TRUE(hasHello);
}

// B) Count mode returns counts per file (structure only)
TEST_F(GrepServiceExpectationsIT, CountModeStructure) {
    fs::create_directories(root_ / "ingest");
    auto fMd = (root_ / "ingest" / "a.md");
    std::ofstream(fMd) << "alpha beta gamma alpha";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = fMd.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    auto add = ing.addViaDaemon(opts);
    ASSERT_TRUE(add) << (add ? "" : add.error().message);

    auto* sm = daemon_->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);
    auto searchSvc2 = yams::app::services::makeSearchService(ctx);
    (void)searchSvc2->lightIndexForHash(add.value().hash);
    // Poll until a.md is visible in metadata
    {
        bool visible = false;
        for (int i = 0; i < 40 && !visible; ++i) {
            auto allDocs = ctx.metadataRepo->findDocumentsByPath("%");
            if (allDocs) {
                for (const auto& d : allDocs.value()) {
                    if (d.filePath.find("a.md") != std::string::npos) {
                        visible = true;
                        break;
                    }
                }
            }
            if (!visible)
                std::this_thread::sleep_for(50ms);
        }
        ASSERT_TRUE(visible);
    }

    yams::app::services::GrepRequest rq;
    rq.pattern = "alpha";
    rq.count = true;
    rq.regexOnly = true;
    rq.paths = {(root_ / "ingest").string()};

    yams::Result<yams::app::services::GrepResponse> res2 =
        yams::Error{yams::ErrorCode::Unknown, "init"};
    bool hasResults = false;
    for (int i = 0; i < 60 && !hasResults; ++i) {
        res2 = grepSvc->grep(rq);
        ASSERT_TRUE(res2) << (res2 ? "" : res2.error().message);
        if (!res2.value().results.empty())
            hasResults = true;
        else
            std::this_thread::sleep_for(50ms);
    }
    const auto& r = res2.value();
    // Structural assertions: results present and matchCount populated for a.md
    ASSERT_FALSE(r.results.empty());
    bool ok = false;
    for (const auto& fr : r.results) {
        if (fr.file.find("a.md") != std::string::npos) {
            EXPECT_GT(fr.matchCount, 0u);
            ok = true;
        }
    }
    EXPECT_TRUE(ok);
}

// C) Negative case: no match → still ok with zero paths
TEST_F(GrepServiceExpectationsIT, NegativeNoMatchPathsOnly) {
    auto* sm = daemon_->getServiceManager();
    auto ctx = sm->getAppContext();
    auto grepSvc = yams::app::services::makeGrepService(ctx);

    yams::app::services::GrepRequest rq;
    rq.pattern = "zzzzzz";
    rq.regexOnly = true;
    rq.pathsOnly = true;
    rq.includePatterns = {(root_ / "ingest" / "**").string()};

    auto res = grepSvc->grep(rq);
    ASSERT_TRUE(res) << (res ? "" : res.error().message);
    const auto& r = res.value();
    EXPECT_TRUE(r.pathsOnly.empty());
}
