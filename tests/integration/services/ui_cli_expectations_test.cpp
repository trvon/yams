#include <gtest/gtest.h>

#include <chrono>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>

#include "../daemon/test_async_helpers.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

// This suite mirrors UI/CLI expectations at the app/services layer
// See: docs/delivery/028/artifacts/ui-cli-tests.md

class UiCliExpectationsIT : public ::testing::Test {
protected:
    fs::path root_;
    fs::path storageDir_;
    fs::path socketPath_;
    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;

    static bool canBindUnixSocketHere() {
        try {
            boost::asio::io_context io;
            boost::asio::local::stream_protocol::acceptor acceptor(io);
            auto path = std::filesystem::path("/tmp") /
                        (std::string("yams-ui-cli-") + std::to_string(::getpid()) + ".sock");
            std::error_code ec;
            std::filesystem::remove(path, ec);
            boost::system::error_code bec;
            acceptor.open(boost::asio::local::stream_protocol::endpoint(path.string()).protocol(),
                          bec);
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

    void SetUp() override {
        if (!canBindUnixSocketHere()) {
            GTEST_SKIP() << "Skipping: AF_UNIX not available in this environment.";
        }
        auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        root_ = fs::temp_directory_path() / ("yams_ui_cli_it_" + unique);
        storageDir_ = root_ / "storage";
        fs::create_directories(storageDir_);
        socketPath_ = fs::path("/tmp") / ("yams-ui-cli-" + unique + ".sock");

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = storageDir_;
        cfg.socketPath = socketPath_;
        cfg.pidFile = root_ / "daemon.pid";
        cfg.logFile = root_ / "daemon.log";
        // Use mock embeddings to keep deterministic and fast
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

// 1) Grep — basic text match, paths only, include filter and tag filter
TEST_F(UiCliExpectationsIT, GrepPathsOnlyHonorsIncludeAndTags) {
    // Create a small directory tree with tags
    fs::create_directories(root_ / "ingest" / "dirA" / "dirB");
    std::ofstream(root_ / "ingest" / "dirA" / "dirB" / "keep.md")
        << "# Title\nhello pattern tags\n";
    std::ofstream(root_ / "ingest" / "dirA" / "dirB" / "skip.bin") << std::string(3, '\0');

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = (root_ / "ingest").string();
    opts.recursive = true;
    opts.includePatterns = {"*.md", "*.bin"};
    opts.excludePatterns = {};
    opts.tags = {"docs", "md", "ingest"};
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    ASSERT_TRUE(addRes) << (addRes ? "" : addRes.error().message);

    // Wait for post-ingest to settle
    std::this_thread::sleep_for(200ms);

    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ropts;
    ropts.socketPath = socketPath_;
    ropts.explicitDataDir = storageDir_;

    yams::daemon::GrepRequest gpreq;
    gpreq.pattern = "hello";
    gpreq.pathsOnly = true;
    gpreq.includePatterns = {(root_ / "ingest" / "**").string()};
    gpreq.filterTags = {"docs", "md"};
    gpreq.matchAllTags = true;

    // Allow brief retry window for metadata visibility
    bool ok = false;
    for (int i = 0; i < 60 && !ok; ++i) {
        auto gpres = rsvc.grep(gpreq, ropts);
        ASSERT_TRUE(gpres) << (gpres ? "" : gpres.error().message);
        for (const auto& m : gpres.value().matches) {
            if (m.file.find("keep.md") != std::string::npos) {
                ok = true;
                break;
            }
        }
        if (!ok)
            std::this_thread::sleep_for(50ms);
    }
    EXPECT_TRUE(ok);
}

// 2) Get — metadataOnly excludes content
TEST_F(UiCliExpectationsIT, GetByHashMetadataOnlyHasNoContent) {
    // Create and ingest a small text file
    fs::create_directories(root_ / "ingest");
    std::ofstream(root_ / "ingest" / "hello.txt") << "hello yams";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = (root_ / "ingest" / "hello.txt").string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    ASSERT_TRUE(addRes) << (addRes ? "" : addRes.error().message);
    ASSERT_FALSE(addRes.value().hash.empty());

    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ropts;
    ropts.socketPath = socketPath_;
    ropts.explicitDataDir = storageDir_;

    yams::daemon::GetRequest greq;
    greq.hash = addRes.value().hash;
    greq.metadataOnly = true;
    auto gres = rsvc.get(greq, ropts);
    ASSERT_TRUE(gres) << (gres ? "" : gres.error().message);
    EXPECT_FALSE(gres.value().hasContent);
    EXPECT_TRUE(gres.value().content.empty());
}

// 3) List — limit + namePattern (structure-focused)
TEST_F(UiCliExpectationsIT, ListLimitAndNamePattern) {
    // Arrange: create mixed files
    fs::create_directories(root_ / "ingest" / "docs");
    std::ofstream(root_ / "ingest" / "docs" / "a.md") << "alpha";
    std::ofstream(root_ / "ingest" / "docs" / "b.md") << "bravo";
    std::ofstream(root_ / "ingest" / "docs" / "c.txt") << "charlie";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = (root_ / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    ASSERT_TRUE(addRes) << (addRes ? "" : addRes.error().message);

    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ropts;
    ropts.socketPath = socketPath_;
    ropts.explicitDataDir = storageDir_;

    yams::daemon::ListRequest lreq;
    lreq.limit = 2; // enforce limit
    lreq.namePattern = (root_ / "ingest" / "**" / "*.md").string();
    // Act/Assert with brief retries for visibility
    bool ok = false;
    for (int i = 0; i < 60 && !ok; ++i) {
        auto lres = rsvc.list(lreq, ropts);
        ASSERT_TRUE(lres) << (lres ? "" : lres.error().message);
        const auto& items = lres.value().items;
        if (!items.empty()) {
            EXPECT_LE(items.size(), static_cast<size_t>(2));
            // All returned entries must be .md
            bool allMd = true;
            for (const auto& e : items) {
                if (e.name.rfind(".md") == std::string::npos) {
                    allMd = false;
                    break;
                }
            }
            EXPECT_TRUE(allMd);
            ok = allMd;
        }
        if (!ok)
            std::this_thread::sleep_for(50ms);
    }
    EXPECT_TRUE(ok);
}

// 4) Search — fuzzy pathsOnly (service-layer)
TEST_F(UiCliExpectationsIT, FuzzySearchPathsOnly) {
    // Arrange: create two small text docs
    fs::create_directories(root_ / "ingest");
    std::ofstream(root_ / "ingest" / "alpha.txt") << "functional programming in yams";
    std::ofstream(root_ / "ingest" / "beta.txt") << "systems programming in c++";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = (root_ / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    ASSERT_TRUE(addRes) << (addRes ? "" : addRes.error().message);

    // Build AppContext and SearchService
    auto* sm = daemon_->getServiceManager();
    ASSERT_NE(sm, nullptr);
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    // Ensure light indexing for new docs to enable fuzzy search immediately
    // Not fatal if skipped for non-text types
    (void)searchSvc->lightIndexForHash(addRes.value().hash);

    yams::app::services::SearchRequest sreq;
    sreq.query = "programing"; // misspelled to exercise fuzzy
    sreq.fuzzy = true;
    sreq.similarity = 0.6f;
    sreq.limit = 10;
    sreq.pathsOnly = true;
    sreq.pathPattern = (root_ / "ingest" / "**").string();

    auto result = yams::test_async::res(searchSvc->search(sreq), 2s);
    ASSERT_TRUE(result) << result.error().message;
    const auto& resp = result.value();
    // Expect at least one path, and all paths under our ingest root
    ASSERT_FALSE(resp.paths.empty());
    for (const auto& p : resp.paths) {
        EXPECT_NE(p.find((root_ / "ingest").string()), std::string::npos);
    }
}

// 5) Search — verbose hybrid includes score breakdowns (when hybrid engine available)
TEST_F(UiCliExpectationsIT, VerboseHybridIncludesScoresWhenAvailable) {
    // Arrange: small docs
    fs::create_directories(root_ / "ingest");
    std::ofstream(root_ / "ingest" / "note1.txt") << "semantic vector keyword";
    std::ofstream(root_ / "ingest" / "note2.txt") << "vector search hybrid";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = (root_ / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true; // indexing focus
    ASSERT_TRUE(ing.addViaDaemon(opts));

    auto* sm = daemon_->getServiceManager();
    ASSERT_NE(sm, nullptr);
    // Poll for hybrid engine readiness
    std::shared_ptr<yams::search::HybridSearchEngine> snap;
    for (int i = 0; i < 60 && !snap; ++i) {
        snap = sm->getSearchEngineSnapshot();
        if (!snap)
            std::this_thread::sleep_for(50ms);
    }
    if (!snap) {
        GTEST_SKIP() << "Hybrid engine not ready; skipping verbose hybrid structure check.";
    }

    auto ctx = sm->getAppContext();
    ctx.hybridEngine = snap;            // ensure available
    ctx.searchRepairInProgress = false; // clear degraded flag
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest sreq;
    sreq.query = "vector";
    sreq.type = "hybrid";
    sreq.limit = 10;
    sreq.verbose = true;

    auto result = yams::test_async::res(searchSvc->search(sreq), 2s);
    ASSERT_TRUE(result) << result.error().message;
    const auto& resp = result.value();
    // If hybrid produced results, at least one item should have score breakdown fields
    bool hasBreakdown = false;
    for (const auto& it : resp.results) {
        if (it.vectorScore.has_value() || it.keywordScore.has_value() ||
            it.kgEntityScore.has_value() || it.structuralScore.has_value()) {
            hasBreakdown = true;
            break;
        }
    }
    EXPECT_TRUE(hasBreakdown);
}

// 6) Search — pathsOnly with pathPattern + tag filter combined
TEST_F(UiCliExpectationsIT, PathsOnlyWithPatternAndTags) {
    fs::create_directories(root_ / "ingest" / "d");
    auto mdPath = (root_ / "ingest" / "d" / "doc.md");
    auto txtPath = (root_ / "ingest" / "d" / "other.txt");
    std::ofstream(mdPath) << "hello tags";
    std::ofstream(txtPath) << "skip";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    // Ingest doc.md with tags
    opts.path = mdPath.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    opts.tags = {"docs", "md"};
    ASSERT_TRUE(ing.addViaDaemon(opts));
    // Ingest other.txt without those tags
    yams::app::services::AddOptions opts2 = opts;
    opts2.path = txtPath.string();
    opts2.tags = {};
    ASSERT_TRUE(ing.addViaDaemon(opts2));

    auto* sm = daemon_->getServiceManager();
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest sreq;
    sreq.query = "hello";
    sreq.fuzzy = true;
    sreq.similarity = 0.6f;
    sreq.pathsOnly = true;
    sreq.limit = 10;
    sreq.pathPattern = (root_ / "ingest" / "**").string();
    sreq.tags = {"docs", "md"};
    sreq.matchAllTags = true;
    sreq.extension = "md"; // combine with pathPattern for stricter filtering

    auto result = yams::test_async::res(searchSvc->search(sreq), 2s);
    ASSERT_TRUE(result) << result.error().message;
    const auto& resp = result.value();
    // Should not include other.txt; may be empty depending on fuzzy/tokenization
    for (const auto& p : resp.paths) {
        EXPECT_EQ(p.find("other.txt"), std::string::npos);
    }
}

// 7) Search — negative case (no match ⇒ empty paths, no error)
TEST_F(UiCliExpectationsIT, NegativeNoMatchPathsOnly) {
    auto* sm = daemon_->getServiceManager();
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest sreq;
    sreq.query = "stringthatwillnotmatch";
    sreq.fuzzy = true;
    sreq.similarity = 0.8f;
    sreq.pathsOnly = true;
    sreq.limit = 5;
    sreq.pathPattern = (root_ / "ingest" / "**").string();

    auto result = yams::test_async::res(searchSvc->search(sreq), 2s);
    ASSERT_TRUE(result) << result.error().message;
    const auto& resp = result.value();
    EXPECT_TRUE(resp.paths.empty());
}

// 8) Search — fuzzy bounds (similarity extremes) structure and monotonicity
TEST_F(UiCliExpectationsIT, FuzzyBoundsSimilarityZeroAndOne) {
    fs::create_directories(root_ / "ingest" / "fuzzy");
    std::ofstream(root_ / "ingest" / "fuzzy" / "exact.txt") << "functional programming";
    std::ofstream(root_ / "ingest" / "fuzzy" / "near.txt") << "functionl programing"; // typos

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = (root_ / "ingest").string();
    opts.recursive = true;
    opts.noEmbeddings = true;
    ASSERT_TRUE(ing.addViaDaemon(opts));

    auto* sm = daemon_->getServiceManager();
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    // Light index the batch (best-effort)
    // We don't have hashes here; rely on post-ingest + brief delay
    std::this_thread::sleep_for(150ms);

    yams::app::services::SearchRequest low;
    low.query = "programming";
    low.fuzzy = true;
    low.similarity = 0.0f;
    low.pathsOnly = true;
    low.limit = 50;
    low.pathPattern = (root_ / "ingest" / "**").string();

    yams::app::services::SearchRequest high = low;
    high.similarity = 1.0f;

    auto rLow = yams::test_async::res(searchSvc->search(low), 2s);
    ASSERT_TRUE(rLow) << rLow.error().message;
    auto rHigh = yams::test_async::res(searchSvc->search(high), 2s);
    ASSERT_TRUE(rHigh) << rHigh.error().message;

    // With a very permissive similarity, expect a superset or equal number of paths
    EXPECT_GE(rLow.value().paths.size(), rHigh.value().paths.size());
    // High similarity should still return exact match when present
    bool highHasExact = false;
    for (const auto& p : rHigh.value().paths) {
        if (p.find("exact.txt") != std::string::npos) {
            highHasExact = true;
            break;
        }
    }
    EXPECT_TRUE(highHasExact);
}

// 9) Search — tag filters: matchAny vs matchAll
TEST_F(UiCliExpectationsIT, TagFilterMatchAnyVsAll) {
    fs::create_directories(root_ / "ingest" / "tags");
    std::ofstream(root_ / "ingest" / "tags" / "d1.md") << "hello tags";
    std::ofstream(root_ / "ingest" / "tags" / "d2.txt") << "hello tags";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions a;
    a.socketPath = socketPath_;
    a.explicitDataDir = storageDir_;
    a.recursive = false;
    a.noEmbeddings = true;
    auto p1 = (root_ / "ingest" / "tags" / "d1.md").string();
    auto p2 = (root_ / "ingest" / "tags" / "d2.txt").string();
    // Ingest individually and capture hashes
    a.tags = {"docs", "md"};
    a.path = p1;
    auto add1 = ing.addViaDaemon(a);
    ASSERT_TRUE(add1) << (add1 ? "" : add1.error().message);
    a.tags = {"docs", "txt"};
    a.path = p2;
    auto add2 = ing.addViaDaemon(a);
    ASSERT_TRUE(add2) << (add2 ? "" : add2.error().message);

    auto* sm = daemon_->getServiceManager();
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);
    // Ensure immediate visibility for small text docs
    (void)searchSvc->lightIndexForHash(add1.value().hash);
    (void)searchSvc->lightIndexForHash(add2.value().hash);
    std::this_thread::sleep_for(100ms);

    yams::app::services::SearchRequest anyReq;
    anyReq.query = "hello";
    anyReq.fuzzy = true;
    anyReq.similarity = 0.6f;
    anyReq.pathsOnly = true;
    anyReq.limit = 10;
    anyReq.pathPattern = (root_ / "ingest" / "**").string();
    anyReq.tags = {"docs", "md"};
    anyReq.matchAllTags = false; // match any

    yams::app::services::SearchRequest allReq = anyReq;
    allReq.matchAllTags = true; // require both
    allReq.extension = "md";

    auto rAny = yams::test_async::res(searchSvc->search(anyReq), 2s);
    ASSERT_TRUE(rAny) << rAny.error().message;
    auto rAll = yams::test_async::res(searchSvc->search(allReq), 2s);
    ASSERT_TRUE(rAll) << rAll.error().message;

    // allReq should only include the .md doc
    bool allOnlyMd = true;
    for (const auto& p : rAll.value().paths) {
        if (p.find("d1.md") == std::string::npos) {
            allOnlyMd = false;
            break;
        }
    }
    EXPECT_TRUE(allOnlyMd);

    // anyReq should include at least the md doc; may include txt as well
    bool anyHasMd = false;
    for (const auto& p : rAny.value().paths) {
        if (p.find("d1.md") != std::string::npos) {
            anyHasMd = true;
            break;
        }
    }
    EXPECT_TRUE(anyHasMd);
}

// 10) Search — negative with tag mismatch under pathsOnly
TEST_F(UiCliExpectationsIT, NegativeTagMismatchPathsOnly) {
    fs::create_directories(root_ / "ingest" / "neg");
    std::ofstream(root_ / "ingest" / "neg" / "z.txt") << "content";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions a;
    a.socketPath = socketPath_;
    a.explicitDataDir = storageDir_;
    a.path = (root_ / "ingest" / "neg" / "z.txt").string();
    a.recursive = false;
    a.noEmbeddings = true;
    a.tags = {"misc"};
    ASSERT_TRUE(ing.addViaDaemon(a));

    auto* sm = daemon_->getServiceManager();
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest s;
    s.query = "content";
    s.fuzzy = true;
    s.similarity = 0.6f;
    s.pathsOnly = true;
    s.limit = 5;
    s.pathPattern = (root_ / "ingest" / "**").string();
    s.tags = {"does_not_exist"};
    s.matchAllTags = true;

    auto r = yams::test_async::res(searchSvc->search(s), 2s);
    ASSERT_TRUE(r) << r.error().message;
    EXPECT_TRUE(r.value().paths.empty());
}
