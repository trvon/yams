#include <gtest/gtest.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>

#include "../daemon/test_async_helpers.h"
#include "../daemon/test_daemon_harness.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/compression/compression_header.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/knowledge_graph_store.h>

// Redefine SKIP_DAEMON_TEST_ON_WINDOWS for gtest (harness header uses Catch2's SKIP)
#ifdef _WIN32
#undef SKIP_DAEMON_TEST_ON_WINDOWS
#define SKIP_DAEMON_TEST_ON_WINDOWS()                                                              \
    GTEST_SKIP() << "Daemon IPC tests unstable on Windows - see windows-daemon-ipc-plan.md"
#endif

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

size_t countOpenFds() {
    std::error_code ec;
    size_t count = 0;
    for (const auto& entry : fs::directory_iterator("/dev/fd", ec)) {
        (void)entry;
        ++count;
    }
    return count;
}

class ScopedEnvVar {
public:
    ScopedEnvVar(std::string key, std::string value) : key_(std::move(key)) {
        const char* existing = std::getenv(key_.c_str());
        if (existing != nullptr) {
            previous_ = std::string(existing);
            hadPrevious_ = true;
        }
#if defined(_WIN32)
        _putenv_s(key_.c_str(), value.c_str());
#else
        setenv(key_.c_str(), value.c_str(), 1);
#endif
    }

    ~ScopedEnvVar() {
#if defined(_WIN32)
        if (hadPrevious_) {
            _putenv_s(key_.c_str(), previous_.c_str());
        } else {
            _putenv_s(key_.c_str(), "");
        }
#else
        if (hadPrevious_) {
            setenv(key_.c_str(), previous_.c_str(), 1);
        } else {
            unsetenv(key_.c_str());
        }
#endif
    }

private:
    std::string key_;
    std::string previous_;
    bool hadPrevious_{false};
};

class CaptureStdout {
public:
    CaptureStdout() : old_(std::cout.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStdout() { std::cout.rdbuf(old_); }

    std::string str() const { return buffer_.str(); }

private:
    std::ostringstream buffer_;
    std::streambuf* old_{nullptr};
};

int runCliCommand(const std::vector<std::string>& args) {
    int rc = 0;
    {
        yams::cli::YamsCLI cli;
        std::vector<char*> argv;
        argv.reserve(args.size());
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        rc = cli.run(static_cast<int>(argv.size()), argv.data());
    }
    yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
    yams::daemon::GlobalIOContext::reset();
    return rc;
}

} // namespace

// This suite mirrors UI/CLI expectations at the app/services layer
// See: docs/delivery/028/artifacts/ui-cli-tests.md

class UiCliExpectationsIT : public ::testing::Test {
protected:
    std::unique_ptr<yams::test::DaemonHarness> harness_;
    std::unique_ptr<ScopedEnvVar> sessionEnvOverride_;
    fs::path root_;
    fs::path storageDir_;
    fs::path socketPath_;

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
        // Skip on Windows - daemon IPC tests are unstable there
        SKIP_DAEMON_TEST_ON_WINDOWS();

        if (!canBindUnixSocketHere()) {
            GTEST_SKIP() << "Skipping: AF_UNIX not available in this environment.";
        }
        GTEST_LOG_(INFO) << "UiCliExpectationsIT SetUp fds(before)=" << countOpenFds();
        sessionEnvOverride_ = std::make_unique<ScopedEnvVar>("YAMS_SESSION_CURRENT", "");
        yams::test::DaemonHarnessOptions options;
        options.isolateState = true;
        harness_ = std::make_unique<yams::test::DaemonHarness>(options);
        // Use 15s timeout for CI environments which can be slower
        ASSERT_TRUE(harness_->start(15s)) << "Failed to start daemon";
        GTEST_LOG_(INFO) << "UiCliExpectationsIT SetUp fds(after)=" << countOpenFds();
        storageDir_ = harness_->dataDir();
        socketPath_ = harness_->socketPath();
        root_ = storageDir_.parent_path();
        fs::create_directories(root_ / "ingest");
        // Allow additional settling time for daemon services to fully initialize
        std::this_thread::sleep_for(100ms);
    }

    void TearDown() override {
        GTEST_LOG_(INFO) << "UiCliExpectationsIT TearDown fds(before)=" << countOpenFds();
        harness_.reset();
        sessionEnvOverride_.reset();
        GTEST_LOG_(INFO) << "UiCliExpectationsIT TearDown fds(after)=" << countOpenFds();
    }

    yams::daemon::ServiceManager* serviceManager() const {
        return harness_ ? harness_->daemon()->getServiceManager() : nullptr;
    }
};

// 6b) Search — pathsOnly with pattern + strict matchAllTags
// Re-enabled with readiness gating and bounded polling to avoid flakes on minimal/degraded runners.
TEST_F(UiCliExpectationsIT, PathsOnlyWithPatternAndMatchAllTagsStrict) {
    fs::create_directories(root_ / "ingest" / "tags2");
    auto p1 = (root_ / "ingest" / "tags2" / "d1.md");
    auto p2 = (root_ / "ingest" / "tags2" / "d2.md");
    std::ofstream(p1) << "hello tags d1";
    std::ofstream(p2) << "hello tags d2";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions a;
    a.socketPath = socketPath_;
    a.explicitDataDir = storageDir_;
    a.recursive = false;
    a.noEmbeddings = true;
    // d1 has both tags A and B
    a.tags = {"docs", "A", "B"};
    a.path = p1.string();
    auto add1 = ing.addViaDaemon(a);
    ASSERT_TRUE(add1) << (add1 ? "" : add1.error().message);
    // d2 has only tag A
    a.tags = {"docs", "A"};
    a.path = p2.string();
    auto add2 = ing.addViaDaemon(a);
    ASSERT_TRUE(add2) << (add2 ? "" : add2.error().message);
    ASSERT_NE(add1.value().hash, add2.value().hash)
        << "Test requires distinct hashes; use distinct file contents.";

    auto* sm = serviceManager();
    ASSERT_NE(sm, nullptr);
    auto ctx = sm->getAppContext();

    // Wait for basic document metadata visibility first
    ASSERT_TRUE(yams::test::waitForDocumentMetadata(ctx.metadataRepo, add1.value().hash, 5000ms))
        << "Document d1 not visible in metadata after ingestion";
    ASSERT_TRUE(yams::test::waitForDocumentMetadata(ctx.metadataRepo, add2.value().hash, 5000ms))
        << "Document d2 not visible in metadata after ingestion";

    auto searchSvc = yams::app::services::makeSearchService(ctx);

    // Best-effort: improve near-term visibility even when hybrid engine is absent.
    (void)searchSvc->lightIndexForHash(add1.value().hash);
    (void)searchSvc->lightIndexForHash(add2.value().hash);

    // Poll metadata visibility for tags on both documents (bounded ~3s)
    auto ensureTagsVisible = [&](const std::string& hash,
                                 const std::vector<std::string>& expected) {
        for (int i = 0; i < 30; ++i) {
            auto dres = ctx.metadataRepo->getDocumentByHash(hash);
            if (dres && dres.value().has_value()) {
                auto di = dres.value().value();
                auto all = ctx.metadataRepo->getAllMetadata(di.id);
                if (all) {
                    bool ok = true;
                    for (const auto& t : expected) {
                        auto it = all.value().find("tag:" + t);
                        if (it == all.value().end()) {
                            ok = false;
                            break;
                        }
                    }
                    if (ok)
                        return true;
                }
            }
            std::this_thread::sleep_for(100ms);
        }
        return false;
    };

    ASSERT_TRUE(ensureTagsVisible(add1.value().hash, {"docs", "A", "B"}));
    ASSERT_TRUE(ensureTagsVisible(add2.value().hash, {"docs", "A"}));

    {
        bool strictReady = false;
        bool lastHasD1 = false;
        bool lastHasD2 = false;
        size_t lastStrictCount = 0;
        std::string lastStrictError;
        for (int i = 0; i < 50; ++i) {
            auto strictDocs = ctx.metadataRepo->findDocumentsByTags({"docs", "A", "B"}, true);
            if (!strictDocs) {
                lastStrictError = strictDocs.error().message;
                std::this_thread::sleep_for(100ms);
                continue;
            }

            bool hasD1 = false;
            bool hasD2 = false;
            for (const auto& doc : strictDocs.value()) {
                if (doc.sha256Hash == add1.value().hash) {
                    hasD1 = true;
                }
                if (doc.sha256Hash == add2.value().hash) {
                    hasD2 = true;
                }
            }

            lastHasD1 = hasD1;
            lastHasD2 = hasD2;
            lastStrictCount = strictDocs.value().size();

            if (hasD1 && !hasD2) {
                strictReady = true;
                break;
            }

            std::this_thread::sleep_for(100ms);
        }

        ASSERT_TRUE(strictReady) << "Strict tag lookup did not converge to {d1 only}. Last error='"
                                 << lastStrictError << "' lastHasD1=" << lastHasD1
                                 << " lastHasD2=" << lastHasD2
                                 << " lastStrictCount=" << lastStrictCount;
    }

    yams::app::services::SearchRequest s;
    s.query = (root_ / "ingest" / "tags2").string();
    s.type = "path";
    s.fuzzy = false;
    s.similarity = 0.6f;
    s.pathsOnly = true;
    const std::string strictPathPattern = (root_ / "ingest" / "tags2" / "**").string();
    s.pathPattern = strictPathPattern;
    s.pathPatterns = {strictPathPattern};
    {
        std::error_code ec;
        auto canonical = fs::weakly_canonical(root_ / "ingest" / "tags2", ec);
        if (!ec && !canonical.empty()) {
            std::string canonicalPattern = (canonical / "**").string();
            if (canonicalPattern != strictPathPattern) {
                s.pathPatterns.push_back(std::move(canonicalPattern));
            }
        }
    }
    s.tags = {"docs", "A", "B"};
    s.matchAllTags = true; // strict: must have all three

    // Ensure both docs become searchable before applying strict tag assertions.
    yams::app::services::SearchRequest warm = s;
    warm.tags.clear();
    warm.matchAllTags = false;
    bool warmReady = false;
    for (int i = 0; i < 80; ++i) {
        auto rr = yams::test_async::res(searchSvc->search(warm), 2s);
        if (rr) {
            for (const auto& p : rr.value().paths) {
                if (p.find("d1.md") != std::string::npos || p.find("d2.md") != std::string::npos) {
                    warmReady = true;
                    break;
                }
            }
            if (warmReady) {
                break;
            }
        }
        std::this_thread::sleep_for(100ms);
    }
    ASSERT_TRUE(warmReady) << "Warmup search did not observe tagged docs in time.";

    // Bounded polling to avoid flakes: strict results may be temporarily empty on cold starts,
    // but they must eventually include d1.md and must never include d2.md.
    bool sawD1 = false;
    bool sawD2 = false;
    for (int i = 0; i < 60 && !sawD1 && !sawD2; ++i) {
        auto r = yams::test_async::res(searchSvc->search(s), 2s);
        ASSERT_TRUE(r) << r.error().message;
        for (const auto& p : r.value().paths) {
            if (p.find("d1.md") != std::string::npos)
                sawD1 = true;
            if (p.find("d2.md") != std::string::npos)
                sawD2 = true;
        }
        if (!sawD1 && !sawD2)
            std::this_thread::sleep_for(100ms);
    }
    EXPECT_TRUE(sawD1) << "Strict matchAllTags should eventually include d1.md.";
    EXPECT_FALSE(sawD2) << "Strict matchAllTags should never include d2.md.";
}

// 6c) Search — pathsOnly with pattern + matchAny tags (tolerant)
TEST_F(UiCliExpectationsIT, PathsOnlyWithPatternAndMatchAnyTags) {
    fs::create_directories(root_ / "ingest" / "tags3");
    auto p1 = (root_ / "ingest" / "tags3" / "d1.md");
    auto p2 = (root_ / "ingest" / "tags3" / "d2.md");
    std::ofstream(p1) << "hello tags";
    std::ofstream(p2) << "hello tags";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions a;
    a.socketPath = socketPath_;
    a.explicitDataDir = storageDir_;
    a.recursive = false;
    a.noEmbeddings = true;
    a.tags = {"docs", "X"};
    a.path = p1.string();
    ASSERT_TRUE(ing.addViaDaemon(a));
    a.tags = {"docs", "Y"};
    a.path = p2.string();
    ASSERT_TRUE(ing.addViaDaemon(a));

    auto* sm = serviceManager();
    ASSERT_NE(sm, nullptr);
    auto ctx = sm->getAppContext();
    auto searchSvc = yams::app::services::makeSearchService(ctx);

    yams::app::services::SearchRequest s;
    s.query = "hello";
    s.fuzzy = true;
    s.similarity = 0.6f;
    s.pathsOnly = true;
    s.pathPattern = (root_ / "ingest" / "tags3" / "**").string();
    s.tags = {"docs", "X"};
    s.matchAllTags = false; // any tag match

    auto r = yams::test_async::res(searchSvc->search(s), 2s);
    ASSERT_TRUE(r) << r.error().message;
    // Tolerant: require zero or more results; when present, paths must fall under pattern
    for (const auto& p : r.value().paths) {
        EXPECT_NE(p.find((root_ / "ingest" / "tags3").string()), std::string::npos);
    }
}

TEST_F(UiCliExpectationsIT, CliSearchJsonIncludesRelationMetadata) {
    fs::create_directories(root_ / "ingest");
    const auto docPath = root_ / "ingest" / "cli_relation_json.txt";
    std::ofstream(docPath) << "cli relation json sentinel";

    yams::app::services::DocumentIngestionService ing;
    yams::app::services::AddOptions opts;
    opts.socketPath = socketPath_;
    opts.explicitDataDir = storageDir_;
    opts.path = docPath.string();
    opts.recursive = false;
    opts.noEmbeddings = true;
    auto addRes = ing.addViaDaemon(opts);
    ASSERT_TRUE(addRes) << (addRes ? "" : addRes.error().message);
    ASSERT_FALSE(addRes.value().hash.empty());

    auto* sm = serviceManager();
    ASSERT_NE(sm, nullptr);
    auto ctx = sm->getAppContext();
    ASSERT_TRUE(yams::test::waitForDocumentMetadata(ctx.metadataRepo, addRes.value().hash, 10000ms))
        << "Document not visible in metadata after ingestion";

    auto docRes = ctx.metadataRepo->getDocumentByHash(addRes.value().hash);
    ASSERT_TRUE(docRes) << docRes.error().message;
    ASSERT_TRUE(docRes.value().has_value());
    const auto& doc = *docRes.value();

    ASSERT_NE(ctx.kgStore, nullptr);
    yams::metadata::KGNode fileNode;
    fileNode.nodeKey = "path:file:" + doc.filePath;
    fileNode.type = std::string("file");
    fileNode.label = doc.fileName;
    auto fileNodeId = ctx.kgStore->upsertNode(fileNode);
    ASSERT_TRUE(fileNodeId) << fileNodeId.error().message;

    yams::metadata::KGNode docNode;
    docNode.nodeKey = "doc:" + addRes.value().hash;
    docNode.type = std::string("document");
    docNode.label = doc.fileName;
    auto docNodeId = ctx.kgStore->upsertNode(docNode);
    ASSERT_TRUE(docNodeId) << docNodeId.error().message;

    yams::metadata::KGNode symbolOne;
    symbolOne.nodeKey = "symbol:cli:json:one:" + addRes.value().hash;
    symbolOne.type = std::string("symbol");
    symbolOne.label = std::string("CliJsonOne");
    auto symbolOneId = ctx.kgStore->upsertNode(symbolOne);
    ASSERT_TRUE(symbolOneId) << symbolOneId.error().message;

    yams::metadata::KGNode symbolTwo;
    symbolTwo.nodeKey = "symbol:cli:json:two:" + addRes.value().hash;
    symbolTwo.type = std::string("symbol");
    symbolTwo.label = std::string("CliJsonTwo");
    auto symbolTwoId = ctx.kgStore->upsertNode(symbolTwo);
    ASSERT_TRUE(symbolTwoId) << symbolTwoId.error().message;

    yams::metadata::KGEdge versionEdge;
    versionEdge.srcNodeId = fileNodeId.value();
    versionEdge.dstNodeId = docNodeId.value();
    versionEdge.relation = "has-version";
    ASSERT_TRUE(ctx.kgStore->addEdge(versionEdge));

    yams::metadata::KGEdge definesOne;
    definesOne.srcNodeId = docNodeId.value();
    definesOne.dstNodeId = symbolOneId.value();
    definesOne.relation = "defines";
    ASSERT_TRUE(ctx.kgStore->addEdge(definesOne));

    yams::metadata::KGEdge definesTwo;
    definesTwo.srcNodeId = docNodeId.value();
    definesTwo.dstNodeId = symbolTwoId.value();
    definesTwo.relation = "defines";
    ASSERT_TRUE(ctx.kgStore->addEdge(definesTwo));

    auto searchSvc = yams::app::services::makeSearchService(ctx);
    (void)searchSvc->lightIndexForHash(addRes.value().hash);
    yams::app::services::SearchRequest pollReq;
    pollReq.query = "json sentinel";
    pollReq.type = "keyword";
    pollReq.pathsOnly = true;
    pollReq.limit = 1;
    for (int i = 0; i < 40; ++i) {
        auto pollRes = yams::test_async::res(searchSvc->search(pollReq), 1s);
        if (pollRes && !pollRes.value().paths.empty())
            break;
        std::this_thread::sleep_for(100ms);
    }

    ScopedEnvVar socketEnv("YAMS_DAEMON_SOCKET", socketPath_.string());
    ScopedEnvVar noAutoStart("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1");
    CaptureStdout capture;
    int rc =
        runCliCommand({"yams", "--data-dir", storageDir_.string(), "search", "--type", "keyword",
                       "--no-group-versions", "--json", "--limit", "5", "json sentinel"});
    EXPECT_EQ(rc, 0);

    auto parsed = nlohmann::json::parse(capture.str(), nullptr, false);
    ASSERT_FALSE(parsed.is_discarded()) << capture.str();
    ASSERT_TRUE(parsed.contains("results"));
    ASSERT_TRUE(parsed["results"].is_array());

    bool foundDoc = false;
    for (const auto& result : parsed["results"]) {
        const std::string path = result.value("path", "");
        if (path.find("cli_relation_json.txt") == std::string::npos) {
            continue;
        }
        foundDoc = true;
        ASSERT_TRUE(result.contains("relation_count"));
        EXPECT_GE(result.value("relation_count", 0), 1);
        ASSERT_TRUE(result.contains("relations"));
        EXPECT_NE(result.value("relations", std::string{}).find("defines"), std::string::npos);
    }
    EXPECT_TRUE(foundDoc) << capture.str();
}
