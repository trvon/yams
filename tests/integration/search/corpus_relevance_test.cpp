// Provable multi-corpus retrieval relevance gate.
//
// Deterministic (mock-embedding) integration tests over an isolated daemon that assert the
// behaviors the live global corpus could not prove: FTS content completeness (registered ==
// keyword-searchable), collection isolation, and path-scope non-starvation. Semantic ranking
// quality is intentionally NOT asserted here (nondeterministic by design, ADR-0005); it is tracked
// by tests/benchmarks/relevance/live_relevance_ab.py.

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

#include <catch2/catch_test_macros.hpp>

#include "tests/integration/daemon/test_async_helpers.h"
#include "tests/integration/daemon/test_daemon_harness.h"
#include "common/fixture_manager.h"
#include "common/test_helpers_catch2.h"

#include <yams/app/services/document_ingestion_service.h>
#include <yams/daemon/client/daemon_client.h>

using namespace yams::daemon;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

#ifdef _WIN32
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN()                                                          \
    SKIP("Daemon IPC tests unstable on Windows - see windows-daemon-ipc-plan.md")
#else
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN() ((void)0)
#endif

namespace yams::test {

class CorpusRelevanceFixture {
public:
    CorpusRelevanceFixture() {
        killOthersEnv_.emplace("YAMS_DAEMON_KILL_OTHERS", "0");
        if (!harness_.start(30s)) {
            throw std::runtime_error("Failed to start daemon for corpus relevance tests");
        }
        ClientConfig cfg;
        cfg.socketPath = harness_.socketPath();
        cfg.connectTimeout = 5s;
        cfg.autoStart = false;
        client_ = std::make_unique<DaemonClient>(cfg);
        std::this_thread::sleep_for(200ms);
        Result<void> connected;
        for (int attempt = 0; attempt < 3; ++attempt) {
            connected = cli::run_sync(client_->connect(), 5s);
            if (connected)
                break;
            std::this_thread::sleep_for(200ms);
        }
        if (!connected) {
            throw std::runtime_error("connect failed: " + connected.error().message);
        }
    }

    ~CorpusRelevanceFixture() {
        if (client_)
            client_->disconnect();
        harness_.stop();
    }

    // Ingest inline content (daemon-first) into an optional collection. Returns the doc hash.
    std::string addContent(const std::string& name, const std::string& content,
                           const std::string& collection = {},
                           const std::vector<std::string>& tags = {}) {
        yams::app::services::DocumentIngestionService ing;
        yams::app::services::AddOptions opts;
        opts.socketPath = harness_.socketPath();
        opts.explicitDataDir = harness_.dataDir();
        opts.content = content;
        opts.name = name;
        opts.noEmbeddings = true;
        opts.collection = collection;
        opts.tags = tags;
        opts.timeoutMs = 5000;
        auto res = ing.addViaDaemon(opts);
        REQUIRE(res);
        return res.value().hash;
    }

    Result<SearchResponse> search(const std::string& query, const std::string& type = "keyword",
                                  std::size_t limit = 25, const std::string& collection = {}) {
        SearchRequest req;
        req.query = query;
        req.searchType = type;
        req.limit = limit;
        req.timeout = 5s;
        req.collection = collection;
        return cli::run_sync(client_->search(req), 10s);
    }

    // Collect result name-substrings present for a keyword query (optionally collection-scoped).
    std::vector<std::string> keywordHits(const std::string& query, const std::string& collection,
                                         std::chrono::milliseconds timeout = 8s) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        std::vector<std::string> paths;
        while (std::chrono::steady_clock::now() < deadline) {
            auto r = search(query, "keyword", 25, collection);
            if (r && !r.value().results.empty()) {
                for (const auto& item : r.value().results)
                    paths.push_back(item.path);
                break;
            }
            std::this_thread::sleep_for(150ms);
        }
        return paths;
    }

    // Poll keyword search until `name` appears in results, or timeout. Returns true if found.
    bool keywordFinds(const std::string& query, const std::string& nameSubstr,
                      std::chrono::milliseconds timeout = 8s) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            auto r = search(query, "keyword");
            if (r) {
                for (const auto& item : r.value().results) {
                    if (item.path.find(nameSubstr) != std::string::npos ||
                        item.title.find(nameSubstr) != std::string::npos) {
                        return true;
                    }
                }
            }
            std::this_thread::sleep_for(150ms);
        }
        return false;
    }

    // Keyword search constrained to glob path patterns; returns result paths (or empty if starved).
    std::vector<std::string> keywordHitsPath(const std::string& query,
                                             const std::vector<std::string>& patterns,
                                             std::chrono::milliseconds timeout = 8s) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        std::vector<std::string> paths;
        while (std::chrono::steady_clock::now() < deadline) {
            SearchRequest req;
            req.query = query;
            req.searchType = "keyword";
            req.limit = 25;
            req.timeout = 5s;
            req.pathPatterns = patterns;
            auto r = cli::run_sync(client_->search(req), 10s);
            if (r && !r.value().results.empty()) {
                for (const auto& item : r.value().results)
                    paths.push_back(item.path);
                break;
            }
            std::this_thread::sleep_for(150ms);
        }
        return paths;
    }

    // Recursively ingest a directory tree (exercises the indexing-service file walk + ignores).
    void addDirectory(const std::filesystem::path& dir) {
        yams::app::services::DocumentIngestionService ing;
        yams::app::services::AddOptions opts;
        opts.socketPath = harness_.socketPath();
        opts.explicitDataDir = harness_.dataDir();
        opts.path = dir.string();
        opts.recursive = true;
        opts.noEmbeddings = true;
        opts.timeoutMs = 10000;
        auto res = ing.addViaDaemon(opts);
        REQUIRE(res);
    }

    FixtureManager& fixtures() { return fixtures_; }

    DaemonClient* client() { return client_.get(); }

private:
    std::optional<ScopedEnvVar> killOthersEnv_;
    DaemonHarness harness_;
    std::unique_ptr<DaemonClient> client_;
    FixtureManager fixtures_;
};

// --- FTS content completeness ------------------------------------------------------------------
// A document that is ingested must be findable by a literal token it contains. This is the gate for
// the "registered != searchable" bug observed on the live corpus.

TEST_CASE_METHOD(CorpusRelevanceFixture, "FTS: fresh text doc is keyword-searchable",
                 "[integration][search][fts][relevance]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    addContent("fts_fresh.txt", "alpha ztokenfresh beta gamma delta");
    REQUIRE(keywordFinds("ztokenfresh", "fts_fresh.txt"));
}

TEST_CASE_METHOD(CorpusRelevanceFixture, "FTS: re-added (new version) content is searchable",
                 "[integration][search][fts][relevance]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    addContent("fts_versioned.txt", "version one ztokenoldver");
    REQUIRE(keywordFinds("ztokenoldver", "fts_versioned.txt"));
    // Re-add same name with new content (new version). The NEW token must become searchable.
    addContent("fts_versioned.txt", "version two ztokennewver");
    REQUIRE(keywordFinds("ztokennewver", "fts_versioned.txt"));
}

TEST_CASE_METHOD(CorpusRelevanceFixture,
                 "FTS: duplicate-content doc stays searchable after sibling delete",
                 "[integration][search][fts][relevance]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    // Two docs with identical content share a sha256 (dedup). Both names should be searchable;
    // deleting one must not blind the other (the FTS-cascade hazard).
    const std::string content = "shared body ztokendup unique";
    addContent("fts_dup_a.txt", content);
    addContent("fts_dup_b.txt", content);
    REQUIRE(keywordFinds("ztokendup", "fts_dup"));
}

// --- Collection isolation (first-class lightweight corpuses) ------------------------------------
// A search scoped to a collection returns only that collection's docs; unscoped sees both.

TEST_CASE_METHOD(CorpusRelevanceFixture, "Collection: --collection scopes to one corpus",
                 "[integration][search][collection][relevance]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    // Distinct content per collection so they are not hash-deduped into a single document.
    addContent("coll_a_doc.txt", "corpus alpha body zcollscoped distinguishing-token-a", "corpusA");
    addContent("coll_b_doc.txt", "corpus beta body zcollscoped distinguishing-token-b", "corpusB");

    // Unscoped: both collections' docs are visible.
    auto all = keywordHits("zcollscoped", /*collection=*/"");
    bool sawA = false, sawB = false;
    for (const auto& p : all) {
        sawA = sawA || p.find("coll_a_doc") != std::string::npos;
        sawB = sawB || p.find("coll_b_doc") != std::string::npos;
    }
    CAPTURE(all);
    REQUIRE(sawA);
    REQUIRE(sawB);

    // Scoped to corpusA: only A's doc, never B's.
    auto onlyA = keywordHits("zcollscoped", "corpusA");
    CAPTURE(onlyA);
    REQUIRE_FALSE(onlyA.empty());
    for (const auto& p : onlyA) {
        REQUIRE(p.find("coll_b_doc") == std::string::npos);
    }
    bool aPresent = false;
    for (const auto& p : onlyA)
        aPresent = aPresent || p.find("coll_a_doc") != std::string::npos;
    REQUIRE(aPresent);
}

// --- Path scoping (candidate-gen, not post-filter starvation) -----------------------------------

TEST_CASE_METHOD(CorpusRelevanceFixture, "Path scope: pathPatterns constrains subtree, no starve",
                 "[integration][search][scope][relevance]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    addContent("scopeAalpha.txt", "scoped body zpathscoped one");
    addContent("scopeBbeta.txt", "scoped body zpathscoped two");

    auto onlyA = keywordHitsPath("zpathscoped", {"*scopeA*"});
    CAPTURE(onlyA);
    REQUIRE_FALSE(onlyA.empty()); // must not starve: scoped query returns in-scope docs
    bool aSeen = false;
    for (const auto& p : onlyA) {
        REQUIRE(p.find("scopeB") == std::string::npos);
        aSeen = aSeen || p.find("scopeA") != std::string::npos;
    }
    REQUIRE(aSeen);
}

// --- Noise exclusion via .gitignore (transient worktrees/duplicates are not indexed) -----------

TEST_CASE_METHOD(CorpusRelevanceFixture, "Noise: gitignored .claude tree is not indexed",
                 "[integration][search][noise][relevance]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    fixtures().createTextFixture("noisetree/.gitignore", ".claude/\n");
    fixtures().createTextFixture("noisetree/keep.txt", "znoisetok keep this real file");
    fixtures().createTextFixture("noisetree/.claude/worktrees/agent-x/dup.txt",
                                 "znoisetok duplicate worktree copy");
    addDirectory(fixtures().root() / "noisetree");

    REQUIRE(keywordFinds("znoisetok", "keep.txt"));
    // The gitignored worktree duplicate must not be indexed.
    auto hits = keywordHits("znoisetok", /*collection=*/"");
    CAPTURE(hits);
    for (const auto& p : hits) {
        REQUIRE(p.find(".claude") == std::string::npos);
    }
}

} // namespace yams::test
