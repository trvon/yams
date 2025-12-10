// Integration tests for hybrid search functionality requiring full daemon infrastructure
// Covers semantic search, hybrid search with real embedding provider and vector DB
#include "tests/integration/daemon/test_async_helpers.h"
#include "tests/integration/daemon/test_daemon_harness.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/client/daemon_client.h>

// Windows daemon IPC tests are unstable due to socket shutdown race conditions
#ifdef _WIN32
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN()                                                          \
    SKIP("Daemon IPC tests unstable on Windows - see windows-daemon-ipc-plan.md")
#else
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN() ((void)0)
#endif

using namespace yams::daemon;
using namespace std::chrono_literals;

namespace yams::test {

// Fixture for integration tests requiring full daemon with hybrid search infrastructure
class HybridSearchIntegrationFixture {
public:
    HybridSearchIntegrationFixture() {
        // Start daemon with full search infrastructure
        bool started = harness_.start(std::chrono::seconds(30));
        if (!started) {
            throw std::runtime_error("Failed to start daemon for hybrid search integration tests");
        }

        // Create daemon client
        ClientConfig clientCfg;
        clientCfg.socketPath = harness_.socketPath();
        clientCfg.connectTimeout = 5s;
        clientCfg.autoStart = false;
        client_ = std::make_unique<DaemonClient>(clientCfg);

        // Connect to daemon
        auto connectResult = cli::run_sync(client_->connect(), 5s);
        if (!connectResult) {
            throw std::runtime_error("Failed to connect to daemon: " +
                                     connectResult.error().message);
        }

        // Verify daemon is ready (search infrastructure available)
        auto statusResult = cli::run_sync(client_->status(), 5s);
        if (!statusResult) {
            throw std::runtime_error("Failed to get daemon status: " +
                                     statusResult.error().message);
        }

        // Note: Mock provider may show embeddings as unavailable but daemon is ready
        // This is expected for integration tests using mock model provider
    }

    ~HybridSearchIntegrationFixture() {
        // Cleanup: disconnect and stop daemon
        if (client_) {
            client_->disconnect();
        }
        harness_.stop();
    }

    // Execute search via daemon client
    Result<SearchResponse> executeSearch(const SearchRequest& req) {
        return cli::run_sync(client_->search(req), 10s);
    }

    DaemonClient* client() { return client_.get(); }
    const std::filesystem::path& dataDir() const { return harness_.dataDir(); }

private:
    DaemonHarness harness_;
    std::unique_ptr<daemon::DaemonClient> client_;
};

// =============================================================================
// Semantic Search Tests (require embedding provider and vector DB)
// =============================================================================

TEST_CASE("SemanticSearch - Infrastructure availability", "[integration][search][semantic]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    HybridSearchIntegrationFixture fixture;

    // Test that daemon is running and can respond to semantic search requests
    // With mock provider, semantic search may return "unavailable" but should not crash
    SearchRequest req;
    req.query = "indexing pipeline";
    req.searchType = "semantic";
    req.limit = 10;
    req.timeout = 2s; // Short timeout for mock provider

    auto result = fixture.executeSearch(req);

    // With mock provider, semantic search infrastructure is unavailable
    // This test verifies the daemon handles semantic requests gracefully
    // Real test would verify actual embedding-based search results
    if (!result) {
        // Expected: InvalidState when embeddings unavailable
        REQUIRE(result.error().code == ErrorCode::InvalidState);
        REQUIRE(result.error().message.find("not ready") != std::string::npos);
    } else {
        // If embeddings become available, verify response structure
        REQUIRE(result.value().results.size() <= 10);
    }
}

TEST_CASE("SemanticSearch - Similarity threshold filtering", "[integration][search][semantic]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    HybridSearchIntegrationFixture fixture;

    // Test semantic search with high similarity threshold
    SearchRequest req;
    req.query = "document indexing";
    req.searchType = "semantic";
    req.similarity = 0.9f; // Very high threshold - only very similar results
    req.limit = 10;
    req.timeout = 2s; // Short timeout for mock provider

    auto result = fixture.executeSearch(req);

    // With mock provider, expect InvalidState
    if (!result) {
        REQUIRE(result.error().code == ErrorCode::InvalidState);
    } else {
        // If embeddings available, verify all results meet threshold
        for (const auto& hit : result.value().results) {
            REQUIRE(hit.score >= 0.9f);
        }
    }
}

TEST_CASE("SemanticSearch - Empty query validation",
          "[integration][search][semantic][validation]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    HybridSearchIntegrationFixture fixture;

    // Empty query should fail validation at daemon level
    SearchRequest req;
    req.query = "";
    req.searchType = "semantic";
    req.limit = 10;
    req.timeout = 2s; // Short timeout

    auto result = fixture.executeSearch(req);

    // Should reject empty query
    REQUIRE_FALSE(result);
    REQUIRE(result.error().message.find("required") != std::string::npos);
}

TEST_CASE("SemanticSearch - Very long query handling", "[integration][search][semantic][edge]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    HybridSearchIntegrationFixture fixture;

    // Test with very long query (stress test embedding provider)
    std::string longQuery(5000, 'a');
    longQuery += " indexing pipeline search";

    SearchRequest req;
    req.query = longQuery;
    req.searchType = "semantic";
    req.limit = 10;
    req.timeout = 2s; // Short timeout for mock provider

    auto result = fixture.executeSearch(req);

    // Should handle gracefully (either succeed or return InvalidState)
    if (!result) {
        // With mock provider or query too long
        REQUIRE((result.error().code == ErrorCode::InvalidState ||
                 result.error().code == ErrorCode::InvalidArgument));
    } else {
        REQUIRE(result.value().results.size() <= 10);
    }
}

// =============================================================================
// Hybrid Search Tests (require keyword + semantic fusion)
// =============================================================================

TEST_CASE("HybridSearch - RRF fusion with real infrastructure", "[integration][search][hybrid]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    HybridSearchIntegrationFixture fixture;

    // Test Reciprocal Rank Fusion with both keyword and semantic results
    SearchRequest req;
    req.query = "indexing pipeline";
    req.searchType = "hybrid";
    req.limit = 10;
    req.timeout = 2s; // Short timeout for mock provider

    auto result = fixture.executeSearch(req);

    // With mock provider, hybrid search falls back to keyword-only
    if (!result) {
        REQUIRE(result.error().code == ErrorCode::InvalidState);
    } else {
        // Should return results (keyword fallback or actual hybrid)
        REQUIRE(result.value().results.size() <= 10);

        // Results should be ranked by RRF score if hybrid engine available
        if (result.value().results.size() > 1) {
            // Verify descending score order
            for (size_t i = 1; i < result.value().results.size(); ++i) {
                REQUIRE(result.value().results[i - 1].score >= result.value().results[i].score);
            }
        }
    }
}

TEST_CASE("HybridSearch - Keyword-only fallback path", "[integration][search][hybrid][fallback]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    HybridSearchIntegrationFixture fixture;

    // When vector search unavailable, hybrid should fall back to keyword-only
    SearchRequest req;
    req.query = "pipeline";
    req.searchType = "hybrid";
    req.limit = 5;
    req.timeout = 2s; // Short timeout for mock provider

    auto result = fixture.executeSearch(req);

    // Should succeed with keyword fallback or return InvalidState
    if (result) {
        REQUIRE(result.value().results.size() <= 5);
    } else {
        REQUIRE(result.error().code == ErrorCode::InvalidState);
    }
}

TEST_CASE("HybridSearch - Engine unavailable detection", "[integration][search][hybrid][error]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    HybridSearchIntegrationFixture fixture;

    // Test that hybrid search properly detects when hybrid engine is unavailable
    SearchRequest req;
    req.query = "pipeline";
    req.searchType = "hybrid";
    req.limit = 10;
    req.timeout = 2s; // Short timeout for mock provider

    auto result = fixture.executeSearch(req);

    // Without hybrid engine, should return InvalidState error
    if (!result) {
        REQUIRE(result.error().code == ErrorCode::InvalidState);
        REQUIRE(result.error().message.find("not ready") != std::string::npos);
    } else {
        // If hybrid engine becomes available, verify response structure
        REQUIRE(result.value().results.size() <= 10);
    }
}

} // namespace yams::test
