// Integration tests for semantic/hybrid daemon request availability and model-free lexical
// fallback through the public hybrid search request.
#include "tests/integration/daemon/test_async_helpers.h"
#include "tests/integration/daemon/test_daemon_harness.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/metadata_repository.h>

#include <array>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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
    struct SeededDocument {
        std::string hash;
        std::string path;
    };

    explicit HybridSearchIntegrationFixture(bool enableModelProvider = true)
        : harness_(makeHarnessOptions(enableModelProvider)) {
        // Start daemon with the requested search infrastructure.
        bool started = harness_.start(std::chrono::seconds(30));
        if (!started) {
            throw std::runtime_error("Failed to start daemon for hybrid search integration tests");
        }

        // Create daemon client
        ClientConfig clientCfg;
        clientCfg.socketPath = harness_.socketPath();
        clientCfg.connectTimeout = 5s;
        clientCfg.autoStart = false;
        spdlog::info("[HybridSearchFixture] Creating DaemonClient for socket: {}",
                     clientCfg.socketPath.string());
        client_ = std::make_unique<DaemonClient>(clientCfg);

        // Initial delay for GlobalIOContext threads to stabilize
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        // Connect to daemon with retry logic for GlobalIOContext timing
        spdlog::info("[HybridSearchFixture] Calling client_->connect()...");
        Result<void> connectResult;
        constexpr int maxRetries = 3;
        for (int attempt = 0; attempt < maxRetries; ++attempt) {
            connectResult = cli::run_sync(client_->connect(), 5s);
            if (connectResult) {
                break;
            }
            spdlog::warn("[HybridSearchFixture] connect() attempt {} failed: {}, retrying...",
                         attempt + 1, connectResult.error().message);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        if (!connectResult) {
            spdlog::error("[HybridSearchFixture] connect() failed after {} attempts: {}",
                          maxRetries, connectResult.error().message);
            throw std::runtime_error("Failed to connect to daemon: " +
                                     connectResult.error().message);
        }
        spdlog::info("[HybridSearchFixture] connect() succeeded");

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

    std::vector<SeededDocument> seedModelFreeHybridCorpus() {
        struct FixtureSpec {
            std::string name;
            std::string content;
            std::vector<std::string> tags;
        };

        const std::array<FixtureSpec, 3> fixtures{{
            {"hybrid_red_alpha.txt",
             "hybridfallbacksentinel red alpha alpha alpha document",
             {"red", "shared"}},
            {"hybrid_red_beta.txt",
             "hybridfallbacksentinel red beta beta document",
             {"red", "shared"}},
            {"hybrid_blue_gamma.txt",
             "hybridfallbacksentinel blue gamma document",
             {"blue", "shared"}},
        }};

        auto* daemon = harness_.daemon();
        REQUIRE(daemon != nullptr);
        auto* serviceManager = daemon->getServiceManager();
        REQUIRE(serviceManager != nullptr);
        auto metadataRepo = serviceManager->getMetadataRepo();
        REQUIRE(metadataRepo != nullptr);

        std::vector<SeededDocument> seeded;
        seeded.reserve(fixtures.size());
        for (const auto& fixture : fixtures) {
            const auto path = harness_.dataDir() / fixture.name;
            std::ofstream output(path, std::ios::binary | std::ios::trunc);
            REQUIRE(output.is_open());
            output << fixture.content;
            output.close();
            REQUIRE(output.good());

            AddDocumentRequest request;
            request.path = path.string();
            request.name = fixture.name;
            request.content = fixture.content;
            request.tags = fixture.tags;
            request.noEmbeddings = true;
            request.waitForProcessing = true;

            auto addResult = cli::run_sync(client_->streamingAddDocument(request), 10s);
            REQUIRE(addResult);
            REQUIRE_FALSE(addResult.value().hash.empty());

            auto documentResult = metadataRepo->getDocumentByHash(addResult.value().hash);
            REQUIRE(documentResult);
            REQUIRE(documentResult.value().has_value());
            const auto& document = *documentResult.value();

            auto indexResult = metadataRepo->indexDocumentContent(document.id, fixture.name,
                                                                  fixture.content, "text/plain");
            REQUIRE(indexResult);

            REQUIRE(document.fileName == fixture.name);
            REQUIRE(document.sha256Hash == addResult.value().hash);
            const auto expectedPath = std::filesystem::canonical(path).string();
            REQUIRE(document.filePath == expectedPath);
            seeded.push_back({addResult.value().hash, expectedPath});
        }
        return seeded;
    }

private:
    static DaemonHarness::Options makeHarnessOptions(bool enableModelProvider) {
        DaemonHarness::Options options;
        options.enableModelProvider = enableModelProvider;
        options.useMockModelProvider = enableModelProvider;
        options.autoLoadPlugins = false;
        if (!enableModelProvider) {
            options.isolateEnvironment = true;
            options.configureDaemon = [](DaemonConfig& config) {
                config.enableModelProvider = false;
                config.modelProviderRequired = false;
                config.autoLoadPlugins = false;
                config.downloadPolicy.enable = false;
            };
        }
        return options;
    }

    DaemonHarness harness_;
    std::unique_ptr<daemon::DaemonClient> client_;
};

// =============================================================================
// Semantic Search Availability Tests
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

TEST_CASE("SemanticSearch - Similarity option availability smoke",
          "[integration][search][semantic]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    HybridSearchIntegrationFixture fixture;

    // Exercise a semantic request carrying a high similarity option. With no seeded corpus this
    // checks request handling, not retrieval quality or threshold recall.
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
        // Preserve the optional semantic-provider behavior when one is available.
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
// Hybrid Search Tests
// =============================================================================

TEST_CASE("HybridSearch - Request availability smoke", "[integration][search][hybrid]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    HybridSearchIntegrationFixture fixture;

    // Exercise the public hybrid request. The unseeded corpus cannot establish two-leg fusion.
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
        // If the infrastructure is available, verify only the public response ordering contract.
        REQUIRE(result.value().results.size() <= 10);
        for (size_t i = 1; i < result.value().results.size(); ++i) {
            REQUIRE(result.value().results[i - 1].score >= result.value().results[i].score);
        }
    }
}

TEST_CASE("HybridSearch - Keyword fallback availability smoke",
          "[integration][search][hybrid][fallback]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    HybridSearchIntegrationFixture fixture;

    // Exercise the request when vector search may be unavailable. With no seeded corpus this is
    // only an availability smoke for the keyword fallback route.
    SearchRequest req;
    req.query = "pipeline";
    req.searchType = "hybrid";
    req.limit = 5;
    req.timeout = 2s; // Short timeout for mock provider

    auto result = fixture.executeSearch(req);

    // Accept either the configured fallback response or an explicit unavailable response.
    if (result) {
        REQUIRE(result.value().results.size() <= 5);
    } else {
        REQUIRE(result.error().code == ErrorCode::InvalidState);
    }
}

TEST_CASE("HybridSearch - Model-free seeded lexical fallback behavior",
          "[integration][search][hybrid][fallback]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
    // This deliberately covers lexical fallback through a hybrid request, not semantic inference
    // or two-leg RRF fusion.
    HybridSearchIntegrationFixture fixture(/*enableModelProvider=*/false);
    const auto seeded = fixture.seedModelFreeHybridCorpus();
    REQUIRE(seeded.size() == 3);

    // Fusion preserves content hash/path identity, not hydrated database row IDs or titles.
    // Check the canonical public identity rather than assuming optional fields are hydrated.
    std::map<std::string, HybridSearchIntegrationFixture::SeededDocument> expectedByHash;
    for (const auto& document : seeded) {
        expectedByHash.emplace(document.hash, document);
    }
    REQUIRE(expectedByHash.size() == 3);
    const auto contentHash = [](const SearchResult& hit) -> const std::string& {
        const auto hash = hit.metadata.find("hash");
        REQUIRE(hash != hit.metadata.end());
        return hash->second;
    };

    SearchRequest request;
    request.query = "hybridfallbacksentinel";
    request.searchType = "hybrid";
    request.globalSearch = true;
    request.limit = 10;

    auto allResult = fixture.executeSearch(request);
    REQUIRE(allResult);
    const auto& allHits = allResult.value().results;
    REQUIRE(allHits.size() == 3);
    REQUIRE(allHits.size() >= 2);

    std::set<std::string> actualHashes;
    for (const auto& hit : allHits) {
        const auto& hash = contentHash(hit);
        CAPTURE(hash, hit.path);
        REQUIRE(std::isfinite(hit.score));
        const auto expected = expectedByHash.find(hash);
        REQUIRE(expected != expectedByHash.end());
        REQUIRE(hit.path == expected->second.path);
        actualHashes.insert(hash);
    }
    std::set<std::string> expectedHashes;
    for (const auto& document : seeded) {
        expectedHashes.insert(document.hash);
    }
    REQUIRE(actualHashes == expectedHashes);
    for (std::size_t i = 1; i < allHits.size(); ++i) {
        REQUIRE(allHits[i - 1].score >= allHits[i].score);
    }

    request.tags = {"red", "shared"};
    request.matchAllTags = true;
    auto redResult = fixture.executeSearch(request);
    REQUIRE(redResult);
    REQUIRE(redResult.value().results.size() == 2);
    const std::set<std::string> expectedRedHashes{seeded[0].hash, seeded[1].hash};
    std::set<std::string> actualRedHashes;
    for (const auto& hit : redResult.value().results) {
        actualRedHashes.insert(contentHash(hit));
    }
    REQUIRE(actualRedHashes == expectedRedHashes);

    request.tags = {"absent-hybrid-contract-tag"};
    auto absentResult = fixture.executeSearch(request);
    REQUIRE(absentResult);
    REQUIRE(absentResult.value().results.empty());

    request.tags.clear();
    request.matchAllTags = false;
    request.limit = 1;
    auto limitedResult = fixture.executeSearch(request);
    REQUIRE(limitedResult);
    REQUIRE(limitedResult.value().results.size() == 1);
    const auto& limitedHit = limitedResult.value().results.front();
    REQUIRE(contentHash(limitedHit) == contentHash(allHits.front()));
    REQUIRE(limitedHit.path == allHits.front().path);
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
