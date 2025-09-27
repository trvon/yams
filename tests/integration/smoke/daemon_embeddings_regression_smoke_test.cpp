#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/fixture_manager.h"
#include "common/search_corpus_presets.h"

#include <yams/app/services/document_ingestion_service.h>
#include <yams/cli/cli_sync.h>
#include <yams/core/types.h>
#include <yams/crypto/hasher.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
// Local-socket bind probe for sandboxed environments
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {
using yams::cli::run_sync;
using StatusClock = std::chrono::steady_clock;

bool truthy_env(const char* value) {
    if (!value || !*value) {
        return false;
    }
    std::string v(value);
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) { return std::tolower(c); });
    return !(v == "0" || v == "false" || v == "off" || v == "no");
}

std::string unique_suffix() {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(now);
}

template <typename Map> size_t parse_counter(const Map& stats, const std::string& key) {
    auto it = stats.find(key);
    if (it == stats.end()) {
        return 0;
    }
    size_t value = 0;
    for (char ch : it->second) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) {
            return 0;
        }
        value = (value * 10) + static_cast<size_t>(ch - '0');
    }
    return value;
}

bool wait_for_daemon_ready(yams::daemon::DaemonClient& client, std::chrono::milliseconds max_wait,
                           std::string* last_status) {
    const auto deadline = StatusClock::now() + max_wait;
    while (StatusClock::now() < deadline) {
        auto statusRes = run_sync(client.status(), 500ms);
        if (statusRes) {
            const auto& status = statusRes.value();
            if (last_status) {
                *last_status = status.overallStatus;
            }
            if (status.ready || status.overallStatus == "ready" ||
                status.overallStatus == "Ready" || status.overallStatus == "degraded" ||
                status.overallStatus == "Degraded") {
                return true;
            }
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

static bool canBindUnixSocketHere() {
    try {
        boost::asio::io_context io;
        boost::asio::local::stream_protocol::acceptor acc(io);
        auto path = fs::path("/tmp") /
                    (std::string("yams-smoke-probe-") + std::to_string(::getpid()) + ".sock");
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
}

bool wait_for_vector_index_growth(yams::daemon::DaemonClient& client, size_t base_bytes,
                                  size_t base_rows, size_t base_embed_consumed,
                                  std::chrono::milliseconds max_wait, std::string* detail) {
    const auto deadline = StatusClock::now() + max_wait;
    yams::daemon::GetStatsRequest req;
    while (StatusClock::now() < deadline) {
        auto statsRes = yams::cli::run_sync(client.getStats(req), 2s);
        if (statsRes) {
            const auto& stats = statsRes.value();
            size_t rows = parse_counter(stats.additionalStats, "vector_rows");
            size_t embed_consumed = parse_counter(stats.additionalStats, "bus_embed_consumed");
            if (stats.vectorIndexSize > base_bytes || rows > base_rows ||
                embed_consumed > base_embed_consumed) {
                if (detail) {
                    *detail = "vector_index_size=" + std::to_string(stats.vectorIndexSize) +
                              ", vector_rows=" + std::to_string(rows) +
                              ", bus_embed_consumed=" + std::to_string(embed_consumed);
                }
                return true;
            }
            if (detail) {
                *detail = "vector_index_size=" + std::to_string(stats.vectorIndexSize) +
                          ", vector_rows=" + std::to_string(rows) +
                          ", bus_embed_consumed=" + std::to_string(embed_consumed);
            }
        } else if (detail) {
            *detail = statsRes.error().message;
        }
        std::this_thread::sleep_for(200ms);
    }
    return false;
}

bool wait_for_documents_available(yams::daemon::DaemonClient& client,
                                  const std::vector<std::string>& hashes,
                                  std::chrono::milliseconds max_wait, std::string* detail) {
    const auto deadline = StatusClock::now() + max_wait;
    while (StatusClock::now() < deadline) {
        bool all_ok = true;
        for (const auto& hash : hashes) {
            yams::daemon::GetRequest req;
            req.hash = hash;
            req.metadataOnly = false;
            req.maxBytes = 65536; // limit payload while ensuring content retrieval
            auto getRes = yams::cli::run_sync(client.get(req), 3s);
            if (!getRes) {
                all_ok = false;
                if (detail) {
                    *detail = "hash=" + hash + " error=" + getRes.error().message;
                }
                break;
            }
            const auto& resp = getRes.value();
            if (!resp.hasContent || resp.content.empty()) {
                all_ok = false;
                if (detail) {
                    *detail = "hash=" + hash + " missing content";
                }
                break;
            }
        }
        if (all_ok) {
            if (detail) {
                *detail = "all documents retrievable";
            }
            return true;
        }
        std::this_thread::sleep_for(200ms);
    }
    return false;
}

bool ensure_embeddings_provider(yams::daemon::DaemonClient& client,
                                std::chrono::milliseconds max_wait, const std::string& model_name,
                                std::string* reason) {
    std::string status;
    if (!wait_for_daemon_ready(client, max_wait, &status)) {
        if (reason)
            *reason = "Daemon not ready (status=" + status + ")";
        return false;
    }

    yams::daemon::GenerateEmbeddingRequest req;
    req.text = "_healthcheck_";
    req.modelName = model_name.empty() ? std::string{"all-MiniLM-L6-v2"} : model_name;
    req.normalize = true;

    const auto timeout = std::min(max_wait, std::chrono::milliseconds{2000});
    auto res = run_sync(client.generateEmbedding(req), timeout);
    if (!res) {
        if (reason)
            *reason = std::string{"GenerateEmbedding failed: "} + res.error().message;
        return false;
    }
    if (res.value().embedding.empty()) {
        if (reason)
            *reason = "Embedding provider returned empty embedding";
        return false;
    }
    return true;
}

bool wait_for_post_ingest_idle(yams::daemon::DaemonClient& client,
                               std::chrono::milliseconds max_wait, std::string* reason) {
    const auto deadline = StatusClock::now() + max_wait;
    while (StatusClock::now() < deadline) {
        auto statusRes = run_sync(client.status(), 500ms);
        if (statusRes) {
            const auto& status = statusRes.value();
            size_t queued = 0;
            size_t inflight = 0;
            if (auto it = status.requestCounts.find("post_ingest_queued");
                it != status.requestCounts.end()) {
                queued = it->second;
            }
            if (auto it = status.requestCounts.find("post_ingest_inflight");
                it != status.requestCounts.end()) {
                inflight = it->second;
            }
            if (queued == 0 && inflight == 0) {
                if (reason)
                    reason->clear();
                return true;
            }
            if (reason) {
                *reason =
                    "queued=" + std::to_string(queued) + " inflight=" + std::to_string(inflight);
            }
        } else if (reason) {
            *reason = statusRes.error().message;
        }
        std::this_thread::sleep_for(200ms);
    }
    return false;
}
} // namespace

TEST(DaemonEmbeddingsRegressionSmoke, GeneratesVectorsForSearchCorpusPresets) {
    if (std::getenv("GTEST_DISCOVERY_MODE")) {
        GTEST_SKIP() << "Skipping during test discovery";
    }
    if (!canBindUnixSocketHere()) {
        GTEST_SKIP() << "Skipping smoke: environment forbids AF_UNIX bind (sandbox).";
    }

#if defined(_WIN32)
    _putenv_s("YAMS_DAEMON_KILL_OTHERS", "0");
#else
    setenv("YAMS_DAEMON_KILL_OTHERS", "0", 1);
#endif
    // Skip content store stats queries that can race when embedding loads are slow.
#if defined(_WIN32)
    _putenv_s("YAMS_DISABLE_STORE_STATS", "1");
#else
    setenv("YAMS_DISABLE_STORE_STATS", "1", 1);
#endif

    const fs::path root = fs::temp_directory_path() / ("yams_embed_regression_" + unique_suffix());
    const fs::path dataDir = root / "data";
    const fs::path fixturesRoot = root / "fixtures";
    fs::create_directories(dataDir);
    fs::create_directories(fixturesRoot);

    yams::test::FixtureManager fixtures(fixturesRoot);

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = dataDir;
    // Prefer /tmp-based path to avoid path-length and sandbox issues
    fs::path sockdir = fs::path("/tmp") / (std::string("yamsr-") + std::to_string(::getpid()));
    std::error_code mkec;
    fs::create_directories(sockdir, mkec);
    cfg.socketPath =
        sockdir / (std::string("yams-daemon-smoke-") + std::to_string(::getpid()) + ".sock");
    cfg.pidFile = root / "daemon.pid";
    cfg.logFile = root / "daemon.log";
    cfg.enableModelProvider = true;
    bool useMockProvider = true;
    if (const char* envMock = std::getenv("YAMS_USE_MOCK_PROVIDER")) {
        useMockProvider = truthy_env(envMock);
    }
    cfg.useMockModelProvider = useMockProvider;
    cfg.autoLoadPlugins = false;
    cfg.modelPoolConfig.lazyLoading = false;
    cfg.modelPoolConfig.preloadModels = {"all-MiniLM-L6-v2"};

    const std::string modelName = "all-MiniLM-L6-v2";

    yams::daemon::YamsDaemon daemon(cfg);
    auto started = daemon.start();
    ASSERT_TRUE(started) << started.error().message;

    struct Guard {
        yams::daemon::YamsDaemon* daemon;
        fs::path root;
        ~Guard() {
            if (daemon) {
                (void)daemon->stop();
            }
            if (!root.empty()) {
                std::error_code ec;
                fs::remove_all(root, ec);
            }
        }
    } guard{&daemon, root};

    // Build deterministic search corpus fixtures.
    auto spec = yams::test::defaultSearchCorpusSpec();
    spec.commonTags.push_back("regression");
    auto corpus = fixtures.createSearchCorpus(spec);
    const std::size_t expectedDocs = corpus.fixtures.size();
    ASSERT_GT(expectedDocs, 0u);

    yams::daemon::ClientConfig clientCfg;
    clientCfg.socketPath = cfg.socketPath;
    clientCfg.requestTimeout = 15s;
    clientCfg.headerTimeout = 15s;
    clientCfg.bodyTimeout = 30s;
    clientCfg.enableChunkedResponses = false;
    clientCfg.autoStart = false;

    yams::daemon::DaemonClient client(clientCfg);
    auto connected = yams::cli::run_sync(client.connect(), 5s);
    ASSERT_TRUE(connected) << "Daemon connect failed: " << connected.error().message;

    client.setStreamingEnabled(false);

    std::string status;
    ASSERT_TRUE(wait_for_daemon_ready(client, 30s, &status)) << "Daemon not ready: " << status;

    std::string reason;
    ASSERT_TRUE(ensure_embeddings_provider(client, 10s, modelName, &reason))
        << "Embeddings provider unavailable: " << reason;

    yams::app::services::DocumentIngestionService ingestion;
    yams::app::services::AddOptions baseOpts;
    baseOpts.socketPath = cfg.socketPath;
    baseOpts.explicitDataDir = dataDir;
    baseOpts.recursive = false;
    baseOpts.noEmbeddings = false;
    baseOpts.timeoutMs = 60000;

    std::vector<std::string> baseTags = spec.commonTags;
    baseTags.push_back("corpus:search");

    std::vector<std::string> daemonHashes;
    daemonHashes.reserve(expectedDocs);

    for (const auto& fixture : corpus.fixtures) {
        yams::app::services::AddOptions fileOpts = baseOpts;
        fileOpts.path = fixture.path.string();
        fileOpts.tags = baseTags;
        fileOpts.tags.insert(fileOpts.tags.end(), fixture.tags.begin(), fixture.tags.end());

        auto addRes = ingestion.addViaDaemon(fileOpts);
        ASSERT_TRUE(addRes) << "addViaDaemon failed for " << fixture.path << ": "
                            << addRes.error().message;
        if (!addRes.value().hash.empty()) {
            daemonHashes.push_back(addRes.value().hash);
        }
    }

    std::string ingestDrainReason;
    ASSERT_TRUE(wait_for_post_ingest_idle(client, 20s, &ingestDrainReason))
        << "Post-ingest queue did not drain: " << ingestDrainReason;

    // Collect the document hashes directly from the fixtures so we can explicitly
    // request embeddings for the known corpus.
    auto hasher = yams::crypto::createSHA256Hasher();
    ASSERT_TRUE(hasher) << "Failed to create SHA256 hasher";

    std::vector<std::string> corpusHashes;
    corpusHashes.reserve(corpus.fixtures.size());
    for (const auto& fixture : corpus.fixtures) {
        std::string hash = hasher->hashFile(fixture.path);
        ASSERT_FALSE(hash.empty()) << "Failed to hash fixture: " << fixture.path.string();
        corpusHashes.push_back(std::move(hash));
    }
    std::sort(corpusHashes.begin(), corpusHashes.end());
    corpusHashes.erase(std::unique(corpusHashes.begin(), corpusHashes.end()), corpusHashes.end());

    std::sort(daemonHashes.begin(), daemonHashes.end());
    daemonHashes.erase(std::unique(daemonHashes.begin(), daemonHashes.end()), daemonHashes.end());
    EXPECT_EQ(daemonHashes, corpusHashes)
        << "Hashes returned from addViaDaemon did not match corpus hashes";

    std::string documentReadyDetail;
    ASSERT_TRUE(wait_for_documents_available(client, corpusHashes, 15s, &documentReadyDetail))
        << "Ingested documents not yet retrievable: " << documentReadyDetail;

    yams::daemon::GetStatsRequest statsReq;
    auto statsBaseline = yams::cli::run_sync(client.getStats(statsReq), 5s);
    ASSERT_TRUE(statsBaseline) << "GetStats baseline failed: " << statsBaseline.error().message;
    const size_t baseVectorBytes = statsBaseline.value().vectorIndexSize;
    const size_t baseVectorRows =
        parse_counter(statsBaseline.value().additionalStats, "vector_rows");
    const size_t baseEmbedConsumed =
        parse_counter(statsBaseline.value().additionalStats, "bus_embed_consumed");

    yams::daemon::EmbedDocumentsRequest embedReq;
    embedReq.documentHashes = corpusHashes;
    embedReq.modelName = modelName;
    embedReq.normalize = true;
    embedReq.batchSize = std::max<std::size_t>(16, corpusHashes.size());
    embedReq.skipExisting = false;

    auto embedRes = yams::cli::run_sync(client.call(embedReq), 60s);
    ASSERT_TRUE(embedRes) << "EmbedDocuments failed: " << embedRes.error().message;
    EXPECT_GE(embedRes.value().embedded, expectedDocs)
        << "Embedding service did not embed expected number of documents";

    std::string vectorGrowthDetail;
    bool vectorGrowthObserved = wait_for_vector_index_growth(
        client, baseVectorBytes, baseVectorRows, baseEmbedConsumed, 20s, &vectorGrowthDetail);
    if (!vectorGrowthObserved) {
        SCOPED_TRACE("Vector growth not observed: " + vectorGrowthDetail);
    }

    // Wait for vector scoring to become active so semantic queries leverage embeddings.
    bool vectorReady = false;
    for (int i = 0; i < 120; ++i) {
        auto stRes = yams::cli::run_sync(client.status(), 500ms);
        if (stRes) {
            const auto& st = stRes.value();
            auto it = st.readinessStates.find("vector_scoring_enabled");
            if (it != st.readinessStates.end() && it->second) {
                vectorReady = true;
                break;
            }
        }
        std::this_thread::sleep_for(200ms);
    }
    EXPECT_TRUE(vectorReady) << "vector scoring never reported ready";

    // Poll stats until embeddings appear in the vector index.
    // Run representative semantic queries drawn from the shared preset.
    auto queries = yams::test::defaultSearchQueries();
    const int maxSearchAttempts = 20;
    std::size_t semanticHits = 0;
    bool searchReady = false;
    for (int attempt = 0; attempt < maxSearchAttempts && !searchReady; ++attempt) {
        semanticHits = 0;
        for (const auto& query : queries) {
            yams::daemon::SearchRequest req;
            req.query = query;
            req.limit = static_cast<size_t>(expectedDocs);
            req.searchType = "semantic";
            req.similarity = 0.1;
            req.timeout = 2s;
            auto res = yams::cli::run_sync(client.search(req), 3s);
            if (res && res.value().totalCount > 0) {
                ++semanticHits;
            }
        }
        if (semanticHits >= queries.size() / 2) {
            searchReady = true;
            break;
        }
        std::this_thread::sleep_for(300ms);
    }
    EXPECT_TRUE(searchReady) << "Semantic search never returned enough results after "
                             << maxSearchAttempts << " attempts";
}
