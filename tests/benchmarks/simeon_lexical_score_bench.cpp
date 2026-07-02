// Simeon lexical score() latency by term rarity (IDF bucket) benchmark.
//
// Produces the decision data for the hot-term posting-list cache (#3):
// p50/p95 score() latency for the 100 most-common (low-IDF) vs 100 rarest
// (high-IDF) corpus terms, plus repeat-query hit rate with the existing
// hot-query score cache on vs off.
//
// Environment variables:
//   YAMS_BENCH_DOC_COUNT   - corpus size in documents (default: 1000)
//   YAMS_BENCH_VOCAB       - vocabulary size (default: 2000)
//   YAMS_BENCH_CALLS       - score() calls per term (default: 3)
//   YAMS_BENCH_OUTPUT      - JSONL output path

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include "bench_utils.h"
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/simeon_lexical_backend.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace yams;
using namespace yams::metadata;
using namespace yams::search;
using yams::bench::isoTimestamp;
using yams::bench::readIntEnv;

namespace {

struct Corpus {
    fs::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repo;
    std::vector<std::int64_t> docIds;
    std::unordered_map<std::string, int> docFrequency;

    ~Corpus() {
        repo.reset();
        if (pool) {
            pool->shutdown();
        }
        pool.reset();
        std::error_code ec;
        fs::remove(dbPath, ec);
        fs::remove(fs::path(dbPath.string() + "-wal"), ec);
        fs::remove(fs::path(dbPath.string() + "-shm"), ec);
    }
};

std::unique_ptr<Corpus> makeZipfCorpus(int docCount, int vocabSize) {
    auto corpus = std::make_unique<Corpus>();
    corpus->dbPath =
        fs::temp_directory_path() /
        ("simeon_score_bench_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".db");

    ConnectionPoolConfig pcfg;
    corpus->pool = std::make_unique<ConnectionPool>(corpus->dbPath.string(), pcfg);
    REQUIRE(corpus->pool->initialize().has_value());
    corpus->repo = std::make_shared<MetadataRepository>(*corpus->pool);

    std::mt19937_64 rng(0x51DEBE7Cull);
    std::vector<double> weights(static_cast<std::size_t>(vocabSize));
    for (int i = 0; i < vocabSize; ++i) {
        weights[static_cast<std::size_t>(i)] = 1.0 / static_cast<double>(i + 1);
    }
    std::discrete_distribution<int> zipf(weights.begin(), weights.end());

    for (int d = 0; d < docCount; ++d) {
        std::string content;
        std::unordered_map<std::string, bool> seenInDoc;
        for (int t = 0; t < 60; ++t) {
            const auto term = "term" + std::to_string(zipf(rng));
            content += term;
            content += ' ';
            if (!seenInDoc[term]) {
                seenInDoc[term] = true;
                corpus->docFrequency[term]++;
            }
        }

        const auto hash = "hash_" + std::to_string(d);
        DocumentInfo info;
        info.filePath = "/bench/doc_" + std::to_string(d) + ".txt";
        info.fileName = "doc_" + std::to_string(d) + ".txt";
        info.fileExtension = ".txt";
        info.fileSize = static_cast<int64_t>(content.size());
        info.sha256Hash = hash;
        info.mimeType = "text/plain";
        info.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        info.modifiedTime = info.createdTime;
        info.indexedTime = info.createdTime;
        auto did = corpus->repo->insertDocument(info);
        REQUIRE(did.has_value());
        corpus->docIds.push_back(did.value());
        REQUIRE(corpus->repo->indexDocumentContent(did.value(), hash, content, "text/plain")
                    .has_value());

        DocumentContent dc;
        dc.documentId = did.value();
        dc.contentText = content;
        dc.contentLength = static_cast<int64_t>(content.size());
        dc.extractionMethod = "bench";
        dc.language = "en";
        REQUIRE(corpus->repo->insertContent(dc).has_value());
    }
    return corpus;
}

bool waitReady(const SimeonLexicalBackend& backend, std::chrono::seconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (backend.ready()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return backend.ready();
}

struct BucketLatency {
    double p50Us = 0.0;
    double p95Us = 0.0;
    double maxUs = 0.0;
};

BucketLatency measureBucket(const SimeonLexicalBackend& backend,
                            const std::vector<std::string>& terms,
                            const std::vector<std::int64_t>& docIds, int callsPerTerm) {
    std::vector<double> samples;
    samples.reserve(terms.size() * static_cast<std::size_t>(callsPerTerm));
    for (const auto& term : terms) {
        for (int c = 0; c < callsPerTerm; ++c) {
            const auto start = std::chrono::steady_clock::now();
            auto result = backend.score(term, docIds);
            const auto us = std::chrono::duration<double, std::micro>(
                                std::chrono::steady_clock::now() - start)
                                .count();
            REQUIRE(result.has_value());
            samples.push_back(us);
        }
    }
    std::sort(samples.begin(), samples.end());
    BucketLatency out;
    if (!samples.empty()) {
        out.p50Us = samples[samples.size() / 2];
        out.p95Us = samples[static_cast<std::size_t>(static_cast<double>(samples.size()) * 0.95)];
        out.maxUs = samples.back();
    }
    return out;
}

} // namespace

TEST_CASE("Simeon lexical score latency by IDF bucket", "[bench][simeon][score-idf]") {
    const int docCount = readIntEnv("YAMS_BENCH_DOC_COUNT", 1000, 10, 100000);
    const int vocabSize = readIntEnv("YAMS_BENCH_VOCAB", 2000, 100, 100000);
    const int callsPerTerm = readIntEnv("YAMS_BENCH_CALLS", 3, 1, 100);

    std::string outputPath = "bench_results/simeon_lexical_score.jsonl";
    if (const char* out = std::getenv("YAMS_BENCH_OUTPUT")) {
        outputPath = out;
    }
    if (auto parent = fs::path(outputPath).parent_path(); !parent.empty()) {
        fs::create_directories(parent);
    }
    std::ofstream outFile(outputPath, std::ios::app);
    REQUIRE(outFile.is_open());

    auto corpus = makeZipfCorpus(docCount, vocabSize);

    std::vector<std::pair<std::string, int>> byFrequency(corpus->docFrequency.begin(),
                                                         corpus->docFrequency.end());
    std::sort(byFrequency.begin(), byFrequency.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    REQUIRE(byFrequency.size() >= 200);

    std::vector<std::string> commonTerms;
    std::vector<std::string> rareTerms;
    for (std::size_t i = 0; i < 100; ++i) {
        commonTerms.push_back(byFrequency[i].first);
        rareTerms.push_back(byFrequency[byFrequency.size() - 1 - i].first);
    }

    SimeonLexicalBackend::Config cfg;
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus->repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(60)));

    const auto common = measureBucket(backend, commonTerms, corpus->docIds, callsPerTerm);
    const auto rare = measureBucket(backend, rareTerms, corpus->docIds, callsPerTerm);

    SimeonLexicalBackend::Config cachedCfg;
    cachedCfg.score_cache_entries = 256;
    SimeonLexicalBackend cachedBackend(cachedCfg);
    REQUIRE(cachedBackend.buildAsync(corpus->repo).has_value());
    REQUIRE(waitReady(cachedBackend, std::chrono::seconds(60)));

    const auto cachedCold = measureBucket(cachedBackend, commonTerms, corpus->docIds, 1);
    const auto hitsBefore = cachedBackend.scoreCacheHits();
    const auto cachedWarm = measureBucket(cachedBackend, commonTerms, corpus->docIds, 1);
    const auto warmHits = cachedBackend.scoreCacheHits() - hitsBefore;

    json record{{"benchmark", "simeon_lexical_score_idf"},
                {"timestamp", isoTimestamp()},
                {"doc_count", docCount},
                {"vocab_size", vocabSize},
                {"calls_per_term", callsPerTerm},
                {"common_df_min", byFrequency[99].second},
                {"rare_df_max", byFrequency[byFrequency.size() - 100].second},
                {"common_p50_us", common.p50Us},
                {"common_p95_us", common.p95Us},
                {"common_max_us", common.maxUs},
                {"rare_p50_us", rare.p50Us},
                {"rare_p95_us", rare.p95Us},
                {"rare_max_us", rare.maxUs},
                {"cache_cold_p50_us", cachedCold.p50Us},
                {"cache_warm_p50_us", cachedWarm.p50Us},
                {"cache_warm_hit_rate", static_cast<double>(warmHits) / 100.0},
                {"score_calls_total", backend.scoreCalls()},
                {"score_micros_total", backend.scoreMicrosTotal()}};
    outFile << record.dump() << '\n';

    spdlog::info("[simeon_score_idf] docs={} vocab={} common p50/p95={:.1f}/{:.1f}us "
                 "rare p50/p95={:.1f}/{:.1f}us cache cold/warm p50={:.1f}/{:.1f}us hit_rate={:.2f}",
                 docCount, vocabSize, common.p50Us, common.p95Us, rare.p50Us, rare.p95Us,
                 cachedCold.p50Us, cachedWarm.p50Us, static_cast<double>(warmHits) / 100.0);
}
