#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <numeric>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

#include <spdlog/spdlog.h>

#include <yams/vector/sqlite_vec_backend.h>

using namespace yams::vector;

namespace {

struct Config {
    size_t corpus = 5000;
    size_t queries = 100;
    size_t dim = 128;
    size_t k = 10;
    unsigned int seed = 42;
};

struct EngineResult {
    const char* name = "";
    double build_ms = 0.0;
    double mean_us = 0.0;
    double p95_us = 0.0;
    double p99_us = 0.0;
    double qps = 0.0;
    double recall = 0.0;
};

Config parseArgs(int argc, char* argv[]) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        auto parse = [&](std::string_view prefix, auto& out) {
            if (arg.rfind(prefix, 0) == 0) {
                out = static_cast<std::decay_t<decltype(out)>>(
                    std::stoull(std::string(arg.substr(prefix.size()))));
                return true;
            }
            return false;
        };
        if (parse("--corpus=", cfg.corpus) || parse("--queries=", cfg.queries) ||
            parse("--dim=", cfg.dim) || parse("--k=", cfg.k) || parse("--seed=", cfg.seed)) {
            continue;
        }
        if (arg == "--help") {
            std::printf("Usage: %s [--corpus=N] [--queries=N] [--dim=N] [--k=N] [--seed=N]\n",
                        argv[0]);
            std::exit(0);
        }
    }
    return cfg;
}

std::vector<float> generateNormalizedVector(size_t dim, std::mt19937& rng) {
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    std::vector<float> vec(dim);
    float norm_sq = 0.0f;
    for (auto& value : vec) {
        value = dist(rng);
        norm_sq += value * value;
    }
    float norm = std::sqrt(norm_sq);
    if (norm > 0.0f) {
        for (auto& value : vec) {
            value /= norm;
        }
    }
    return vec;
}

std::vector<std::vector<float>> generateCorpus(size_t count, size_t dim, std::mt19937& rng) {
    std::vector<std::vector<float>> corpus;
    corpus.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        corpus.push_back(generateNormalizedVector(dim, rng));
    }
    return corpus;
}

std::vector<size_t> bruteForceTopK(const std::vector<float>& query,
                                   const std::vector<std::vector<float>>& corpus, size_t k) {
    std::vector<std::pair<size_t, float>> scored;
    scored.reserve(corpus.size());
    for (size_t i = 0; i < corpus.size(); ++i) {
        float dot = 0.0f;
        for (size_t j = 0; j < query.size(); ++j) {
            dot += query[j] * corpus[i][j];
        }
        scored.emplace_back(i, dot);
    }

    const size_t top = std::min(k, scored.size());
    std::partial_sort(scored.begin(), scored.begin() + top, scored.end(),
                      [](const auto& lhs, const auto& rhs) { return lhs.second > rhs.second; });

    std::vector<size_t> ids;
    ids.reserve(top);
    for (size_t i = 0; i < top; ++i) {
        ids.push_back(scored[i].first);
    }
    return ids;
}

double percentile(std::vector<double> values, double p) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    size_t idx = static_cast<size_t>(std::floor((values.size() - 1) * p));
    return values[idx];
}

EngineResult runEngine(VectorSearchEngine engine, const Config& cfg,
                       const std::vector<std::vector<float>>& corpus,
                       const std::vector<std::vector<float>>& queries,
                       const std::vector<std::vector<size_t>>& ground_truth) {
    SqliteVecBackend::Config backend_cfg;
    backend_cfg.embedding_dim = cfg.dim;
    backend_cfg.search_engine = engine;

    SqliteVecBackend backend(backend_cfg);
    auto init = backend.initialize(":memory:");
    if (!init) {
        throw std::runtime_error(init.error().message);
    }
    auto create = backend.createTables(cfg.dim);
    if (!create) {
        throw std::runtime_error(create.error().message);
    }

    std::vector<VectorRecord> records;
    records.reserve(corpus.size());
    for (size_t i = 0; i < corpus.size(); ++i) {
        VectorRecord rec;
        rec.chunk_id = "chunk_" + std::to_string(i);
        rec.document_hash = "doc_" + std::to_string(i / 4);
        rec.embedding = corpus[i];
        rec.content = "benchmark content " + std::to_string(i);
        records.push_back(std::move(rec));
    }
    auto insert = backend.insertVectorsBatch(records);
    if (!insert) {
        throw std::runtime_error(insert.error().message);
    }

    const auto build_start = std::chrono::steady_clock::now();
    auto build = backend.buildIndex();
    if (!build) {
        throw std::runtime_error(build.error().message);
    }
    const auto build_end = std::chrono::steady_clock::now();

    std::vector<double> latencies_us;
    latencies_us.reserve(queries.size());
    size_t total_hits = 0;

    for (size_t i = 0; i < queries.size(); ++i) {
        const auto start = std::chrono::steady_clock::now();
        auto result = backend.searchSimilar(queries[i], cfg.k, 0.0f, std::nullopt, {});
        const auto end = std::chrono::steady_clock::now();
        if (!result) {
            throw std::runtime_error(result.error().message);
        }

        latencies_us.push_back(std::chrono::duration<double, std::micro>(end - start).count());

        std::unordered_set<std::string> expected;
        for (size_t idx : ground_truth[i]) {
            expected.insert("chunk_" + std::to_string(idx));
        }
        for (const auto& rec : result.value()) {
            total_hits += expected.contains(rec.chunk_id) ? 1 : 0;
        }
    }

    const double total_us = std::accumulate(latencies_us.begin(), latencies_us.end(), 0.0);
    EngineResult out;
    switch (engine) {
        case VectorSearchEngine::SimeonPqAdc:
            out.name = "simeon-pq";
            break;
        case VectorSearchEngine::Vec0L2:
            out.name = "vec0-l2";
            break;
    }
    out.build_ms = std::chrono::duration<double, std::milli>(build_end - build_start).count();
    out.mean_us = total_us / static_cast<double>(latencies_us.size());
    out.p95_us = percentile(latencies_us, 0.95);
    out.p99_us = percentile(latencies_us, 0.99);
    out.qps = out.mean_us > 0.0 ? (1e6 / out.mean_us) : 0.0;
    out.recall = static_cast<double>(total_hits) / static_cast<double>(queries.size() * cfg.k);
    return out;
}

void printResult(const EngineResult& result, size_t k) {
    std::printf("%-12s build=%8.1f ms mean=%8.1f us p95=%8.1f us p99=%8.1f us qps=%8.0f "
                "recall@%zu=%5.1f%%\n",
                result.name, result.build_ms, result.mean_us, result.p95_us, result.p99_us,
                result.qps, k, result.recall * 100.0);
}

} // namespace

int main(int argc, char* argv[]) {
    spdlog::set_level(spdlog::level::warn);

    try {
        const Config cfg = parseArgs(argc, argv);
        std::mt19937 rng(cfg.seed);
        auto corpus = generateCorpus(cfg.corpus, cfg.dim, rng);
        auto queries = generateCorpus(cfg.queries, cfg.dim, rng);

        std::vector<std::vector<size_t>> ground_truth;
        ground_truth.reserve(queries.size());
        for (const auto& query : queries) {
            ground_truth.push_back(bruteForceTopK(query, corpus, cfg.k));
        }

        std::printf("============================================================\n");
        std::printf("YAMS Vector Backend Engine Compare\n");
        std::printf("============================================================\n");
        std::printf("corpus=%zu queries=%zu dim=%zu k=%zu seed=%u\n", cfg.corpus, cfg.queries,
                    cfg.dim, cfg.k, cfg.seed);
        std::printf("note: corpus/query vectors are normalized, so cosine and L2 rankings are "
                    "comparable\n\n");

        const auto spq =
            runEngine(VectorSearchEngine::SimeonPqAdc, cfg, corpus, queries, ground_truth);
        const auto vec0 = runEngine(VectorSearchEngine::Vec0L2, cfg, corpus, queries, ground_truth);

        printResult(spq, cfg.k);
        printResult(vec0, cfg.k);
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Benchmark failed: %s\n", e.what());
        return 1;
    }
}
