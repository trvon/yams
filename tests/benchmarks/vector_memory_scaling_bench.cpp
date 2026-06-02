#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <vector>

#include <yams/vector/sqlite_vec_backend.h>

using namespace yams::vector;

namespace {

struct Config {
    size_t dim = 128;
    size_t vectors = 1000;
    unsigned seed = 42;
};

std::vector<float> makeRandomVec(size_t dim, std::mt19937& rng) {
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    std::vector<float> v(dim);
    for (auto& x : v)
        x = dist(rng);
    return v;
}

void bench(const char* engineLabel, VectorSearchEngine engine, const Config& cfg) {
    std::mt19937 rng(cfg.seed);

    SqliteVecBackend::Config beCfg;
    beCfg.embedding_dim = cfg.dim;
    beCfg.search_engine = engine;
    SqliteVecBackend backend(beCfg);
    auto init = backend.initialize(":memory:");
    if (!init) {
        std::printf("%-14s d=%zu n=%zu ERROR: %s\n", engineLabel, cfg.dim, cfg.vectors,
                    init.error().message.c_str());
        return;
    }
    auto create = backend.createTables(cfg.dim);
    if (!create) {
        std::printf("%-14s d=%zu n=%zu ERROR: %s\n", engineLabel, cfg.dim, cfg.vectors,
                    create.error().message.c_str());
        return;
    }

    std::vector<VectorRecord> records;
    records.reserve(cfg.vectors);
    for (size_t i = 0; i < cfg.vectors; ++i) {
        VectorRecord rec;
        rec.chunk_id = "chunk_" + std::to_string(i);
        rec.document_hash = "doc_" + std::to_string(i / 4);
        rec.embedding = makeRandomVec(cfg.dim, rng);
        rec.content = "benchmark content " + std::to_string(i);
        records.push_back(std::move(rec));
    }

    auto t0 = std::chrono::steady_clock::now();
    auto insert = backend.insertVectorsBatch(records);
    if (!insert) {
        std::printf("%-14s d=%zu n=%zu INSERT ERROR\n", engineLabel, cfg.dim, cfg.vectors);
        return;
    }

    auto build = backend.buildIndex();
    if (!build) {
        std::printf("%-14s d=%zu n=%zu BUILD ERROR\n", engineLabel, cfg.dim, cfg.vectors);
        return;
    }
    auto t1 = std::chrono::steady_clock::now();

    // Run a few queries to get steady-state latency
    std::vector<float> query = makeRandomVec(cfg.dim, rng);
    const int queryCount = 20;
    double totalUs = 0;
    for (int q = 0; q < queryCount; ++q) {
        auto qs = std::chrono::steady_clock::now();
        auto result = backend.searchSimilar(query, 10, 0.0f);
        auto qe = std::chrono::steady_clock::now();
        if (!result)
            continue;
        totalUs += std::chrono::duration<double, std::micro>(qe - qs).count();
    }
    double meanUs = totalUs / queryCount;

    double buildMs = std::chrono::duration<double, std::milli>(t1 - t0).count();

    std::printf("%-14s d=%-4zu n=%-6zu  build=%8.1f ms  query=%8.1f us\n", engineLabel, cfg.dim,
                cfg.vectors, buildMs, meanUs);
}

} // namespace

int main(int argc, char** argv) {
    std::printf("%-14s %-5s %-7s  %-12s %-12s\n", "engine", "dim", "vectors", "build_ms",
                "query_us");
    std::printf("%s\n", std::string(56, '-').c_str());

    // Sweep dims at a fixed corpus size
    for (size_t dim : {128ul, 384ul, 1024ul}) {
        Config cfg;
        cfg.dim = dim;
        cfg.vectors = 2000;
        bench("simeon-pq", VectorSearchEngine::SimeonPqAdc, cfg);
        bench("vec0-l2", VectorSearchEngine::Vec0L2, cfg);
    }

    // Sweep corpus sizes at a fixed dim
    for (size_t n : {500ul, 2000ul, 10000ul}) {
        Config cfg;
        cfg.dim = 384;
        cfg.vectors = n;
        bench("simeon-pq", VectorSearchEngine::SimeonPqAdc, cfg);
        bench("vec0-l2", VectorSearchEngine::Vec0L2, cfg);
    }

    return 0;
}
