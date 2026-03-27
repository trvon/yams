/**
 * compressed_ann_graph_real_bench.cpp
 *
 * Milestone 11 Step 2: Graph traversal recall on real embeddings.
 *
 * Verify that HNSW/NSW traversal with TurboQuant packed scores
 * maintains R@1 >= 50% on real embeddings (from sentence-transformers).
 *
 * If graph R@1 >= 50%: compressed ANN is production viable.
 * If graph R@1 < 50%: fall back to TurboQuant reranker as primary path.
 */

#include <yams/vector/compressed_ann.h>
#include <yams/vector/turboquant.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <random>
#include <vector>
#include <string>

using json = nlohmann::json;

static std::vector<std::vector<float>> loadEmbeddings(const std::string& path) {
    std::ifstream f(path);
    json data = json::parse(f);
    std::vector<std::vector<float>> result;
    for (const auto& emb : data["embeddings"]) {
        std::vector<float> v;
        for (float x : emb)
            v.push_back(x);
        result.push_back(std::move(v));
    }
    return result;
}

static std::vector<std::vector<float>> makeUnitVecs(size_t n, size_t dim, std::mt19937& rng) {
    std::vector<std::vector<float>> result(n);
    std::normal_distribution<float> dist(0.0f, 1.0f);
    for (auto& v : result) {
        v.resize(dim);
        for (auto& x : v)
            x = dist(rng);
        float norm = std::sqrt(std::inner_product(v.begin(), v.end(), v.begin(), 0.0f));
        if (norm > 1e-6f)
            for (auto& x : v)
                x /= norm;
    }
    return result;
}

// Brute-force exact search for ground truth
static std::vector<size_t> bruteForceTopK(const std::vector<std::vector<float>>& corpus,
                                          const std::vector<float>& query, size_t k) {
    const size_t n = corpus.size();
    std::vector<std::pair<double, size_t>> scores;
    scores.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        double s = std::inner_product(query.begin(), query.end(), corpus[i].begin(), 0.0);
        scores.emplace_back(s, i);
    }
    std::partial_sort(scores.begin(), scores.begin() + k, scores.end(),
                      [](const auto& a, const auto& b) { return a.first > b.first; });
    std::vector<size_t> result;
    for (size_t i = 0; i < std::min(k, scores.size()); ++i)
        result.push_back(scores[i].second);
    return result;
}

struct GraphResult {
    uint8_t bits;
    size_t corpus;
    size_t ef;
    float recall_at_1;
    float recall_at_10;
    double build_ms;
    double search_ms;
    size_t candidates_per_search;
};

template <typename IndexT>
static GraphResult runGraphSearch(IndexT& index, const std::vector<std::vector<float>>& corpus,
                                  const std::vector<std::vector<float>>& queries,
                                  const std::vector<size_t>& true_top1_per_query, uint8_t bits) {
    GraphResult r;
    r.bits = bits;
    r.corpus = corpus.size();

    // Warm up search (first call may include initialization overhead)
    if (!queries.empty()) {
        index.search(queries[0], 1);
    }

    // Time search queries
    size_t total_candidates = 0;
    size_t r1_correct = 0, r10_correct = 0;
    auto t0 = std::chrono::steady_clock::now();

    for (size_t qi = 0; qi < queries.size(); ++qi) {
        auto stats = index.searchWithStats(queries[qi], 10, nullptr, nullptr);
        const auto& top1 = stats.results.empty() ? size_t(~0) : stats.results[0].id;
        if (top1 == true_top1_per_query[qi])
            ++r1_correct;

        std::vector<size_t> packed_top10;
        for (const auto& res : stats.results)
            packed_top10.push_back(res.id);

        std::vector<size_t> true_top10 = bruteForceTopK(corpus, queries[qi], 10);
        for (size_t t : true_top10) {
            if (std::find(packed_top10.begin(), packed_top10.end(), t) != packed_top10.end()) {
                ++r10_correct;
                break;
            }
        }
        total_candidates += stats.results.size();
    }

    auto t1 = std::chrono::steady_clock::now();
    r.build_ms = 0; // already built
    r.search_ms = std::chrono::duration<double>(t1 - t0).count() * 1000.0;
    r.recall_at_1 = static_cast<float>(r1_correct) / static_cast<float>(queries.size());
    r.recall_at_10 = static_cast<float>(r10_correct) / static_cast<float>(queries.size());
    r.candidates_per_search = queries.empty() ? 0 : total_candidates / queries.size();
    return r;
}

int main() {
    std::cerr << "\n=== Milestone 11 Step 2: Graph Traversal Recall ===\n\n";

    const char* data_root = std::getenv("YAMS_EMB_DATA");
    if (!data_root)
        data_root = "data/benchmarks";

    char path[512];
    snprintf(path, sizeof(path), "%s/real_embeddings_384d.json", data_root);
    std::cerr << "Loading: " << path << "\n";

    auto real_emb = loadEmbeddings(path);
    if (real_emb.empty()) {
        std::cerr << "ERROR: Could not load embeddings. Run: YAMS_EMB_DATA=data/benchmarks\n";
        return 1;
    }
    std::cerr << "Loaded " << real_emb.size() << " embeddings, dim=" << real_emb[0].size() << "\n";

    // Split: 80% corpus, 20% queries
    size_t corpus_size = static_cast<size_t>(std::floor(real_emb.size() * 0.80));
    size_t num_q = std::min(static_cast<size_t>(30), real_emb.size() - corpus_size);
    std::vector<std::vector<float>> corpus(real_emb.begin(), real_emb.begin() + corpus_size);
    std::vector<std::vector<float>> queries(real_emb.begin() + corpus_size,
                                            real_emb.begin() + corpus_size + num_q);

    std::cerr << "Corpus: " << corpus.size() << ", Queries: " << queries.size() << "\n\n";

    // Precompute ground truth for queries
    std::vector<size_t> true_top1(num_q);
    std::vector<std::vector<size_t>> true_top10(num_q);
    std::cerr << "Computing ground truth...\n";
    for (size_t qi = 0; qi < num_q; ++qi) {
        auto top = bruteForceTopK(corpus, queries[qi], 10);
        true_top1[qi] = top[0];
        true_top10[qi] = top;
    }
    std::cerr << "\n";

    // Configurations to test
    struct Config {
        uint8_t bits;
        bool fit;
        size_t ef;
        size_t m;
    };
    const Config configs[] = {
        // Fitted TurboQuant (k-means) - the key cases
        {4, true, 50, 16},  // 4b fitted, ef=50
        {4, true, 100, 16}, // 4b fitted, ef=100
        {8, true, 50, 16},  // 8b fitted, ef=50
        {8, true, 100, 16}, // 8b fitted, ef=100
        {8, true, 200, 16}, // 8b fitted, ef=200
    };

    std::cout << std::left << std::setw(8) << "bits"
              << "  " << std::right << std::setw(6) << "fit"
              << "  " << std::setw(5) << "ef"
              << "  " << std::setw(5) << "M"
              << "  " << std::setw(10) << "corpus"
              << "  " << std::setw(8) << "R@1"
              << "  " << std::setw(8) << "R@10"
              << "  " << std::setw(10) << "build_ms"
              << "  " << std::setw(10) << "search_ms"
              << "  " << std::setw(10) << "candidates"
              << "  " << std::setw(12) << "verdict\n";
    std::cout << std::string(90, '-') << "\n";

    float best_r1 = 0.0f;

    for (const auto& cfg : configs) {
        std::cerr << "  [config] b=" << (int)cfg.bits << " fit=" << (cfg.fit ? "yes" : "no")
                  << " ef=" << cfg.ef << " M=" << cfg.m << std::flush;

        // Create TurboQuant quantizer
        yams::vector::TurboQuantConfig tq_cfg;
        tq_cfg.dimension = corpus[0].size();
        tq_cfg.bits_per_channel = cfg.bits;
        tq_cfg.seed = 42;
        yams::vector::TurboQuantMSE quantizer(tq_cfg);

        if (cfg.fit) {
            quantizer.fit(corpus, 5);
        }

        // Create compressed ANN index and inject the fitted scorer
        yams::vector::CompressedANNIndex::Config idx_cfg;
        idx_cfg.dimension = corpus[0].size();
        idx_cfg.bits_per_channel = cfg.bits;
        idx_cfg.seed = 42;
        idx_cfg.m = cfg.m;
        idx_cfg.ef_search = cfg.ef;
        idx_cfg.max_elements = corpus_size + 1000;
        yams::vector::CompressedANNIndex index(idx_cfg);
        index.setScorer(quantizer); // inject fitted scorer (copy is fine)

        // Add corpus vectors (pre-encode using the fitted scorer)
        auto t0 = std::chrono::steady_clock::now();
        for (size_t i = 0; i < corpus.size(); ++i) {
            auto packed = quantizer.packedEncode(corpus[i]);
            index.add(i, std::span<const uint8_t>(packed));
        }

        // Build graph
        index.build();
        auto t1 = std::chrono::steady_clock::now();
        double build_ms = std::chrono::duration<double>(t1 - t0).count() * 1000.0;

        std::cerr << " (building " << build_ms << "ms)...\n";

        // Search
        size_t r1_correct = 0, r10_correct = 0;
        size_t total_candidates = 0;
        auto st0 = std::chrono::steady_clock::now();

        for (size_t qi = 0; qi < num_q; ++qi) {
            auto stats = index.searchWithStats(queries[qi], 10, nullptr, nullptr);

            size_t packed_top1 = stats.results.empty() ? ~0ULL : stats.results[0].id;
            if (packed_top1 == true_top1[qi])
                ++r1_correct;

            std::vector<size_t> packed_top10;
            for (const auto& res : stats.results)
                packed_top10.push_back(res.id);

            for (size_t t : true_top10[qi]) {
                if (std::find(packed_top10.begin(), packed_top10.end(), t) != packed_top10.end()) {
                    ++r10_correct;
                    break;
                }
            }
        }

        auto st1 = std::chrono::steady_clock::now();
        double search_ms = std::chrono::duration<double>(st1 - st0).count() * 1000.0;
        float r1 = static_cast<float>(r1_correct) / static_cast<float>(num_q);
        float r10 = static_cast<float>(r10_correct) / static_cast<float>(num_q);

        if (r1 > best_r1)
            best_r1 = r1;

        const char* verdict = r1 >= 0.50f   ? "ANN-VIABLE"
                              : r1 >= 0.30f ? "rerank-ok"
                                            : "insufficient";

        std::cout << std::left << std::setw(8) << (int)cfg.bits << "  " << std::right
                  << std::setw(6) << (cfg.fit ? "yes" : "no") << "  " << std::setw(5) << cfg.ef
                  << "  " << std::setw(5) << cfg.m << "  " << std::setw(10) << corpus.size() << "  "
                  << std::fixed << std::setprecision(1) << std::setw(8) << (r1 * 100) << "%"
                  << "  " << std::setw(8) << (r10 * 100) << "%"
                  << "  " << std::setw(10) << std::fixed << std::setprecision(0) << build_ms << "ms"
                  << "  " << std::setw(9) << search_ms << "ms"
                  << "  " << std::setw(10) << (num_q > 0 ? (corpus.size() / num_q) : 0) << "  "
                  << std::setw(12) << verdict << "\n";
    }

    std::cout << "\n";
    std::cout << "Best graph R@1: " << std::fixed << std::setprecision(1) << (best_r1 * 100)
              << "%\n";
    std::cout << "Gate: R@1 >= 50% on graph traversal.\n";
    if (best_r1 >= 0.50f) {
        std::cout << "RESULT: PASS -- compressed ANN graph traversal viable on real embeddings.\n";
    } else {
        std::cout << "RESULT: FAIL -- best graph R@1 = " << (best_r1 * 100)
                  << "% < 50%. TurboQuant reranker remains primary path.\n";
    }

    return 0;
}
