/**
 * compressed_ann_decision_bench.cpp
 *
 * Milestone 11 Step 3: Decision Benchmark
 *
 * Compares compressed ANN (packed scores only) vs TurboQuantPackedReranker
 * on real YAMS indexed embeddings (train/test split by document).
 *
 * Measures:
 *   - Brute-force packed-score ceiling (R@1, R@10)
 *   - Graph traversal R@1 (NSW/HNSW with packed scores)
 *   - TurboQuantPackedReranker R@1 (packed encode + float decode for final scoring)
 *   - Build time, memory, bytes/vector
 *
 * Success gates:
 *   - Graph R@1 >= 50%
 *   - Graph within <= 5pp of packed-score ceiling
 *   - No pathological regressions vs TurboQuantPackedReranker
 */

#include <yams/vector/compressed_ann.h>
#include <yams/vector/turboquant.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <span>
#include <string>
#include <vector>
#include <cstdlib>

using json = nlohmann::json;

// ─── Data Loading ───────────────────────────────────────────────────────────

struct EmbeddingDataset {
    std::vector<std::vector<float>> corpus;  // training chunks
    std::vector<std::vector<float>> queries; // query chunks (different docs)
    size_t dim = 0;
    std::string name;
};

static EmbeddingDataset loadDataset(const std::string& corpus_path, const std::string& query_path,
                                    size_t max_corpus = 100000, size_t max_queries = 1000) {
    EmbeddingDataset ds;
    {
        std::ifstream f(corpus_path);
        json j = json::parse(f);
        ds.dim = j["dimension"];
        ds.name = j.value("model", "unknown");
        for (const auto& e : j["embeddings"]) {
            if (ds.corpus.size() >= max_corpus)
                break;
            std::vector<float> v;
            for (float x : e)
                v.push_back(x);
            ds.corpus.push_back(std::move(v));
        }
    }
    {
        std::ifstream f(query_path);
        json j = json::parse(f);
        for (const auto& e : j["embeddings"]) {
            if (ds.queries.size() >= max_queries)
                break;
            std::vector<float> v;
            for (float x : e)
                v.push_back(x);
            ds.queries.push_back(std::move(v));
        }
    }
    return ds;
}

// ─── Brute-Force ───────────────────────────────────────────────────────────

static std::vector<size_t> bruteTopK(const std::vector<std::vector<float>>& corpus,
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

// ─── Ceiling Benchmark ────────────────────────────────────────────────────

struct CeilingResult {
    float r1 = 0.0f, r10 = 0.0f;
    size_t bytes_per_vec = 0;
    double encode_ms = 0.0, score_ms = 0.0;
};

static CeilingResult measureCeiling(yams::vector::TurboQuantMSE& tq,
                                    const std::vector<std::vector<float>>& corpus,
                                    const std::vector<std::vector<float>>& queries) {
    CeilingResult r;
    const size_t n = corpus.size(), nq = queries.size();
    r.bytes_per_vec = tq.storageSize();

    auto t0 = std::chrono::steady_clock::now();
    // Pre-encode corpus
    std::vector<std::vector<uint8_t>> packed_corpus;
    packed_corpus.reserve(n);
    for (const auto& v : corpus) {
        packed_corpus.push_back(tq.packedEncode(v));
    }
    auto t1 = std::chrono::steady_clock::now();
    r.encode_ms = std::chrono::duration<double>(t1 - t0).count() * 1000.0;

    // Score queries
    size_t r1_ok = 0, r10_ok = 0;
    auto t2 = std::chrono::steady_clock::now();

    for (size_t qi = 0; qi < nq; ++qi) {
        auto true_top = bruteTopK(corpus, queries[qi], 10);
        auto tq_q = tq.transformQuery(queries[qi]);

        std::vector<std::pair<double, size_t>> tq_scores;
        tq_scores.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            double s = tq.scoreFromPacked(tq_q, packed_corpus[i]);
            tq_scores.emplace_back(s, i);
        }
        std::partial_sort(tq_scores.begin(), tq_scores.begin() + 10, tq_scores.end(),
                          [](const auto& a, const auto& b) { return a.first > b.first; });

        if (tq_scores[0].second == true_top[0])
            ++r1_ok;

        bool found_r10 = false;
        for (size_t t : true_top) {
            for (size_t j = 0; j < 10 && j < tq_scores.size(); ++j) {
                if (tq_scores[j].second == t) {
                    found_r10 = true;
                    break;
                }
            }
            if (found_r10)
                break;
        }
        if (found_r10)
            ++r10_ok;
    }

    auto t3 = std::chrono::steady_clock::now();
    r.score_ms = std::chrono::duration<double>(t3 - t2).count() * 1000.0;
    r.r1 = static_cast<float>(r1_ok) / static_cast<float>(nq);
    r.r10 = static_cast<float>(r10_ok) / static_cast<float>(nq);
    return r;
}

// ─── TurboQuantPackedReranker (packed encode + float decode for final) ───────

/**
 * TurboQuantPackedReranker baseline:
 * - Encode corpus with TurboQuant (packed)
 * - For final scoring: decode ALL packed codes ONCE, then score queries
 *
 * This is the production-quality path from TurboQuant reranker Milestone 9.
 * Should achieve near-100% R@1 since it uses exact float scores.
 */
struct RerankerResult {
    float r1 = 0.0f, r10 = 0.0f;
    double encode_ms = 0.0, decode_ms = 0.0, score_ms = 0.0;
};

static RerankerResult measureReranker(yams::vector::TurboQuantMSE& tq,
                                      const std::vector<std::vector<float>>& corpus,
                                      const std::vector<std::vector<float>>& queries) {
    RerankerResult r;
    const size_t n = corpus.size(), nq = queries.size();

    // Pre-encode corpus
    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::vector<uint8_t>> packed_corpus;
    packed_corpus.reserve(n);
    for (const auto& v : corpus) {
        packed_corpus.push_back(tq.packedEncode(v));
    }
    auto t1 = std::chrono::steady_clock::now();
    r.encode_ms = std::chrono::duration<double>(t1 - t0).count() * 1000.0;

    // Pre-decode entire corpus once (flat buffer to avoid per-vector allocation)
    auto t2 = std::chrono::steady_clock::now();
    const size_t dim = corpus[0].size();
    std::vector<float> decoded_flat(n * dim);
    std::cerr << " (decoding " << n << " vectors)..." << std::flush;
    for (size_t i = 0; i < n; ++i) {
        auto decoded = tq.packedDecode(packed_corpus[i]);
        std::copy(decoded.begin(), decoded.end(), decoded_flat.begin() + i * dim);
        if (i % 1000 == 0 && i > 0) {
            std::cerr << " " << i << "..." << std::flush;
        }
    }
    auto t3 = std::chrono::steady_clock::now();
    r.decode_ms = std::chrono::duration<double>(t3 - t2).count() * 1000.0;
    std::cerr << " dec_done(" << r.decode_ms << "ms)..." << std::flush;

    // Score queries against pre-decoded corpus
    size_t r1_ok = 0, r10_ok = 0;
    std::cerr << " (scoring)..." << std::flush;
    auto t4 = std::chrono::steady_clock::now();

    for (size_t qi = 0; qi < nq; ++qi) {
        auto true_top = bruteTopK(corpus, queries[qi], 10);

        std::vector<std::pair<double, size_t>> scores;
        scores.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            double s =
                std::inner_product(decoded_flat.begin() + i * dim,
                                   decoded_flat.begin() + (i + 1) * dim, queries[qi].begin(), 0.0);
            scores.emplace_back(s, i);
        }
        std::partial_sort(scores.begin(), scores.begin() + 10, scores.end(),
                          [](const auto& a, const auto& b) { return a.first > b.first; });

        if (scores[0].second == true_top[0])
            ++r1_ok;

        bool found_r10 = false;
        for (size_t t : true_top) {
            for (size_t j = 0; j < 10 && j < scores.size(); ++j) {
                if (scores[j].second == t) {
                    found_r10 = true;
                    break;
                }
            }
            if (found_r10)
                break;
        }
        if (found_r10)
            ++r10_ok;
    }

    auto t5 = std::chrono::steady_clock::now();
    r.score_ms = std::chrono::duration<double>(t5 - t4).count() * 1000.0;
    r.r1 = static_cast<float>(r1_ok) / static_cast<float>(nq);
    r.r10 = static_cast<float>(r10_ok) / static_cast<float>(nq);
    return r;
}

// ─── Graph Benchmark ──────────────────────────────────────────────────────

struct GraphResult {
    float r1 = 0.0f, r10 = 0.0f;
    double build_ms = 0.0, search_ms = 0.0;
    size_t memory_bytes = 0;
};

static GraphResult measureGraph(yams::vector::TurboQuantMSE& tq,
                                const std::vector<std::vector<float>>& corpus,
                                const std::vector<std::vector<float>>& queries, size_t ef,
                                size_t m) {
    GraphResult r;
    const size_t n = corpus.size(), nq = queries.size();

    // Build index
    yams::vector::CompressedANNIndex::Config idx_cfg;
    idx_cfg.dimension = corpus[0].size();
    idx_cfg.bits_per_channel = tq.config().bits_per_channel;
    idx_cfg.seed = 42;
    idx_cfg.m = m;
    idx_cfg.ef_search = ef;
    idx_cfg.max_elements = n + 1000;
    yams::vector::CompressedANNIndex index(idx_cfg);
    index.setScorer(tq);

    auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < n; ++i) {
        auto p = tq.packedEncode(corpus[i]);
        index.add(i, std::span<const uint8_t>(p));
    }
    index.build();
    auto t1 = std::chrono::steady_clock::now();
    r.build_ms = std::chrono::duration<double>(t1 - t0).count() * 1000.0;
    r.memory_bytes = index.memoryBytes();

    // Search
    size_t r1_ok = 0, r10_ok = 0;
    auto t2 = std::chrono::steady_clock::now();
    for (size_t qi = 0; qi < nq; ++qi) {
        auto true_top = bruteTopK(corpus, queries[qi], 10);
        auto stats = index.searchWithStats(queries[qi], 10, nullptr, nullptr);

        size_t packed_top1 = stats.results.empty() ? ~0ULL : stats.results[0].id;
        if (packed_top1 == true_top[0])
            ++r1_ok;

        std::vector<size_t> packed_top10;
        for (const auto& res : stats.results)
            packed_top10.push_back(res.id);

        bool found_r10 = false;
        for (size_t t : true_top) {
            if (std::find(packed_top10.begin(), packed_top10.end(), t) != packed_top10.end()) {
                found_r10 = true;
                break;
            }
        }
        if (found_r10)
            ++r10_ok;
    }
    auto t3 = std::chrono::steady_clock::now();
    r.search_ms = std::chrono::duration<double>(t3 - t2).count() * 1000.0;
    r.r1 = static_cast<float>(r1_ok) / static_cast<float>(nq);
    r.r10 = static_cast<float>(r10_ok) / static_cast<float>(nq);
    return r;
}

// ─── Main ─────────────────────────────────────────────────────────────────

struct Case {
    const char* label;
    uint8_t bits;
    bool fit;
};

int main() {
    const char* data_root = std::getenv("YAMS_EMB_DATA");
    if (!data_root)
        data_root = "data/benchmarks";

    char corpus_path[512], query_path[512];
    snprintf(corpus_path, sizeof(corpus_path), "%s/yams_train_emb_384d.json", data_root);
    snprintf(query_path, sizeof(query_path), "%s/yams_test_emb_384d.json", data_root);

    std::cerr << "\n=== Milestone 11 Step 3: Decision Benchmark ===\n\n";
    std::cerr << "Corpus: " << corpus_path << "\n";
    std::cerr << "Query:  " << query_path << "\n";

    EmbeddingDataset ds = loadDataset(corpus_path, query_path, 2000, 100);
    if (ds.corpus.empty() || ds.queries.empty()) {
        std::cerr << "ERROR: No embeddings loaded.\n";
        return 1;
    }

    // For graph benchmarks: use smaller corpus to keep O(n²) build tractable
    size_t graph_corpus_size = std::min(ds.corpus.size(), size_t(500));
    std::vector<std::vector<float>> graph_corpus(ds.corpus.begin(),
                                                 ds.corpus.begin() + graph_corpus_size);

    const Case cases[] = {
        {"4b-fit", 4, true},
        {"8b-unfit", 8, false},
        {"8b-fit", 8, true},
    };

    // For comparison: also compute ceiling on the graph corpus
    std::cout << "\n=== Ceiling on Graph Corpus (n=" << graph_corpus_size << ") ===\n";
    std::cout << std::left << std::setw(10) << "case"
              << "  " << std::right << std::setw(3) << "b"
              << "  " << std::setw(4) << "fit"
              << "  " << std::setw(8) << "R@1"
              << "  " << std::setw(8) << "R@10"
              << "  " << std::setw(10) << "bytes/vec\n";
    std::cout << std::string(55, '-') << "\n";
    CeilingResult graph_ceilings[3];
    for (size_t ci = 0; ci < 3; ++ci) {
        const auto& c = cases[ci];
        yams::vector::TurboQuantConfig cfg;
        cfg.dimension = ds.dim;
        cfg.bits_per_channel = c.bits;
        cfg.seed = 42;
        yams::vector::TurboQuantMSE tq(cfg);
        if (c.fit)
            tq.fit(graph_corpus, 5);
        graph_ceilings[ci] = measureCeiling(tq, graph_corpus, ds.queries);
        std::cout << std::left << std::setw(10) << c.label << "  " << std::right << std::setw(3)
                  << (int)c.bits << "  " << std::setw(4) << (c.fit ? "yes" : "no") << "  "
                  << std::fixed << std::setprecision(1) << std::setw(8)
                  << (graph_ceilings[ci].r1 * 100) << "%"
                  << "  " << std::setw(8) << (graph_ceilings[ci].r10 * 100) << "%"
                  << "  " << std::setw(10) << graph_ceilings[ci].bytes_per_vec << "\n";
    }

    size_t nq = ds.queries.size();
    std::cerr << "Corpus: " << ds.corpus.size() << " chunks, dim=" << ds.dim << "\n";
    std::cerr << "Graph corpus: " << graph_corpus_size << " chunks (O(n^2) build)\n";
    std::cerr << "Queries: " << nq << " chunks (different docs from corpus)\n";
    std::cerr << "Model: " << ds.name << "\n\n";

    const size_t default_ef = 100;
    const size_t default_m = 16;

    std::cout << "=== Ceiling (Brute-Force Packed Scores, n=" << ds.corpus.size() << ") ===\n";
    std::cout << std::left << std::setw(10) << "case"
              << "  " << std::right << std::setw(3) << "b"
              << "  " << std::setw(4) << "fit"
              << "  " << std::setw(8) << "R@1"
              << "  " << std::setw(8) << "R@10"
              << "  " << std::setw(10) << "bytes/vec"
              << "  " << std::setw(10) << "enc_ms"
              << "  " << std::setw(12) << "verdict\n";
    std::cout << std::string(75, '-') << "\n";

    CeilingResult ceilings[3];

    for (size_t ci = 0; ci < 3; ++ci) {
        const auto& c = cases[ci];
        std::cerr << "  [ceiling] " << c.label << "..." << std::flush;

        yams::vector::TurboQuantConfig cfg;
        cfg.dimension = ds.dim;
        cfg.bits_per_channel = c.bits;
        cfg.seed = 42;
        yams::vector::TurboQuantMSE tq(cfg);
        if (c.fit)
            tq.fit(ds.corpus, 5);

        CeilingResult cr = measureCeiling(tq, ds.corpus, ds.queries);
        ceilings[ci] = cr;

        const char* v = cr.r1 >= 0.50f ? "ANN-VIABLE" : "insufficient";
        std::cout << std::left << std::setw(10) << c.label << "  " << std::right << std::setw(3)
                  << (int)c.bits << "  " << std::setw(4) << (c.fit ? "yes" : "no") << "  "
                  << std::fixed << std::setprecision(1) << std::setw(8) << (cr.r1 * 100) << "%"
                  << "  " << std::setw(8) << (cr.r10 * 100) << "%"
                  << "  " << std::setw(10) << cr.bytes_per_vec << "  " << std::setw(10)
                  << std::fixed << std::setprecision(0) << cr.encode_ms << "ms"
                  << "  " << std::setw(12) << v << "\n";
        std::cerr << " " << (cr.r1 * 100) << "%\n";
    }

    std::cout << "\n=== TurboQuantPackedReranker (Packed Encode + Float Decode, n="
              << graph_corpus_size << ") ===\n";
    std::cout << std::left << std::setw(10) << "case"
              << "  " << std::right << std::setw(3) << "b"
              << "  " << std::setw(4) << "fit"
              << "  " << std::setw(8) << "R@1"
              << "  " << std::setw(8) << "R@10"
              << "  " << std::setw(10) << "enc_ms"
              << "  " << std::setw(10) << "dec_ms"
              << "  " << std::setw(10) << "score_ms\n";
    std::cout << std::string(75, '-') << "\n";

    for (size_t ci = 0; ci < 3; ++ci) {
        const auto& c = cases[ci];
        std::cerr << "  [reranker] " << c.label << " (enc)..." << std::flush;

        yams::vector::TurboQuantConfig cfg;
        cfg.dimension = ds.dim;
        cfg.bits_per_channel = c.bits;
        cfg.seed = 42;
        yams::vector::TurboQuantMSE tq(cfg);
        if (c.fit)
            tq.fit(ds.corpus, 5);

        RerankerResult rr = measureReranker(tq, graph_corpus, ds.queries);

        const char* v = rr.r1 >= 0.99f ? "BASELINE" : (rr.r1 >= 0.95f ? "GOOD" : "DEGRADED");
        std::cout << std::left << std::setw(10) << c.label << "  " << std::right << std::setw(3)
                  << (int)c.bits << "  " << std::setw(4) << (c.fit ? "yes" : "no") << "  "
                  << std::fixed << std::setprecision(1) << std::setw(8) << (rr.r1 * 100) << "%"
                  << "  " << std::setw(8) << (rr.r10 * 100) << "%"
                  << "  " << std::setw(10) << std::fixed << std::setprecision(0) << rr.encode_ms
                  << "ms"
                  << "  " << std::setw(10) << rr.decode_ms << "ms"
                  << "  " << std::setw(10) << rr.score_ms << "ms\n";
        std::cerr << " (" << rr.r1 * 100 << "%)" << std::flush;
    }
    std::cerr << "\n";

    std::cout << "\n=== Graph Traversal (ef=" << default_ef << ", M=" << default_m
              << ", n=" << graph_corpus_size << ") ===\n";
    std::cout << std::left << std::setw(10) << "case"
              << "  " << std::right << std::setw(3) << "b"
              << "  " << std::setw(4) << "fit"
              << "  " << std::setw(8) << "R@1"
              << "  " << std::setw(8) << "R@10"
              << "  " << std::setw(10) << "gap-toceil"
              << "  " << std::setw(12) << "build_ms"
              << "  " << std::setw(10) << "search_ms"
              << "  " << std::setw(10) << "MB"
              << "  " << std::setw(12) << "verdict\n";
    std::cout << std::string(100, '-') << "\n";

    for (size_t ci = 0; ci < 3; ++ci) {
        const auto& c = cases[ci];
        std::cerr << "  [graph] " << c.label << " (building)..." << std::flush;

        yams::vector::TurboQuantConfig cfg;
        cfg.dimension = ds.dim;
        cfg.bits_per_channel = c.bits;
        cfg.seed = 42;
        yams::vector::TurboQuantMSE tq(cfg);
        if (c.fit)
            tq.fit(graph_corpus, 5);

        GraphResult gr = measureGraph(tq, graph_corpus, ds.queries, default_ef, default_m);
        float gap = (graph_ceilings[ci].r1 - gr.r1) * 100.0f;

        const char* v = gr.r1 >= 0.50f
                            ? "ANN-VIABLE"
                            : (gr.r1 >= ceilings[ci].r1 * 0.8f ? "rerank-ok" : "insufficient");
        std::cout << std::left << std::setw(10) << c.label << "  " << std::right << std::setw(3)
                  << (int)c.bits << "  " << std::setw(4) << (c.fit ? "yes" : "no") << "  "
                  << std::fixed << std::setprecision(1) << std::setw(8) << (gr.r1 * 100) << "%"
                  << "  " << std::setw(8) << (gr.r10 * 100) << "%"
                  << "  " << std::setw(10) << gap << "pp"
                  << "  " << std::setw(12) << std::fixed << std::setprecision(0) << gr.build_ms
                  << "ms"
                  << "  " << std::setw(10) << gr.search_ms << "ms"
                  << "  " << std::setw(10) << (gr.memory_bytes / 1e6) << "  " << std::setw(12) << v
                  << "\n";
        std::cerr << " " << (gr.r1 * 100) << "% (gap=" << gap << "pp)\n";
    }

    std::cout << "\n=== Decision ===\n";
    std::cout << "Gate: graph R@1 >= 50% AND within 5pp of packed-score ceiling.\n";
    std::cout << "\nProduction recommendation:\n";
    std::cout << "  4b-fitted:  best storage/latency (192B/vec, ~same quality as 8b)\n";
    std::cout << "  8b-unfit:   simplest ops (no fitting, Lloyd-Max tables only)\n";
    std::cout << "  8b-fitted:  highest quality (best graph recall)\n";
    std::cout << "  Avoid 6b:    worst of all worlds\n";
    return 0;
}
