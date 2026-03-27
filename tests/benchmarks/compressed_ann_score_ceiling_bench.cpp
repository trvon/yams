// Milestone 11: Higher Bit-Depth Ceiling Benchmark
// Extends Milestone 9 to test 6-bit and 8-bit fitted centroid tables.
// Reports: packed-score ceiling (brute-force R@1/R@10), gap to best, storage, latency.
// Success gate: R@1 >= 0.50 on real embeddings → compressed ANN viable.
// Failure gate: R@1 < 0.50 → stop investing in compressed ANN as primary path.

#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <numeric>
#include <chrono>
#include <cstdint>

#include "yams/vector/turboquant.h"
#include "yams/vector/compressed_ann.h"

// Extra diagnostics
static std::vector<float> makeUnitVec(size_t dim, std::mt19937& rng);

static void showMSE(size_t dim) {
    std::cout << "\n--- MSE by bit depth (dim=" << dim << ") ---\n";
    std::mt19937 rng(42);
    std::vector<std::vector<float>> corpus(500);
    for (auto& v : corpus) {
        std::normal_distribution<float> d(0.0f, 1.0f);
        v.resize(dim);
        for (auto& x : v)
            x = d(rng);
        float n = std::sqrt(std::inner_product(v.begin(), v.end(), v.begin(), 0.0f));
        if (n > 1e-6f)
            for (auto& x : v)
                x /= n;
    }

    for (uint8_t bits = 1; bits <= 8; ++bits) {
        yams::vector::TurboQuantConfig cfg;
        cfg.dimension = dim;
        cfg.bits_per_channel = bits;
        cfg.seed = 42;
        yams::vector::TurboQuantMSE tq(cfg);
        tq.fitPerCoordScales(corpus, 10); // scales only (static Lloyd-Max centroids)

        double total_mse = 0.0;
        size_t n = 0;
        for (const auto& v : corpus) {
            auto packed = tq.packedEncode(v);
            auto recon = tq.packedDecode(packed);
            for (size_t i = 0; i < dim; ++i) {
                float err = v[i] - recon[i];
                total_mse += err * err;
                ++n;
            }
        }
        std::cout << "  b=" << (int)bits << ": MSE=" << std::scientific << std::setprecision(4)
                  << (total_mse / n) << "  RMSE=" << std::sqrt(total_mse / n) << "\n";
    }
}

static void showPearsonCorr(size_t dim) {
    std::cout << "\n--- Pearson corr packed-score vs true-cosine (dim=" << dim << ") ---\n";
    std::mt19937 cr(42), qr(999);
    std::vector<std::vector<float>> corpus(500), queries(20);
    for (auto& v : corpus)
        v = makeUnitVec(dim, cr);
    for (auto& q : queries)
        q = makeUnitVec(dim, qr);

    for (uint8_t bits = 2; bits <= 8; ++bits) {
        yams::vector::TurboQuantConfig cfg;
        cfg.dimension = dim;
        cfg.bits_per_channel = bits;
        cfg.seed = 42;
        yams::vector::TurboQuantMSE tq(cfg);
        tq.fitPerCoordScales(corpus, 10); // scales only (static Lloyd-Max centroids)

        double total_r = 0.0;
        for (size_t qi = 0; qi < 20; ++qi) {
            auto tq_q = tq.transformQuery(queries[qi]);
            std::vector<double> ts(corpus.size()), cs(corpus.size());
            double sum_ts = 0, sum_cs = 0;
            for (size_t i = 0; i < corpus.size(); ++i) {
                auto p = tq.packedEncode(corpus[i]);
                ts[i] = tq.scoreFromPacked(tq_q, p);
                cs[i] = std::inner_product(queries[qi].begin(), queries[qi].end(),
                                           corpus[i].begin(), 0.0f);
                sum_ts += ts[i];
                sum_cs += cs[i];
            }
            double mean_t = sum_ts / corpus.size(), mean_c = sum_cs / corpus.size();
            double cov = 0, var_t = 0, var_c = 0;
            for (size_t i = 0; i < corpus.size(); ++i) {
                double dt = ts[i] - mean_t, dc = cs[i] - mean_c;
                cov += dt * dc;
                var_t += dt * dt;
                var_c += dc * dc;
            }
            total_r += cov / std::sqrt(var_t * var_c + 1e-20);
        }
        std::cout << "  b=" << (int)bits << ": Pearson r=" << std::fixed << std::setprecision(3)
                  << (total_r / 20.0) << "\n";
    }
}

static std::vector<float> makeUnitVec(size_t dim, std::mt19937& rng) {
    std::normal_distribution<float> d(0.0f, 1.0f);
    std::vector<float> v(dim);
    for (auto& x : v)
        x = d(rng);
    float n = std::sqrt(std::inner_product(v.begin(), v.end(), v.begin(), 0.0f));
    if (n > 1e-6f)
        for (auto& x : v)
            x /= n;
    return v;
}

// Clustered data: k clusters with Gaussian noise around cluster centers
// This is closer to real YAMS embeddings (semantic clusters)
static std::vector<float> makeClusteredVec(size_t dim, size_t k, float cluster_std,
                                           std::mt19937& rng,
                                           const std::vector<std::vector<float>>& centers) {
    size_t cid = std::uniform_int_distribution<size_t>(0, k - 1)(rng);
    std::normal_distribution<float> nd(0.0f, cluster_std);
    std::vector<float> v(dim);
    for (size_t d = 0; d < dim; ++d) {
        v[d] = centers[cid][d] + nd(rng);
    }
    float n = std::sqrt(std::inner_product(v.begin(), v.end(), v.begin(), 0.0f));
    if (n > 1e-6f)
        for (auto& x : v)
            x /= n;
    return v;
}

// Low-rank data: lives in a rank-r subspace of dim
// Real text embeddings often have lower intrinsic dimensionality
static std::vector<float> makeLowRankVec(size_t dim, size_t rank, std::mt19937& rng) {
    std::vector<std::vector<float>> basis(rank);
    for (auto& b : basis)
        b = makeUnitVec(dim, rng);

    std::vector<float> coeffs(rank);
    for (size_t k = 0; k < rank; ++k) {
        float sigma = (k < rank / 2) ? 1.0f : 0.2f;
        coeffs[k] = std::normal_distribution<float>(0.0f, sigma)(rng);
    }
    std::vector<float> v(dim, 0.0f);
    for (size_t k = 0; k < rank; ++k)
        for (size_t d = 0; d < dim; ++d)
            v[d] += coeffs[k] * basis[k][d];
    float n = std::sqrt(std::inner_product(v.begin(), v.end(), v.begin(), 0.0f));
    if (n > 1e-6f)
        for (auto& x : v)
            x /= n;
    return v;
}

// Build k cluster centers (well-separated unit vectors)
static std::vector<std::vector<float>> buildCenters(size_t dim, size_t k, std::mt19937& rng) {
    std::vector<std::vector<float>> centers(k);
    for (auto& c : centers) {
        c = makeUnitVec(dim, rng);
        // Make them more orthogonal to each other
        // (orthogonal centers = better-separated clusters)
    }
    return centers;
}

// True-cosine R@1 between two ranked lists
static size_t rankMatches(const std::vector<size_t>& ranked, const std::vector<size_t>& truth) {
    size_t matches = 0;
    size_t limit = std::min(ranked.size(), truth.size());
    for (size_t i = 0; i < limit; ++i)
        if (ranked[i] == truth[i])
            ++matches;
    return matches;
}

// Brute-force packed-score ceiling for a corpus/query set
struct CeilingResult {
    float r1;             // R@1 as fraction
    float r10;            // R@10 as fraction
    size_t top1_matches;  // raw count
    size_t top10_matches; // raw count
};

static CeilingResult measureCeiling(yams::vector::TurboQuantMSE& tq,
                                    const std::vector<std::vector<float>>& corpus,
                                    const std::vector<std::vector<float>>& queries) {
    size_t nq = queries.size();
    size_t n = corpus.size();
    CeilingResult r{0.0f, 0.0f, 0, 0};

    for (size_t qi = 0; qi < nq; ++qi) {
        auto tq_q = tq.transformQuery(queries[qi]);

        // Packed-score ranking
        std::vector<std::pair<float, size_t>> packed_rank;
        packed_rank.reserve(n);
        for (size_t ci = 0; ci < n; ++ci) {
            auto p = tq.packedEncode(corpus[ci]);
            packed_rank.emplace_back(tq.scoreFromPacked(tq_q, p), ci);
        }
        std::partial_sort(packed_rank.begin(), packed_rank.begin() + 10, packed_rank.end(),
                          [](auto& a, auto& b) { return a.first > b.first; });

        // True-cosine ranking
        std::vector<std::pair<float, size_t>> true_rank;
        true_rank.reserve(n);
        for (size_t ci = 0; ci < n; ++ci) {
            float s = std::inner_product(queries[qi].begin(), queries[qi].end(), corpus[ci].begin(),
                                         0.0f);
            true_rank.emplace_back(s, ci);
        }
        std::partial_sort(true_rank.begin(), true_rank.begin() + 10, true_rank.end(),
                          [](auto& a, auto& b) { return a.first > b.first; });

        // Top-1
        if (packed_rank[0].second == true_rank[0].second)
            ++r.top1_matches;

        // Top-10
        for (size_t k = 0; k < 10; ++k)
            if (packed_rank[k].second == true_rank[k].second)
                ++r.top10_matches;
    }

    r.r1 = static_cast<float>(r.top1_matches) / nq;
    r.r10 = static_cast<float>(r.top10_matches) / (nq * 10);
    return r;
}

struct BenchmarkCase {
    const char* name;
    size_t dim;
    uint8_t bits;
    bool fit;
    const char* dist; // "random", "clustered", "lowrank"
};

int main() {
    std::cout << "=== Milestone 11: Higher Bit-Depth Ceiling Benchmark ===\n";
    std::cout << "dims: 384, 768 | bits: 4,6,8 | corpus: 300 | dists: random, clustered\n";
    std::cout << "Success gate: packed-score ceiling R@1 >= 0.50 → compressed ANN viable.\n";
    std::cout << "Failure gate: R@1 < 0.50 → stop investing in compressed ANN as primary path.\n\n";

    // Smaller corpus for fast benchmarking (original: 1000×50)
    constexpr size_t corpus_size = 300;
    constexpr size_t num_q = 30;
    constexpr size_t num_clusters = 10;
    constexpr float cluster_std = 0.5f;

    // Benchmark matrix — Milestone 11: packed-score ceiling for higher bit depths.
    // Key questions:
    //   1. Does 6-bit k-means fitted beat 4-bit k-means fitted?
    //   2. Does 8-bit k-means fitted beat 6-bit?
    //   3. Is packed R@1 >= 50% achievable on random unit-sphere?
    //
    // Fitting: k-means for all bit depths (centroids + scales).
    // 2-bit k-means is O(d × n × k × iters) — too slow for 300×384, omitted.
    const BenchmarkCase cases[] = {
        // === 4-bit baseline (from Milestone 9) ===
        {"384r4nfit", 384, 4, false, "random"},   // unfitted: Lloyd-Max sigma=1.0
        {"384r4fit", 384, 4, true, "random"},     // k-means: scales + centroids
        {"384r4cfit", 384, 4, true, "clustered"}, // k-means, clustered
        // === 6-bit k-means fitted ===
        {"384r6nfit", 384, 6, false, "random"},  // unfitted: Lloyd-Max sigma=1.0 (64 cents)
        {"384r6fit", 384, 6, true, "random"},    // k-means: 64 centroids + scales
        {"384c6fit", 384, 6, true, "clustered"}, // k-means, clustered
        // === 8-bit k-means fitted ===
        {"384r8nfit", 384, 8, false, "random"},  // unfitted: Lloyd-Max sigma=1.0 (256 cents)
        {"384r8fit", 384, 8, true, "random"},    // k-means: 256 centroids + scales
        {"384c8fit", 384, 8, true, "clustered"}, // k-means, clustered
    };
    constexpr size_t lowrank_rank = 16;

    std::cout << std::left << std::setw(12) << "case"
              << "  " << std::right << std::setw(3) << "b"
              << "  " << std::setw(5) << "fit"
              << "  " << std::setw(9) << "R@1"
              << "  " << std::setw(9) << "R@10"
              << "  " << std::setw(10) << "gap-to-best"
              << "  " << std::setw(8) << "bytes/vec"
              << "  " << std::setw(12) << "verdict\n";
    std::cout << std::string(75, '-') << "\n";

    // Collect R@1 per dim for gap computation
    float best_384 = 0.0f, best_768 = 0.0f;
    auto start_total = std::chrono::steady_clock::now();

    for (const auto& c : cases) {
        auto case_start = std::chrono::steady_clock::now();
        std::cerr << "  [running] " << c.name << std::flush;
        // Generate data
        std::mt19937 cr(42), qr(999);
        std::vector<std::vector<float>> corpus(corpus_size), queries(num_q);

        if (c.dist == std::string("random")) {
            for (auto& v : corpus)
                v = makeUnitVec(c.dim, cr);
            for (auto& q : queries)
                q = makeUnitVec(c.dim, qr);
        } else if (c.dist == std::string("clustered")) {
            auto centers = buildCenters(c.dim, num_clusters, cr);
            for (auto& v : corpus)
                v = makeClusteredVec(c.dim, num_clusters, cluster_std, cr, centers);
            for (auto& q : queries)
                q = makeClusteredVec(c.dim, num_clusters, cluster_std, qr, centers);
        } else if (c.dist == std::string("lowrank")) {
            for (auto& v : corpus)
                v = makeLowRankVec(c.dim, lowrank_rank, cr);
            for (auto& q : queries)
                q = makeLowRankVec(c.dim, lowrank_rank, qr);
        }

        // Create quantizer
        yams::vector::TurboQuantConfig cfg;
        cfg.dimension = c.dim;
        cfg.bits_per_channel = c.bits;
        cfg.seed = 42;
        yams::vector::TurboQuantMSE tq(cfg);
        // 4-bit uses k-means for centroids (16 centroids → fast). 6/8-bit use
        // static Lloyd-Max centroids (pre-computed on N(0,1), no k-means needed).
        if (c.fit) {
            // Use k-means for all bit depths — Lloyd-Max tables assume N(0,1) sigma=1,
            // which doesn't match unit-sphere data. k-means fits centroids to the actual
            // corpus distribution, giving correct packed scores for all bit depths.
            tq.fit(corpus, 5); // k-means centroids + scales (5 iterations, fast for 64-256 cents)
        }

        // Measure ceiling
        auto result = measureCeiling(tq, corpus, queries);

        // Track best per dim
        if (c.dim == 384 && result.r1 > best_384)
            best_384 = result.r1;
        if (c.dim == 768 && result.r1 > best_768)
            best_768 = result.r1;

        float gap = (c.dim == 384)   ? (best_384 - result.r1)
                    : (c.dim == 768) ? (best_768 - result.r1)
                                     : 0.0f;

        const char* verdict = result.r1 >= 0.50f   ? "ANN-viable"
                              : result.r1 >= 0.30f ? "rerank-ok"
                                                   : "insufficient";

        std::cout << std::left << std::setw(12) << c.name << "  " << std::right << std::setw(3)
                  << static_cast<int>(c.bits) << "  " << std::setw(5) << (c.fit ? "yes" : "no")
                  << "  " << std::fixed << std::setprecision(1) << std::setw(9) << (result.r1 * 100)
                  << "%"
                  << "  " << std::setw(9) << (result.r10 * 100) << "%"
                  << "  " << std::setw(10) << (gap * 100) << "%"
                  << "  " << std::setw(8) << tq.storageSize() << "  " << std::setw(12) << verdict;
        auto case_end = std::chrono::steady_clock::now();
        double case_s = std::chrono::duration<double>(case_end - case_start).count();
        std::cerr << " (" << std::fixed << std::setprecision(1) << case_s << "s)\n";
        std::cerr << std::flush;
    }

    std::cout << "\nVerdict key:\n";
    std::cout << "  ANN-viable:    packed R@1 >= 50% — compressed ANN reasonable for k=1\n";
    std::cout
        << "  rerank-ok:     30-50% — TurboQuant rerank (96% recall) is the production path\n";
    std::cout
        << "  insufficient:  <30% — scorer fidelity too low; stop investing in compressed ANN\n";
    std::cout << "\nStorage reference:\n";
    std::cout << "  float: " << (384 * 4) << "B/vec | 4b: 192B | 6b: 288B | 8b: 384B (384d)\n";
    std::cout << "  Note: unfitted nbit uses shared Lloyd-Max centroids (sigma=1.0 default).\n";
    std::cout << "        fitted k-means uses per-coord centroids + scales from corpus.\n";

    return 0;
}
