// SPDX-License-Identifier: GPL-3.0-or-later
// Focused benchmark: compressed ANN at corpus scales from 1k to 50k vectors.
// Run with: ./builddir/tests/benchmarks/compressed_ann_scale_bench
//
// Metrics captured per config:
//   - Build time (ms)
//   - Search latency p50/p95 (ms)
//   - Recall@1, Recall@10 vs brute-force decode+cosine
//   - Candidate count (compressed) vs corpus size
//   - Memory bytes
//   - Throughput (searches/sec)

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <span>
#include <sstream>
#include <string>
#include <vector>

#include <yams/vector/compressed_ann.h>
#include <yams/vector/turboquant.h>

namespace {

constexpr float kPi = 3.14159265358979323846f;

std::vector<float> generateUnitVector(size_t dim, std::mt19937& rng) {
    std::vector<float> v(dim);
    float norm_sq = 0.0f;
    for (size_t i = 0; i < dim; ++i) {
        v[i] = std::normal_distribution<float>(0.0f, 1.0f)(rng);
        norm_sq += v[i] * v[i];
    }
    float inv_norm = 1.0f / std::sqrt(std::max(norm_sq, 1e-9f));
    for (size_t i = 0; i < dim; ++i)
        v[i] *= inv_norm;
    return v;
}

struct ScaleConfig {
    size_t dim;
    uint8_t bits;
    size_t corpus;
    size_t queries;
    size_t ef_search;
    size_t m; // graph degree
};

struct ScaleResult {
    size_t dim;
    uint8_t bits;
    size_t corpus;
    size_t ef_search;
    size_t m;
    double build_ms;
    double search_p50_ms;
    double search_p95_ms;
    double search_p99_ms;
    double recall_at_1;
    double recall_at_10;
    double throughput;
    double candidate_avg;
    double candidate_max;
    double memory_mb;
    double decode_escape_rate; // fraction of searches with decode escapes
    std::string status;
};

std::vector<ScaleResult> runScaleBenchmark(const std::vector<ScaleConfig>& configs, uint32_t seed) {
    std::vector<ScaleResult> results;
    results.reserve(configs.size());

    for (const auto& cfg : configs) {
        ScaleResult r;
        r.dim = cfg.dim;
        r.bits = cfg.bits;
        r.corpus = cfg.corpus;
        r.ef_search = cfg.ef_search;
        r.m = cfg.m;
        r.status = "OK";

        std::cout << std::flush;
        std::cerr << "[bench] dim=" << cfg.dim << " bits=" << (int)cfg.bits
                  << " corpus=" << cfg.corpus << " ef=" << cfg.ef_search << " m=" << cfg.m
                  << std::endl;

        std::mt19937 rng(seed);
        std::mt19937 q_rng(seed + 99999);

        // --- Generate corpus ---
        std::cerr << "  [1/6] Generating corpus (" << cfg.corpus << " vectors)..." << std::flush;
        std::vector<std::vector<float>> corpus;
        corpus.reserve(cfg.corpus);
        for (size_t i = 0; i < cfg.corpus; ++i) {
            corpus.push_back(generateUnitVector(cfg.dim, rng));
        }
        std::cerr << " done" << std::endl;

        // --- Generate queries (held-out from corpus) ---
        std::cerr << "  [2/6] Generating " << cfg.queries << " queries..." << std::flush;
        std::vector<std::vector<float>> queries;
        queries.reserve(cfg.queries);
        for (size_t i = 0; i < cfg.queries; ++i) {
            queries.push_back(generateUnitVector(cfg.dim, q_rng));
        }
        std::cerr << " done" << std::endl;

        // --- Encode corpus with TurboQuant ---
        std::cerr << "  [3/6] Encoding corpus..." << std::flush;
        yams::vector::TurboQuantConfig tq_cfg;
        tq_cfg.dimension = cfg.dim;
        tq_cfg.bits_per_channel = cfg.bits;
        tq_cfg.seed = seed;
        yams::vector::TurboQuantMSE tq(tq_cfg);

        std::vector<std::vector<uint8_t>> packed_corpus;
        packed_corpus.reserve(cfg.corpus);
        for (size_t i = 0; i < cfg.corpus; ++i) {
            packed_corpus.push_back(tq.packedEncode(corpus[i]));
        }
        std::cerr << " done" << std::endl;

        // --- Build CompressedANNIndex ---
        std::cerr << "  [4/6] Building compressed ANN index..." << std::flush;
        yams::vector::CompressedANNIndex::Config ann_cfg;
        ann_cfg.dimension = cfg.dim;
        ann_cfg.bits_per_channel = cfg.bits;
        ann_cfg.seed = seed;
        ann_cfg.m = cfg.m;
        ann_cfg.ef_search = cfg.ef_search;
        ann_cfg.max_elements = cfg.corpus;

        yams::vector::CompressedANNIndex index(ann_cfg);
        for (size_t i = 0; i < cfg.corpus; ++i) {
            index.add(i, std::span<const uint8_t>(packed_corpus[i]));
        }

        auto build_start = std::chrono::high_resolution_clock::now();
        auto build_result = index.build();
        auto build_end = std::chrono::high_resolution_clock::now();
        if (!build_result) {
            r.status = "BUILD_FAILED: " + build_result.error().message;
            results.push_back(r);
            std::cerr << " FAILED: " << r.status << std::endl;
            continue;
        }
        r.build_ms = std::chrono::duration<double, std::milli>(build_end - build_start).count();
        std::cerr << " done (" << std::fixed << std::setprecision(1) << r.build_ms << " ms)"
                  << std::endl;

        r.memory_mb = static_cast<double>(index.memoryBytes()) / (1024.0 * 1024.0);

        // --- Compute baseline top-10 for every query (brute-force decode+cosine) ---
        std::cerr << "  [5/6] Computing baseline (brute-force decode+cosine, " << cfg.corpus
                  << " × " << cfg.queries << ")..." << std::flush;
        std::vector<std::vector<size_t>> baseline_top10(cfg.queries);
        std::vector<std::vector<size_t>> baseline_top1(cfg.queries);

        for (size_t qi = 0; qi < cfg.queries; ++qi) {
            const auto& q = queries[qi];
            std::vector<std::pair<float, size_t>> scores;
            scores.reserve(cfg.corpus);
            for (size_t vi = 0; vi < cfg.corpus; ++vi) {
                auto decoded = tq.packedDecode(packed_corpus[vi]);
                float dot = 0.0f;
                for (size_t d = 0; d < cfg.dim; ++d) {
                    dot += q[d] * decoded[d];
                }
                scores.emplace_back(dot, vi);
            }
            std::partial_sort(scores.begin(), scores.begin() + 10, scores.end(),
                              [](const auto& a, const auto& b) { return a.first > b.first; });
            baseline_top1[qi].push_back(scores[0].second);
            for (size_t k = 0; k < 10; ++k) {
                baseline_top10[qi].push_back(scores[k].second);
            }
        }
        std::cerr << " done" << std::endl;

        // --- Run compressed ANN search and measure ---
        std::cerr << "  [6/6] Running compressed ANN search (" << cfg.queries << " queries)..."
                  << std::flush;
        std::vector<double> latencies;
        latencies.reserve(cfg.queries);
        std::vector<size_t> candidate_counts;
        candidate_counts.reserve(cfg.queries);
        std::vector<size_t> decode_escapes;
        decode_escapes.reserve(cfg.queries);
        std::vector<std::vector<size_t>> ann_top10(cfg.queries);
        std::vector<std::vector<size_t>> ann_top1(cfg.queries);

        size_t total_decode_escapes = 0;

        for (size_t qi = 0; qi < cfg.queries; ++qi) {
            size_t c_count = 0;
            float d_escapes = 0.0f;

            auto t_start = std::chrono::high_resolution_clock::now();
            auto search_stats = index.searchWithStats(queries[qi], 10, &c_count, &d_escapes);
            auto t_end = std::chrono::high_resolution_clock::now();

            double lat_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
            latencies.push_back(lat_ms);
            candidate_counts.push_back(c_count);
            total_decode_escapes += static_cast<size_t>(d_escapes);

            for (const auto& res : search_stats.results) {
                if (ann_top1[qi].empty()) {
                    ann_top1[qi].push_back(res.id);
                }
                if (ann_top10[qi].size() < 10) {
                    ann_top10[qi].push_back(res.id);
                }
            }
        }
        std::cerr << " done" << std::endl;

        // --- Compute latency percentiles ---
        std::sort(latencies.begin(), latencies.end());
        auto percentile = [&](double p) {
            size_t idx = static_cast<size_t>(std::ceil(p * latencies.size())) - 1;
            return latencies[std::min(idx, latencies.size() - 1)];
        };
        r.search_p50_ms = percentile(0.50);
        r.search_p95_ms = percentile(0.95);
        r.search_p99_ms = percentile(0.99);
        r.throughput =
            cfg.queries / (std::accumulate(latencies.begin(), latencies.end(), 0.0) / 1000.0);

        // --- Compute recall ---
        size_t hit_at_1 = 0, hit_at_10 = 0;
        for (size_t qi = 0; qi < cfg.queries; ++qi) {
            if (!ann_top1[qi].empty() && !baseline_top1[qi].empty() &&
                ann_top1[qi][0] == baseline_top1[qi][0]) {
                ++hit_at_1;
            }
            for (size_t k = 0; k < ann_top10[qi].size(); ++k) {
                for (size_t bk = 0; bk < std::min(baseline_top10[qi].size(), size_t(10)); ++bk) {
                    if (ann_top10[qi][k] == baseline_top10[qi][bk]) {
                        ++hit_at_10;
                        break;
                    }
                }
            }
        }
        r.recall_at_1 = static_cast<double>(hit_at_1) / static_cast<double>(cfg.queries);
        r.recall_at_10 = static_cast<double>(hit_at_10) / static_cast<double>(cfg.queries * 10);

        r.candidate_avg = std::accumulate(candidate_counts.begin(), candidate_counts.end(), 0ULL) /
                          static_cast<double>(cfg.queries);
        r.candidate_max = *std::max_element(candidate_counts.begin(), candidate_counts.end());
        r.decode_escape_rate =
            static_cast<double>(total_decode_escapes) / static_cast<double>(cfg.queries);

        results.push_back(r);
        std::cerr << "  RESULT: recall@1=" << std::fixed << std::setprecision(3) << r.recall_at_1
                  << " recall@10=" << r.recall_at_10 << " p50=" << r.search_p50_ms << "ms"
                  << " candidates=" << r.candidate_avg << "/" << cfg.corpus << std::endl;
    }

    return results;
}

void printTable(const std::vector<ScaleResult>& results) {
    std::cout << "\n=== Compressed ANN Scale Validation ===\n\n";
    std::cout << std::left << std::setw(6) << "dim" << std::setw(5) << "bits" << std::setw(8)
              << "corpus" << std::setw(7) << "ef" << std::setw(6) << "m" << std::setw(9) << "R@1"
              << std::setw(9) << "R@10" << std::setw(9) << "p50(ms)" << std::setw(9) << "p95(ms)"
              << std::setw(8) << "cand% " << std::setw(9) << "build(ms)" << std::setw(9) << "MB"
              << std::setw(12) << "status"
              << "\n";
    std::cout << std::string(100, '-') << "\n";

    for (const auto& r : results) {
        double cand_pct = (r.corpus > 0) ? (100.0 * r.candidate_avg / r.corpus) : 0.0;
        std::cout << std::left << std::setw(6) << r.dim << std::setw(5) << (int)r.bits
                  << std::setw(8) << r.corpus << std::setw(7) << r.ef_search << std::setw(6) << r.m
                  << std::setw(9) << std::fixed << std::setprecision(3) << r.recall_at_1
                  << std::setw(9) << std::fixed << std::setprecision(3) << r.recall_at_10
                  << std::setw(9) << std::fixed << std::setprecision(2) << r.search_p50_ms
                  << std::setw(9) << std::fixed << std::setprecision(2) << r.search_p95_ms
                  << std::setw(7) << std::fixed << std::setprecision(1) << cand_pct << "%"
                  << std::setw(9) << std::fixed << std::setprecision(1) << r.build_ms
                  << std::setw(9) << std::fixed << std::setprecision(2) << r.memory_mb
                  << std::setw(12) << r.status << "\n";
    }
}

void emitJson(const std::vector<ScaleResult>& results) {
    std::cout << "{\"scale_results\":[\n";
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& r = results[i];
        std::cout << "  {\n";
        std::cout << "    \"dim\": " << r.dim << ",\n";
        std::cout << "    \"bits\": " << (int)r.bits << ",\n";
        std::cout << "    \"corpus\": " << r.corpus << ",\n";
        std::cout << "    \"ef_search\": " << r.ef_search << ",\n";
        std::cout << "    \"m\": " << r.m << ",\n";
        std::cout << "    \"recall_at_1\": " << std::fixed << std::setprecision(6) << r.recall_at_1
                  << ",\n";
        std::cout << "    \"recall_at_10\": " << std::fixed << std::setprecision(6)
                  << r.recall_at_10 << ",\n";
        std::cout << "    \"search_p50_ms\": " << std::fixed << std::setprecision(4)
                  << r.search_p50_ms << ",\n";
        std::cout << "    \"search_p95_ms\": " << std::fixed << std::setprecision(4)
                  << r.search_p95_ms << ",\n";
        std::cout << "    \"search_p99_ms\": " << std::fixed << std::setprecision(4)
                  << r.search_p99_ms << ",\n";
        std::cout << "    \"candidate_avg\": " << std::fixed << std::setprecision(2)
                  << r.candidate_avg << ",\n";
        std::cout << "    \"candidate_max\": " << r.candidate_max << ",\n";
        std::cout << "    \"build_ms\": " << std::fixed << std::setprecision(2) << r.build_ms
                  << ",\n";
        std::cout << "    \"memory_mb\": " << std::fixed << std::setprecision(4) << r.memory_mb
                  << ",\n";
        std::cout << "    \"throughput_qps\": " << std::fixed << std::setprecision(2)
                  << r.throughput << ",\n";
        std::cout << "    \"decode_escape_rate\": " << std::fixed << std::setprecision(6)
                  << r.decode_escape_rate << ",\n";
        std::cout << "    \"status\": \"" << r.status << "\"\n";
        std::cout << "  }" << (i + 1 < results.size() ? "," : "") << "\n";
    }
    std::cout << "]}\n";
}

} // anonymous namespace

int main(int argc, char* argv[]) {
    // Parse --json flag
    bool json_only = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg == "--json" || arg == "-j")
            json_only = true;
    }

    if (!json_only) {
        std::cerr << "Compressed ANN Scale Benchmark\n";
        std::cerr
            << "Staged rollout preparation — measuring recall, latency, candidate count at scale\n";
        std::cerr << "Run with --json for machine-readable output\n\n";
    }

    // Test configs: dim × bits × corpus × ef × m
    // Staged rollout thresholds:
    //   Phase A (1k):   128/384 dims, 2/4 bits, 1k vectors
    //   Phase B (10k):  384/768 dims, 2/4 bits, 10k vectors
    //   Phase C (50k):  384/768 dims, 4 bits, 50k vectors
    std::vector<ScaleConfig> configs = {
        // Phase A — 1k scale (near current working set)
        {128, 4, 1000, 100, 50, 8},
        {128, 2, 1000, 100, 50, 8},
        {384, 4, 1000, 100, 50, 8},
        {384, 2, 1000, 100, 50, 8},
        {768, 4, 1000, 100, 50, 8},
        {768, 2, 1000, 100, 50, 8},
        {1536, 4, 1000, 100, 50, 8},
        {1536, 2, 1000, 100, 50, 8},
        // Phase B — 10k scale (target for opt-in rollout)
        {384, 4, 10000, 100, 50, 12},
        {384, 2, 10000, 100, 50, 12},
        {768, 4, 10000, 100, 50, 12},
        {768, 2, 10000, 100, 50, 12},
        // Phase C — 50k scale (target for default-on decision)
        {384, 4, 50000, 100, 50, 16},
        {768, 4, 50000, 100, 50, 16},
        // Scaling study at 10k (ef search space)
        {384, 4, 10000, 100, 20, 12},
        {384, 4, 10000, 100, 100, 12},
        {768, 4, 10000, 100, 20, 12},
        {768, 4, 10000, 100, 100, 12},
    };

    uint32_t seed = 42;
    auto results = runScaleBenchmark(configs, seed);

    if (json_only) {
        emitJson(results);
    } else {
        printTable(results);
        std::cerr << "\nTo get JSON output: " << argv[0] << " --json\n";
    }

    return 0;
}
