// Quick validation: metric-aligned graph fix for compressed ANN
// Run: ./compressed_ann_quick_check

#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <nlohmann/json.hpp>

#include "yams/vector/turboquant.h"
#include "yams/vector/compressed_ann.h"

using json = nlohmann::json;

struct QuickConfig {
    size_t dim;
    size_t bits;
    size_t corpus;
    size_t queries;
    size_t m;
    size_t ef_search;
};

static std::vector<float> generateRandomUnitVector(size_t dim, std::mt19937& rng) {
    std::normal_distribution<float> dist(0.0f, 1.0f);
    std::vector<float> v(dim);
    for (auto& x : v)
        x = dist(rng);
    float norm = std::sqrt(std::inner_product(v.begin(), v.end(), v.begin(), 0.0f));
    for (auto& x : v)
        x /= norm;
    return v;
}

static float cosineScore(const std::vector<float>& a, const std::vector<float>& b) {
    return std::inner_product(a.begin(), a.end(), b.begin(), 0.0f);
}

json runQuickTest(const QuickConfig& cfg) {
    std::mt19937 rng(42);

    std::vector<std::vector<float>> corpus(cfg.corpus);
    for (auto& v : corpus)
        v = generateRandomUnitVector(cfg.dim, rng);

    std::vector<std::vector<float>> queries(cfg.queries);
    for (auto& q : queries)
        q = generateRandomUnitVector(cfg.dim, rng);

    yams::vector::TurboQuantConfig tq_cfg;
    tq_cfg.dimension = cfg.dim;
    tq_cfg.bits_per_channel = static_cast<uint8_t>(cfg.bits);
    tq_cfg.seed = 42;

    yams::vector::TurboQuantMSE quantizer(tq_cfg);

    // --- Diagnostic: check scoreFromPacked vs true cosine ---
    if (cfg.corpus <= 2000) {
        auto tq = quantizer;
        auto tq0 = tq.transformQuery(queries[0]);
        auto p0 = tq.packedEncode(corpus[0]);
        float sp0 = tq.scoreFromPacked(tq0, p0);
        float tc0 = cosineScore(queries[0], corpus[0]);

        // Correlate top-50 scored vs true-cosine top-50
        std::vector<float> sp_scores(cfg.corpus), tc_scores(cfg.corpus);
        for (size_t i = 0; i < cfg.corpus; ++i) {
            auto ti = tq.transformQuery(queries[0]);
            auto pi = tq.packedEncode(corpus[i]);
            sp_scores[i] = tq.scoreFromPacked(ti, pi);
            tc_scores[i] = cosineScore(queries[0], corpus[i]);
        }

        // Spearman-like rank correlation: count matching top-K positions
        std::vector<size_t> sp_rank(cfg.corpus), tc_rank(cfg.corpus);
        for (size_t i = 0; i < cfg.corpus; ++i)
            sp_rank[i] = tc_rank[i] = i;
        auto cmp_sp = [&](size_t a, size_t b) { return sp_scores[a] > sp_scores[b]; };
        auto cmp_tc = [&](size_t a, size_t b) { return tc_scores[a] > tc_scores[b]; };
        std::sort(sp_rank.begin(), sp_rank.end(), cmp_sp);
        std::sort(tc_rank.begin(), tc_rank.end(), cmp_tc);

        size_t top1_match = 0;
        for (size_t k = 0; k < 10; ++k) {
            if (sp_rank[k] == tc_rank[k])
                top1_match++;
        }
        std::cerr << "  [diag] scoreFromPacked top-1=" << sp_rank[0]
                  << " (score=" << sp_scores[sp_rank[0]] << ")"
                  << "  true_cosine top-1=" << tc_rank[0] << " (score=" << tc_scores[tc_rank[0]]
                  << ")"
                  << "  top-10 rank match=" << top1_match << "/10"
                  << "  [note: if match<3, scoring is the bottleneck]\n";
    }

    yams::vector::CompressedANNIndex::Config ann_cfg;
    ann_cfg.dimension = cfg.dim;
    ann_cfg.bits_per_channel = static_cast<uint8_t>(cfg.bits);
    ann_cfg.seed = 42;
    ann_cfg.m = cfg.m;
    ann_cfg.ef_search = cfg.ef_search;
    ann_cfg.max_elements = cfg.corpus + cfg.queries;

    yams::vector::CompressedANNIndex index(ann_cfg);

    for (size_t i = 0; i < cfg.corpus; ++i) {
        auto packed = quantizer.packedEncode(corpus[i]);
        auto add_result = index.add(i, std::span<const uint8_t>(packed));
        if (!add_result) {
            std::cerr << "  Add failed: " << add_result.error().message << "\n";
            return json{};
        }
    }

    auto build_result = index.build();
    if (!build_result) {
        std::cerr << "  Build failed: " << build_result.error().message << "\n";
        return json{};
    }

    // Ground truth: top-1 by TRUE cosine
    std::vector<size_t> gt_top1(cfg.queries);
    for (size_t qi = 0; qi < cfg.queries; ++qi) {
        size_t best = 0;
        float best_score = -1.0f;
        for (size_t ci = 0; ci < cfg.corpus; ++ci) {
            float s = cosineScore(queries[qi], corpus[ci]);
            if (s > best_score) {
                best_score = s;
                best = ci;
            }
        }
        gt_top1[qi] = best;
    }

    // Search
    std::vector<double> latencies;
    size_t correct_top1 = 0;

    for (size_t qi = 0; qi < cfg.queries; ++qi) {
        auto t0 = std::chrono::high_resolution_clock::now();
        auto results = index.search(queries[qi], 1);
        auto t1 = std::chrono::high_resolution_clock::now();
        latencies.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());

        if (!results.empty()) {
            if (results[0].id == gt_top1[qi])
                correct_top1++;
        }
    }

    double total_ms = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    double avg_latency_ms = total_ms / std::max<size_t>(latencies.size(), 1);
    double qps = cfg.queries / (total_ms / 1000.0);

    json r;
    r["corpus"] = cfg.corpus;
    r["dim"] = cfg.dim;
    r["bits"] = cfg.bits;
    r["m"] = cfg.m;
    r["ef_search"] = cfg.ef_search;
    r["recall_at_1"] = static_cast<double>(correct_top1) / cfg.queries;
    r["correct_top1"] = correct_top1;
    r["total_queries"] = cfg.queries;
    r["avg_latency_ms"] = avg_latency_ms;
    r["qps"] = qps;
    r["memory_mb"] = static_cast<double>(index.memoryBytes()) / (1024.0 * 1024.0);

    return r;
}

int main() {
    std::cerr << "Compressed ANN — Metric-Aligned Graph Fix Quick Check\n";
    std::cerr << "======================================================\n\n";

    std::vector<QuickConfig> configs = {
        {384, 4, 1000, 100, 50, 50}, // 1k baseline
        {384, 4, 3000, 100, 64, 64}, // 3k @ m=64 (reduced for build-time budget)
    };

    std::vector<json> all_results;

    for (const auto& cfg : configs) {
        std::cerr << "--- corpus=" << cfg.corpus << " dim=" << cfg.dim << " bits=" << cfg.bits
                  << " m=" << cfg.m << " ef=" << cfg.ef_search << " ---\n";

        auto result = runQuickTest(cfg);
        if (!result.empty()) {
            double recall = result["recall_at_1"];
            std::string status;
            if (cfg.corpus == 1000) {
                status = (recall >= 0.80 ? "PASS" : "WARN");
            } else {
                status = (recall >= 0.70 ? "FIXED" : (recall >= 0.30 ? "PARTIAL" : "BROKEN"));
            }

            std::cerr << "  R@1=" << std::fixed << std::setprecision(3) << recall
                      << "  latency=" << std::setprecision(1)
                      << result["avg_latency_ms"].get<double>() << "ms"
                      << "  qps=" << std::setprecision(0) << result["qps"].get<double>()
                      << "  mem=" << std::fixed << std::setprecision(1)
                      << result["memory_mb"].get<double>() << "MB"
                      << "  [" << status << "]\n";

            all_results.push_back(result);
        } else {
            std::cerr << "  FAILED\n";
        }
    }

    std::cout << "\n=== JSON OUTPUT ===\n" << json(all_results).dump(2) << "\n";

    bool ok = all_results.size() == 2 && all_results[1]["recall_at_1"].get<double>() >= 0.70;
    return ok ? 0 : 1;
}
