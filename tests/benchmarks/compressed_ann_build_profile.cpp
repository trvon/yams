// Build-time + recall profiler: measure build time and recall at various scales

#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <algorithm>
#include <numeric>

#include "yams/vector/turboquant.h"
#include "yams/vector/compressed_ann.h"

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

int main() {
    std::vector<size_t> corpus_sizes = {1000, 2000, 5000};
    size_t dim = 384;
    size_t bits = 4;
    size_t m = 32;
    size_t ef_search = 200;

    // Use a DIFFERENT seed for queries to avoid trivial self-match
    std::mt19937 corpus_rng(42); // seed for corpus
    std::mt19937 query_rng(999); // DIFFERENT seed for queries

    std::cerr << "Build + Recall Profile (corpus/queries from different RNG seeds)\n";
    std::cerr << "dim=" << dim << " bits=" << bits << " m=" << m << " ef=" << ef_search << "\n\n";

    yams::vector::TurboQuantConfig tq_cfg;
    tq_cfg.dimension = dim;
    tq_cfg.bits_per_channel = static_cast<uint8_t>(bits);
    tq_cfg.seed = 42;

    yams::vector::TurboQuantMSE quantizer(tq_cfg);

    for (size_t n : corpus_sizes) {
        // Generate corpus
        std::vector<std::vector<float>> corpus(n);
        for (auto& v : corpus)
            v = generateRandomUnitVector(dim, corpus_rng);

        // Generate queries with DIFFERENT seed
        std::vector<std::vector<float>> queries(100);
        for (auto& q : queries)
            q = generateRandomUnitVector(dim, query_rng);

        // Encode corpus
        std::vector<std::vector<uint8_t>> packed(n);
        for (size_t i = 0; i < n; ++i)
            packed[i] = quantizer.packedEncode(corpus[i]);

        // Build index
        yams::vector::CompressedANNIndex::Config ann_cfg;
        ann_cfg.dimension = dim;
        ann_cfg.bits_per_channel = static_cast<uint8_t>(bits);
        ann_cfg.seed = 42;
        ann_cfg.m = m;
        ann_cfg.ef_search = ef_search;
        ann_cfg.max_elements = n + 100;

        yams::vector::CompressedANNIndex index(ann_cfg);
        for (size_t i = 0; i < n; ++i) {
            index.add(i, std::span<const uint8_t>(packed[i]));
        }

        auto t0 = std::chrono::high_resolution_clock::now();
        auto build_result = index.build();
        auto t1 = std::chrono::high_resolution_clock::now();
        double build_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

        if (!build_result) {
            std::cerr << "n=" << std::setw(5) << n << "  BUILD FAILED\n";
            continue;
        }

        // Check graph degrees
        const auto& nodes = index.nodes();
        size_t min_deg = nodes[0].neighbors.size();
        size_t max_deg = min_deg;
        for (size_t i = 1; i < n; ++i) {
            min_deg = std::min(min_deg, nodes[i].neighbors.size());
            max_deg = std::max(max_deg, nodes[i].neighbors.size());
        }

        // Ground truth
        std::vector<size_t> gt_top1(queries.size());
        for (size_t qi = 0; qi < queries.size(); ++qi) {
            size_t best = 0;
            float best_score = -1.0f;
            for (size_t ci = 0; ci < n; ++ci) {
                float s = cosineScore(queries[qi], corpus[ci]);
                if (s > best_score) {
                    best_score = s;
                    best = ci;
                }
            }
            gt_top1[qi] = best;
        }

        // Search
        size_t correct = 0;
        double total_lat_ms = 0;
        for (size_t qi = 0; qi < queries.size(); ++qi) {
            auto t_start = std::chrono::high_resolution_clock::now();
            auto results = index.search(queries[qi], 1);
            auto t_end = std::chrono::high_resolution_clock::now();
            total_lat_ms += std::chrono::duration<double, std::milli>(t_end - t_start).count();
            if (!results.empty() && results[0].id == gt_top1[qi])
                correct++;
        }

        std::cerr << "n=" << std::setw(5) << n << "  build=" << std::fixed << std::setprecision(0)
                  << build_ms << "ms"
                  << "  R@1=" << std::setprecision(1) << (correct * 100.0 / queries.size()) << "%"
                  << "  lat=" << std::setprecision(1) << (total_lat_ms / queries.size()) << "ms"
                  << "  deg=[" << min_deg << "," << max_deg << "]"
                  << "  mem=" << std::fixed << std::setprecision(1)
                  << (index.memoryBytes() / (1024.0 * 1024.0)) << "MB\n";
    }

    return 0;
}
