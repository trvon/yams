// Diagnostic: measure if graph topology matters or if scoring is broken

#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include <cmath>
#include <iomanip>

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

int main() {
    size_t dim = 384;
    size_t bits = 4;
    size_t n = 1000;

    std::mt19937 corpus_rng(42);
    std::mt19937 query_rng(999);

    std::vector<std::vector<float>> corpus(n);
    for (auto& v : corpus)
        v = generateRandomUnitVector(dim, corpus_rng);
    std::vector<std::vector<float>> queries(10);
    for (auto& q : queries)
        q = generateRandomUnitVector(dim, query_rng);

    yams::vector::TurboQuantConfig cfg;
    cfg.dimension = dim;
    cfg.bits_per_channel = static_cast<uint8_t>(bits);
    cfg.seed = 42;
    yams::vector::TurboQuantMSE quantizer(cfg);

    std::cout << "=== Direct brute-force search using scoreFromPacked ===\n";
    std::cout << "If this gives low recall, the scoring function itself is broken.\n\n";

    for (size_t qi = 0; qi < 3; ++qi) {
        auto tq = quantizer.transformQuery(queries[qi]);
        std::vector<std::pair<float, size_t>> scores;
        for (size_t i = 0; i < n; ++i) {
            auto p = quantizer.packedEncode(corpus[i]);
            scores.emplace_back(quantizer.scoreFromPacked(tq, p), i);
        }
        std::sort(scores.begin(), scores.end(), [](auto& a, auto& b) { return a.first > b.first; });

        // True top-5
        std::vector<std::pair<float, size_t>> true_scores;
        for (size_t i = 0; i < n; ++i) {
            float dot = 0.0f;
            for (size_t d = 0; d < dim; ++d)
                dot += queries[qi][d] * corpus[i][d];
            true_scores.emplace_back(dot, i);
        }
        std::sort(true_scores.begin(), true_scores.end(),
                  [](auto& a, auto& b) { return a.first > b.first; });

        std::cout << "Query " << qi << ":\n";
        std::cout << "  scoreFromPacked top-5: ";
        for (size_t k = 0; k < 5; ++k)
            std::cout << scores[k].second << " ";
        std::cout << "\n  true cosine top-5:       ";
        for (size_t k = 0; k < 5; ++k)
            std::cout << true_scores[k].second << " ";
        std::cout << "\n  match at top-1: "
                  << (scores[0].second == true_scores[0].second ? "YES" : "NO") << "\n";
    }

    std::cout << "\n=== Now test: graph with decoded vectors (control) ===\n";
    std::cout << "If decoded-vector graph gives good recall, the GRAPH is the issue.\n";
    std::cout << "If both fail, SCORING is fundamentally broken.\n";

    return 0;
}
