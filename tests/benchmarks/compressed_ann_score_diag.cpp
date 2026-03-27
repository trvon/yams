// Compact R@1 ceiling test for all configurations.
// Key question: does the fitted scorer (scales*centroids) or unfitted (raw centroids) rank better?

#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <numeric>

#include "yams/vector/turboquant.h"

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

int main() {
    constexpr size_t dim = 384;
    constexpr size_t n = 1000;
    constexpr size_t nq = 50;

    std::mt19937 cr(42), qr(999);

    std::vector<std::vector<float>> corpus(n), queries(nq);
    for (auto& v : corpus)
        v = makeUnitVec(dim, cr);
    for (auto& q : queries)
        q = makeUnitVec(dim, qr);

    std::cout << "dim=" << dim << " n=" << n << " nq=" << nq << "\n\n";

    // Show true-cosine R@1 baseline (sanity: should be near 0% for random data)
    {
        size_t ok = 0;
        for (size_t qi = 0; qi < nq; ++qi) {
            size_t best = 0;
            float bs = -1.0f;
            for (size_t ci = 0; ci < n; ++ci) {
                float s = std::inner_product(queries[qi].begin(), queries[qi].end(),
                                             corpus[ci].begin(), 0.0f);
                if (s > bs) {
                    bs = s;
                    best = ci;
                }
            }
            size_t true_best = 0;
            float ts = -1.0f;
            for (size_t ci = 0; ci < n; ++ci) {
                float s = std::inner_product(queries[qi].begin(), queries[qi].end(),
                                             corpus[ci].begin(), 0.0f);
                if (s > ts) {
                    ts = s;
                    true_best = ci;
                }
            }
            if (best == true_best)
                ok++;
        }
        std::cout << "[sanity] true-cosine R@1=" << std::fixed << std::setprecision(1)
                  << (ok * 100.0 / nq) << "% (expect ~0% for random data, n=" << n << ")\n\n";
    }

    struct Row {
        const char* name;
        size_t bits;
        bool fit;
    };
    const Row rows[] = {
        {"random", 4, false}, {"random", 4, true}, {"random", 3, false},
        {"random", 3, true},  {"random", 2, true},
    };

    std::cout << std::left << std::setw(12) << "dataset" << std::right << std::setw(5) << "b"
              << std::setw(7) << "fit" << std::setw(8) << "R@1" << std::setw(9) << "top10"
              << std::setw(14) << "verdict\n";
    std::cout << std::string(60, '-') << "\n";

    for (const auto& row : rows) {
        yams::vector::TurboQuantConfig cfg;
        cfg.dimension = dim;
        cfg.bits_per_channel = static_cast<uint8_t>(row.bits);
        cfg.seed = 42;
        yams::vector::TurboQuantMSE tq(cfg);

        if (row.fit)
            tq.fitPerCoordScales(corpus, 0);

        size_t ok1 = 0;
        size_t ok10 = 0;

        for (size_t qi = 0; qi < nq; ++qi) {
            auto tq_q = tq.transformQuery(queries[qi]);

            std::vector<std::pair<float, size_t>> scored;
            scored.reserve(n);
            for (size_t ci = 0; ci < n; ++ci) {
                auto p = tq.packedEncode(corpus[ci]);
                scored.emplace_back(tq.scoreFromPacked(tq_q, p), ci);
            }
            std::sort(scored.begin(), scored.end(),
                      [](auto& a, auto& b) { return a.first > b.first; });

            std::vector<std::pair<float, size_t>> true_s;
            true_s.reserve(n);
            for (size_t ci = 0; ci < n; ++ci) {
                float s = std::inner_product(queries[qi].begin(), queries[qi].end(),
                                             corpus[ci].begin(), 0.0f);
                true_s.emplace_back(s, ci);
            }
            std::partial_sort(true_s.begin(), true_s.begin() + 10, true_s.end(),
                              [](auto& a, auto& b) { return a.first > b.first; });

            if (scored[0].second == true_s[0].second)
                ok1++;
            for (size_t k = 0; k < 10; ++k)
                if (scored[k].second == true_s[k].second)
                    ok10++;
        }

        float r1 = ok1 * 100.0f / nq;
        float t10 = ok10 * 100.0f / (nq * 10);
        const char* verdict = r1 >= 50 ? "✅ R@1>=50%" : r1 >= 30 ? "⚠️ 30-50%" : "❌ <30%";

        std::cout << std::left << std::setw(12) << row.name << std::right << std::setw(5)
                  << row.bits << std::setw(7) << (row.fit ? "yes" : "no") << std::fixed
                  << std::setprecision(1) << std::setw(8) << r1 << "%" << std::setw(9) << t10 << "%"
                  << std::setw(14) << verdict << "\n";
    }

    std::cout << "\nConclusion:\n";
    std::cout << "  R@1 >= 50%: compressed ANN viable for this data profile\n";
    std::cout << "  R@1 30-50%: usable but expect search-quality ceiling\n";
    std::cout << "  R@1 < 30%: scorer fidelity is the primary bottleneck\n";
    return 0;
}
