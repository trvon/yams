// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/topological_quality.h>

#include <cmath>
#include <random>
#include <vector>

using Catch::Approx;
using yams::search::computePersistenceH0;
using yams::search::deterministicSubsample;

namespace {

std::vector<std::vector<float>> makeTwoClusters(std::size_t nPerCluster, std::size_t dim,
                                                float separation, std::uint32_t seed) {
    std::mt19937 gen{seed};
    std::normal_distribution<float> noise{0.0F, 0.05F};
    std::vector<std::vector<float>> out;
    out.reserve(2 * nPerCluster);
    for (std::size_t i = 0; i < nPerCluster; ++i) {
        std::vector<float> v(dim, 0.0F);
        for (auto& x : v) {
            x = noise(gen);
        }
        out.push_back(v);
    }
    for (std::size_t i = 0; i < nPerCluster; ++i) {
        std::vector<float> v(dim, 0.0F);
        v[0] = separation;
        for (std::size_t k = 1; k < dim; ++k) {
            v[k] = noise(gen);
        }
        v[0] += noise(gen);
        out.push_back(v);
    }
    return out;
}

std::vector<std::vector<float>> makeCollapsed(std::size_t n, std::size_t dim, std::uint32_t seed) {
    std::mt19937 gen{seed};
    std::normal_distribution<float> tiny{0.0F, 1e-5F};
    std::vector<std::vector<float>> out(n, std::vector<float>(dim, 0.0F));
    for (auto& row : out) {
        for (auto& x : row) {
            x = tiny(gen);
        }
    }
    return out;
}

} // namespace

TEST_CASE("computePersistenceH0: empty / degenerate inputs",
          "[unit][search][tda][topological_quality][catch2]") {
    CHECK(computePersistenceH0(std::span<const float>{}, 0, 0) == Approx(0.0));
    CHECK(computePersistenceH0(std::vector<std::vector<float>>{}) == Approx(0.0));
    std::vector<std::vector<float>> single{{0.1F, 0.2F, 0.3F}};
    CHECK(computePersistenceH0(single) == Approx(0.0));
}

TEST_CASE("computePersistenceH0: collapsed cluster yields low persistence",
          "[unit][search][tda][topological_quality][catch2]") {
    auto pts = makeCollapsed(64, 8, 42);
    const double r = computePersistenceH0(pts);
    CHECK(r < 100.0);
}

TEST_CASE("computePersistenceH0: well-separated clusters produce measurable persistence",
          "[unit][search][tda][topological_quality][catch2]") {
    auto pts = makeTwoClusters(32, 16, 5.0F, 7);
    const double r = computePersistenceH0(pts);
    CHECK(r > 0.1);
}

TEST_CASE("computePersistenceH0: dispersed points > collapsed points",
          "[unit][search][tda][topological_quality][catch2]") {
    auto collapsed = makeCollapsed(64, 8, 123);
    std::mt19937 gen{99};
    std::uniform_real_distribution<float> uni{-1.0F, 1.0F};
    std::vector<std::vector<float>> dispersed(64, std::vector<float>(8, 0.0F));
    for (auto& row : dispersed) {
        for (auto& x : row) {
            x = uni(gen);
        }
    }
    const double rCollapsed = computePersistenceH0(collapsed);
    const double rDispersed = computePersistenceH0(dispersed);
    CHECK(rDispersed > rCollapsed);
}

TEST_CASE("computePersistenceH0: overload matches span form",
          "[unit][search][tda][topological_quality][catch2]") {
    auto pts = makeTwoClusters(16, 4, 2.0F, 1);
    const double rA = computePersistenceH0(pts);
    std::vector<float> flat;
    for (const auto& row : pts) {
        flat.insert(flat.end(), row.begin(), row.end());
    }
    const double rB = computePersistenceH0(std::span<const float>(flat), pts.size(), 4);
    CHECK(rA == Approx(rB));
}

TEST_CASE("deterministicSubsample: reproducible + bounded",
          "[unit][search][tda][topological_quality][catch2]") {
    auto a = deterministicSubsample(1000, 100, 42);
    auto b = deterministicSubsample(1000, 100, 42);
    CHECK(a == b);
    CHECK(a.size() == 100);
    for (auto i : a) {
        CHECK(i < 1000);
    }
    auto c = deterministicSubsample(1000, 100, 43);
    CHECK(a != c);
    auto d = deterministicSubsample(50, 100, 42);
    CHECK(d.size() == 50);
    for (std::size_t k = 0; k < 50; ++k) {
        CHECK(d[k] == k);
    }
}
