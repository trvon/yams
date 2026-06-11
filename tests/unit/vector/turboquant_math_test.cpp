// Catch2 unit tests for the QJL mathematical contract
// Tests the QJL gamma / sign-agreement / arcsin relationship.
// Validates the Joint Normal Lemma result:
//
//   E[sgn(S·r1)_m · sgn(S·r2)_m] = (2/π) · arcsin(ρ_res)
//
// where ρ_res = (r1·r2) / (||r1||·||r2||) is the residual correlation.

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>
#include <yams/vector/turboquant.h>

using namespace yams::vector;

namespace {

constexpr double kPi = 3.14159265358979323846;

// Verify E[sgn(a)·sgn(b)] = (2/π)·arcsin(ρ) for Gaussian pairs
// by generating many trials and comparing empirical mean to theoretical value.
TEST_CASE("QJL gamma contract: E[sgn·sgn] = (2/π)·arcsin(ρ)", "[turboquant][math][qjl][catch2]") {
    std::mt19937 rng(137);

    // Test several correlation values: 0 (orthogonal), 0.3, 0.7, 1.0 (identical)
    std::vector<double> rhos = {0.0, 0.3, 0.7, 1.0};
    constexpr size_t kTrials = 10000;

    for (double rho : rhos) {
        double sum_sign_product = 0.0;
        for (size_t t = 0; t < kTrials; ++t) {
            // Generate two correlated standard normal variables (X, Y) with correlation rho
            // X ~ N(0,1), Y = rho·X + sqrt(1-rho²)·Z, Z~N(0,1) independent of X
            std::normal_distribution<float> dist(0.0f, 1.0f);
            float x = dist(rng);
            float z = dist(rng);
            float rhoFloat = static_cast<float>(rho);
            float y = rhoFloat * x + std::sqrt(1.0f - rhoFloat * rhoFloat) * z;

            float sx = (x >= 0.0f) ? 1.0f : -1.0f;
            float sy = (y >= 0.0f) ? 1.0f : -1.0f;
            float sp = sx * sy; // sgn(a)·sgn(b)

            sum_sign_product += sp;
        }

        double empirical = sum_sign_product / static_cast<double>(kTrials);
        double theoretical = (2.0 / kPi) * std::asin(rho);

        INFO("rho=" << rho << ": empirical=" << empirical << ", theoretical=" << theoretical
                    << ", diff=" << (empirical - theoretical));

        // Allow generous tolerance due to variance in finite samples (10% or 0.05 absolute)
        double abs_diff = std::abs(empirical - theoretical);
        CHECK(abs_diff < std::max(0.05, std::abs(theoretical) * 0.15));
    }
}

// Verify the inverse: arcsin(E[sgn·sgn] · π/2) ≈ ρ
// This is what estimateInnerProductFull() does internally.
TEST_CASE("QJL gamma contract: arcsin inversion recovers rho", "[turboquant][math][qjl][catch2]") {
    std::mt19937 rng(911);
    constexpr size_t kTrials = 10000;

    // Test with a fixed rho but many trials
    double rho = 0.5;
    double sum_sign_product = 0.0;

    for (size_t t = 0; t < kTrials; ++t) {
        std::normal_distribution<float> dist(0.0f, 1.0f);
        float x = dist(rng);
        float z = dist(rng);
        float rhoFloat = static_cast<float>(rho);
        float y = rhoFloat * x + std::sqrt(1.0f - rhoFloat * rhoFloat) * z;

        float sx = (x >= 0.0f) ? 1.0f : -1.0f;
        float sy = (y >= 0.0f) ? 1.0f : -1.0f;
        sum_sign_product += sx * sy;
    }

    double empirical_sp = sum_sign_product / static_cast<double>(kTrials);
    double recovered_rho = std::sin((kPi / 2.0) * empirical_sp);

    INFO("Input rho=" << rho << ", empirical E[sgn·sgn]=" << empirical_sp
                      << ", recovered rho=" << recovered_rho);
    CHECK(std::abs(recovered_rho - rho) < 0.05);
}

// Verify the sign agreement rate gamma_raw vs sign product relationship:
// gamma_raw = (1 + E[sgn·sgn]) / 2
// For orthogonal vectors (E[sgn·sgn]=0): gamma_raw ≈ 0.5
TEST_CASE("QJL gamma contract: orthogonal vectors give gamma_raw ≈ 0.5",
          "[turboquant][math][qjl][catch2]") {
    std::mt19937 rng(777);
    constexpr size_t kTrials = 50000;

    double sum_sign_product = 0.0;
    for (size_t t = 0; t < kTrials; ++t) {
        std::normal_distribution<float> dist(0.0f, 1.0f);
        float x = dist(rng);
        float y = dist(rng); // Independent of x

        float sx = (x >= 0.0f) ? 1.0f : -1.0f;
        float sy = (y >= 0.0f) ? 1.0f : -1.0f;
        sum_sign_product += sx * sy;
    }

    double empirical_sp = sum_sign_product / static_cast<double>(kTrials);
    double gamma_raw = (1.0 + empirical_sp) / 2.0;

    INFO("Orthogonal Gaussian pair: E[sgn·sgn]=" << empirical_sp << ", gamma_raw=" << gamma_raw);
    CHECK(std::abs(empirical_sp) < 0.03);     // Should be close to 0
    CHECK(std::abs(gamma_raw - 0.5) < 0.015); // gamma_raw should be close to 0.5
}

// Correct inversion: arcsin(rho) = pi*gamma - pi/2  ->  rho = sin(pi*gamma - pi/2) = -cos(pi*gamma)
// For orthogonal (rho=0): gamma_raw=0.5, rho_res=sin(pi/2-pi/2)=sin(0)=0
// For identical (rho=1): gamma_raw=1.0, rho_res=sin(pi-pi/2)=sin(pi/2)=1
TEST_CASE("QJL gamma contract: raw agreement maps to rho via -cos(pi*gamma)",
          "[turboquant][math][qjl][catch2]") {
    std::mt19937 rng(333);
    constexpr size_t kTrials = 50000;

    // Compute empirical gamma_raw from many orthogonal Gaussian pairs
    size_t matching = 0;
    for (size_t t = 0; t < kTrials; ++t) {
        std::normal_distribution<float> dist(0.0f, 1.0f);
        float x = dist(rng);
        float y = dist(rng);
        float sx = (x >= 0.0f) ? 1.0f : -1.0f;
        float sy = (y >= 0.0f) ? 1.0f : -1.0f;
        if (sx == sy)
            matching++;
    }

    double gamma_raw = static_cast<double>(matching) / kTrials;
    // Correct formula: rho_res = -cos(pi*gamma) = sin(pi*gamma - pi/2)
    double rho_res = -std::cos(kPi * gamma_raw);

    INFO("Orthogonal: gamma_raw=" << gamma_raw << ", rho_res=" << rho_res);
    // For orthogonal: gamma_raw~0.5, rho_res = -cos(pi*0.5) = -cos(pi/2) = 0
    CHECK(gamma_raw > 0.48);
    CHECK(gamma_raw < 0.52); // gamma_raw ~ 0.5
    CHECK(rho_res > -0.1);   // rho_res ~ 0 (orthogonal)
    CHECK(rho_res < 0.1);
}

} // namespace
