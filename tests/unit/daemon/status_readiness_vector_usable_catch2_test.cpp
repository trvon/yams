// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/metric_keys.h>

#include <string>
#include <unordered_map>

using yams::daemon::dispatch::compute_vector_backend_usable;
namespace readiness = yams::daemon::readiness;

TEST_CASE("vector backend usable: predicate semantics", "[daemon][readiness]") {
    SECTION("fresh daemon: dim known, provider ready, zero rows → usable") {
        REQUIRE(compute_vector_backend_usable(true, true) == true);
    }
    SECTION("backend up, rows present → usable") {
        REQUIRE(compute_vector_backend_usable(true, true) == true);
    }
    SECTION("provider not ready → not usable") {
        REQUIRE(compute_vector_backend_usable(true, false) == false);
    }
    SECTION("dim unknown → not usable") {
        REQUIRE(compute_vector_backend_usable(false, true) == false);
    }
    SECTION("neither signal → not usable") {
        REQUIRE(compute_vector_backend_usable(false, false) == false);
    }
}

TEST_CASE("vector backend usable: status fallback wiring uses dim + provider, not rows",
          "[daemon][readiness]") {
    std::unordered_map<std::string, bool> readinessStates;
    readinessStates[std::string(readiness::kVectorDbReady)] = false;
    readinessStates[std::string(readiness::kVectorDbDim)] = true;
    readinessStates[std::string(readiness::kModelProvider)] = true;

    const bool dimKnown = readinessStates[std::string(readiness::kVectorDbDim)];
    const bool providerReady = readinessStates[std::string(readiness::kModelProvider)];
    const bool vectorDbReady = readinessStates[std::string(readiness::kVectorDbReady)];

    const bool vectorBackendUsable = compute_vector_backend_usable(dimKnown, providerReady);

    REQUIRE(vectorBackendUsable == true);
    REQUIRE(vectorDbReady == false);
    REQUIRE(vectorBackendUsable != vectorDbReady);
}

TEST_CASE("vector backend usable: key constants remain distinct", "[daemon][readiness]") {
    REQUIRE(readiness::kVectorDbReady != readiness::kVectorDbDim);
    REQUIRE(readiness::kVectorDbReady != readiness::kSearchEngineVectorUsable);
    REQUIRE(readiness::kSearchEngineVectorUsable != readiness::kSearchEngineHybridUsable);
    REQUIRE(readiness::kSearchEngineVectorUsable != readiness::kSearchEngineLexicalReady);
}
