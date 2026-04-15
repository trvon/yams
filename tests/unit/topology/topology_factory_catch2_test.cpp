#include <catch2/catch_test_macros.hpp>

#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_factory.h>

#include <algorithm>
#include <string>
#include <vector>

using yams::topology::ITopologyEngine;
using yams::topology::listAlgorithms;
using yams::topology::makeEngine;
using yams::topology::resolveFactoryKey;

TEST_CASE("topology::makeEngine returns a usable engine for the default key",
          "[topology][factory][p3_1][catch2]") {
    auto engine = makeEngine("connected");
    REQUIRE(engine != nullptr);

    yams::topology::TopologyBuildConfig cfg;
    cfg.reciprocalOnly = true;
    cfg.inputKind = yams::topology::TopologyInputKind::Hybrid;

    std::vector<yams::topology::TopologyDocumentInput> empty;
    auto result = engine->buildArtifacts(empty, cfg);
    REQUIRE(result);
    // The connected-components engine stamps its own label on the batch; the
    // factory key ("connected") is distinct from the engine label.
    CHECK(result.value().algorithm == "connected_components_v1");
}

TEST_CASE("topology::makeEngine resolves empty / unknown keys to connected",
          "[topology][factory][p3_1][catch2]") {
    auto e1 = makeEngine("");
    auto e2 = makeEngine("nonexistent_algorithm");
    REQUIRE(e1 != nullptr);
    REQUIRE(e2 != nullptr);
    // Both fallback paths should yield the baseline engine, which stamps
    // "connected_components_v1" on empty inputs.
    yams::topology::TopologyBuildConfig cfg;
    std::vector<yams::topology::TopologyDocumentInput> empty;
    auto r1 = e1->buildArtifacts(empty, cfg);
    auto r2 = e2->buildArtifacts(empty, cfg);
    REQUIRE(r1);
    REQUIRE(r2);
    CHECK(r1.value().algorithm == "connected_components_v1");
    CHECK(r2.value().algorithm == "connected_components_v1");
}

TEST_CASE("topology::resolveFactoryKey normalizes unknown inputs",
          "[topology][factory][p3_1][catch2]") {
    CHECK(resolveFactoryKey("connected") == std::string_view{"connected"});
    CHECK(resolveFactoryKey("") == std::string_view{"connected"});
    CHECK(resolveFactoryKey("not_registered") == std::string_view{"connected"});
}

TEST_CASE("topology::listAlgorithms includes the default key",
          "[topology][factory][p3_1][catch2]") {
    const auto algos = listAlgorithms();
    REQUIRE(!algos.empty());
    const bool hasConnected =
        std::find(algos.begin(), algos.end(), std::string{"connected"}) != algos.end();
    CHECK(hasConnected);
}
