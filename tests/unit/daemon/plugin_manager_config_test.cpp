// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>

#include <yams/daemon/components/PluginManager.h>

namespace yams::daemon::test {

TEST_CASE("PluginManager: ONNX config JSON enforces minimum pool size", "[daemon]") {
    SECTION("empty config uses minimum") {
        auto adjusted = ::yams::daemon::adjustOnnxConfigJson("", 1);
        auto json = nlohmann::json::parse(adjusted);
        REQUIRE(json.contains("max_loaded_models"));
        CHECK(json["max_loaded_models"].get<std::size_t>() == 3);
    }

    SECTION("config respects higher max_loaded_models") {
        auto adjusted = ::yams::daemon::adjustOnnxConfigJson("{\"max_loaded_models\": 5}", 2);
        auto json = nlohmann::json::parse(adjusted);
        CHECK(json["max_loaded_models"].get<std::size_t>() == 5);
    }

    SECTION("config respects daemon default when higher") {
        auto adjusted = ::yams::daemon::adjustOnnxConfigJson("{\"max_loaded_models\": 1}", 4);
        auto json = nlohmann::json::parse(adjusted);
        CHECK(json["max_loaded_models"].get<std::size_t>() == 4);
    }
}

} // namespace yams::daemon::test
