// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <yams/search/hotzone_manager.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

std::filesystem::path makeTempPath(const std::string& suffix) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           ("yams-hotzone-" + std::to_string(stamp) + suffix);
}

} // namespace

TEST_CASE("HotzoneManager records boosts and ignores invalid input",
          "[unit][search][hotzone][catch2]") {
    yams::search::HotzoneConfig cfg;
    cfg.max_boost_factor = 1.5;
    yams::search::HotzoneManager manager(cfg);

    CHECK(manager.size() == 0);
    manager.record("", 2.0);
    manager.record("doc-a", 0.0);
    manager.record("doc-a", -1.0);
    CHECK(manager.size() == 0);

    manager.record("doc-a", 1.0);
    CHECK(manager.size() == 1);
    CHECK(manager.getBoost("") == 1.0);
    CHECK(manager.getBoost("missing") == 1.0);

    const double boost = manager.getBoost("doc-a");
    CHECK(boost > 1.0);
    CHECK(boost <= cfg.max_boost_factor);

    yams::search::HotzoneConfig mergeCfg;
    mergeCfg.max_boost_factor = 3.0;
    yams::search::HotzoneManager merging(mergeCfg);
    merging.record("doc-b", 0.25);
    const double firstBoost = merging.getBoost("doc-b");
    merging.record("doc-b", 0.25);
    CHECK(merging.getBoost("doc-b") > firstBoost);
}

TEST_CASE("HotzoneManager decays entries and supports immediate decay",
          "[unit][search][hotzone][catch2]") {
    yams::search::HotzoneConfig cfg;
    cfg.half_life_hours = 1.0;
    yams::search::HotzoneManager manager(cfg);

    manager.record("doc-a", 1.0);
    const double initial = manager.getBoost("doc-a");
    manager.decaySweep(std::chrono::system_clock::now() + std::chrono::hours(2));
    CHECK(manager.getBoost("doc-a") < initial);

    cfg.half_life_hours = 0.0;
    manager.setConfig(cfg);
    manager.decaySweep(std::chrono::system_clock::now() + std::chrono::hours(1));
    CHECK(manager.size() == 0);
}

TEST_CASE("HotzoneManager saves, loads, and rejects invalid files",
          "[unit][search][hotzone][catch2]") {
    const auto path = makeTempPath(".json");
    const auto missingPath = makeTempPath("-missing.json");
    const auto invalidPath = makeTempPath("-invalid.json");
    const auto invalidSchemaPath = makeTempPath("-invalid-schema.json");
    const auto partialEntriesPath = makeTempPath("-partial-entries.json");

    yams::search::HotzoneManager empty;
    CHECK(empty.save(path));
    CHECK_FALSE(std::filesystem::exists(path));

    yams::search::HotzoneManager manager;
    manager.record("doc-a", 1.0);
    REQUIRE(manager.save(path));
    REQUIRE(std::filesystem::exists(path));

    yams::search::HotzoneManager loaded;
    REQUIRE(loaded.load(path));
    CHECK(loaded.size() == 1);
    CHECK(loaded.getBoost("doc-a") > 1.0);

    CHECK_FALSE(loaded.load(missingPath));

    {
        std::ofstream ofs(invalidPath);
        ofs << "not-json";
    }
    CHECK_FALSE(loaded.load(invalidPath));

    {
        std::ofstream ofs(invalidSchemaPath);
        ofs << R"({"version":1})";
    }
    CHECK_FALSE(loaded.load(invalidSchemaPath));

    {
        std::ofstream ofs(partialEntriesPath);
        ofs << R"({"version":1,"entries":[{"key":"bad"},{"key":"doc-b","score":1.0,"updated_ms":0}]})";
    }
    REQUIRE(loaded.load(partialEntriesPath));
    CHECK(loaded.size() == 1);

    std::filesystem::remove(path);
    std::filesystem::remove(invalidPath);
    std::filesystem::remove(invalidSchemaPath);
    std::filesystem::remove(partialEntriesPath);
}
