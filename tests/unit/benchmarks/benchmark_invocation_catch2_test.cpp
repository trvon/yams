// SPDX-License-Identifier: GPL-3.0-or-later

#include "../../benchmarks/benchmark_cli.h"
#include "../../common/benchmark_invocation.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <filesystem>
#include <fstream>
#include <string>

using yams::test::isBenchmarkListArgument;
using yams::test::normalizeBenchmarkListArguments;

namespace fs = std::filesystem;

TEST_CASE("benchmark list aliases are registration-only", "[benchmark][harness][arguments]") {
    CHECK(isBenchmarkListArgument("--benchmark_list_tests"));
    CHECK(isBenchmarkListArgument("--benchmark_list_tests=true"));
    CHECK(isBenchmarkListArgument("--benchmark_list_tests=1"));
    CHECK(isBenchmarkListArgument("--benchmark_list_tests=yes"));
    CHECK(isBenchmarkListArgument("--benchmark_list_tests=on"));
    CHECK(isBenchmarkListArgument("--list-tests"));
    CHECK_FALSE(isBenchmarkListArgument("--benchmark_list_tests=false"));
    CHECK_FALSE(isBenchmarkListArgument("--benchmark_list_tests=0"));
    CHECK_FALSE(isBenchmarkListArgument("--benchmark_list_tests=no"));
    CHECK_FALSE(isBenchmarkListArgument("--benchmark_list_tests=off"));
    CHECK_FALSE(isBenchmarkListArgument("--benchmark_filter=BM_Ingestion"));
}

TEST_CASE("Catch2 list alias is normalized for Google Benchmark",
          "[benchmark][harness][arguments]") {
    std::array<std::string, 3> storage{"bench", "--list-tests", "--benchmark_min_time=1ms"};
    std::array<char*, 3> argv{storage[0].data(), storage[1].data(), storage[2].data()};

    CHECK(normalizeBenchmarkListArguments(static_cast<int>(argv.size()), argv.data()));
    CHECK((std::string{argv[1]} == "--benchmark_list_tests"));
}

TEST_CASE("normal benchmark invocation still requires setup", "[benchmark][harness][arguments]") {
    std::array<std::string, 2> storage{"bench", "--benchmark_filter=BM_Ingestion"};
    std::array<char*, 2> argv{storage[0].data(), storage[1].data()};

    CHECK_FALSE(normalizeBenchmarkListArguments(static_cast<int>(argv.size()), argv.data()));
    CHECK((std::string{argv[1]} == "--benchmark_filter=BM_Ingestion"));
}

TEST_CASE("BenchConfig defaults to a timestamp and config hash run directory",
          "[benchmark][harness][config][manifest]") {
    std::array<std::string, 1> storage{"bench"};
    std::array<char*, 1> argv{storage[0].data()};

    const auto config = yams::benchmark::parseBenchConfig(static_cast<int>(argv.size()),
                                                          argv.data(), "config_contract");

    CHECK((config.suiteName == "config_contract"));
    CHECK((config.outDir.parent_path().filename() == "config_contract"));
    CHECK(config.outDir.filename().string().ends_with(config.configHash));
    CHECK((config.configHash.size() == 16));
    CHECK((config.sources.at("out_dir") == "default:timestamp-config-hash"));

    const auto custom = yams::benchmark::parseBenchConfig(
        static_cast<int>(argv.size()), argv.data(), "custom_defaults",
        yams::benchmark::BenchConfigDefaults{.warmupIterations = 5, .iterations = 20});
    CHECK((custom.warmupIterations == 5));
    CHECK((custom.iterations == 20));
    CHECK((custom.sources.at("warmup_iterations") == "default:5"));
    CHECK((custom.sources.at("iterations") == "default:20"));
}

TEST_CASE("BenchConfig preserves explicit output and serializes effective provenance",
          "[benchmark][harness][config][manifest]") {
    const auto root = fs::temp_directory_path() / "yams_bench_config_contract";
    std::error_code ec;
    fs::remove_all(root, ec);
    std::array<std::string, 7> storage{"bench",       "--iterations", "7", "--out-dir",
                                       root.string(), "--seed",       "42"};
    std::array<char*, 7> argv{};
    for (std::size_t index = 0; index < storage.size(); ++index) {
        argv[index] = storage[index].data();
    }

    auto config = yams::benchmark::parseBenchConfig(static_cast<int>(argv.size()), argv.data(),
                                                    "config_contract");
    if (!config.outputFile) {
        config.outputFile = config.outDir / "config_contract.jsonl";
    }
    REQUIRE(yams::benchmark::prepareBenchmarkRun(config));

    CHECK((config.outDir == root));
    CHECK((config.iterations == 7));
    CHECK((config.seed == 42));
    CHECK((config.sources.at("iterations") == "cli:--iterations"));
    CHECK((config.sources.at("out_dir") == "cli:--out-dir"));
    const auto manifestPath = root / "run_manifest.json";
    REQUIRE(fs::is_regular_file(manifestPath));
    std::ifstream input{manifestPath};
    const std::string manifest{std::istreambuf_iterator<char>{input},
                               std::istreambuf_iterator<char>{}};
    CHECK((manifest.find("\"schema_version\": 1") != std::string::npos));
    CHECK((manifest.find("\"iterations\": 7") != std::string::npos));
    CHECK((manifest.find("\"source\": \"cli:--iterations\"") != std::string::npos));
    CHECK((manifest.find(config.configHash) != std::string::npos));

    fs::remove_all(root, ec);
}

TEST_CASE("BenchConfig default directory creation is exclusive and collision-safe",
          "[benchmark][harness][config][manifest]") {
    const auto root = fs::temp_directory_path() / "yams_bench_config_collision";
    std::error_code error;
    fs::remove_all(root, error);
    const auto occupied = root / "stamp-hash";
    REQUIRE(fs::create_directories(occupied));
    std::ofstream{occupied / "sentinel"} << "owned\n";

    std::array<std::string, 1> storage{"bench"};
    std::array<char*, 1> argv{storage[0].data()};
    auto config = yams::benchmark::parseBenchConfig(static_cast<int>(argv.size()), argv.data(),
                                                    "collision_contract");
    config.outDir = occupied;
    config.outputFile = occupied / "collision.jsonl";
    config.archiveDir = occupied / "archive";

    REQUIRE(yams::benchmark::prepareBenchmarkRun(config));
    CHECK((config.outDir != occupied));
    CHECK((config.outDir.filename() == "stamp-hash-1"));
    CHECK(fs::is_regular_file(occupied / "sentinel"));
    CHECK(fs::is_regular_file(config.outDir / "run_manifest.json"));
    CHECK((config.outputFile == config.outDir / "collision.jsonl"));
    CHECK((config.archiveDir == config.outDir / "archive"));

    fs::remove_all(root, error);
}

TEST_CASE("BenchConfig locates repository from an absolute executable path",
          "[benchmark][harness][config][path]") {
    const auto repository = fs::current_path();
    REQUIRE(fs::is_regular_file(repository / "meson.build"));
    const auto outside = fs::temp_directory_path() / "yams_bench_config_cwd";
    std::error_code error;
    fs::create_directories(outside, error);
    REQUIRE_FALSE(error);

    fs::current_path(outside, error);
    REQUIRE_FALSE(error);
    const auto resolved = yams::benchmark::benchmarkRepositoryRoot(
        (repository / "build" / "debug" / "tests" / "fake-benchmark").string());
    fs::current_path(repository, error);
    REQUIRE_FALSE(error);

    REQUIRE(resolved.has_value());
    CHECK((*resolved == repository));
    fs::remove_all(outside, error);
}
