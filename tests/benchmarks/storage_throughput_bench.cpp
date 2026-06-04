#include <cstddef>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <benchmark/benchmark.h>

#include "../common/test_data_generator.h"
#include "../common/test_helpers_catch2.h"

#include <yams/storage/storage_engine.h>

namespace {

class StorageFixture : public benchmark::Fixture {
public:
    void SetUp(const benchmark::State& state) override {
        tempDir_ = yams::test::make_temp_dir("yams_bench_storage_");

        yams::storage::StorageConfig cfg;
        cfg.basePath = tempDir_;
        cfg.shardDepth = 2;
        cfg.mutexPoolSize = 128;

        engine_ = std::make_unique<yams::storage::StorageEngine>(cfg);

        auto gen = yams::test::TestDataGenerator();
        const auto dataSize = static_cast<size_t>(state.range(0));
        data_ = gen.generateRandomBytes(dataSize);
        hash_ = "benchmark-test-hash-00000000000000000000000000000000";
    }

    void TearDown(const benchmark::State&) override {
        engine_.reset();
        std::filesystem::remove_all(tempDir_);
    }

protected:
    std::filesystem::path tempDir_;
    std::unique_ptr<yams::storage::StorageEngine> engine_;
    std::vector<std::byte> data_;
    std::string hash_;
};

BENCHMARK_DEFINE_F(StorageFixture, Store)(benchmark::State& state) {
    int iter = 0;
    for (auto _ : state) {
        // Use unique hash per iteration to avoid dedup-path measurement.
        auto hash = "bench-" + std::to_string(iter++);
        auto result = engine_->store(hash, data_);
        benchmark::DoNotOptimize(result);
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * state.range(0));
}
BENCHMARK_REGISTER_F(StorageFixture, Store)
    ->Arg(1024)     // 1KB
    ->Arg(65536)    // 64KB
    ->Arg(1048576); // 1MB

BENCHMARK_DEFINE_F(StorageFixture, Retrieve)(benchmark::State& state) {
    // Pre-store one object for retrieval
    engine_->store(hash_, data_);

    for (auto _ : state) {
        auto result = engine_->retrieveRaw(hash_);
        benchmark::DoNotOptimize(result);
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * state.range(0));
}
BENCHMARK_REGISTER_F(StorageFixture, Retrieve)->Arg(1024)->Arg(65536)->Arg(1048576);

} // namespace
