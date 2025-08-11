#include <benchmark/benchmark.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/compression_utils.h>
#include <random>
#include <vector>
#include <memory>

using namespace yams;
using namespace yams::compression;

// Helper to generate test data
static std::vector<std::byte> GenerateData(size_t size, const std::string& pattern) {
    std::vector<std::byte> data(size);
    
    if (pattern == "random") {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);
        for (auto& b : data) {
            b = std::byte{static_cast<uint8_t>(dis(gen))};
        }
    } else if (pattern == "text") {
        const char* lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
                           "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ";
        size_t loremLen = std::strlen(lorem);
        for (size_t i = 0; i < size; ++i) {
            data[i] = std::byte{static_cast<uint8_t>(lorem[i % loremLen])};
        }
    } else if (pattern == "zeros") {
        std::fill(data.begin(), data.end(), std::byte{0});
    } else if (pattern == "binary") {
        for (size_t i = 0; i < size; ++i) {
            data[i] = std::byte{static_cast<uint8_t>((i % 2) ? 0xFF : 0x00)};
        }
    }
    
    return data;
}

// Benchmark Zstandard compression
static void BM_ZstandardCompress(benchmark::State& state) {
    auto& registry = CompressionRegistry::instance();
    auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
    if (!compressor) {
        state.SkipWithError("Zstandard compressor not available");
        return;
    }
    
    size_t dataSize = state.range(0);
    uint8_t level = static_cast<uint8_t>(state.range(1));
    auto data = GenerateData(dataSize, "text");
    
    for (auto _ : state) {
        auto result = compressor->compress(data, level);
        benchmark::DoNotOptimize(result);
    }
    
    state.SetBytesProcessed(state.iterations() * dataSize);
    state.counters["throughput_Bps"] = benchmark::Counter(
        static_cast<double>(state.iterations() * dataSize),
        benchmark::Counter::kIsRate);
}

// Benchmark Zstandard decompression
static void BM_ZstandardDecompress(benchmark::State& state) {
    auto& registry = CompressionRegistry::instance();
    auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
    if (!compressor) {
        state.SkipWithError("Zstandard compressor not available");
        return;
    }
    
    size_t dataSize = state.range(0);
    auto data = GenerateData(dataSize, "text");
    
    // Pre-compress the data
    auto compressed = compressor->compress(data, 3);
    if (!compressed.has_value()) {
        state.SkipWithError("Failed to compress test data");
        return;
    }
    
    for (auto _ : state) {
        auto result = compressor->decompress(compressed.value().data);
        benchmark::DoNotOptimize(result);
    }
    
    state.SetBytesProcessed(state.iterations() * dataSize);
    state.counters["throughput_Bps"] = benchmark::Counter(
        static_cast<double>(state.iterations() * dataSize),
        benchmark::Counter::kIsRate);
}

// Benchmark compression with different data patterns
static void BM_CompressionPatterns(benchmark::State& state) {
    auto& registry = CompressionRegistry::instance();
    auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
    if (!compressor) {
        state.SkipWithError("Compressor not available");
        return;
    }
    
    size_t dataSize = 100 * 1024; // 100KB
    std::string pattern;
    
    switch (state.range(0)) {
        case 0: pattern = "zeros"; break;
        case 1: pattern = "text"; break;
        case 2: pattern = "binary"; break;
        case 3: pattern = "random"; break;
        default: pattern = "text";
    }
    
    auto data = GenerateData(dataSize, pattern);
    
    for (auto _ : state) {
        auto result = compressor->compress(data, 3);
        benchmark::DoNotOptimize(result);
    }
    
    state.SetBytesProcessed(state.iterations() * dataSize);
    state.counters["throughput_Bps"] = benchmark::Counter(
        static_cast<double>(state.iterations() * dataSize),
        benchmark::Counter::kIsRate);
    state.SetLabel(pattern);
}

// Benchmark compression ratio vs speed tradeoff
static void BM_CompressionLevelTradeoff(benchmark::State& state) {
    auto& registry = CompressionRegistry::instance();
    auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
    if (!compressor) {
        state.SkipWithError("Compressor not available");
        return;
    }
    
    size_t dataSize = 1024 * 1024; // 1MB
    uint8_t level = static_cast<uint8_t>(state.range(0));
    auto data = GenerateData(dataSize, "text");
    
    size_t totalCompressed = 0;
    double totalRatio = 0.0;
    
    for (auto _ : state) {
        auto result = compressor->compress(data, level);
        if (result.has_value()) {
            totalCompressed += result.value().compressedSize;
            totalRatio += static_cast<double>(dataSize) / result.value().compressedSize;
        }
        benchmark::DoNotOptimize(result);
    }
    
    state.SetBytesProcessed(state.iterations() * dataSize);
    state.counters["throughput_Bps"] = benchmark::Counter(
        static_cast<double>(state.iterations() * dataSize),
        benchmark::Counter::kIsRate);
    
    // Report compression ratio as a custom counter
    if (state.iterations() > 0) {
        state.counters["compression_ratio"] = totalRatio / state.iterations();
        state.counters["avg_compressed_size"] = 
            static_cast<double>(totalCompressed) / state.iterations();
    }
}

// Benchmark memory allocation overhead
static void BM_CompressionMemoryOverhead(benchmark::State& state) {
    auto& registry = CompressionRegistry::instance();
    auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
    if (!compressor) {
        state.SkipWithError("Compressor not available");
        return;
    }
    
    // Test with varying sizes to see memory allocation patterns
    size_t dataSize = state.range(0);
    auto data = GenerateData(dataSize, "text");
    
    for (auto _ : state) {
        // Include allocation in the benchmark
        state.PauseTiming();
        auto testData = data; // Copy to ensure fresh allocation
        state.ResumeTiming();
        
        auto result = compressor->compress(testData);
        benchmark::DoNotOptimize(result);
    }
    
    state.SetBytesProcessed(state.iterations() * dataSize);
    state.counters["throughput_Bps"] = benchmark::Counter(
        static_cast<double>(state.iterations() * dataSize),
        benchmark::Counter::kIsRate);
}

// Benchmark concurrent compression
static void BM_ConcurrentCompression(benchmark::State& state) {
    auto& registry = CompressionRegistry::instance();

    const size_t dataSize = 10 * 1024; // 10KB per thread
    const auto data = GenerateData(dataSize, "text");

    for (auto _ : state) {
        auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
        if (!compressor) {
            state.SkipWithError("Compressor not available");
            break;
        }
        auto result = compressor->compress(data);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(state.iterations() * dataSize * state.threads());
    state.SetItemsProcessed(state.iterations() * state.threads());
}

// Register benchmarks
BENCHMARK(BM_ZstandardCompress)
    ->Args({1024, 1})      // 1KB, level 1
    ->Args({1024, 3})      // 1KB, level 3
    ->Args({1024, 9})      // 1KB, level 9
    ->Args({10240, 1})     // 10KB, level 1
    ->Args({10240, 3})     // 10KB, level 3
    ->Args({10240, 9})     // 10KB, level 9
    ->Args({102400, 1})    // 100KB, level 1
    ->Args({102400, 3})    // 100KB, level 3
    ->Args({102400, 9})    // 100KB, level 9
    ->Args({1048576, 1})   // 1MB, level 1
    ->Args({1048576, 3})   // 1MB, level 3
    ->Args({1048576, 9})  // 1MB, level 9
    ->UseRealTime();

BENCHMARK(BM_ZstandardDecompress)
    ->Arg(1024)      // 1KB
    ->Arg(10240)     // 10KB
    ->Arg(102400)    // 100KB
    ->Arg(1048576)  // 1MB
    ->UseRealTime();

BENCHMARK(BM_CompressionPatterns)
    ->Arg(0)  // zeros
    ->Arg(1)  // text
    ->Arg(2)  // binary
    ->Arg(3)  // random
    ->UseRealTime();

BENCHMARK(BM_CompressionLevelTradeoff)
    ->DenseRange(1, 9)  // Test all compression levels
    ->UseRealTime();

BENCHMARK(BM_CompressionMemoryOverhead)
    ->RangeMultiplier(10)
    ->Range(1024, 10485760)  // 1KB to 10MB
    ->UseRealTime();

BENCHMARK(BM_ConcurrentCompression)
    ->Threads(1)
    ->Threads(2)
    ->Threads(4)
    ->Threads(8)
    ->Threads(16)
    ->UseRealTime();

// Custom main to handle benchmark-specific setup
int main(int argc, char** argv) {
    // Initialize Google Benchmark
    ::benchmark::Initialize(&argc, argv);
    
    // Run benchmarks
    ::benchmark::RunSpecifiedBenchmarks();
    
    return 0;
}