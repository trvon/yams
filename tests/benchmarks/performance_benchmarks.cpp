#if __has_include(<benchmark/benchmark.h>)
#include <filesystem>
#include <iostream>
#include <random>
#include <vector>
#include <benchmark/benchmark.h>
#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include <yams/manifest/manifest_manager.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

namespace fs = std::filesystem;
using namespace yams;

// Global test data
static std::vector<std::byte> testData1MB;
static std::vector<std::byte> testData10MB;
static std::vector<std::byte> testData100MB;
static fs::path tempDir;

// Initialize test data
static void InitializeTestData() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);

    // Generate random test data
    auto generateData = [&](size_t size) {
        std::vector<std::byte> data(size);
        for (auto& byte : data) {
            byte = static_cast<std::byte>(dis(gen));
        }
        return data;
    };

    testData1MB = generateData(1 * 1024 * 1024);
    testData10MB = generateData(10 * 1024 * 1024);
    testData100MB = generateData(100 * 1024 * 1024);

    // Create temp directory
    tempDir = fs::temp_directory_path() / "yams_bench";
    fs::create_directories(tempDir);
}

// Cleanup
static void CleanupTestData() {
    fs::remove_all(tempDir);
}

// SHA256 Hashing Benchmarks
static void BM_SHA256_Small(benchmark::State& state) {
    auto hasher = crypto::createSHA256Hasher();
    std::vector<std::byte> data(state.range(0));
    std::fill(data.begin(), data.end(), std::byte{0x42});

    for (auto _ : state) {
        auto hash = hasher->hash(data);
        benchmark::DoNotOptimize(hash);
    }

    state.SetBytesProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_SHA256_Small)->Range(1024, 64 * 1024);

static void BM_SHA256_Large(benchmark::State& state) {
    auto hasher = crypto::createSHA256Hasher();

    for (auto _ : state) {
        auto hash = hasher->hash(testData10MB);
        benchmark::DoNotOptimize(hash);
    }

    state.SetBytesProcessed(state.iterations() * testData10MB.size());
}
BENCHMARK(BM_SHA256_Large);

static void BM_SHA256_Streaming(benchmark::State& state) {
    auto hasher = crypto::createSHA256Hasher();

    for (auto _ : state) {
        hasher->init();

        // Process in chunks
        size_t chunkSize = 64 * 1024;
        for (size_t i = 0; i < testData10MB.size(); i += chunkSize) {
            size_t size = std::min(chunkSize, testData10MB.size() - i);
            std::span<const std::byte> chunk(testData10MB.data() + i, size);
            hasher->update(chunk);
        }

        auto hash = hasher->finalize();
        benchmark::DoNotOptimize(hash);
    }

    state.SetBytesProcessed(state.iterations() * testData10MB.size());
}
BENCHMARK(BM_SHA256_Streaming);

// Chunking Benchmarks
static void BM_RabinChunking_Small(benchmark::State& state) {
    chunking::ChunkingConfig config{
        .minChunkSize = 4096, .targetChunkSize = 16384, .maxChunkSize = 65536};

    auto chunker = std::make_unique<chunking::RabinChunker>(config);

    for (auto _ : state) {
        auto chunks = chunker->chunkData(testData1MB);
        benchmark::DoNotOptimize(chunks);
    }

    state.SetBytesProcessed(state.iterations() * testData1MB.size());
}
BENCHMARK(BM_RabinChunking_Small);

static void BM_RabinChunking_Large(benchmark::State& state) {
    chunking::ChunkingConfig config{
        .minChunkSize = 4096, .targetChunkSize = 65536, .maxChunkSize = 262144};

    auto chunker = std::make_unique<chunking::RabinChunker>(config);

    for (auto _ : state) {
        auto chunks = chunker->chunkData(testData10MB);
        benchmark::DoNotOptimize(chunks);
    }

    state.SetBytesProcessed(state.iterations() * testData10MB.size());
}
BENCHMARK(BM_RabinChunking_Large);

// Storage Engine Benchmarks
class StorageBenchmark : public benchmark::Fixture {
protected:
    std::unique_ptr<storage::StorageEngine> storage;
    std::vector<std::string> testHashes;

    void SetUp(const ::benchmark::State& state) override {
        try {
            // Use a safe default for thread index during setup
            // Google Benchmark may not have thread_index properly initialized in SetUp
            int thread_id = 0;
            try {
                thread_id = state.thread_index();
            } catch (...) {
                // If thread_index() fails, use 0 as default
                thread_id = 0;
            }

            auto benchPath = tempDir / ("storage_bench_" + std::to_string(thread_id));
            fs::create_directories(benchPath);

            storage::StorageConfig config{
                .basePath = benchPath, .shardDepth = 2, .mutexPoolSize = 256};

            storage = std::make_unique<storage::StorageEngine>(std::move(config));

            // Pre-generate hashes
            auto hasher = crypto::createSHA256Hasher();
            for (int i = 0; i < 1000; ++i) {
                auto data = std::to_string(i);
                std::span<const std::byte> span(reinterpret_cast<const std::byte*>(data.data()),
                                                data.size());
                testHashes.push_back(hasher->hash(span));
            }
        } catch (const std::exception& e) {
            // Log error and mark benchmark as skipped
            std::cerr << "StorageBenchmark SetUp failed: " << e.what() << std::endl;
            throw; // Re-throw to let benchmark framework handle it
        }
    }

    void TearDown(const ::benchmark::State& state) override {
        try {
            storage.reset();

            // Use safe thread index access
            int thread_id = 0;
            try {
                thread_id = state.thread_index();
            } catch (...) {
                thread_id = 0;
            }

            fs::remove_all(tempDir / ("storage_bench_" + std::to_string(thread_id)));
        } catch (const std::exception& e) {
            // Log error but don't throw in TearDown
            std::cerr << "StorageBenchmark TearDown error: " << e.what() << std::endl;
        }
    }
};

BENCHMARK_F(StorageBenchmark, BM_Storage_Store)(benchmark::State& state) {
    size_t index = 0;
    std::vector<std::byte> data(state.range(0));
    std::fill(data.begin(), data.end(), std::byte{0x42});

    for (auto _ : state) {
        auto result = storage->store(testHashes[index % testHashes.size()], data);
        benchmark::DoNotOptimize(result);
        index++;
    }

    state.SetBytesProcessed(state.iterations() * state.range(0));
    state.counters["throughput_Bps"] = benchmark::Counter(
        static_cast<double>(state.iterations() * state.range(0)), benchmark::Counter::kIsRate);
}
BENCHMARK_REGISTER_F(StorageBenchmark, BM_Storage_Store)->Range(1024, 1024 * 1024)->UseRealTime();

BENCHMARK_F(StorageBenchmark, BM_Storage_Retrieve)(benchmark::State& state) {
    // Pre-store some data
    std::vector<std::byte> data(64 * 1024);
    std::fill(data.begin(), data.end(), std::byte{0x42});

    for (size_t i = 0; i < 100; ++i) {
        storage->store(testHashes[i], data);
    }

    size_t index = 0;
    for (auto _ : state) {
        auto result = storage->retrieve(testHashes[index % 100]);
        benchmark::DoNotOptimize(result);
        index++;
    }

    state.SetBytesProcessed(state.iterations() * data.size());
    state.counters["throughput_Bps"] = benchmark::Counter(
        static_cast<double>(state.iterations() * data.size()), benchmark::Counter::kIsRate);
}
BENCHMARK_REGISTER_F(StorageBenchmark, BM_Storage_Retrieve)->UseRealTime();

// Reference Counter Benchmarks
static void BM_ReferenceCounter_Increment(benchmark::State& state) {
    auto storagePath = tempDir / "refcount_bench";
    fs::create_directories(storagePath);

    storage::ReferenceCounter::Config refConfig{.databasePath = storagePath / "refs.db"};
    storage::ReferenceCounter refCounter(refConfig);

    std::vector<std::string> hashes;
    for (int i = 0; i < 1000; ++i) {
        hashes.push_back("hash_" + std::to_string(i));
    }

    size_t index = 0;
    for (auto _ : state) {
        auto result = refCounter.increment(hashes[index % hashes.size()], 1024);
        benchmark::DoNotOptimize(result);
        index++;
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["ops_per_sec"] =
        benchmark::Counter(static_cast<double>(state.iterations()), benchmark::Counter::kIsRate);
    fs::remove_all(storagePath);
}
BENCHMARK(BM_ReferenceCounter_Increment)->UseRealTime();

static void BM_ReferenceCounter_BatchOps(benchmark::State& state) {
    auto storagePath = tempDir / "refcount_batch_bench";
    fs::create_directories(storagePath);

    storage::ReferenceCounter::Config refConfig{.databasePath = storagePath / "refs.db"};
    storage::ReferenceCounter refCounter(refConfig);

    for (auto _ : state) {
        struct BlockInfo {
            std::string hash;
            size_t size;
        };
        std::vector<BlockInfo> batch;
        for (int i = 0; i < state.range(0); ++i) {
            batch.emplace_back("batch_hash_" + std::to_string(i), 1024 * (i + 1));
        }

        auto result = refCounter.incrementBatchWithSizes(batch);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations() * state.range(0));
    state.counters["ops_per_sec"] = benchmark::Counter(
        static_cast<double>(state.iterations() * state.range(0)), benchmark::Counter::kIsRate);
    fs::remove_all(storagePath);
}
BENCHMARK(BM_ReferenceCounter_BatchOps)->Range(10, 1000)->UseRealTime();

// Manifest Benchmarks
static void BM_Manifest_Serialize(benchmark::State& state) {
    manifest::ManifestManager::Config config{.enableCompression = true, .enableChecksums = true};

    auto manager = std::make_unique<manifest::ManifestManager>(std::move(config));

    // Create test manifest
    manifest::Manifest manifest;
    manifest.version = manifest::Manifest::CURRENT_VERSION;
    manifest.fileHash = "test_file_hash";
    manifest.fileSize = state.range(0) * 1024;
    manifest.originalName = "test_file.bin";
    manifest.mimeType = "application/octet-stream";
    manifest.createdAt = std::chrono::system_clock::now();

    // Add chunks
    size_t numChunks = state.range(0); // Number of chunks
    for (size_t i = 0; i < numChunks; ++i) {
        manifest.chunks.push_back(
            {.hash = "chunk_hash_" + std::to_string(i), .offset = i * 65536, .size = 65536});
    }

    for (auto _ : state) {
        auto result = manager->serialize(manifest);
        benchmark::DoNotOptimize(result);
    }
    state.SetBytesProcessed(state.iterations() * manifest.fileSize);
    state.counters["throughput_Bps"] = benchmark::Counter(
        static_cast<double>(state.iterations() * manifest.fileSize), benchmark::Counter::kIsRate);
}
BENCHMARK(BM_Manifest_Serialize)->Range(10, 10000)->UseRealTime();

static void BM_Manifest_Deserialize(benchmark::State& state) {
    manifest::ManifestManager::Config config{.enableCompression = true, .enableChecksums = true};

    auto manager = std::make_unique<manifest::ManifestManager>(std::move(config));

    // Create and serialize test manifest
    manifest::Manifest manifest;
    manifest.version = manifest::Manifest::CURRENT_VERSION;
    manifest.fileHash = "test_file_hash";
    manifest.fileSize = 10 * 1024 * 1024;
    manifest.originalName = "test_file.bin";
    manifest.mimeType = "application/octet-stream";
    manifest.createdAt = std::chrono::system_clock::now();

    for (size_t i = 0; i < 1000; ++i) {
        manifest.chunks.push_back(
            {.hash = "chunk_hash_" + std::to_string(i), .offset = i * 10240, .size = 10240});
    }

    auto serialized = manager->serialize(manifest).value();

    for (auto _ : state) {
        auto result = manager->deserialize(serialized);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(state.iterations() * serialized.size());
    state.counters["throughput_Bps"] = benchmark::Counter(
        static_cast<double>(state.iterations() * serialized.size()), benchmark::Counter::kIsRate);
}
BENCHMARK(BM_Manifest_Deserialize)->UseRealTime();

// Parallel retrieve benchmark using multiple threads and real-time measurement
static void BM_Storage_Retrieve_MT(benchmark::State& state) {
    // Prepare storage in a dedicated temp subdir (outside timing)
    state.PauseTiming();
    auto benchPath = tempDir / ("storage_bench_mt_" + std::to_string(state.thread_index()));
    fs::create_directories(benchPath);

    storage::StorageConfig config{.basePath = benchPath, .shardDepth = 2, .mutexPoolSize = 256};

    auto engine = std::make_unique<storage::StorageEngine>(std::move(config));

    // Pre-store a fixed set of objects (e.g., 100 entries of 64 KiB) for retrieval
    const size_t kPrestoreCount = 100;
    const size_t kObjSize = 64 * 1024; // 64 KiB
    std::vector<std::string> keys;
    keys.reserve(kPrestoreCount);

    // Generate proper SHA256 hashes as keys
    auto hasher = crypto::createSHA256Hasher();
    for (size_t i = 0; i < kPrestoreCount; ++i) {
        auto keyData =
            "bench_key_" + std::to_string(i) + "_thread_" + std::to_string(state.thread_index());
        std::span<const std::byte> span(reinterpret_cast<const std::byte*>(keyData.data()),
                                        keyData.size());
        keys.push_back(hasher->hash(span));
    }

    std::vector<std::byte> data(kObjSize);
    std::fill(data.begin(), data.end(), std::byte{0x42});

    // Store objects and verify success
    for (size_t i = 0; i < kPrestoreCount; ++i) {
        auto res = engine->store(keys[i], data);
        if (!res) {
            state.SkipWithError("Failed to store test data");
            return;
        }
    }
    state.ResumeTiming();

    // Each thread retrieves keys in a local round-robin to avoid synchronization
    size_t idx = 0;
    for (auto _ : state) {
        auto result = engine->retrieve(keys[idx % kPrestoreCount]);
        benchmark::DoNotOptimize(result);
        idx++;
    }

    // Report throughput and operations
    state.SetBytesProcessed(state.iterations() * kObjSize);
    state.SetItemsProcessed(state.iterations());

    // Cleanup (outside timing)
    state.PauseTiming();
    engine.reset();
    fs::remove_all(benchPath);
    // Do NOT call ResumeTiming() here - benchmark iteration is complete
}

BENCHMARK(BM_Storage_Retrieve_MT)->Threads(1)->Threads(2)->Threads(4)->Threads(8)->UseRealTime();

// End-to-end Benchmarks
static void BM_EndToEnd_StoreFile(benchmark::State& state) {
    // This would require the full content store API
    // Placeholder for now
    for (auto _ : state) {
        // Simulate file store operation
        state.PauseTiming();
        // Setup
        state.ResumeTiming();

        // Operation

        state.PauseTiming();
        // Cleanup
        state.ResumeTiming();
    }
}

// Memory allocation benchmarks
static void BM_Memory_ChunkAllocation(benchmark::State& state) {
    size_t chunkSize = state.range(0);

    for (auto _ : state) {
        std::vector<std::byte> chunk(chunkSize);
        benchmark::DoNotOptimize(chunk.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(state.iterations() * chunkSize);
}
BENCHMARK(BM_Memory_ChunkAllocation)->Range(4096, 1024 * 1024);

// Custom main to handle initialization
int main(int argc, char** argv) {
    InitializeTestData();

    ::benchmark::Initialize(&argc, argv);
    ::benchmark::RunSpecifiedBenchmarks();

    CleanupTestData();

    return 0;
}
#else
#include <iostream>
int main(int, char**) {
    std::cerr << "Benchmarks are disabled: Google Benchmark not available.\n";
    return 0;
}
#endif