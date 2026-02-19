// PostIngestQueue benchmark using DaemonHarness
// Ingests the YAMS codebase to measure real-world throughput

#include <catch2/catch_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <algorithm>
#include <numeric>
#include "test_daemon_harness.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <spdlog/spdlog.h>

namespace fs = std::filesystem;
using namespace yams::test;

TEST_CASE("PostIngestQueue: Real-world ingestion benchmark", "[daemon][benchmark][stress]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Collect source files from the YAMS codebase
    fs::path sourceDir = fs::current_path();
    // Navigate up to find src directory
    while (!fs::exists(sourceDir / "src") && sourceDir.has_parent_path()) {
        sourceDir = sourceDir.parent_path();
    }
    sourceDir = sourceDir / "src";

    if (!fs::exists(sourceDir)) {
        SKIP("Source directory not found for benchmarking");
    }

    // Collect files to ingest
    std::vector<fs::path> filesToIngest;
    const size_t maxFiles = 500; // Limit for benchmark
    size_t totalSize = 0;

    for (const auto& entry : fs::recursive_directory_iterator(sourceDir)) {
        if (filesToIngest.size() >= maxFiles)
            break;

        if (entry.is_regular_file()) {
            auto ext = entry.path().extension();
            if (ext == ".cpp" || ext == ".h" || ext == ".hpp" || ext == ".c") {
                auto size = entry.file_size();
                if (size > 0 && size < 1024 * 1024) { // Skip empty or large files
                    filesToIngest.push_back(entry.path());
                    totalSize += size;
                }
            }
        }
    }

    if (filesToIngest.size() < 10) {
        SKIP("Not enough files found for meaningful benchmark");
    }

    spdlog::info("========================================");
    spdlog::info("PostIngestQueue Benchmark");
    spdlog::info("Files to ingest: {}", filesToIngest.size());
    spdlog::info("Total size: {:.2f} MB", totalSize / (1024.0 * 1024.0));
    spdlog::info("========================================");

    // Start daemon
    DaemonHarness harness(DaemonHarnessOptions{
        .enableModelProvider = false,
        .useMockModelProvider = true,
        .autoLoadPlugins = false,
    });

    REQUIRE(harness.start());

    // Setup ingestion service
    yams::app::services::DocumentIngestionService docSvc;
    yams::app::services::AddOptions opts;
    opts.socketPath = harness.socketPath().string();
    opts.explicitDataDir = harness.dataDir().string();
    opts.noEmbeddings = true; // Focus on post-ingest stages

    // Ingest files and measure
    auto ingestStart = std::chrono::steady_clock::now();
    size_t ingested = 0;
    size_t failed = 0;
    std::vector<double> individualTimes;

    for (const auto& filePath : filesToIngest) {
        auto fileStart = std::chrono::steady_clock::now();

        opts.path = filePath.string();
        auto result = docSvc.addViaDaemon(opts);

        auto fileEnd = std::chrono::steady_clock::now();
        double fileMs = std::chrono::duration<double, std::milli>(fileEnd - fileStart).count();
        individualTimes.push_back(fileMs);

        if (result && !result.value().hash.empty()) {
            ingested++;
        } else {
            failed++;
        }

        // Log progress every 50 files
        if (ingested % 50 == 0) {
            auto elapsed = std::chrono::steady_clock::now() - ingestStart;
            double elapsedSec = std::chrono::duration<double>(elapsed).count();
            double rate = elapsedSec > 0 ? ingested / elapsedSec : 0;

            // Calculate recent throughput from last 50 files
            double recentTime = 0;
            size_t startIdx = individualTimes.size() > 50 ? individualTimes.size() - 50 : 0;
            for (size_t i = startIdx; i < individualTimes.size(); ++i) {
                recentTime += individualTimes[i];
            }
            double recentRate = recentTime > 0 ? 50.0 / (recentTime / 1000.0) : 0;

            spdlog::info(
                "Progress: {}/{} ingested ({:.1f} docs/sec overall, {:.1f} docs/sec recent)",
                ingested, filesToIngest.size(), rate, recentRate);
        }
    }

    auto ingestEnd = std::chrono::steady_clock::now();
    auto ingestDuration = ingestEnd - ingestStart;
    double ingestSeconds = std::chrono::duration<double>(ingestDuration).count();

    spdlog::info("Ingestion complete: {} succeeded, {} failed in {:.2f}s", ingested, failed,
                 ingestSeconds);

    // Calculate ingestion statistics
    std::sort(individualTimes.begin(), individualTimes.end());
    double p50 = individualTimes[individualTimes.size() / 2];
    double p95 = individualTimes[static_cast<size_t>(individualTimes.size() * 0.95)];
    double avg = std::accumulate(individualTimes.begin(), individualTimes.end(), 0.0) /
                 individualTimes.size();

    // Wait for post-ingest processing to complete
    spdlog::info("Waiting for post-ingest processing...");
    auto processingStart = std::chrono::steady_clock::now();

    auto deadline = processingStart + std::chrono::minutes(5);
    size_t lastProcessed = 0;
    int stallCount = 0;
    std::vector<std::pair<double, size_t>> throughputSamples; // (time_sec, processed_count)

    while (std::chrono::steady_clock::now() < deadline) {
        size_t currentProcessed = 0;
        size_t currentQueued = 0;
        size_t currentInflight = 0;

        if (auto* daemon = harness.daemon()) {
            if (auto* sm = daemon->getServiceManager()) {
                if (auto pq = sm->getPostIngestQueue()) {
                    currentProcessed = pq->processed();
                    currentQueued = pq->size();
                    currentInflight = pq->totalInFlight();
                }
            }
        }

        // Check if processing is done (no more queued items)
        if (currentQueued == 0 && currentInflight == 0 && currentProcessed >= ingested) {
            spdlog::info("Post-ingest processing complete");
            break;
        }

        // Detect stall
        if (currentProcessed == lastProcessed) {
            stallCount++;
            if (stallCount > 100) { // 10 seconds of no progress
                spdlog::warn("Processing appears stalled");
                break;
            }
        } else {
            stallCount = 0;
            lastProcessed = currentProcessed;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Log progress every second and record throughput sample
        static auto lastLog = std::chrono::steady_clock::now();
        auto now = std::chrono::steady_clock::now();
        if (now - lastLog > std::chrono::seconds(1)) {
            double elapsedSec = std::chrono::duration<double>(now - processingStart).count();
            throughputSamples.push_back({elapsedSec, currentProcessed});
            spdlog::info("Post-ingest: {} processed, {} queued, {} in-flight", currentProcessed,
                         currentQueued, currentInflight);
            lastLog = now;
        }
    }

    auto processingEnd = std::chrono::steady_clock::now();
    auto processingDuration = processingEnd - processingStart;
    double processingSeconds = std::chrono::duration<double>(processingDuration).count();

    // Get final stats
    size_t finalProcessed = 0;
    size_t finalFailed = 0;
    if (auto* daemon = harness.daemon()) {
        if (auto* sm = daemon->getServiceManager()) {
            if (auto pq = sm->getPostIngestQueue()) {
                finalProcessed = pq->processed();
                finalFailed = pq->failed();
            }
        }
    }

    double totalTime = ingestSeconds + processingSeconds;
    double throughput = totalTime > 0 ? finalProcessed / totalTime : 0;

    // Calculate processing throughput (excluding ingestion time)
    double processingThroughput = processingSeconds > 0 ? finalProcessed / processingSeconds : 0;

    spdlog::info("========================================");
    spdlog::info("Benchmark Results:");
    spdlog::info("  Files ingested: {}", ingested);
    spdlog::info("  Ingestion time: {:.2f}s", ingestSeconds);
    spdlog::info("  Processing time: {:.2f}s", processingSeconds);
    spdlog::info("  Total time: {:.2f}s", totalTime);
    spdlog::info("  Documents processed: {}", finalProcessed);
    spdlog::info("  Documents failed: {}", finalFailed);
    spdlog::info("  Overall throughput: {:.2f} docs/sec", throughput);
    spdlog::info("  Processing throughput: {:.2f} docs/sec", processingThroughput);
    spdlog::info("  Per-file latency (ms): avg={:.1f}, p50={:.1f}, p95={:.1f}", avg, p50, p95);
    spdlog::info("========================================");

    // Cleanup

    // Requirements
    REQUIRE(finalProcessed > 0);
    REQUIRE(throughput > 0);
    REQUIRE(processingThroughput > 0);

    // Performance assertions - adjust thresholds based on hardware
    // These are reasonable minimums for modern hardware
    CHECK(throughput >= 1.0);           // At least 1 doc/sec overall
    CHECK(processingThroughput >= 2.0); // At least 2 docs/sec processing

    BENCHMARK("PostIngestQueue end-to-end throughput") {
        return throughput;
    };
    BENCHMARK("PostIngestQueue processing throughput") {
        return processingThroughput;
    };
}

TEST_CASE("PostIngestQueue: Parallel processing stress test", "[daemon][benchmark][stress]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Start daemon
    DaemonHarness harness(DaemonHarnessOptions{
        .enableModelProvider = false,
        .useMockModelProvider = true,
        .autoLoadPlugins = false,
    });

    REQUIRE(harness.start());

    // Create multiple batches of files to stress parallel processing
    const int numBatches = 5;
    const int filesPerBatch = 50;
    const int totalFiles = numBatches * filesPerBatch;

    spdlog::info("========================================");
    spdlog::info("PostIngestQueue Parallel Stress Test");
    spdlog::info("Batches: {}, Files per batch: {}, Total: {}", numBatches, filesPerBatch,
                 totalFiles);
    spdlog::info("========================================");

    // Setup ingestion service
    yams::app::services::DocumentIngestionService docSvc;
    yams::app::services::AddOptions opts;
    opts.socketPath = harness.socketPath().string();
    opts.explicitDataDir = harness.dataDir().string();
    opts.noEmbeddings = true;

    // Create test files in batches
    auto startTime = std::chrono::steady_clock::now();
    int ingested = 0;

    for (int batch = 0; batch < numBatches; ++batch) {
        spdlog::info("Ingesting batch {}/{}...", batch + 1, numBatches);

        for (int i = 0; i < filesPerBatch; ++i) {
            // Create test file
            std::string docName =
                "stress_batch" + std::to_string(batch) + "_file" + std::to_string(i) + ".txt";
            auto path = harness.dataDir() / docName;
            std::ofstream ofs(path);
            ofs << "Batch " << batch << " File " << i << " content\n";
            ofs << std::string(1024, 'x') << "\n";
            ofs.close();

            opts.path = path.string();
            auto result = docSvc.addViaDaemon(opts);

            if (result && !result.value().hash.empty()) {
                ingested++;
            }
        }

        auto elapsed = std::chrono::steady_clock::now() - startTime;
        double elapsedSec = std::chrono::duration<double>(elapsed).count();
        spdlog::info("Batch {}/{} complete. Total ingested: {}/{} ({:.1f} docs/sec)", batch + 1,
                     numBatches, ingested, totalFiles, elapsedSec > 0 ? ingested / elapsedSec : 0);
    }

    // Wait for all post-ingest processing
    spdlog::info("Waiting for post-ingest processing to complete...");
    auto waitStart = std::chrono::steady_clock::now();
    auto deadline = waitStart + std::chrono::minutes(5);

    size_t lastProcessed = 0;
    int stableCount = 0;
    std::vector<std::pair<int, size_t>> progressHistory;

    while (std::chrono::steady_clock::now() < deadline) {
        size_t currentProcessed = 0;
        size_t currentQueued = 0;
        size_t currentInflight = 0;

        if (auto* daemon = harness.daemon()) {
            if (auto* sm = daemon->getServiceManager()) {
                if (auto pq = sm->getPostIngestQueue()) {
                    currentProcessed = pq->processed();
                    currentQueued = pq->size();
                    currentInflight = pq->totalInFlight();
                }
            }
        }

        auto now = std::chrono::steady_clock::now();
        double elapsedSec = std::chrono::duration<double>(now - waitStart).count();
        progressHistory.push_back({static_cast<int>(elapsedSec), currentProcessed});

        if (currentQueued == 0 && currentInflight == 0 && currentProcessed >= ingested) {
            spdlog::info("Processing complete after {:.1f}s", elapsedSec);
            break;
        }

        if (currentProcessed == lastProcessed) {
            stableCount++;
            if (stableCount > 50) {
                spdlog::warn("Processing stalled after {:.1f}s", elapsedSec);
                break;
            }
        } else {
            stableCount = 0;
            lastProcessed = currentProcessed;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    auto endTime = std::chrono::steady_clock::now();
    auto totalDuration = endTime - startTime;
    double totalSeconds = std::chrono::duration<double>(totalDuration).count();

    // Calculate final stats
    size_t finalProcessed = 0;
    if (auto* daemon = harness.daemon()) {
        if (auto* sm = daemon->getServiceManager()) {
            if (auto pq = sm->getPostIngestQueue()) {
                finalProcessed = pq->processed();
            }
        }
    }

    double throughput = totalSeconds > 0 ? finalProcessed / totalSeconds : 0;

    spdlog::info("========================================");
    spdlog::info("Stress Test Results:");
    spdlog::info("  Total files: {}", totalFiles);
    spdlog::info("  Successfully ingested: {}", ingested);
    spdlog::info("  Successfully processed: {}", finalProcessed);
    spdlog::info("  Total time: {:.2f}s", totalSeconds);
    spdlog::info("  Throughput: {:.2f} docs/sec", throughput);
    spdlog::info("========================================");

    REQUIRE(ingested == totalFiles);
    REQUIRE(finalProcessed >= ingested * 0.95); // Allow 5% failure rate
    REQUIRE(throughput >= 5.0);                 // Expect at least 5 docs/sec under stress

    BENCHMARK("Parallel stress throughput") {
        return throughput;
    };
}
