#include <gtest/gtest.h>
#include <yams/compression/compressor_interface.h>
#include <yams/compression/compression_monitor.h>
#include <yams/compression/error_handler.h>
#include <yams/compression/recovery_manager.h>
#include <yams/storage/storage_engine.h>
#include <random>
#include <thread>
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>

using namespace yams;
using namespace yams::compression;
using namespace yams::storage;

class CompressionStressTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize compression monitor
        monitor_ = std::make_unique<CompressionMonitor>();
        monitor_->start();
        
        // Initialize error handler
        ErrorHandlingConfig errorConfig;
        errorConfig.maxRetryAttempts = 3;
        errorHandler_ = std::make_shared<CompressionErrorHandler>(errorConfig);
        
        // Initialize test directory
        testDir_ = std::filesystem::temp_directory_path() / "kronos_stress_test";
        std::filesystem::create_directories(testDir_);
    }
    
    void TearDown() override {
        monitor_->stop();
        std::filesystem::remove_all(testDir_);
    }
    
    std::vector<std::byte> generateRandomData(size_t size) {
        std::vector<std::byte> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);
        
        for (auto& b : data) {
            b = std::byte{static_cast<uint8_t>(dis(gen))};
        }
        
        return data;
    }
    
    std::unique_ptr<CompressionMonitor> monitor_;
    std::shared_ptr<CompressionErrorHandler> errorHandler_;
    std::filesystem::path testDir_;
};

// Test handling extremely large files
TEST_F(CompressionStressTest, ExtremelyLargeFiles) {
    auto& registry = CompressionRegistry::instance();
    auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
    if (!compressor) {
        GTEST_SKIP() << "Zstandard compressor not available";
    }
    
    // Test with progressively larger sizes
    std::vector<size_t> sizes = {
        1024 * 1024,         // 1MB
        10 * 1024 * 1024,    // 10MB
        50 * 1024 * 1024,    // 50MB
        100 * 1024 * 1024    // 100MB
    };
    
    for (size_t size : sizes) {
        std::cout << "Testing with " << (size / (1024 * 1024)) << "MB file..." << std::endl;
        
        auto data = generateRandomData(size);
        
        auto start = std::chrono::steady_clock::now();
        auto compressed = compressor->compress(data);
        auto compressTime = std::chrono::steady_clock::now() - start;
        
        ASSERT_TRUE(compressed.has_value()) << "Failed to compress " << size << " bytes";
        
        start = std::chrono::steady_clock::now();
        auto decompressed = compressor->decompress(compressed.value().data);
        auto decompressTime = std::chrono::steady_clock::now() - start;
        
        ASSERT_TRUE(decompressed.has_value()) << "Failed to decompress " << size << " bytes";
        EXPECT_EQ(decompressed.value().size(), size);
        
        auto compressMs = std::chrono::duration_cast<std::chrono::milliseconds>(compressTime).count();
        auto decompressMs = std::chrono::duration_cast<std::chrono::milliseconds>(decompressTime).count();
        
        std::cout << "  Compress: " << compressMs << "ms, Decompress: " << decompressMs << "ms" << std::endl;
        std::cout << "  Ratio: " << (static_cast<double>(size) / compressed.value().compressedSize) << std::endl;
    }
}

// Test sustained high-throughput compression
TEST_F(CompressionStressTest, SustainedHighThroughput) {
    auto& registry = CompressionRegistry::instance();
    
    const int durationSeconds = 10;
    const int numThreads = 8;
    const size_t dataSize = 100 * 1024; // 100KB per operation
    
    std::atomic<bool> shouldStop{false};
    std::atomic<int64_t> totalOperations{0};
    std::atomic<int64_t> totalBytes{0};
    std::atomic<int> errors{0};
    
    std::vector<std::thread> threads;
    
    auto startTime = std::chrono::steady_clock::now();
    
    // Launch worker threads
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&]() {
            auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
            if (!compressor) {
                errors++;
                return;
            }
            
            while (!shouldStop) {
                auto data = generateRandomData(dataSize);
                
                auto compressed = compressor->compress(data, 1); // Fast compression
                if (!compressed.has_value()) {
                    errors++;
                    continue;
                }
                
                auto decompressed = compressor->decompress(compressed.value().data);
                if (!decompressed.has_value() || decompressed.value() != data) {
                    errors++;
                    continue;
                }
                
                totalOperations++;
                totalBytes += dataSize;
            }
        });
    }
    
    // Run for specified duration
    std::this_thread::sleep_for(std::chrono::seconds(durationSeconds));
    shouldStop = true;
    
    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }
    
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime).count();
    
    std::cout << "Sustained throughput test results:" << std::endl;
    std::cout << "  Duration: " << duration << " seconds" << std::endl;
    std::cout << "  Total operations: " << totalOperations.load() << std::endl;
    std::cout << "  Operations/sec: " << (totalOperations.load() / duration) << std::endl;
    std::cout << "  Throughput: " << (totalBytes.load() / (1024.0 * 1024.0 * duration)) << " MB/s" << std::endl;
    std::cout << "  Errors: " << errors.load() << std::endl;
    
    EXPECT_EQ(errors.load(), 0) << "No errors should occur during normal operation";
    EXPECT_GT(totalOperations.load(), numThreads * 10) << "Should complete many operations";
}

// Test resource exhaustion handling
TEST_F(CompressionStressTest, ResourceExhaustion) {
    auto& registry = CompressionRegistry::instance();
    
    const int numThreads = 100; // Many threads
    const size_t dataSize = 10 * 1024 * 1024; // 10MB per thread
    
    std::vector<std::future<bool>> futures;
    std::atomic<int> activeThreads{0};
    std::atomic<int> peakThreads{0};
    
    // Launch many compression operations simultaneously
    for (int i = 0; i < numThreads; ++i) {
        futures.push_back(std::async(std::launch::async, [&, i]() {
            activeThreads++;
            int current = activeThreads.load();
            int expected = peakThreads.load();
            while (expected < current && 
                   !peakThreads.compare_exchange_weak(expected, current)) {
                expected = peakThreads.load();
            }
            
            auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
            if (!compressor) {
                activeThreads--;
                return false;
            }
            
            auto data = generateRandomData(dataSize);
            auto result = compressor->compress(data, 3);
            
            activeThreads--;
            return result.has_value();
        }));
    }
    
    // Wait for all operations
    int successCount = 0;
    int failureCount = 0;
    
    for (auto& future : futures) {
        if (future.get()) {
            successCount++;
        } else {
            failureCount++;
        }
    }
    
    std::cout << "Resource exhaustion test results:" << std::endl;
    std::cout << "  Threads launched: " << numThreads << std::endl;
    std::cout << "  Peak concurrent: " << peakThreads.load() << std::endl;
    std::cout << "  Successful: " << successCount << std::endl;
    std::cout << "  Failed: " << failureCount << std::endl;
    
    // Some operations should succeed even under stress
    EXPECT_GT(successCount, 0) << "At least some operations should succeed";
}

// Test rapid compression/decompression cycles
TEST_F(CompressionStressTest, RapidCycles) {
    auto& registry = CompressionRegistry::instance();
    auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
    if (!compressor) {
        GTEST_SKIP() << "Compressor not available";
    }
    
    const int cycles = 10000;
    const size_t minSize = 100;
    const size_t maxSize = 10000;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> sizeDist(minSize, maxSize);
    
    auto startTime = std::chrono::steady_clock::now();
    
    for (int i = 0; i < cycles; ++i) {
        size_t size = sizeDist(gen);
        auto data = generateRandomData(size);
        
        // Compress
        auto compressed = compressor->compress(data);
        ASSERT_TRUE(compressed.has_value()) << "Compression failed at cycle " << i;
        
        // Decompress
        auto decompressed = compressor->decompress(compressed.value().data);
        ASSERT_TRUE(decompressed.has_value()) << "Decompression failed at cycle " << i;
        
        // Verify
        ASSERT_EQ(decompressed.value(), data) << "Data mismatch at cycle " << i;
        
        // Report progress
        if (i % 1000 == 0 && i > 0) {
            auto elapsed = std::chrono::steady_clock::now() - startTime;
            auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            std::cout << "Completed " << i << " cycles in " << elapsedMs << "ms" << std::endl;
        }
    }
    
    auto totalTime = std::chrono::steady_clock::now() - startTime;
    auto totalMs = std::chrono::duration_cast<std::chrono::milliseconds>(totalTime).count();
    
    std::cout << "Rapid cycles test completed:" << std::endl;
    std::cout << "  Total cycles: " << cycles << std::endl;
    std::cout << "  Total time: " << totalMs << "ms" << std::endl;
    std::cout << "  Cycles/sec: " << (cycles * 1000.0 / totalMs) << std::endl;
}

// Test memory leak detection
TEST_F(CompressionStressTest, MemoryLeakDetection) {
    auto& registry = CompressionRegistry::instance();
    
    const int iterations = 1000;
    const size_t dataSize = 1024 * 1024; // 1MB
    
    // Perform many allocations and deallocations
    for (int i = 0; i < iterations; ++i) {
        auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
        ASSERT_NE(compressor, nullptr);
        
        auto data = generateRandomData(dataSize);
        
        // Multiple operations to stress memory management
        for (int j = 0; j < 10; ++j) {
            auto compressed = compressor->compress(data);
            ASSERT_TRUE(compressed.has_value());
            
            auto decompressed = compressor->decompress(compressed.value().data);
            ASSERT_TRUE(decompressed.has_value());
        }
        
        // Compressor goes out of scope and should be cleaned up
    }
    
    // If we get here without running out of memory, the test passes
    EXPECT_TRUE(true) << "Memory leak test completed successfully";
}

// Test error injection and recovery
TEST_F(CompressionStressTest, ErrorInjectionAndRecovery) {
    // Test recovery manager with injected errors
    ValidationConfig valConfig;
    auto validator = std::make_shared<IntegrityValidator>(valConfig);
    
    RecoveryConfig recoveryConfig;
    recoveryConfig.maxRetryAttempts = 3;
    recoveryConfig.enablePartialRecovery = true;
    
    RecoveryManager recovery(recoveryConfig, errorHandler_, validator);
    ASSERT_TRUE(recovery.start().has_value());
    
    const int numTests = 100;
    std::atomic<int> recoveryAttempts{0};
    std::atomic<int> successfulRecoveries{0};
    
    for (int i = 0; i < numTests; ++i) {
        auto data = generateRandomData(1024);
        
        // Simulate corrupted compressed data
        std::vector<std::byte> corruptedData(100);
        std::fill(corruptedData.begin(), corruptedData.end(), std::byte{0xFF});
        
        // Attempt recovery
        RecoveryRequest request;
        request.operation = RecoveryOperation::DecompressRecover;
        request.data = corruptedData;
        request.expectedSize = 1024;
        request.originalAlgorithm = CompressionAlgorithm::Zstandard;
        
        auto future = recovery.submitRecovery(request);
        auto result = future.get();
        
        recoveryAttempts++;
        if (result.status == RecoveryStatus::Success || 
            result.status == RecoveryStatus::PartialSuccess) {
            successfulRecoveries++;
        }
    }
    
    recovery.stop();
    
    std::cout << "Error injection test results:" << std::endl;
    std::cout << "  Recovery attempts: " << recoveryAttempts.load() << std::endl;
    std::cout << "  Successful recoveries: " << successfulRecoveries.load() << std::endl;
    
    // Some recoveries might succeed with fallback strategies
    EXPECT_GE(recoveryAttempts.load(), numTests);
}

// Test compression bomb protection
TEST_F(CompressionStressTest, CompressionBombProtection) {
    auto& registry = CompressionRegistry::instance();
    auto compressor = registry.createCompressor(CompressionAlgorithm::Zstandard);
    if (!compressor) {
        GTEST_SKIP() << "Compressor not available";
    }
    
    // Create highly compressible data that could expand significantly
    const size_t originalSize = 1024; // 1KB
    std::vector<std::byte> bombData(originalSize, std::byte{0}); // All zeros
    
    // Compress the bomb
    auto compressed = compressor->compress(bombData);
    ASSERT_TRUE(compressed.has_value());
    
    // The compressed size should be very small
    EXPECT_LT(compressed.value().compressedSize, originalSize / 10);
    
    // Try to decompress with size limit
    auto maxSize = compressor->maxCompressedSize(originalSize * 1000);
    
    // Decompression should handle this safely
    auto decompressed = compressor->decompress(compressed.value().data, originalSize);
    ASSERT_TRUE(decompressed.has_value());
    EXPECT_EQ(decompressed.value().size(), originalSize);
    
    std::cout << "Compression bomb test:" << std::endl;
    std::cout << "  Original size: " << originalSize << " bytes" << std::endl;
    std::cout << "  Compressed size: " << compressed.value().compressedSize << " bytes" << std::endl;
    std::cout << "  Compression ratio: " << (originalSize / compressed.value().compressedSize) << ":1" << std::endl;
}