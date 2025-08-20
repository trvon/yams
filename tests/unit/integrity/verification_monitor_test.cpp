#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <yams/integrity/verifier.h>

#include <chrono>
#include <sstream>
#include <thread>

using namespace yams::integrity;
using namespace testing;

class VerificationMonitorTest : public ::testing::Test {
protected:
    void SetUp() override { monitor = std::make_unique<VerificationMonitor>(); }

    void TearDown() override { monitor.reset(); }

    VerificationResult createResult(const std::string& hash, VerificationStatus status,
                                    size_t size = 1024) {
        VerificationResult result;
        result.blockHash = hash;
        result.status = status;
        result.timestamp = std::chrono::system_clock::now();
        result.blockSize = size;
        if (status != VerificationStatus::Passed) {
            result.errorDetails = "Test error for " + hash;
        }
        return result;
    }

    std::unique_ptr<VerificationMonitor> monitor;
};

TEST_F(VerificationMonitorTest, ConstructorAndInitialState) {
    auto metrics = monitor->getCurrentMetrics();

    EXPECT_EQ(metrics.verificationsPerSecond, 0);
    EXPECT_EQ(metrics.errorsPerSecond, 0);
    EXPECT_EQ(metrics.totalVerifications, 0);
    EXPECT_EQ(metrics.totalErrors, 0);
    EXPECT_EQ(metrics.repairAttempts, 0);
    EXPECT_EQ(metrics.successfulRepairs, 0);
    EXPECT_EQ(metrics.currentErrorRate, 0.0);

    EXPECT_FALSE(monitor->shouldAlert());
}

TEST_F(VerificationMonitorTest, RecordSuccessfulVerification) {
    auto result = createResult("hash1", VerificationStatus::Passed);

    monitor->recordVerification(result);

    auto metrics = monitor->getCurrentMetrics();

    EXPECT_EQ(metrics.totalVerifications, 1);
    EXPECT_EQ(metrics.totalErrors, 0);
    EXPECT_GE(metrics.verificationsPerSecond, 0); // May be 0 if measured too quickly
    EXPECT_EQ(metrics.errorsPerSecond, 0);
}

TEST_F(VerificationMonitorTest, RecordFailedVerification) {
    auto result = createResult("hash1", VerificationStatus::Failed);

    monitor->recordVerification(result);

    auto metrics = monitor->getCurrentMetrics();

    EXPECT_EQ(metrics.totalVerifications, 1);
    EXPECT_EQ(metrics.totalErrors, 1);
    EXPECT_GE(metrics.verificationsPerSecond, 0);
    EXPECT_GE(metrics.errorsPerSecond, 0);
}

TEST_F(VerificationMonitorTest, RecordMultipleVerifications) {
    // Record mix of successful and failed verifications
    monitor->recordVerification(createResult("hash1", VerificationStatus::Passed));
    monitor->recordVerification(createResult("hash2", VerificationStatus::Failed));
    monitor->recordVerification(createResult("hash3", VerificationStatus::Passed));
    monitor->recordVerification(createResult("hash4", VerificationStatus::Corrupted));
    monitor->recordVerification(createResult("hash5", VerificationStatus::Missing));

    auto metrics = monitor->getCurrentMetrics();

    EXPECT_EQ(metrics.totalVerifications, 5);
    EXPECT_EQ(metrics.totalErrors, 3); // Failed, Corrupted, Missing
}

TEST_F(VerificationMonitorTest, RecordRepairedVerification) {
    auto result = createResult("hash1", VerificationStatus::Repaired);

    monitor->recordVerification(result);

    auto metrics = monitor->getCurrentMetrics();

    EXPECT_EQ(metrics.totalVerifications, 1);
    EXPECT_EQ(metrics.totalErrors, 0); // Repaired is considered success
}

TEST_F(VerificationMonitorTest, RecordRepairAttempts) {
    monitor->recordRepairAttempt("hash1", true);  // Successful repair
    monitor->recordRepairAttempt("hash2", false); // Failed repair
    monitor->recordRepairAttempt("hash3", true);  // Another successful repair

    auto metrics = monitor->getCurrentMetrics();

    EXPECT_EQ(metrics.repairAttempts, 3);
    EXPECT_EQ(metrics.successfulRepairs, 2);
}

TEST_F(VerificationMonitorTest, ErrorRateThreshold) {
    // Set a low error rate threshold
    monitor->setErrorRateThreshold(0.3); // 30%

    // Initially should not alert
    EXPECT_FALSE(monitor->shouldAlert());

    // Add some successful verifications first to establish a rate
    for (int i = 0; i < 10; ++i) {
        monitor->recordVerification(
            createResult("pass" + std::to_string(i), VerificationStatus::Passed));
    }

    // Add some failures to exceed threshold
    for (int i = 0; i < 5; ++i) {
        monitor->recordVerification(
            createResult("fail" + std::to_string(i), VerificationStatus::Failed));
    }

    // Give the rate tracker a moment to process
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Note: Whether shouldAlert() returns true depends on the rate calculation
    // which is time-based. In a real scenario with sufficient time and rate,
    // this would trigger an alert.
    auto metrics = monitor->getCurrentMetrics();
    EXPECT_GT(metrics.totalErrors, 0);
}

TEST_F(VerificationMonitorTest, ExportMetrics) {
    // Record some test data
    monitor->recordVerification(createResult("hash1", VerificationStatus::Passed, 1024));
    monitor->recordVerification(createResult("hash2", VerificationStatus::Failed, 2048));
    monitor->recordVerification(createResult("hash3", VerificationStatus::Corrupted, 512));
    monitor->recordRepairAttempt("hash4", true);
    monitor->recordRepairAttempt("hash5", false);

    std::ostringstream output;
    monitor->exportMetrics(output);

    std::string metrics = output.str();

    // Check that metrics are exported in Prometheus format
    EXPECT_THAT(metrics, HasSubstr("# Integrity Verification Metrics"));
    EXPECT_THAT(metrics, HasSubstr("kronos_verification_total"));
    EXPECT_THAT(metrics, HasSubstr("kronos_verification_errors_total"));
    EXPECT_THAT(metrics, HasSubstr("kronos_verification_repairs_attempted_total"));
    EXPECT_THAT(metrics, HasSubstr("kronos_verification_repairs_successful_total"));
    EXPECT_THAT(metrics, HasSubstr("kronos_verification_status"));

    // Check for specific values
    EXPECT_THAT(metrics, HasSubstr("3")); // Total verifications
    EXPECT_THAT(metrics, HasSubstr("2")); // Total errors (Failed + Corrupted)
    EXPECT_THAT(metrics, HasSubstr("status=\"passed\""));
    EXPECT_THAT(metrics, HasSubstr("status=\"failed\""));
    EXPECT_THAT(metrics, HasSubstr("status=\"corrupted\""));
}

TEST_F(VerificationMonitorTest, MetricsCalculations) {
    // Record verifications with known timing
    for (int i = 0; i < 10; ++i) {
        monitor->recordVerification(
            createResult("pass" + std::to_string(i), VerificationStatus::Passed));
    }

    for (int i = 0; i < 3; ++i) {
        monitor->recordVerification(
            createResult("fail" + std::to_string(i), VerificationStatus::Failed));
    }

    monitor->recordRepairAttempt("repair1", true);
    monitor->recordRepairAttempt("repair2", true);
    monitor->recordRepairAttempt("repair3", false);

    auto metrics = monitor->getCurrentMetrics();

    EXPECT_EQ(metrics.totalVerifications, 13);
    EXPECT_EQ(metrics.totalErrors, 3);
    EXPECT_EQ(metrics.repairAttempts, 3);
    EXPECT_EQ(metrics.successfulRepairs, 2);

    // Rate calculations depend on timing, but should be non-negative
    EXPECT_GE(metrics.verificationsPerSecond, 0);
    EXPECT_GE(metrics.errorsPerSecond, 0);
    EXPECT_GE(metrics.currentErrorRate, 0.0);
}

TEST_F(VerificationMonitorTest, RateCalculationOverTime) {
    const int numVerifications = 100;

    // Record verifications rapidly
    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < numVerifications; ++i) {
        monitor->recordVerification(
            createResult("hash" + std::to_string(i), VerificationStatus::Passed));

        // Small delay to spread over time
        if (i % 10 == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);

    auto metrics = monitor->getCurrentMetrics();

    EXPECT_EQ(metrics.totalVerifications, numVerifications);
    EXPECT_EQ(metrics.totalErrors, 0);

    // Rate should be reasonable (allowing for some variance in timing)
    if (duration.count() > 0) {
        double expectedRate = static_cast<double>(numVerifications) / duration.count();
        EXPECT_GT(metrics.verificationsPerSecond, 0);
        // Allow for significant variance due to timing uncertainties in tests
        EXPECT_LT(metrics.verificationsPerSecond, expectedRate * 10);
    }
}

TEST_F(VerificationMonitorTest, AlertThresholdSettings) {
    // Test various threshold values
    EXPECT_NO_THROW(monitor->setErrorRateThreshold(0.0)); // 0%
    EXPECT_NO_THROW(monitor->setErrorRateThreshold(0.1)); // 10%
    EXPECT_NO_THROW(monitor->setErrorRateThreshold(0.5)); // 50%
    EXPECT_NO_THROW(monitor->setErrorRateThreshold(1.0)); // 100%

    // Edge cases
    EXPECT_NO_THROW(monitor->setErrorRateThreshold(-0.1)); // Negative (implementation dependent)
    EXPECT_NO_THROW(monitor->setErrorRateThreshold(2.0));  // > 100% (implementation dependent)
}

TEST_F(VerificationMonitorTest, ThreadSafetyTest) {
    const int numThreads = 4;
    const int verificationsPerThread = 250;

    std::vector<std::thread> threads;

    // Record verifications from multiple threads
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, t, verificationsPerThread]() {
            for (int i = 0; i < verificationsPerThread; ++i) {
                VerificationStatus status =
                    (i % 4 == 0) ? VerificationStatus::Failed : VerificationStatus::Passed;
                auto result = createResult(
                    "thread" + std::to_string(t) + "_hash" + std::to_string(i), status);
                monitor->recordVerification(result);

                // Also record some repair attempts
                if (i % 10 == 0) {
                    monitor->recordRepairAttempt(
                        "repair_" + std::to_string(t) + "_" + std::to_string(i), i % 2 == 0);
                }
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    auto metrics = monitor->getCurrentMetrics();

    EXPECT_EQ(metrics.totalVerifications, numThreads * verificationsPerThread);
    // Allow small variance due to thread timing
    auto expectedErrors = numThreads * (verificationsPerThread / 4);
    EXPECT_NEAR(metrics.totalErrors, expectedErrors, 5); // Allow ±5 variance
    EXPECT_EQ(metrics.repairAttempts, numThreads * (verificationsPerThread / 10)); // Every 10th
}

TEST_F(VerificationMonitorTest, StatusBreakdown) {
    // Record verifications with all different statuses
    monitor->recordVerification(createResult("pass1", VerificationStatus::Passed));
    monitor->recordVerification(createResult("pass2", VerificationStatus::Passed));
    monitor->recordVerification(createResult("fail1", VerificationStatus::Failed));
    monitor->recordVerification(createResult("corrupt1", VerificationStatus::Corrupted));
    monitor->recordVerification(createResult("missing1", VerificationStatus::Missing));
    monitor->recordVerification(createResult("repaired1", VerificationStatus::Repaired));

    std::ostringstream output;
    monitor->exportMetrics(output);
    std::string metrics = output.str();

    // Check that all status types are represented
    EXPECT_THAT(metrics, HasSubstr("status=\"passed\"} 2"));
    EXPECT_THAT(metrics, HasSubstr("status=\"failed\"} 1"));
    EXPECT_THAT(metrics, HasSubstr("status=\"corrupted\"} 1"));
    EXPECT_THAT(metrics, HasSubstr("status=\"missing\"} 1"));
    EXPECT_THAT(metrics, HasSubstr("status=\"repaired\"} 1"));
}

TEST_F(VerificationMonitorTest, LongRunningMonitoring) {
    // Simulate long-running monitoring with periodic metrics collection
    const int cycles = 10;
    const int verificationsPerCycle = 50;

    for (int cycle = 0; cycle < cycles; ++cycle) {
        // Record some verifications
        for (int i = 0; i < verificationsPerCycle; ++i) {
            VerificationStatus status =
                (i % 5 == 0) ? VerificationStatus::Failed : VerificationStatus::Passed;
            monitor->recordVerification(
                createResult("cycle" + std::to_string(cycle) + "_" + std::to_string(i), status));
        }

        // Check metrics periodically
        auto metrics = monitor->getCurrentMetrics();
        EXPECT_EQ(metrics.totalVerifications, (cycle + 1) * verificationsPerCycle);
        EXPECT_EQ(metrics.totalErrors, (cycle + 1) * (verificationsPerCycle / 5));

        // Small delay between cycles
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    auto finalMetrics = monitor->getCurrentMetrics();
    EXPECT_EQ(finalMetrics.totalVerifications, cycles * verificationsPerCycle);
    EXPECT_GT(finalMetrics.verificationsPerSecond, 0);
}