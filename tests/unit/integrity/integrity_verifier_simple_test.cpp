#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../../utils/test_helpers.h"
#include <yams/crypto/hasher.h>
#include <yams/integrity/verifier.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include <cstddef>
#include <filesystem>
#include <memory>

using namespace yams;
using namespace yams::integrity;
using namespace yams::storage;
using namespace testing;

class IntegrityVerifierSimpleTest : public yams::test::YamsTest {
protected:
    void SetUp() override {
        YamsTest::SetUp();

        // Create storage configuration
        StorageConfig storageConfig{.basePath = testDir, .shardDepth = 2, .mutexPoolSize = 64};

        storageEngine = std::make_unique<StorageEngine>(std::move(storageConfig));

        // Create reference counter configuration
        ReferenceCounter::Config refConfig{.databasePath = testDir / "refs.db",
                                           .enableWAL = true,
                                           .enableStatistics = true,
                                           .cacheSize = 1000,
                                           .busyTimeout = 1000};

        refCounter = std::make_unique<ReferenceCounter>(std::move(refConfig));

        // Create test verifier with simple config
        VerificationConfig verifyConfig;
        verifyConfig.maxConcurrentVerifications = 1; // Keep it simple
        verifyConfig.blocksPerSecond = 10;
        verifyConfig.enableAutoRepair = false;

        verifier = std::make_unique<IntegrityVerifier>(*storageEngine, *refCounter, verifyConfig);

        // Set up test data
        testData = yams::test::TestVectors::getHelloWorldData();
        hasher = crypto::createSHA256Hasher();
        testHash = hasher->hash(testData);
    }

    void TearDown() override {
        if (verifier && verifier->isRunning()) {
            [[maybe_unused]] auto result = verifier->stopBackgroundVerification();
        }

        verifier.reset();
        refCounter.reset();
        storageEngine.reset();

        YamsTest::TearDown();
    }

    void storeTestData() {
        // Store the data using hash + data format
        auto storeResult = storageEngine->store(testHash, testData);
        ASSERT_TRUE(storeResult.has_value()) << "Failed to store test data";

        // Add reference count
        auto txn = refCounter->beginTransaction();
        txn->increment(testHash, testData.size());
        auto commitResult = txn->commit();
        ASSERT_TRUE(commitResult.has_value()) << "Failed to commit reference";
    }

    std::unique_ptr<StorageEngine> storageEngine;
    std::unique_ptr<ReferenceCounter> refCounter;
    std::unique_ptr<IntegrityVerifier> verifier;
    std::unique_ptr<crypto::IContentHasher> hasher;
    std::vector<std::byte> testData;
    std::string testHash;
};

TEST_F(IntegrityVerifierSimpleTest, ConstructorBasics) {
    EXPECT_FALSE(verifier->isRunning());
    EXPECT_FALSE(verifier->isPaused());

    const auto& stats = verifier->getStatistics();
    EXPECT_EQ(stats.blocksVerifiedTotal.load(), 0u);
    EXPECT_EQ(stats.verificationErrorsTotal.load(), 0u);
}

TEST_F(IntegrityVerifierSimpleTest, VerifyExistingBlock) {
    storeTestData();

    auto result = verifier->verifyBlock(testHash);

    EXPECT_EQ(result.status, VerificationStatus::Passed);
    EXPECT_EQ(result.blockHash, testHash);
    EXPECT_EQ(result.blockSize, testData.size());
    EXPECT_TRUE(result.isSuccess());

    const auto& stats = verifier->getStatistics();
    EXPECT_EQ(stats.blocksVerifiedTotal.load(), 1u);
    EXPECT_EQ(stats.verificationErrorsTotal.load(), 0u);
}

TEST_F(IntegrityVerifierSimpleTest, VerifyMissingBlock) {
    std::string missingHash = "0000000000000000000000000000000000000000000000000000000000000000";

    auto result = verifier->verifyBlock(missingHash);

    EXPECT_EQ(result.status, VerificationStatus::Missing);
    EXPECT_EQ(result.blockHash, missingHash);
    EXPECT_FALSE(result.isSuccess());

    const auto& stats = verifier->getStatistics();
    EXPECT_EQ(stats.blocksVerifiedTotal.load(), 0u);
    EXPECT_EQ(stats.verificationErrorsTotal.load(), 1u);
}

TEST_F(IntegrityVerifierSimpleTest, BackgroundVerificationControl) {
    EXPECT_FALSE(verifier->isRunning());

    auto startResult = verifier->startBackgroundVerification();
    EXPECT_TRUE(startResult.has_value());
    EXPECT_TRUE(verifier->isRunning());

    auto stopResult = verifier->stopBackgroundVerification();
    EXPECT_TRUE(stopResult.has_value());
    EXPECT_FALSE(verifier->isRunning());
}

TEST_F(IntegrityVerifierSimpleTest, GenerateReport) {
    storeTestData();

    // Verify a block to have some data
    auto result = verifier->verifyBlock(testHash);
    ASSERT_TRUE(result.isSuccess());

    auto report = verifier->generateReport(std::chrono::hours{1});

    EXPECT_EQ(report.blocksVerified, 1u);
    EXPECT_EQ(report.blocksPassed, 1u);
    EXPECT_EQ(report.blocksFailed, 0u);
    EXPECT_GT(report.totalBytes, std::size_t{0});
    EXPECT_NEAR(report.getSuccessRate(), 1.0, 0.01);
}

TEST_F(IntegrityVerifierSimpleTest, StatisticsReset) {
    storeTestData();
    auto result = verifier->verifyBlock(testHash);
    ASSERT_TRUE(result.isSuccess());

    const auto& statsBefore = verifier->getStatistics();
    EXPECT_GT(statsBefore.blocksVerifiedTotal.load(), 0u);

    verifier->resetStatistics();

    const auto& statsAfter = verifier->getStatistics();
    EXPECT_EQ(statsAfter.blocksVerifiedTotal.load(), 0u);
    EXPECT_EQ(statsAfter.verificationErrorsTotal.load(), 0u);
}

TEST_F(IntegrityVerifierSimpleTest, ConfigurationUpdate) {
    VerificationConfig newConfig;
    newConfig.maxConcurrentVerifications = 2;
    newConfig.blocksPerSecond = 50;

    EXPECT_NO_THROW(verifier->updateConfig(newConfig));
}

TEST_F(IntegrityVerifierSimpleTest, SchedulingStrategies) {
    // Test that all scheduling strategies can be set
    EXPECT_NO_THROW(verifier->setSchedulingStrategy(SchedulingStrategy::ByAge));
    EXPECT_NO_THROW(verifier->setSchedulingStrategy(SchedulingStrategy::BySize));
    EXPECT_NO_THROW(verifier->setSchedulingStrategy(SchedulingStrategy::ByFailures));
    EXPECT_NO_THROW(verifier->setSchedulingStrategy(SchedulingStrategy::ByAccess));
    EXPECT_NO_THROW(verifier->setSchedulingStrategy(SchedulingStrategy::Balanced));
}

TEST_F(IntegrityVerifierSimpleTest, CallbackFunctionality) {
    storeTestData();

    bool progressCalled = false;
    verifier->setProgressCallback([&](const VerificationResult& result) {
        progressCalled = true;
        EXPECT_EQ(result.blockHash, testHash);
    });

    auto result = verifier->verifyBlock(testHash);
    ASSERT_TRUE(result.isSuccess());

    EXPECT_TRUE(progressCalled);
}