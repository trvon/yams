#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yams/integrity/verifier.h>
#include <yams/storage/storage_engine.h>
#include <yams/storage/reference_counter.h>
#include <yams/crypto/hasher.h>
#include "../../utils/test_helpers.h"

#include <filesystem>
#include <fstream>
#include <random>
#include <chrono>
#include <thread>
#include <span>

using namespace yams;
using namespace yams::integrity;
using namespace yams::storage;
using namespace testing;

class IntegrityVerifierTest : public yams::test::YamsTest {
protected:
    void SetUp() override {
        YamsTest::SetUp();
        
        // Create storage configuration
        StorageConfig config{
            .basePath = testDir,
            .shardDepth = 2,
            .mutexPoolSize = 128,
            .enableCompression = false
        };
        
        // Initialize storage engine and reference counter
        storageEngine = std::make_unique<StorageEngine>(std::move(config));
        
        refCounter = std::make_unique<ReferenceCounter>(testDir / "refs.db");
        
        // Create test verifier with custom config
        VerificationConfig verifyConfig;
        verifyConfig.maxConcurrentVerifications = 2;
        verifyConfig.blocksPerSecond = 100;
        verifyConfig.enableAutoRepair = false; // Disable for testing
        
        verifier = std::make_unique<IntegrityVerifier>(*storageEngine, *refCounter, verifyConfig);
        
        // Create test data
        testDataBytes = yams::test::TestVectors::getHelloWorldData();
        testDataString = "Hello World";
        
        // Create content hasher and compute hash
        hasher = crypto::createSHA256Hasher();
        testHash = hasher->hash(testDataBytes);
    }
    
    void TearDown() override {
        // Stop verification if running
        if (verifier && verifier->isRunning()) {
            verifier->stopBackgroundVerification();
        }
        
        // Clean up
        verifier.reset();
        refCounter.reset();
        storageEngine.reset();
        
        YamsTest::TearDown();
    }
    
    void storeTestBlock() {
        auto result = storageEngine->store(testDataBytes);
        ASSERT_TRUE(result.has_value()) << "Failed to store test data";
        EXPECT_EQ(result.value(), testHash);
        
        // Add reference
        auto refResult = refCounter->incrementReference(testHash);
        ASSERT_TRUE(refResult.has_value()) << "Failed to add reference";
    }
    
    void corruptTestBlock() {
        // Find the storage file and corrupt it
        auto blockPath = testDir / "blocks" / testHash.substr(0, 2) / testHash;
        if (std::filesystem::exists(blockPath)) {
            std::ofstream file(blockPath, std::ios::binary | std::ios::out);
            if (file.is_open()) {
                std::string corruptData = "CORRUPTED DATA";
                file.write(corruptData.data(), corruptData.size());
                file.close();
            }
        }
    }
    
    std::unique_ptr<StorageEngine> storageEngine;
    std::unique_ptr<ReferenceCounter> refCounter;
    std::unique_ptr<IntegrityVerifier> verifier;
    std::unique_ptr<crypto::IContentHasher> hasher;
    std::vector<std::byte> testDataBytes;
    std::string testDataString;
    std::string testHash;
};

TEST_F(IntegrityVerifierTest, ConstructorAndDestructor) {
    // Verifier should be constructed successfully in SetUp
    EXPECT_FALSE(verifier->isRunning());
    EXPECT_FALSE(verifier->isPaused());
    
    auto stats = verifier->getStatistics();
    EXPECT_EQ(stats.blocksVerifiedTotal.load(), 0);
    EXPECT_EQ(stats.verificationErrorsTotal.load(), 0);
}

TEST_F(IntegrityVerifierTest, VerifyValidBlock) {
    storeTestBlock();
    
    auto result = verifier->verifyBlock(testHash);
    
    EXPECT_EQ(result.status, VerificationStatus::Passed);
    EXPECT_EQ(result.blockHash, testHash);
    EXPECT_EQ(result.blockSize, testDataBytes.size());
    EXPECT_TRUE(result.errorDetails.empty());
    EXPECT_TRUE(result.isSuccess());
    
    auto stats = verifier->getStatistics();
    EXPECT_EQ(stats.blocksVerifiedTotal.load(), 1);
    EXPECT_EQ(stats.verificationErrorsTotal.load(), 0);
    EXPECT_EQ(stats.bytesVerifiedTotal.load(), testDataBytes.size());
}

TEST_F(IntegrityVerifierTest, VerifyMissingBlock) {
    std::string missingHash = "0123456789abcdef0123456789abcdef01234567";
    
    auto result = verifier->verifyBlock(missingHash);
    
    EXPECT_EQ(result.status, VerificationStatus::Missing);
    EXPECT_EQ(result.blockHash, missingHash);
    EXPECT_EQ(result.blockSize, 0);
    EXPECT_FALSE(result.errorDetails.empty());
    EXPECT_FALSE(result.isSuccess());
    
    auto stats = verifier->getStatistics();
    EXPECT_EQ(stats.blocksVerifiedTotal.load(), 0);
    EXPECT_EQ(stats.verificationErrorsTotal.load(), 1);
}

TEST_F(IntegrityVerifierTest, VerifyCorruptedBlock) {
    storeTestBlock();
    corruptTestBlock();
    
    auto result = verifier->verifyBlock(testHash);
    
    EXPECT_EQ(result.status, VerificationStatus::Corrupted);
    EXPECT_EQ(result.blockHash, testHash);
    EXPECT_GT(result.blockSize, 0);
    EXPECT_FALSE(result.errorDetails.empty());
    EXPECT_FALSE(result.isSuccess());
    
    auto stats = verifier->getStatistics();
    EXPECT_EQ(stats.blocksVerifiedTotal.load(), 0);
    EXPECT_EQ(stats.verificationErrorsTotal.load(), 1);
}

TEST_F(IntegrityVerifierTest, VerifyMultipleBlocks) {
    // Store multiple test blocks
    std::vector<std::vector<std::byte>> testBlocks = {
        yams::test::TestVectors::getEmptyData(),
        yams::test::TestVectors::getABCData(),
        yams::test::TestVectors::getHelloWorldData()
    };
    
    std::vector<std::string> hashes;
    
    for (const auto& data : testBlocks) {
        auto result = storageEngine->store(data);
        ASSERT_TRUE(result.has_value());
        hashes.push_back(result.value());
        
        auto refResult = refCounter->incrementReference(result.value());
        ASSERT_TRUE(refResult.has_value());
    }
    
    auto results = verifier->verifyBlocks(hashes);
    
    EXPECT_EQ(results.size(), testBlocks.size());
    
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i].status, VerificationStatus::Passed);
        EXPECT_EQ(results[i].blockHash, hashes[i]);
        // Note: blockSize will be different since we're storing byte vectors
        EXPECT_TRUE(results[i].isSuccess());
    }
    
    auto stats = verifier->getStatistics();
    EXPECT_EQ(stats.blocksVerifiedTotal.load(), testBlocks.size());
    EXPECT_EQ(stats.verificationErrorsTotal.load(), 0);
}

TEST_F(IntegrityVerifierTest, BackgroundVerificationLifecycle) {
    EXPECT_FALSE(verifier->isRunning());
    
    auto startResult = verifier->startBackgroundVerification();
    EXPECT_TRUE(startResult.has_value());
    EXPECT_TRUE(verifier->isRunning());
    EXPECT_FALSE(verifier->isPaused());
    
    // Starting again should fail
    auto startAgainResult = verifier->startBackgroundVerification();
    EXPECT_FALSE(startAgainResult.has_value());
    EXPECT_EQ(startAgainResult.error(), ErrorCode::OperationInProgress);
    
    auto stopResult = verifier->stopBackgroundVerification();
    EXPECT_TRUE(stopResult.has_value());
    EXPECT_FALSE(verifier->isRunning());
    
    // Stopping again should succeed (idempotent)
    auto stopAgainResult = verifier->stopBackgroundVerification();
    EXPECT_TRUE(stopAgainResult.has_value());
}

TEST_F(IntegrityVerifierTest, PauseResumeVerification) {
    auto startResult = verifier->startBackgroundVerification();
    EXPECT_TRUE(startResult.has_value());
    EXPECT_TRUE(verifier->isRunning());
    EXPECT_FALSE(verifier->isPaused());
    
    auto pauseResult = verifier->pauseVerification();
    EXPECT_TRUE(pauseResult.has_value());
    EXPECT_TRUE(verifier->isRunning());
    EXPECT_TRUE(verifier->isPaused());
    
    auto resumeResult = verifier->resumeVerification();
    EXPECT_TRUE(resumeResult.has_value());
    EXPECT_TRUE(verifier->isRunning());
    EXPECT_FALSE(verifier->isPaused());
    
    verifier->stopBackgroundVerification();
    
    // Pause/resume when not running should fail
    auto pauseFailResult = verifier->pauseVerification();
    EXPECT_FALSE(pauseFailResult.has_value());
    EXPECT_EQ(pauseFailResult.error(), ErrorCode::InvalidOperation);
    
    auto resumeFailResult = verifier->resumeVerification();
    EXPECT_FALSE(resumeFailResult.has_value());
    EXPECT_EQ(resumeFailResult.error(), ErrorCode::InvalidOperation);
}

TEST_F(IntegrityVerifierTest, StatisticsAndReporting) {
    storeTestBlock();
    
    // Initial statistics
    const auto& initialStats = verifier->getStatistics();
    EXPECT_EQ(initialStats.blocksVerifiedTotal.load(), 0);
    
    // Verify a block
    verifier->verifyBlock(testHash);
    
    const auto& stats = verifier->getStatistics();
    EXPECT_EQ(stats.blocksVerifiedTotal.load(), 1);
    EXPECT_GT(stats.totalVerificationTime.count(), 0);
    EXPECT_GT(stats.getVerificationRate(), 0.0);
    
    // Generate report
    auto report = verifier->generateReport(std::chrono::hours{1});
    EXPECT_EQ(report.blocksVerified, 1);
    EXPECT_EQ(report.blocksPassed, 1);
    EXPECT_EQ(report.blocksFailed, 0);
    EXPECT_GT(report.totalBytes, 0);
    EXPECT_GT(report.getSuccessRate(), 0.99);
    EXPECT_LT(report.getErrorRate(), 0.01);
    
    // Reset statistics
    verifier->resetStatistics();
    
    const auto& resetStats = verifier->getStatistics();
    EXPECT_EQ(resetStats.blocksVerifiedTotal.load(), 0);
    EXPECT_EQ(resetStats.verificationErrorsTotal.load(), 0);
}

TEST_F(IntegrityVerifierTest, RecentFailures) {
    storeTestBlock();
    corruptTestBlock();
    
    // Verify corrupted block to generate failure
    verifier->verifyBlock(testHash);
    
    auto failures = verifier->getRecentFailures(10);
    EXPECT_EQ(failures.size(), 1);
    EXPECT_EQ(failures[0].status, VerificationStatus::Corrupted);
    EXPECT_EQ(failures[0].blockHash, testHash);
    EXPECT_FALSE(failures[0].isSuccess());
}

TEST_F(IntegrityVerifierTest, ConfigurationUpdate) {
    VerificationConfig newConfig;
    newConfig.maxConcurrentVerifications = 8;
    newConfig.blocksPerSecond = 500;
    newConfig.enableAutoRepair = true;
    
    // This should not throw
    EXPECT_NO_THROW(verifier->updateConfig(newConfig));
}

TEST_F(IntegrityVerifierTest, CallbackFunctionality) {
    storeTestBlock();
    
    bool progressCallbackCalled = false;
    VerificationResult capturedResult;
    
    verifier->setProgressCallback([&](const VerificationResult& result) {
        progressCallbackCalled = true;
        capturedResult = result;
    });
    
    bool alertCallbackCalled = false;
    verifier->setAlertCallback([&](const IntegrityReport& report) {
        alertCallbackCalled = true;
    });
    
    verifier->verifyBlock(testHash);
    
    EXPECT_TRUE(progressCallbackCalled);
    EXPECT_EQ(capturedResult.blockHash, testHash);
    EXPECT_EQ(capturedResult.status, VerificationStatus::Passed);
    
    // Alert callback typically requires error threshold to be exceeded
    // so it may not be called for a single successful verification
}

TEST_F(IntegrityVerifierTest, SchedulingStrategyChange) {
    // This should not throw
    EXPECT_NO_THROW(verifier->setSchedulingStrategy(SchedulingStrategy::ByAge));
    EXPECT_NO_THROW(verifier->setSchedulingStrategy(SchedulingStrategy::BySize));
    EXPECT_NO_THROW(verifier->setSchedulingStrategy(SchedulingStrategy::ByFailures));
    EXPECT_NO_THROW(verifier->setSchedulingStrategy(SchedulingStrategy::ByAccess));
    EXPECT_NO_THROW(verifier->setSchedulingStrategy(SchedulingStrategy::Balanced));
}

TEST_F(IntegrityVerifierTest, VerifyAllOperation) {
    // Store a few test blocks
    storeTestBlock();
    
    auto report = verifier->verifyAll();
    
    // verifyAll is a simplified implementation, but should not crash
    EXPECT_GE(report.generatedAt.time_since_epoch().count(), 0);
    EXPECT_GE(report.duration.count(), 0);
}

// Edge cases and error conditions
TEST_F(IntegrityVerifierTest, VerifyEmptyHash) {
    auto result = verifier->verifyBlock("");
    
    EXPECT_EQ(result.status, VerificationStatus::Missing);
    EXPECT_FALSE(result.isSuccess());
}

TEST_F(IntegrityVerifierTest, VerifyInvalidHash) {
    auto result = verifier->verifyBlock("invalid_hash");
    
    EXPECT_NE(result.status, VerificationStatus::Passed);
    EXPECT_FALSE(result.isSuccess());
}

TEST_F(IntegrityVerifierTest, MoveConstructorAndAssignment) {
    // Create verifier
    VerificationConfig config;
    auto verifier1 = std::make_unique<IntegrityVerifier>(*storageEngine, *refCounter, config);
    
    // Move constructor
    auto verifier2 = std::move(*verifier1);
    
    // Move assignment
    IntegrityVerifier verifier3(*storageEngine, *refCounter, config);
    verifier3 = std::move(verifier2);
    
    // Original verifier should still be functional after moves
    EXPECT_FALSE(verifier3.isRunning());
}