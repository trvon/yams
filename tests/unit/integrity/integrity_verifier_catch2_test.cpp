#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"
#include <yams/crypto/hasher.h>
#include <yams/integrity/verifier.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <string>
#include <vector>

using namespace yams;
using namespace yams::integrity;
using namespace yams::storage;

namespace {

struct IntegrityVerifierFixture {
    yams::test::TempDirGuard tempDirGuard{"yams_integrity_verifier_"};
    std::filesystem::path testDir{tempDirGuard.path()};
    std::unique_ptr<StorageEngine> storageEngine;
    std::unique_ptr<ReferenceCounter> refCounter;
    std::unique_ptr<IntegrityVerifier> verifier;
    std::unique_ptr<crypto::IContentHasher> hasher;
    std::vector<std::byte> testDataBytes;
    std::string testDataString;
    std::string testHash;

    IntegrityVerifierFixture() {
        StorageConfig config{
            .basePath = testDir, .shardDepth = 2, .mutexPoolSize = 128, .enableCompression = false};
        storageEngine = std::make_unique<StorageEngine>(std::move(config));

        ReferenceCounter::Config refConfig;
        refConfig.databasePath = testDir / "refs.db";
        refCounter = std::make_unique<ReferenceCounter>(refConfig);

        VerificationConfig verifyConfig;
        verifyConfig.maxConcurrentVerifications = 2;
        verifyConfig.blocksPerSecond = 100;
        verifyConfig.enableAutoRepair = false;

        verifier = std::make_unique<IntegrityVerifier>(*storageEngine, *refCounter, verifyConfig);

        testDataBytes = yams::test::TestVectors::getHelloWorldData();
        testDataString = "Hello World";

        hasher = crypto::createSHA256Hasher();
        testHash = hasher->hash(testDataBytes);
    }

    ~IntegrityVerifierFixture() {
        if (verifier && verifier->isRunning()) {
            verifier->stopBackgroundVerification();
        }
        verifier.reset();
        refCounter.reset();
        storageEngine.reset();
    }

    IntegrityVerifierFixture(const IntegrityVerifierFixture&) = delete;
    IntegrityVerifierFixture& operator=(const IntegrityVerifierFixture&) = delete;

    std::string storeBlock(const std::vector<std::byte>& data) {
        auto dataHash = hasher->hash(data);
        auto storeResult =
            storageEngine->store(dataHash, std::span<const std::byte>(data.data(), data.size()));
        REQUIRE(storeResult.has_value());

        auto refResult = refCounter->increment(dataHash, data.size());
        REQUIRE(refResult.has_value());
        return dataHash;
    }

    void storeTestBlock() { storeBlock(testDataBytes); }

    void corruptTestBlock() {
        // Mirror StorageEngine::getObjectPath sharding (shardDepth=2).
        auto blockPath = testDir / "objects" / testHash.substr(0, 2) / testHash.substr(2);
        if (std::filesystem::exists(blockPath)) {
            std::ofstream file(blockPath, std::ios::binary | std::ios::out);
            if (file.is_open()) {
                std::string corruptData = "CORRUPTED DATA";
                file.write(corruptData.data(), corruptData.size());
                file.close();
            }
        }
    }
};

} // namespace

TEST_CASE_METHOD(IntegrityVerifierFixture, "ConstructorAndDestructor", "[integrity][verifier]") {
    CHECK_FALSE(verifier->isRunning());
    CHECK_FALSE(verifier->isPaused());

    const auto& stats = verifier->getStatistics();
    CHECK(stats.blocksVerifiedTotal.load() == 0);
    CHECK(stats.verificationErrorsTotal.load() == 0);
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "VerifyValidBlock", "[integrity][verifier]") {
    storeTestBlock();

    auto result = verifier->verifyBlock(testHash);

    CHECK(result.status == VerificationStatus::Passed);
    CHECK(result.blockHash == testHash);
    CHECK(result.blockSize == testDataBytes.size());
    CHECK(result.errorDetails.empty());
    CHECK(result.isSuccess());

    const auto& stats = verifier->getStatistics();
    CHECK(stats.blocksVerifiedTotal.load() == 1);
    CHECK(stats.verificationErrorsTotal.load() == 0);
    CHECK(stats.bytesVerifiedTotal.load() == testDataBytes.size());
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "VerifyMissingBlock", "[integrity][verifier]") {
    std::string missingHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    auto result = verifier->verifyBlock(missingHash);

    CHECK(result.status == VerificationStatus::Missing);
    CHECK(result.blockHash == missingHash);
    CHECK(result.blockSize == 0);
    CHECK_FALSE(result.errorDetails.empty());
    CHECK_FALSE(result.isSuccess());

    const auto& stats = verifier->getStatistics();
    CHECK(stats.blocksVerifiedTotal.load() == 0);
    CHECK(stats.verificationErrorsTotal.load() == 1);
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "VerifyCorruptedBlock", "[integrity][verifier]") {
    storeTestBlock();
    corruptTestBlock();

    auto result = verifier->verifyBlock(testHash);

    CHECK(result.status == VerificationStatus::Corrupted);
    CHECK(result.blockHash == testHash);
    CHECK(result.blockSize > 0);
    CHECK_FALSE(result.errorDetails.empty());
    CHECK_FALSE(result.isSuccess());

    const auto& stats = verifier->getStatistics();
    CHECK(stats.blocksVerifiedTotal.load() == 0);
    CHECK(stats.verificationErrorsTotal.load() == 1);
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "VerifyMultipleBlocks", "[integrity][verifier]") {
    std::vector<std::vector<std::byte>> testBlocks = {yams::test::TestVectors::getABCData(),
                                                      yams::test::TestVectors::getHelloWorldData()};

    std::vector<std::string> hashes;
    hashes.reserve(testBlocks.size());
    for (const auto& data : testBlocks) {
        hashes.push_back(storeBlock(data));
    }

    auto results = verifier->verifyBlocks(hashes);

    REQUIRE(results.size() == testBlocks.size());

    for (size_t i = 0; i < results.size(); ++i) {
        CHECK(results[i].status == VerificationStatus::Passed);
        CHECK(results[i].blockHash == hashes[i]);
        CHECK(results[i].isSuccess());
    }

    const auto& stats = verifier->getStatistics();
    CHECK(stats.blocksVerifiedTotal.load() == testBlocks.size());
    CHECK(stats.verificationErrorsTotal.load() == 0);
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "BackgroundVerificationLifecycle",
                 "[integrity][verifier]") {
    CHECK_FALSE(verifier->isRunning());

    auto startResult = verifier->startBackgroundVerification();
    CHECK(startResult.has_value());
    CHECK(verifier->isRunning());
    CHECK_FALSE(verifier->isPaused());

    auto startAgainResult = verifier->startBackgroundVerification();
    CHECK_FALSE(startAgainResult.has_value());
    CHECK(startAgainResult.error() == ErrorCode::OperationInProgress);

    auto stopResult = verifier->stopBackgroundVerification();
    CHECK(stopResult.has_value());
    CHECK_FALSE(verifier->isRunning());

    auto stopAgainResult = verifier->stopBackgroundVerification();
    CHECK(stopAgainResult.has_value());
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "PauseResumeVerification", "[integrity][verifier]") {
    auto startResult = verifier->startBackgroundVerification();
    CHECK(startResult.has_value());
    CHECK(verifier->isRunning());
    CHECK_FALSE(verifier->isPaused());

    auto pauseResult = verifier->pauseVerification();
    CHECK(pauseResult.has_value());
    CHECK(verifier->isRunning());
    CHECK(verifier->isPaused());

    auto resumeResult = verifier->resumeVerification();
    CHECK(resumeResult.has_value());
    CHECK(verifier->isRunning());
    CHECK_FALSE(verifier->isPaused());

    verifier->stopBackgroundVerification();

    auto pauseFailResult = verifier->pauseVerification();
    CHECK_FALSE(pauseFailResult.has_value());
    CHECK(pauseFailResult.error() == ErrorCode::InvalidOperation);

    auto resumeFailResult = verifier->resumeVerification();
    CHECK_FALSE(resumeFailResult.has_value());
    CHECK(resumeFailResult.error() == ErrorCode::InvalidOperation);
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "StatisticsAndReporting", "[integrity][verifier]") {
    storeTestBlock();

    const auto& initialStats = verifier->getStatistics();
    CHECK(initialStats.blocksVerifiedTotal.load() == 0);

    verifier->verifyBlock(testHash);

    const auto& stats = verifier->getStatistics();
    CHECK(stats.blocksVerifiedTotal.load() == 1);
    CHECK(stats.totalVerificationTime.count() >= 0);
    CHECK(stats.getVerificationRate() >= 0.0);

    auto report = verifier->generateReport(std::chrono::hours{1});
    CHECK(report.blocksVerified == 1);
    CHECK(report.blocksPassed == 1);
    CHECK(report.blocksFailed == 0);
    CHECK(report.totalBytes > 0);
    CHECK(report.getSuccessRate() > 0.99);
    CHECK(report.getErrorRate() < 0.01);

    verifier->resetStatistics();

    const auto& resetStats = verifier->getStatistics();
    CHECK(resetStats.blocksVerifiedTotal.load() == 0);
    CHECK(resetStats.verificationErrorsTotal.load() == 0);
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "RecentFailures", "[integrity][verifier]") {
    storeTestBlock();
    corruptTestBlock();

    verifier->verifyBlock(testHash);

    auto failures = verifier->getRecentFailures(10);
    REQUIRE(failures.size() == 1);
    CHECK(failures[0].status == VerificationStatus::Corrupted);
    CHECK(failures[0].blockHash == testHash);
    CHECK_FALSE(failures[0].isSuccess());
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "ConfigurationUpdate", "[integrity][verifier]") {
    VerificationConfig newConfig;
    newConfig.maxConcurrentVerifications = 8;
    newConfig.blocksPerSecond = 500;
    newConfig.enableAutoRepair = true;

    CHECK_NOTHROW(verifier->updateConfig(newConfig));
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "CallbackFunctionality", "[integrity][verifier]") {
    storeTestBlock();

    bool progressCallbackCalled = false;
    VerificationResult capturedResult;

    verifier->setProgressCallback([&](const VerificationResult& result) {
        progressCallbackCalled = true;
        capturedResult = result;
    });

    bool alertCallbackCalled = false;
    verifier->setAlertCallback(
        [&](const IntegrityReport& /*report*/) { alertCallbackCalled = true; });

    verifier->verifyBlock(testHash);

    CHECK(progressCallbackCalled);
    CHECK(capturedResult.blockHash == testHash);
    CHECK(capturedResult.status == VerificationStatus::Passed);
    (void)alertCallbackCalled;
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "SchedulingStrategyChange", "[integrity][verifier]") {
    CHECK_NOTHROW(verifier->setSchedulingStrategy(SchedulingStrategy::ByAge));
    CHECK_NOTHROW(verifier->setSchedulingStrategy(SchedulingStrategy::BySize));
    CHECK_NOTHROW(verifier->setSchedulingStrategy(SchedulingStrategy::ByFailures));
    CHECK_NOTHROW(verifier->setSchedulingStrategy(SchedulingStrategy::ByAccess));
    CHECK_NOTHROW(verifier->setSchedulingStrategy(SchedulingStrategy::Balanced));
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "VerifyAllOperation", "[integrity][verifier]") {
    storeTestBlock();

    auto report = verifier->verifyAll();

    CHECK(report.generatedAt.time_since_epoch().count() >= 0);
    CHECK(report.duration.count() >= 0);
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "VerifyEmptyHash", "[integrity][verifier]") {
    auto result = verifier->verifyBlock("");

    CHECK(result.status != VerificationStatus::Passed);
    CHECK_FALSE(result.isSuccess());
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "VerifyInvalidHash", "[integrity][verifier]") {
    auto result = verifier->verifyBlock("invalid_hash");

    CHECK(result.status != VerificationStatus::Passed);
    CHECK_FALSE(result.isSuccess());
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "MoveConstructorAndAssignment",
                 "[integrity][verifier]") {
    VerificationConfig config;
    auto verifier1 = std::make_unique<IntegrityVerifier>(*storageEngine, *refCounter, config);

    auto verifier2 = std::move(*verifier1);

    IntegrityVerifier verifier3(*storageEngine, *refCounter, config);
    verifier3 = std::move(verifier2);

    CHECK_FALSE(verifier3.isRunning());
}
