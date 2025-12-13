#include <catch2/catch_test_macros.hpp>

#include <yams/integrity/verifier.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace yams::integrity;
using namespace yams::storage;

namespace {

struct TempDir {
    std::filesystem::path path;

    TempDir() {
        path = std::filesystem::temp_directory_path() /
               ("yams_verifier_test_" +
                std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + "_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }

    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

struct IntegrityVerifierFixture {
    TempDir tempDir;
    std::unique_ptr<StorageEngine> storage;
    std::unique_ptr<ReferenceCounter> refCounter;
    std::unique_ptr<IntegrityVerifier> verifier;
    VerificationConfig config;

    IntegrityVerifierFixture() {
        // Set up storage engine
        StorageConfig storageConfig;
        storageConfig.basePath = tempDir.path / "storage";
        std::filesystem::create_directories(storageConfig.basePath);
        storage = std::make_unique<StorageEngine>(storageConfig);

        // Set up reference counter
        ReferenceCounter::Config refConfig;
        refConfig.databasePath = tempDir.path / "refs.db";
        refCounter = std::make_unique<ReferenceCounter>(refConfig);

        // Set up verifier with conservative config for tests
        config.maxConcurrentVerifications = 2;
        config.enableAutoRepair = false;
        verifier = std::make_unique<IntegrityVerifier>(*storage, *refCounter, config);
    }
};

} // namespace

TEST_CASE_METHOD(IntegrityVerifierFixture, "IntegrityVerifier basic state",
                 "[integrity][verifier]") {
    SECTION("initial state is not running") {
        CHECK_FALSE(verifier->isRunning());
        CHECK_FALSE(verifier->isPaused());
    }
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "IntegrityVerifier block verification",
                 "[integrity][verifier]") {
    SECTION("verifying missing block returns failure status") {
        auto result = verifier->verifyBlock("nonexistent-hash");
        // Block verification fails for non-existent blocks
        CHECK_FALSE(result.isSuccess());
    }
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "IntegrityVerifier report generation",
                 "[integrity][verifier]") {
    SECTION("generates empty report when no verifications") {
        auto report = verifier->generateReport();
        CHECK(report.blocksVerified == 0u);
        CHECK(report.blocksPassed == 0u);
        CHECK(report.blocksFailed == 0u);
    }

    SECTION("reports recent failures") {
        auto failures = verifier->getRecentFailures();
        CHECK(failures.empty());
    }
}

TEST_CASE_METHOD(IntegrityVerifierFixture, "IntegrityVerifier statistics",
                 "[integrity][verifier]") {
    SECTION("statistics are accessible") {
        const auto& stats = verifier->getStatistics();
        CHECK(stats.blocksVerifiedTotal.load() == 0u);
        CHECK(stats.verificationErrorsTotal.load() == 0u);
    }
}
