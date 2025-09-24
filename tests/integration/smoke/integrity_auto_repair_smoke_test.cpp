#include <CLI/CLI.hpp>
#include <gtest/gtest.h>

#include <yams/crypto/hasher.h>
#include <yams/integrity/repair_manager.h>
#include <yams/integrity/verifier.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

using namespace yams;
using namespace yams::integrity;

namespace {

std::vector<std::byte> makeBytes(std::string_view payload) {
    std::vector<std::byte> bytes(payload.size());
    std::memcpy(bytes.data(), payload.data(), payload.size());
    return bytes;
}

} // namespace

TEST(IntegrationSmoke, IntegrityAutoRepair_FromBackupDir) {
    namespace fs = std::filesystem;
    // Prepare a temp working area
    fs::path temp = fs::temp_directory_path() / "yams-int-repair-smoke";
    std::error_code ec;
    fs::remove_all(temp, ec);
    fs::create_directories(temp / "objects", ec);
    fs::create_directories(temp / "backup", ec);

    // Create storage engine
    storage::StorageEngine storage({.basePath = temp / "objects"});

    // Create ref counter
    storage::ReferenceCounter::Config rcfg;
    rcfg.databasePath = temp / "refs.db";
    auto refCounter = storage::createReferenceCounter(rcfg);

    // Prepare a block payload and its hash
    const std::string payload = "auto-repair";
    std::vector<std::byte> bytes = makeBytes(payload);
    auto hasher = crypto::createSHA256Hasher();
    hasher->init();
    hasher->update(std::span<const std::byte>(bytes.data(), bytes.size()));
    auto hash = hasher->finalize();

    // Simulate that primary storage is missing the block, but backup dir has it
    {
        std::ofstream out(temp / "backup" / hash, std::ios::binary | std::ios::trunc);
        out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    }

    // Build verifier with auto repair
    VerificationConfig vcfg;
    vcfg.enableAutoRepair = true;
    vcfg.maxRepairAttempts = 1;
    IntegrityVerifier verifier(storage, static_cast<storage::ReferenceCounter&>(*refCounter), vcfg);

    // Wire RepairManager with a backup fetcher to our backup dir
    RepairManagerConfig rmCfg;
    rmCfg.backupFetcher = [&](const std::string& requested) -> Result<std::vector<std::byte>> {
        fs::path p = temp / "backup" / requested;
        if (!fs::exists(p))
            return Error{ErrorCode::NotFound, "no backup"};
        std::ifstream in(p, std::ios::binary);
        std::vector<char> buf((std::istreambuf_iterator<char>(in)), {});
        std::vector<std::byte> out(buf.size());
        std::memcpy(out.data(), buf.data(), buf.size());
        return out;
    };
    auto rm = makeRepairManager(storage, rmCfg);
    verifier.setRepairManager(rm);

    // Verify the block: should be repaired
    auto result = verifier.verifyBlock(hash);
    EXPECT_EQ(result.status, VerificationStatus::Repaired);

    // Confirm block is now stored
    auto retrieved = storage.retrieve(hash);
    ASSERT_TRUE(retrieved.has_value());
    std::string restored(reinterpret_cast<const char*>(retrieved.value().data()),
                         retrieved.value().size());
    EXPECT_EQ(restored, payload);

    fs::remove_all(temp, ec);
}

TEST(IntegrationSmoke, IntegrityAutoRepair_FromP2PDir) {
    namespace fs = std::filesystem;
    fs::path temp = fs::temp_directory_path() / "yams-int-repair-p2p-smoke";
    std::error_code ec;
    fs::remove_all(temp, ec);
    fs::create_directories(temp / "objects", ec);
    fs::create_directories(temp / "p2p", ec);

    storage::StorageEngine storage({.basePath = temp / "objects"});
    storage::ReferenceCounter::Config rcfg;
    rcfg.databasePath = temp / "refs.db";
    auto refCounter = storage::createReferenceCounter(rcfg);

    const std::string payload = "auto-repair-p2p";
    std::vector<std::byte> bytes = makeBytes(payload);
    auto hasher = crypto::createSHA256Hasher();
    hasher->init();
    hasher->update(std::span<const std::byte>(bytes.data(), bytes.size()));
    auto hash = hasher->finalize();

    {
        std::ofstream out(temp / "p2p" / hash, std::ios::binary | std::ios::trunc);
        out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    }

    VerificationConfig vcfg;
    vcfg.enableAutoRepair = true;
    IntegrityVerifier verifier(storage, static_cast<storage::ReferenceCounter&>(*refCounter), vcfg);

    RepairManagerConfig rmCfg;
    rmCfg.p2pFetcher = [&](const std::string& requested) -> Result<std::vector<std::byte>> {
        fs::path p = temp / "p2p" / requested;
        if (!fs::exists(p))
            return Error{ErrorCode::NotFound, "no p2p"};
        std::ifstream in(p, std::ios::binary);
        std::vector<char> buf((std::istreambuf_iterator<char>(in)), {});
        std::vector<std::byte> out(buf.size());
        std::memcpy(out.data(), buf.data(), buf.size());
        return out;
    };
    verifier.setRepairManager(makeRepairManager(storage, rmCfg));

    auto result = verifier.verifyBlock(hash);
    EXPECT_EQ(result.status, VerificationStatus::Repaired);
    auto retrieved = storage.retrieve(hash);
    ASSERT_TRUE(retrieved.has_value());
    std::string restored(reinterpret_cast<const char*>(retrieved.value().data()),
                         retrieved.value().size());
    EXPECT_EQ(restored, payload);

    fs::remove_all(temp, ec);
}

TEST(IntegrationSmoke, VerifyCLIRepairsFromBackupFlag) {
    namespace fs = std::filesystem;

    fs::path temp = fs::temp_directory_path() / "yams-verify-cli-repair-smoke";
    std::error_code ec;
    fs::remove_all(temp, ec);
    fs::create_directories(temp / "backup", ec);
    fs::create_directories(temp / "blocks", ec);

    const std::string payload = "cli-repair-backup";
    std::vector<std::byte> bytes = makeBytes(payload);
    auto hasher = crypto::createSHA256Hasher();
    hasher->init();
    hasher->update(std::span<const std::byte>(bytes.data(), bytes.size()));
    const std::string hash = hasher->finalize();

    {
        std::ofstream out(temp / "backup" / hash, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.is_open());
        out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
        ASSERT_TRUE(out.good());
    }

    CLI::App verifyApp{"verify"};
    bool attemptRepair = false;
    std::string hashArg;
    std::string storageArg;
    std::string backupArg;

    verifyApp.add_option("--hash", hashArg)->required();
    verifyApp.add_flag("--repair", attemptRepair);
    verifyApp.add_option("--repair-backup-dir", backupArg)->required();
    verifyApp.add_option("--storage", storageArg)->required();

    std::vector<std::string> args = {
        "verify",
        "--hash",
        hash,
        "--repair",
        "--repair-backup-dir",
        (temp / "backup").string(),
        "--storage",
        temp.string(),
    };
    std::vector<char*> argv;
    argv.reserve(args.size());
    for (auto& arg : args) {
        argv.push_back(arg.data());
    }
    int argc = static_cast<int>(argv.size());
    ASSERT_NO_THROW(verifyApp.parse(argc, argv.data()));

    ASSERT_TRUE(attemptRepair);
    ASSERT_EQ(hashArg, hash);

    fs::path storageRoot = fs::path(storageArg);
    fs::path backupRoot = fs::path(backupArg);

    storage::StorageEngine storageEngine({.basePath = storageRoot / "blocks"});
    storage::ReferenceCounter::Config rcfg;
    rcfg.databasePath = storageRoot / "refs.db";
    auto refCounterPtr = storage::createReferenceCounter(rcfg);

    VerificationConfig vcfg;
    vcfg.enableAutoRepair = attemptRepair;
    vcfg.maxRepairAttempts = 1;
    IntegrityVerifier verifier(storageEngine,
                               static_cast<storage::ReferenceCounter&>(*refCounterPtr), vcfg);

    RepairManagerConfig rmCfg;
    rmCfg.backupFetcher =
        [backupRoot](const std::string& requested) -> Result<std::vector<std::byte>> {
        fs::path p = backupRoot / requested;
        if (!fs::exists(p))
            return Error{ErrorCode::NotFound, "backup not found"};
        std::ifstream in(p, std::ios::binary);
        if (!in)
            return Error{ErrorCode::IOError, "open failed"};
        std::vector<char> buf((std::istreambuf_iterator<char>(in)), {});
        std::vector<std::byte> out(buf.size());
        std::memcpy(out.data(), buf.data(), buf.size());
        return out;
    };
    verifier.setRepairManager(makeRepairManager(storageEngine, rmCfg));

    auto result = verifier.verifyBlock(hashArg);
    EXPECT_EQ(result.status, VerificationStatus::Repaired);

    auto retrieved = storageEngine.retrieve(hashArg);
    ASSERT_TRUE(retrieved.has_value());

    std::string restored(reinterpret_cast<const char*>(retrieved.value().data()),
                         retrieved.value().size());
    EXPECT_EQ(restored, payload);

    fs::remove_all(temp, ec);
}
