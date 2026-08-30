// Catch2 migration of service_manager_test.cpp
// Migration: yams-3s4 (daemon unit tests)
// Unit tests for ServiceManager component - construction, initialization, and service access

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <thread>

#ifdef _WIN32
#include <aclapi.h>
#include <windows.h>
#endif

#include "../../common/test_helpers_catch2.h"

#include "../../../include/yams/daemon/components/db_integrity_stamp.h"
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/repair/repair_health_probe.h>
#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/daemon.h>
#include <yams/memory_sync/writer_auth.h>
#include <yams/metadata/database.h>

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::daemon;

namespace yams::daemon::test {
namespace {

fs::path metadataDbPath(const DaemonConfig& config) {
    return config.dataDir / "yams.db";
}

void seedMetadataDb(const fs::path& dbPath, const std::string& value = "seed") {
    yams::metadata::Database db;
    REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::Create));
    REQUIRE(
        db.execute("CREATE TABLE IF NOT EXISTS startup_probe(id INTEGER PRIMARY KEY, value TEXT)"));
    REQUIRE(db.execute("DELETE FROM startup_probe"));
    REQUIRE(db.execute("INSERT INTO startup_probe(value) VALUES('" + value + "')"));
    REQUIRE(db.execute("PRAGMA wal_checkpoint(TRUNCATE)"));
    db.close();
    REQUIRE(clearDbCleanShutdownSidecars(dbPath));
}

void writeCorruptDb(const fs::path& dbPath) {
    std::ofstream out(dbPath, std::ios::binary | std::ios::trunc);
    REQUIRE(out.good());
    static constexpr char kSqliteHeader[] = "SQLite format 3";
    out.write(kSqliteHeader, static_cast<std::streamsize>(sizeof(kSqliteHeader)));
    const std::string garbage(4096, '\xff');
    out.write(garbage.data(), static_cast<std::streamsize>(garbage.size()));
    out.close();
}

std::optional<fs::path> findQuarantinedFile(const fs::path& dataDir) {
    std::error_code ec;
    if (!fs::exists(dataDir, ec)) {
        return std::nullopt;
    }
    const std::string prefix = "yams.db.corrupt-";
    for (fs::directory_iterator it(dataDir, ec), end; it != end; it.increment(ec)) {
        if (ec) {
            ec.clear();
            continue;
        }
        const auto name = it->path().filename().string();
        if (name.starts_with(prefix) && !name.ends_with("-wal") && !name.ends_with("-shm")) {
            return it->path();
        }
    }
    return std::nullopt;
}

std::size_t startupProbeRowCount(const fs::path& dbPath) {
    yams::metadata::Database db;
    REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::ReadWrite));
    auto stmtR = db.prepare("SELECT COUNT(*) FROM startup_probe");
    REQUIRE(stmtR);
    auto& stmt = stmtR.value();
    REQUIRE(stmt.step());
    auto count = static_cast<std::size_t>(stmt.getInt(0));
    db.close();
    return count;
}

void seedDummyWalSidecar(const fs::path& dbPath, const std::string& payload = "stale-wal") {
    std::ofstream out(dbPath.string() + "-wal", std::ios::binary | std::ios::trunc);
    REQUIRE(out.good());
    out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    out.close();
    REQUIRE(fs::exists(dbPath.string() + "-wal"));
}

bool walSidecarCleared(const fs::path& dbPath) {
    std::error_code ec;
    const fs::path walPath(dbPath.string() + "-wal");
    if (!fs::exists(walPath, ec)) {
        return true;
    }
    return fs::file_size(walPath, ec) == 0;
}

bool walSidecarStillMatchesPayload(const fs::path& dbPath, const std::string& payload) {
    const fs::path walPath(dbPath.string() + "-wal");
    std::error_code ec;
    if (!fs::exists(walPath, ec) || fs::file_size(walPath, ec) != payload.size()) {
        return false;
    }
    std::ifstream in(walPath, std::ios::binary);
    std::string actual(payload.size(), '\0');
    in.read(actual.data(), static_cast<std::streamsize>(actual.size()));
    return in.good() && actual == payload;
}

class SessionWatchIndexingService final : public app::services::IIndexingService {
public:
    Result<app::services::AddDirectoryResponse>
    addDirectory(const app::services::AddDirectoryRequest& request) override {
        requests.push_back(request);
        if (failuresRemaining > 0) {
            --failuresRemaining;
            return Error{ErrorCode::IOError, "simulated indexing failure"};
        }
        app::services::AddDirectoryResponse response;
        response.filesProcessed = request.includePatterns.size();
        response.filesIndexed = request.includePatterns.size();
        return response;
    }

    std::size_t failuresRemaining{0};
    std::vector<app::services::AddDirectoryRequest> requests;
};

class SessionWatchDocumentService final : public app::services::IDocumentService {
public:
    Result<app::services::StoreDocumentResponse>
    store(const app::services::StoreDocumentRequest&) override {
        return ErrorCode::NotImplemented;
    }
    Result<app::services::RetrieveDocumentResponse>
    retrieve(const app::services::RetrieveDocumentRequest&) override {
        return ErrorCode::NotImplemented;
    }
    Result<app::services::CatDocumentResponse>
    cat(const app::services::CatDocumentRequest&) override {
        return ErrorCode::NotImplemented;
    }
    Result<app::services::ListDocumentsResponse>
    list(const app::services::ListDocumentsRequest&) override {
        return ErrorCode::NotImplemented;
    }
    Result<app::services::UpdateMetadataResponse>
    updateMetadata(const app::services::UpdateMetadataRequest&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::string> resolveNameToHash(const std::string&, bool) override {
        return ErrorCode::NotImplemented;
    }
    Result<app::services::DeleteByNameResponse>
    deleteByName(const app::services::DeleteByNameRequest& request) override {
        deleteRequests.push_back(request);
        if (failuresRemaining > 0) {
            --failuresRemaining;
            return Error{ErrorCode::IOError, "simulated deletion failure"};
        }
        app::services::DeleteByNameResponse response;
        response.count = 1;
        response.deleted.push_back(
            {.name = request.name, .deleted = true, .errorCode = ErrorCode::Success});
        return response;
    }
    Result<app::services::DeleteByNameResponse>
    pruneVersions(const app::services::PruneVersionsRequest&) override {
        return ErrorCode::NotImplemented;
    }

    std::size_t failuresRemaining{0};
    std::vector<app::services::DeleteByNameRequest> deleteRequests;
};

void requireReadyDatabaseState(const StateComponent& state) {
    CHECK(state.readiness.databaseReady.load());
    std::lock_guard<std::mutex> lk(state.readiness.recoveryMutex);
    CHECK((state.readiness.databasePhase == "ready"));
}

struct PluginHostLifecycleCounters {
    std::atomic<std::size_t> listCalls{0};
    std::atomic<std::size_t> unloadCalls{0};
};

class CountingAbiPluginHost final : public AbiPluginHost {
public:
    explicit CountingAbiPluginHost(std::shared_ptr<PluginHostLifecycleCounters> counters)
        : counters_(counters) {}

    std::vector<PluginDescriptor> listLoaded() const override {
        counters_->listCalls.fetch_add(1, std::memory_order_relaxed);
        if (counters_->unloadCalls.load(std::memory_order_relaxed) == 0) {
            return {{.name = "counted_plugin"}};
        }
        return {};
    }

    Result<void> unload(const std::string&) override {
        counters_->unloadCalls.fetch_add(1, std::memory_order_relaxed);
        return {};
    }

private:
    std::shared_ptr<PluginHostLifecycleCounters> counters_;
};

fs::path writeProtectedWriterManifest(const fs::path& directory, std::string_view nodeId,
                                      std::string_view corpusId, std::uint64_t corpusEpoch) {
    auto keys = memory_sync::generateWriterKeyPair();
    REQUIRE(keys.has_value());
    const auto privateKey = directory / "writer-private.pem";
    const auto publicKey = directory / "writer-public.pem";
    REQUIRE(
        ServiceManager::__test_writeProtectedP2pPrivateKey(privateKey, keys.value().privateKeyPem)
            .has_value());
    REQUIRE(ServiceManager::__test_writeProtectedP2pPrivateKey(publicKey, keys.value().publicKeyPem)
                .has_value());
    const auto manifest = directory / "writers.json";
    const nlohmann::json content{
        {"schema_version", 1},
        {"corpus_id", corpusId},
        {"corpus_epoch", corpusEpoch},
        {"local_key",
         {{"writer_id", nodeId},
          {"key_id", "local-v1"},
          {"private_key_path", privateKey.string()}}},
        {"trusted_writers", nlohmann::json::array({{{"writer_id", nodeId},
                                                    {"key_id", "local-v1"},
                                                    {"public_key_path", publicKey.string()}}})}};
    REQUIRE(
        ServiceManager::__test_writeProtectedP2pPrivateKey(manifest, content.dump()).has_value());
    return manifest;
}

void grantUntrustedKeyAccess(const fs::path& keyPath, bool writable) {
#ifdef _WIN32
    BYTE worldSid[SECURITY_MAX_SID_SIZE];
    DWORD worldSidBytes = sizeof(worldSid);
    REQUIRE(CreateWellKnownSid(WinWorldSid, nullptr, worldSid, &worldSidBytes));
    PACL currentAcl = nullptr;
    PSECURITY_DESCRIPTOR descriptor = nullptr;
    REQUIRE(GetNamedSecurityInfoW(const_cast<wchar_t*>(keyPath.c_str()), SE_FILE_OBJECT,
                                  DACL_SECURITY_INFORMATION, nullptr, nullptr, &currentAcl, nullptr,
                                  &descriptor) == ERROR_SUCCESS);
    EXPLICIT_ACCESSW access{};
    access.grfAccessPermissions = writable ? GENERIC_WRITE : GENERIC_READ;
    access.grfAccessMode = GRANT_ACCESS;
    access.grfInheritance = NO_INHERITANCE;
    BuildTrusteeWithSidW(&access.Trustee, worldSid);
    PACL broadenedAcl = nullptr;
    REQUIRE(SetEntriesInAclW(1, &access, currentAcl, &broadenedAcl) == ERROR_SUCCESS);
    REQUIRE(SetNamedSecurityInfoW(const_cast<wchar_t*>(keyPath.c_str()), SE_FILE_OBJECT,
                                  DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                                  nullptr, nullptr, broadenedAcl, nullptr) == ERROR_SUCCESS);
    LocalFree(broadenedAcl);
    LocalFree(descriptor);
#else
    fs::permissions(keyPath, writable ? fs::perms::group_write : fs::perms::group_read,
                    fs::perm_options::add);
#endif
}

} // namespace

// Test fixture for ServiceManager tests
struct ServiceManagerFixture {
    yams::test::ScopedEnvVar disableWatcher_{"YAMS_DISABLE_SESSION_WATCHER", std::string("1")};
    DaemonConfig config_;
    StateComponent state_;
    DaemonLifecycleFsm lifecycleFsm_;
    fs::path testDir_;

    ServiceManagerFixture() {
        // Create isolated test directory
        testDir_ =
            fs::temp_directory_path() /
            ("sm_test_" + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) +
             "_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);

        // Setup basic config
        config_.dataDir = testDir_ / "data";
        config_.socketPath = testDir_ / "daemon.sock";
        config_.pidFile = testDir_ / "daemon.pid";
        config_.logFile = testDir_ / "daemon.log";

        fs::create_directories(config_.dataDir);
    }

    ~ServiceManagerFixture() {
        // Cleanup test directory
        if (fs::exists(testDir_)) {
            std::error_code ec;
            fs::remove_all(testDir_, ec);
        }
    }
};

struct ScopedCurrentPath {
    fs::path original_;

    explicit ScopedCurrentPath(const fs::path& target) : original_(fs::current_path()) {
        fs::current_path(target);
    }

    ~ScopedCurrentPath() {
        std::error_code ec;
        fs::current_path(original_, ec);
    }
};

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager construction succeeds",
                 "[daemon][service_manager]") {
    REQUIRE_NOTHROW(ServiceManager(config_, state_, lifecycleFsm_));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager getName returns correct component name",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    REQUIRE((std::string(sm.getName()) == "ServiceManager"));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager service accessors after construction",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);

    // PBI-057: Vector DB initialization is deferred to async phase
    REQUIRE((sm.getVectorDatabase() == nullptr));

    // Other services are initialized during initialize(), not in constructor
    REQUIRE((sm.getContentStore() == nullptr));
    REQUIRE((sm.getMetadataRepo() == nullptr));
    REQUIRE((sm.getModelProvider() == nullptr));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager multiple construction is idempotent",
                 "[daemon][service_manager]") {
    // Create two instances sequentially - should not throw
    ServiceManager sm1(config_, state_, lifecycleFsm_);
    ServiceManager sm2(config_, state_, lifecycleFsm_);
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "Concurrent embedded ServiceManager preserves active tuning lifecycle",
                 "[daemon][service_manager][config][tuning][lifecycle]") {
    yams::test::ScopedEnvVar timeout{"YAMS_IPC_TIMEOUT_MS", std::nullopt};
    TuneAdvisor::setIpcTimeoutMs(4321);
    config_.tuning.tuneAdvisorOverridesResolved = true;
    ServiceManager owner(config_, state_, lifecycleFsm_);
    REQUIRE((owner.getRuntimeTuningStatus().at("ipc.timeout_ms") == "4321"));

    auto directConfig = config_;
    directConfig.tuning.tuneAdvisorOverridesResolved = false;
    ServiceManager embedded(directConfig, state_, lifecycleFsm_);

    CHECK((TuneAdvisor::ipcTimeoutMs() == 4321u));
    CHECK((owner.getRuntimeTuningStatus().at("ipc.timeout_ms") == "4321"));
    CHECK((embedded.getRuntimeTuningStatus().at("ipc.timeout_ms") == "4321"));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager construction with missing data directory",
                 "[daemon][service_manager]") {
    fs::remove_all(config_.dataDir);
    REQUIRE_NOTHROW(ServiceManager(config_, state_, lifecycleFsm_));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager initialize reports data directory creation failures",
                 "[daemon][service_manager][startup][error]") {
    fs::remove_all(config_.dataDir);
    {
        std::ofstream blocker(config_.dataDir);
        REQUIRE(blocker.good());
        blocker << "not a directory";
    }

    ServiceManager sm(config_, state_, lifecycleFsm_);
    const auto result = sm.initialize();

    REQUIRE_FALSE(result);
    CHECK((result.error().code == ErrorCode::IOError));
    CHECK((result.error().message.find("Failed to create data directory") != std::string::npos));
    CHECK((result.error().message.find(config_.dataDir.string()) != std::string::npos));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager rejects direct P2P without usable writer authentication",
                 "[daemon][service_manager][p2p][security][config]") {
    config_.memorySync.enabled = true;
    config_.memorySync.transport = "direct";
    config_.memorySync.nodeId = "123e4567-e89b-42d3-a456-42661417400a";
    config_.memorySync.corpusId = "missing-writer-auth";
    config_.memorySync.corpusEpoch = 1;

    ServiceManager manager(config_, state_, lifecycleFsm_);
    REQUIRE(manager.initialize().has_value());
    boost::asio::io_context io;
    compat::stop_source stopSource;
    auto future = boost::asio::co_spawn(
        io, manager.initializeAsyncAwaitable(stopSource.get_token()), boost::asio::use_future);
    io.run();
    const auto initialized = future.get();
    REQUIRE_FALSE(initialized.has_value());
    CHECK(initialized.error().code == ErrorCode::InvalidArgument);
    CHECK(initialized.error().message.find("writer_auth_required=true") != std::string::npos);
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager rejects provenance-unknown direct operation stores",
                 "[daemon][service_manager][p2p][security][migration]") {
    const std::string nodeId = "123e4567-e89b-42d3-a456-42661417400a";
    const std::string corpusId = "legacy-direct-store";
    const auto manifest = writeProtectedWriterManifest(testDir_, nodeId, corpusId, 2);
    const auto legacyObject = config_.dataDir / "p2p" / "op-store" / "legacy" / "record.json";
    fs::create_directories(legacyObject.parent_path());
    std::ofstream(legacyObject) << "unsigned-history";

    config_.memorySync.enabled = true;
    config_.memorySync.transport = "direct";
    config_.memorySync.nodeId = nodeId;
    config_.memorySync.corpusId = corpusId;
    config_.memorySync.corpusEpoch = 2;
    config_.memorySync.writerAuthRequired = true;
    config_.memorySync.writerAuthManifestPath = manifest.string();

    ServiceManager manager(config_, state_, lifecycleFsm_);
    REQUIRE(manager.initialize().has_value());
    boost::asio::io_context io;
    compat::stop_source stopSource;
    auto future = boost::asio::co_spawn(
        io, manager.initializeAsyncAwaitable(stopSource.get_token()), boost::asio::use_future);
    io.run();
    const auto initialized = future.get();
    REQUIRE_FALSE(initialized.has_value());
    CHECK(initialized.error().code == ErrorCode::InvalidState);
    CHECK(initialized.error().message.find("provenance-unknown") != std::string::npos);
    CHECK(fs::exists(legacyObject));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager rejects symbolic-link P2P identity keys",
                 "[daemon][service_manager][p2p][security]") {
#ifdef _WIN32
    SKIP("symbolic-link permission semantics differ on Windows");
#else
    auto generated = memory_sync::generateWriterKeyPair();
    REQUIRE(generated.has_value());
    const auto victim = testDir_ / "victim.pem";
    {
        std::ofstream output(victim, std::ios::binary | std::ios::trunc);
        REQUIRE(output.good());
        output << generated.value().privateKeyPem;
    }
    fs::permissions(victim, fs::perms::owner_read | fs::perms::owner_write,
                    fs::perm_options::replace);
    const auto publicKey = testDir_ / "identity.pub.pem";
    {
        std::ofstream output(publicKey, std::ios::binary | std::ios::trunc);
        REQUIRE(output.good());
        output << generated.value().publicKeyPem;
    }
    const auto manifest = testDir_ / "writers.json";
    {
        nlohmann::json content{
            {"schema_version", 1},
            {"corpus_id", "identity-security-test"},
            {"corpus_epoch", 1},
            {"local_key",
             {{"writer_id", "123e4567-e89b-42d3-a456-42661417400a"},
              {"key_id", "local-v1"},
              {"private_key_path", victim.string()}}},
            {"trusted_writers",
             nlohmann::json::array({{{"writer_id", "123e4567-e89b-42d3-a456-42661417400a"},
                                     {"key_id", "local-v1"},
                                     {"public_key_path", publicKey.string()}}})}};
        std::ofstream output(manifest, std::ios::binary | std::ios::trunc);
        REQUIRE(output.good());
        output << content.dump();
    }
    const auto identity = testDir_ / "identity.pem";
    fs::create_symlink(victim, identity);

    config_.memorySync.enabled = true;
    config_.memorySync.transport = "direct";
    config_.memorySync.nodeId = "123e4567-e89b-42d3-a456-42661417400a";
    config_.memorySync.corpusId = "identity-security-test";
    config_.memorySync.corpusEpoch = 1;
    config_.memorySync.identityKeyPath = identity.string();
    config_.memorySync.writerAuthRequired = true;
    config_.memorySync.writerAuthManifestPath = manifest.string();

    ServiceManager manager(config_, state_, lifecycleFsm_);
    REQUIRE(manager.initialize().has_value());
    boost::asio::io_context io;
    compat::stop_source stopSource;
    auto future = boost::asio::co_spawn(
        io, manager.initializeAsyncAwaitable(stopSource.get_token()), boost::asio::use_future);
    io.run();
    const auto initialized = future.get();
    REQUIRE_FALSE(initialized.has_value());
    CHECK(initialized.error().code == ErrorCode::Unauthorized);
    CHECK(initialized.error().message.find("symbolic link") != std::string::npos);
    CHECK(fs::is_symlink(fs::symlink_status(identity)));
#endif
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager rejects broadly accessible direct writer private keys",
                 "[daemon][service_manager][p2p][security][platform]") {
    const std::string nodeId = "123e4567-e89b-42d3-a456-42661417400a";
    const std::string corpusId = "writer-key-security";
    const auto manifest = writeProtectedWriterManifest(testDir_, nodeId, corpusId, 1);
    const auto identity = testDir_ / "separate-identity.pem";
    REQUIRE(ServiceManager::__test_loadOrCreateP2pPrivateKey(identity).has_value());
    grantUntrustedKeyAccess(testDir_ / "writer-private.pem", false);

    config_.memorySync.enabled = true;
    config_.memorySync.transport = "direct";
    config_.memorySync.nodeId = nodeId;
    config_.memorySync.corpusId = corpusId;
    config_.memorySync.corpusEpoch = 1;
    config_.memorySync.identityKeyPath = identity.string();
    config_.memorySync.writerAuthRequired = true;
    config_.memorySync.writerAuthManifestPath = manifest.string();

    ServiceManager manager(config_, state_, lifecycleFsm_);
    REQUIRE(manager.initialize().has_value());
    boost::asio::io_context io;
    compat::stop_source stopSource;
    auto future = boost::asio::co_spawn(
        io, manager.initializeAsyncAwaitable(stopSource.get_token()), boost::asio::use_future);
    io.run();
    const auto initialized = future.get();
    REQUIRE_FALSE(initialized.has_value());
    CHECK(initialized.error().code == ErrorCode::Unauthorized);
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager rejects writable direct writer trust files",
                 "[daemon][service_manager][p2p][security][platform]") {
    const std::string nodeId = "123e4567-e89b-42d3-a456-42661417400a";
    const std::string corpusId = "writer-trust-security";
    const auto manifest = writeProtectedWriterManifest(testDir_, nodeId, corpusId, 1);
    const auto identity = testDir_ / "trust-test-identity.pem";
    REQUIRE(ServiceManager::__test_loadOrCreateP2pPrivateKey(identity).has_value());

    SECTION("writable manifest") {
        grantUntrustedKeyAccess(manifest, true);
    }
    SECTION("writable public key") {
        grantUntrustedKeyAccess(testDir_ / "writer-public.pem", true);
    }

    config_.memorySync.enabled = true;
    config_.memorySync.transport = "direct";
    config_.memorySync.nodeId = nodeId;
    config_.memorySync.corpusId = corpusId;
    config_.memorySync.corpusEpoch = 1;
    config_.memorySync.identityKeyPath = identity.string();
    config_.memorySync.writerAuthRequired = true;
    config_.memorySync.writerAuthManifestPath = manifest.string();

    ServiceManager manager(config_, state_, lifecycleFsm_);
    REQUIRE(manager.initialize().has_value());
    boost::asio::io_context io;
    compat::stop_source stopSource;
    auto future = boost::asio::co_spawn(
        io, manager.initializeAsyncAwaitable(stopSource.get_token()), boost::asio::use_future);
    io.run();
    const auto initialized = future.get();
    REQUIRE_FALSE(initialized.has_value());
    CHECK(initialized.error().code == ErrorCode::Unauthorized);
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager creates owner-only P2P identity keys and rejects broad access",
                 "[daemon][service_manager][p2p][security][platform]") {
    const auto identity = testDir_ / "generated-identity.pem";
    auto created = ServiceManager::__test_loadOrCreateP2pPrivateKey(identity);
    REQUIRE(created.has_value());
    REQUIRE_FALSE(created.value().empty());
    REQUIRE(ServiceManager::__test_loadOrCreateP2pPrivateKey(identity).has_value());

    grantUntrustedKeyAccess(identity, false);

    auto rejected = ServiceManager::__test_loadOrCreateP2pPrivateKey(identity);
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == ErrorCode::Unauthorized);
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager rejects inherited Windows P2P identity ACLs",
                 "[daemon][service_manager][p2p][security][platform]") {
#ifndef _WIN32
    SKIP("Windows ACL-specific coverage");
#else
    const auto identity = testDir_ / "inherited-identity.pem";
    REQUIRE(ServiceManager::__test_loadOrCreateP2pPrivateKey(identity).has_value());
    PACL currentAcl = nullptr;
    PSECURITY_DESCRIPTOR descriptor = nullptr;
    REQUIRE(GetNamedSecurityInfoW(const_cast<wchar_t*>(identity.c_str()), SE_FILE_OBJECT,
                                  DACL_SECURITY_INFORMATION, nullptr, nullptr, &currentAcl, nullptr,
                                  &descriptor) == ERROR_SUCCESS);
    REQUIRE(SetNamedSecurityInfoW(const_cast<wchar_t*>(identity.c_str()), SE_FILE_OBJECT,
                                  DACL_SECURITY_INFORMATION | UNPROTECTED_DACL_SECURITY_INFORMATION,
                                  nullptr, nullptr, currentAcl, nullptr) == ERROR_SUCCESS);
    LocalFree(descriptor);
    auto rejected = ServiceManager::__test_loadOrCreateP2pPrivateKey(identity);
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == ErrorCode::Unauthorized);
#endif
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager pool configuration failures propagate through Result",
                 "[daemon][service_manager][startup][error]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);

    const auto result = sm.__test_initializeWithPoolConfiguration(
        [] { throw std::runtime_error("forced pool configuration failure"); });

    REQUIRE_FALSE(result);
    CHECK((result.error().code == ErrorCode::InternalError));
    CHECK((result.error().message.find("forced pool configuration failure") != std::string::npos));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager destructor handles cleanup",
                 "[daemon][service_manager]") {
    auto sm = std::make_unique<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE_NOTHROW(sm.reset());
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager delegates shared plugin host shutdown once",
                 "[daemon][service_manager][plugin][ownership][shutdown]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    auto counters = std::make_shared<PluginHostLifecycleCounters>();
    sm.__test_setAbiHost(std::make_unique<CountingAbiPluginHost>(counters));

    sm.shutdown();

    CHECK((counters->listCalls.load(std::memory_order_relaxed) == 1));
    CHECK((counters->unloadCalls.load(std::memory_order_relaxed) == 1));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager getConfig returns configuration",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);

    const auto& cfg = sm.getConfig();
    REQUIRE((cfg.dataDir == config_.dataDir));
    REQUIRE((cfg.socketPath == config_.socketPath));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager snapshots search rebuild compatibility policy once",
                 "[daemon][service_manager][config][search]") {
    yams::test::ScopedEnvVar disableRebuilds{"YAMS_DISABLE_SEARCH_REBUILDS", std::string{"1"}};

    ServiceManager sm(config_, state_, lifecycleFsm_);
    const auto& policy = sm.getConfig().searchMaintenance;
    REQUIRE(policy.automaticRebuildsEnabled.has_value());
    CHECK_FALSE(*policy.automaticRebuildsEnabled);
    CHECK((policy.automaticRebuildsSource ==
           "compatibility-environment:YAMS_DISABLE_SEARCH_REBUILDS"));
    REQUIRE((sm.getSearchComponent() != nullptr));
    CHECK_FALSE(sm.getSearchComponent()->getConfig().automaticRebuildsEnabled);

    disableRebuilds.set("0");
    CHECK_FALSE(*sm.getConfig().searchMaintenance.automaticRebuildsEnabled);
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "Typed search rebuild policy outranks compatibility environment",
                 "[daemon][service_manager][config][search]") {
    yams::test::ScopedEnvVar disableRebuilds{"YAMS_DISABLE_SEARCH_REBUILDS", std::string{"1"}};
    config_.searchMaintenance.automaticRebuildsEnabled = true;
    config_.searchMaintenance.automaticRebuildsSource = "typed:test";

    ServiceManager sm(config_, state_, lifecycleFsm_);
    const auto& policy = sm.getConfig().searchMaintenance;
    REQUIRE(policy.automaticRebuildsEnabled.has_value());
    CHECK(*policy.automaticRebuildsEnabled);
    CHECK((policy.automaticRebuildsSource == "typed:test"));
    REQUIRE((sm.getSearchComponent() != nullptr));
    CHECK(sm.getSearchComponent()->getConfig().automaticRebuildsEnabled);
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager snapshots runtime tuning provenance across live refresh",
                 "[daemon][service_manager][config][tuning][snapshot]") {
    yams::test::ScopedEnvVar timeout{"YAMS_IPC_TIMEOUT_MS", std::string{"4321"}};

    ServiceManager sm(config_, state_, lifecycleFsm_);
    const auto snapshot = sm.getTuningConfig();
    REQUIRE((sm.getRuntimeTuningStatus().at("ipc.timeout_ms.source") ==
             "compatibility-environment:YAMS_IPC_TIMEOUT_MS"));

    timeout.unset();
    sm.setTuningConfig(snapshot);

    CHECK((sm.getRuntimeTuningStatus().at("ipc.timeout_ms.source") ==
           "compatibility-environment:YAMS_IPC_TIMEOUT_MS"));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "Live tuning refresh reports preserved construction-time capacity",
                 "[daemon][service_manager][config][tuning][physical-state]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;
    config_.tuning.postIngestCapacity = 111;
    config_.tuning.provenance["tuning.post_ingest_capacity"] = "config:tuning.post_ingest_capacity";

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto beforeInitialization = sm->getTuningConfig();
    beforeInitialization.postIngestCapacity = 222;
    beforeInitialization.provenance["tuning.post_ingest_capacity"] =
        "config:tuning.post_ingest_capacity";
    sm->setTuningConfig(beforeInitialization);

    REQUIRE(sm->initialize());
    sm->startAsyncInit();
    const auto ready = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((ready.state == ServiceManagerState::Ready));
    REQUIRE(sm->getPostIngestQueue() != nullptr);
    CHECK((sm->getPostIngestQueue()->capacity() == 222u));

    TuningConfig refreshed;
    refreshed.tuneAdvisorOverridesResolved = true;
    sm->setTuningConfig(refreshed);

    const auto effective = sm->getTuningConfig();
    CHECK((effective.postIngestCapacity == 222u));
    CHECK((effective.provenance.at("tuning.post_ingest_capacity") ==
           "runtime:construction-time-post-ingest-channel"));
    const auto status = sm->getRuntimeTuningStatus();
    CHECK((status.at("post_ingest.capacity") == "222"));
    CHECK((status.at("post_ingest.capacity.source") ==
           "runtime:construction-time-post-ingest-channel"));
    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "Runtime tuning status publishes values from one configured generation",
                 "[daemon][service_manager][config][tuning][snapshot]") {
    config_.tuning.tuneAdvisorOverridesResolved = true;
    TuneAdvisor::setIpcTimeoutMs(1001);
    TuneAdvisor::setMemoryWarningThreshold(0.61);
    ServiceManager sm(config_, state_, lifecycleFsm_);

    std::atomic<bool> stop{false};
    std::thread writer([&] {
        bool alternate = false;
        while (!stop.load(std::memory_order_acquire)) {
            [[maybe_unused]] auto update = TuneAdvisor::beginConfiguredOverrideUpdate();
            TuneAdvisor::setIpcTimeoutMs(alternate ? 1001 : 2002);
            TuneAdvisor::setMemoryWarningThreshold(alternate ? 0.61 : 0.82);
            alternate = !alternate;
        }
    });

    for (int i = 0; i < 500; ++i) {
        sm.setTuningConfig(sm.getTuningConfig());
        const auto status = sm.getRuntimeTuningStatus();
        const auto timeout = status.at("ipc.timeout_ms");
        const auto warning = status.at("resource.memory_warning_threshold");
        CHECK(((timeout == "1001" && warning == "0.610000") ||
               (timeout == "2002" && warning == "0.820000")));
    }

    stop.store(true, std::memory_order_release);
    writer.join();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "Direct embedded ServiceManager starts a fresh tuning lifecycle",
                 "[daemon][service_manager][config][tuning][lifecycle]") {
    yams::test::ScopedEnvVar timeout{"YAMS_IPC_TIMEOUT_MS", std::nullopt};
    TuneAdvisor::setIpcTimeoutMs(4321);
    REQUIRE_FALSE(config_.tuning.tuneAdvisorOverridesResolved);

    ServiceManager sm(config_, state_, lifecycleFsm_);

    CHECK(sm.getTuningConfig().tuneAdvisorOverridesResolved);
    CHECK((sm.getRuntimeTuningStatus().at("ipc.timeout_ms") == "15000"));
}

TEST_CASE("ServiceManager session watcher honors only the test disable gate",
          "[daemon][service_manager][session_watch]") {
    CHECK(ServiceManager::__test_shouldStartSessionWatcher(""));
    CHECK(ServiceManager::__test_shouldStartSessionWatcher("0"));
    CHECK_FALSE(ServiceManager::__test_shouldStartSessionWatcher("true"));
}

TEST_CASE("ServiceManager session watcher uses configured intervals without busy polling",
          "[daemon][service_manager][session_watch]") {
    using namespace std::chrono_literals;

    CHECK((ServiceManager::__test_sessionWatcherDelay(false, 0) == 2s));
    CHECK((ServiceManager::__test_sessionWatcherDelay(true, 250) == 250ms));
    CHECK((ServiceManager::__test_sessionWatcherDelay(true, 20) == 100ms));
}

TEST_CASE("ServiceManager session watcher refreshes every retrieval index",
          "[daemon][service_manager][session_watch]") {
    auto request = ServiceManager::__test_makeSessionWatchRequest("coding", "/workspace/yams",
                                                                  {"src/search/search_engine.cpp"});

    CHECK((request.directoryPath == "/workspace/yams"));
    CHECK((request.includePatterns == std::vector<std::string>{"src/search/search_engine.cpp"}));
    CHECK(request.recursive);
    CHECK((request.sessionId == "coding"));
    CHECK_FALSE(request.noEmbeddings);
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager session watcher retries failed indexing and deletion",
                 "[daemon][service_manager][session_watch]") {
    ServiceManager serviceManager(config_, state_, lifecycleFsm_);
    SessionWatchIndexingService indexingService;
    SessionWatchDocumentService documentService;
    const auto watchedDirectory = testDir_ / "watched";
    const auto watchedFile = watchedDirectory / "changed.cpp";
    fs::create_directories(watchedDirectory);
    {
        std::ofstream output(watchedFile);
        REQUIRE(output.good());
        output << "int changed = 1;\n";
    }

    indexingService.failuresRemaining = 1;
    CHECK_FALSE(serviceManager.__test_scanSessionWatchDirectory(indexingService, &documentService,
                                                                "coding", watchedDirectory));
    REQUIRE((indexingService.requests.size() == 1));
    CHECK((indexingService.requests.front().includePatterns ==
           std::vector<std::string>{"changed.cpp"}));

    CHECK(serviceManager.__test_scanSessionWatchDirectory(indexingService, &documentService,
                                                          "coding", watchedDirectory));
    CHECK((indexingService.requests.size() == 2));

    CHECK(serviceManager.__test_scanSessionWatchDirectory(indexingService, &documentService,
                                                          "coding", watchedDirectory));
    CHECK((indexingService.requests.size() == 2));

    REQUIRE(fs::remove(watchedFile));
    documentService.failuresRemaining = 1;
    CHECK_FALSE(serviceManager.__test_scanSessionWatchDirectory(indexingService, &documentService,
                                                                "coding", watchedDirectory));
    REQUIRE((documentService.deleteRequests.size() == 1));
    CHECK((documentService.deleteRequests.front().name == watchedFile.string()));

    CHECK(serviceManager.__test_scanSessionWatchDirectory(indexingService, &documentService,
                                                          "coding", watchedDirectory));
    CHECK((documentService.deleteRequests.size() == 2));

    CHECK(serviceManager.__test_scanSessionWatchDirectory(indexingService, &documentService,
                                                          "coding", watchedDirectory));
    CHECK((documentService.deleteRequests.size() == 2));
}

TEST_CASE("ServiceManager topology readiness follows artifact freshness",
          "[daemon][service_manager][topology]") {
    yams::search::IndexFreshnessSnapshot snapshot;
    snapshot.topologyStatusKnown = true;
    snapshot.topologyEpoch = 42;
    snapshot.topologyArtifactsFresh = true;

    CHECK(ServiceManager::isTopologyRoutingReady(snapshot));

    snapshot.topologyEpoch = 0;
    CHECK_FALSE(ServiceManager::isTopologyRoutingReady(snapshot));

    snapshot.topologyEpoch = 42;
    snapshot.topologyArtifactsFresh = false;
    CHECK_FALSE(ServiceManager::isTopologyRoutingReady(snapshot));

    snapshot.topologyArtifactsFresh = true;
    snapshot.topologyRebuildRunning = true;
    CHECK_FALSE(ServiceManager::isTopologyRoutingReady(snapshot));

    snapshot.topologyRebuildRunning = false;
    snapshot.postIngestQueued = 1;
    CHECK_FALSE(ServiceManager::isTopologyRoutingReady(snapshot));

    snapshot.postIngestQueued = 0;
    snapshot.topologyDirtyDocuments = 1;
    CHECK_FALSE(ServiceManager::isTopologyRoutingReady(snapshot));

    snapshot.topologyDirtyDocuments = 0;
    snapshot.awaitingDrain = true;
    snapshot.lexicalReady = false;
    snapshot.kgReady = false;
    CHECK(ServiceManager::isTopologyRoutingReady(snapshot));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager PostIngestQueue accessor before init",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);

    // May be null before initialization
    auto piq = sm.getPostIngestQueue();
    (void)piq;
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager tuning config getter doesn't crash",
                 "[daemon][service_manager]") {
    config_.tuning.ingestStoreBatchSize = 19;
    ServiceManager sm(config_, state_, lifecycleFsm_);
    const auto& tuning = sm.getTuningConfig();
    CHECK((tuning.ingestStoreBatchSize == 19));
    CHECK((sm.getIngestStoreBatchSize() == 19));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager set tuning config doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);

    TuningConfig tc;
    tc.postIngestCapacity = 1000;
    tc.postIngestThreadsMin = 2;
    tc.postIngestThreadsMax = 4;
    tc.ingestStoreBatchSize = 23;

    REQUIRE_NOTHROW(sm.setTuningConfig(tc));
    CHECK((sm.getIngestStoreBatchSize() == 23));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager getWorkerQueueDepth doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    auto depth = sm.getWorkerQueueDepth();
    (void)depth;
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager enqueuePostIngest doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    REQUIRE_NOTHROW(sm.enqueuePostIngest("test_hash", "text/plain"));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager search engine snapshot doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    auto snapshot = sm.getSearchEngineFsmSnapshot();
    (void)snapshot.buildReason;
    (void)snapshot.vectorEnabled;
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager cached search engine access doesn't crash",
                 "[daemon][service_manager]") {
    ServiceManager sm(config_, state_, lifecycleFsm_);
    auto engine = sm.getCachedSearchEngine();
    (void)engine; // May be null, that's OK
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager memory cleanup verification",
                 "[daemon][service_manager][.slow]") {
    // Note: This test rapidly creates/destroys ServiceManagers which may be flaky
    // on some platforms due to timing-sensitive cleanup
    for (int i = 0; i < 3; ++i) {
        auto sm = std::make_unique<ServiceManager>(config_, state_, lifecycleFsm_);
        sm.reset();
    }
    SUCCEED();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager restart creates fresh io_context (PBI-066-38)",
                 "[daemon][service_manager]") {
    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);

    // First init
    auto r1 = sm->initialize();
    REQUIRE(r1);

    // Stop services
    sm->shutdown();

    // Second init should succeed and not throw bad executor
    auto r2 = sm->initialize();
    REQUIRE(r2);
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager initializes WAL under configured data directory",
                 "[daemon][service_manager][wal]") {
    const auto runRoot = testDir_ / "cwd";
    fs::create_directories(runRoot);
    ScopedCurrentPath cwdGuard(runRoot);

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);

    const auto expectedWalDir = config_.dataDir / "wal";
    REQUIRE(fs::exists(expectedWalDir));
    REQUIRE(fs::is_directory(expectedWalDir));

    const auto misplacedWalDir = runRoot / "wal";
    CHECK_FALSE(fs::exists(misplacedWalDir));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager async init starts only once per initialize cycle",
                 "[daemon][service_manager][startup][async_init]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);

    std::promise<void> firstBarrierPromise;
    auto firstBarrierFuture = firstBarrierPromise.get_future();
    std::atomic<bool> firstBarrierSet{false};
    sm->startAsyncInit(&firstBarrierPromise, &firstBarrierSet);

    REQUIRE((firstBarrierFuture.wait_for(std::chrono::seconds(2)) == std::future_status::ready));
    CHECK(firstBarrierSet.load(std::memory_order_acquire));

    std::promise<void> secondBarrierPromise;
    auto secondBarrierFuture = secondBarrierPromise.get_future();
    std::atomic<bool> secondBarrierSet{false};
    sm->startAsyncInit(&secondBarrierPromise, &secondBarrierSet);

    CHECK((secondBarrierFuture.wait_for(std::chrono::milliseconds(200)) ==
           std::future_status::timeout));
    CHECK_FALSE(secondBarrierSet.load(std::memory_order_acquire));

    const auto snapshot = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((snapshot.state == ServiceManagerState::Ready));
    requireReadyDatabaseState(state_);

    sm->shutdown();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager consumes clean shutdown proof and republishes it on shutdown",
                 "[daemon][service_manager][startup][integrity_stamp]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    const auto dbPath = metadataDbPath(config_);
    seedMetadataDb(dbPath, "clean_restart");
    const auto initialStamp = publishDbCleanShutdownStamp(dbPath);
    const std::string initialStampInfo =
        initialStamp ? "clean stamp published" : initialStamp.error().message;
    INFO(initialStampInfo);
    REQUIRE(initialStamp);

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    REQUIRE(sm->initialize());
    sm->startAsyncInit();
    const auto ready = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((ready.state == ServiceManagerState::Ready));
    CHECK(state_.readiness.databaseIntegrityFastPath.load(std::memory_order_acquire));

    sm->shutdown();
    const auto nextStartup = consumeDbCleanShutdownStamp(dbPath);
    CHECK(nextStartup.trustedCleanShutdown);
    CHECK(nextStartup.invalidationPersisted);
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager records recovery provenance for corrupt metadata DB startup",
                 "[daemon][service_manager][startup][recovery]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    const auto dbPath = metadataDbPath(config_);
    writeCorruptDb(dbPath);
    seedDummyWalSidecar(dbPath, "corrupt-sidecar");

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    const auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((smSnap.state == ServiceManagerState::Ready));

    requireReadyDatabaseState(state_);
    std::optional<fs::path> quarantinedPath;
    {
        std::lock_guard<std::mutex> lk(state_.readiness.recoveryMutex);
        REQUIRE_FALSE(state_.readiness.databaseRecoveredFrom.empty());
        quarantinedPath = findQuarantinedFile(config_.dataDir);
        REQUIRE(quarantinedPath.has_value());
        CHECK((state_.readiness.databaseRecoveredFrom == quarantinedPath->string()));
    }
    CHECK(fs::exists(*quarantinedPath));
    CHECK_FALSE(walSidecarStillMatchesPayload(dbPath, "corrupt-sidecar"));

    sm->shutdown();
    CHECK(walSidecarCleared(dbPath));
}

TEST_CASE_METHOD(ServiceManagerFixture, "ServiceManager clears stale metadata WAL during startup",
                 "[daemon][service_manager][wal][startup]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    const auto dbPath = metadataDbPath(config_);
    seedMetadataDb(dbPath, "stale_wal_seed");
    REQUIRE(publishDbCleanShutdownStamp(dbPath));
    const std::string staleWalPayload = "stale-wal";
    seedDummyWalSidecar(dbPath, staleWalPayload);

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    auto smSnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((smSnap.state == ServiceManagerState::Ready));

    requireReadyDatabaseState(state_);
    CHECK_FALSE(state_.readiness.databaseIntegrityFastPath.load(std::memory_order_acquire));
    {
        std::lock_guard<std::mutex> lk(state_.readiness.recoveryMutex);
        CHECK(state_.readiness.databaseRecoveredFrom.empty());
    }
    CHECK_FALSE(walSidecarStillMatchesPayload(dbPath, staleWalPayload));
    CHECK((startupProbeRowCount(dbPath) == 1U));

    sm->shutdown();
    CHECK(walSidecarCleared(dbPath));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager shutdown remains idempotent after async init reaches ready",
                 "[daemon][service_manager][shutdown][idempotent]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    const auto readySnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((readySnap.state == ServiceManagerState::Ready));

    sm->shutdown();
    const auto firstShutdown = sm->getServiceManagerFsmSnapshot();
    CHECK((firstShutdown.state == ServiceManagerState::Stopped));
    CHECK((sm->__test_getAbiHost() == nullptr));

    REQUIRE_NOTHROW(sm->shutdown());
    const auto secondShutdown = sm->getServiceManagerFsmSnapshot();
    CHECK((secondShutdown.state == ServiceManagerState::Stopped));
    CHECK((sm->getMetadataRepo() == nullptr));
    CHECK((sm->getContentStore() == nullptr));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager shutdown waits for an in-flight worker task",
                 "[daemon][service_manager][shutdown][regression]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;
    config_.enableAutoRepair = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    const auto readySnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((readySnap.state == ServiceManagerState::Ready));

    std::mutex gateMutex;
    std::condition_variable gateCv;
    bool allowWorkerExit = false;
    std::atomic<bool> workerEntered{false};
    std::atomic<bool> workerFinished{false};
    std::atomic<bool> shutdownReturned{false};

    boost::asio::post(sm->getWorkerExecutor(), [&]() {
        {
            std::lock_guard<std::mutex> lk(gateMutex);
            workerEntered.store(true, std::memory_order_release);
        }
        gateCv.notify_all();

        std::unique_lock<std::mutex> lk(gateMutex);
        gateCv.wait(lk, [&] { return allowWorkerExit; });
        workerFinished.store(true, std::memory_order_release);
    });

    {
        std::unique_lock<std::mutex> lk(gateMutex);
        REQUIRE(gateCv.wait_for(lk, std::chrono::seconds(5),
                                [&] { return workerEntered.load(std::memory_order_acquire); }));
    }

    std::thread shutdownThread([&]() {
        sm->shutdown();
        shutdownReturned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    CHECK_FALSE(shutdownReturned.load(std::memory_order_acquire));
    CHECK_FALSE(workerFinished.load(std::memory_order_acquire));

    {
        std::lock_guard<std::mutex> lk(gateMutex);
        allowWorkerExit = true;
    }
    gateCv.notify_all();

    {
        std::unique_lock<std::mutex> lk(gateMutex);
        REQUIRE(gateCv.wait_for(lk, std::chrono::seconds(5),
                                [&] { return workerFinished.load(std::memory_order_acquire); }));
    }

    shutdownThread.join();
    CHECK(shutdownReturned.load(std::memory_order_acquire));
    CHECK((sm->getServiceManagerFsmSnapshot().state == ServiceManagerState::Stopped));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager shutdown remains bounded during auto-repair graph health probe",
                 "[daemon][service_manager][shutdown][auto_repair][regression]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;
    config_.enableAutoRepair = false;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    const auto readySnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((readySnap.state == ServiceManagerState::Ready));

    auto writePool = sm->getWriteConnectionPool();
    REQUIRE((writePool != nullptr));

    std::vector<std::unique_ptr<metadata::PooledConnection>> heldConnections;
    heldConnections.reserve(32);
    for (std::size_t i = 0; i < 128; ++i) {
        auto conn = writePool->acquire(std::chrono::milliseconds(0));
        if (!conn) {
            break;
        }
        heldConnections.emplace_back();
        std::swap(heldConnections.back(), conn.value());
    }
    REQUIRE_FALSE(heldConnections.empty());
    REQUIRE_FALSE(writePool->acquire(std::chrono::milliseconds(0)).has_value());

    std::atomic<bool> probeStarted{false};
    std::atomic<bool> probeFinished{false};
    std::atomic<bool> shutdownReturned{false};

    boost::asio::post(sm->getWorkerExecutor(), [sm, &probeStarted, &probeFinished]() {
        probeStarted.store(true, std::memory_order_release);
        repair::RepairHealthProbe probe(sm->getMetadataRepo(), sm->getVectorDatabase(),
                                        sm->getKgStore());
        repair::RepairHealthOptions opts;
        opts.checkFts5 = false;
        opts.checkEmbeddings = false;
        opts.checkGraph = true;
        opts.scanDocuments = false;
        (void)probe.probe(opts);
        probeFinished.store(true, std::memory_order_release);
    });

    REQUIRE(yams::test::wait_for_condition(
        std::chrono::seconds(5), std::chrono::milliseconds(5),
        [&]() { return probeStarted.load(std::memory_order_acquire); }));

    std::thread shutdownThread([&]() {
        sm->shutdown();
        shutdownReturned.store(true, std::memory_order_release);
    });

    REQUIRE(yams::test::wait_for_condition(
        std::chrono::seconds(5), std::chrono::milliseconds(5), [&]() {
            return shutdownReturned.load(std::memory_order_acquire) &&
                   probeFinished.load(std::memory_order_acquire);
        }));

    shutdownThread.join();
    CHECK(shutdownReturned.load(std::memory_order_acquire));
    CHECK(probeFinished.load(std::memory_order_acquire));
    CHECK((sm->getServiceManagerFsmSnapshot().state == ServiceManagerState::Stopped));

    heldConnections.clear();
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager shutdown cancels salvage auto-repair with full embed queue",
                 "[daemon][service_manager][shutdown][auto_repair][cancellation][regression]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableVectorDb("YAMS_DISABLE_VECTOR_DB",
                                             std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar autoInitialDelay("YAMS_REPAIR_AUTO_INITIAL_DELAY_MIN",
                                              std::optional<std::string>{"0"});
    yams::test::ScopedEnvVar autoFastInterval("YAMS_REPAIR_AUTO_FAST_MIN",
                                              std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar autoWarmInterval("YAMS_REPAIR_AUTO_WARM_HOURS",
                                              std::optional<std::string>{"0"});
    yams::test::ScopedEnvVar autoColdInterval("YAMS_REPAIR_AUTO_COLD_HOURS",
                                              std::optional<std::string>{"0"});
    yams::test::ScopedEnvVar embedChannelCapacity("YAMS_EMBED_CHANNEL_CAPACITY",
                                                  std::optional<std::string>{"256"});

    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;
    config_.enableAutoRepair = true;
    config_.autoRepairBatchSize = 1;

    auto sm = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
    auto init = sm->initialize();
    REQUIRE(init);
    sm->startAsyncInit();
    const auto readySnap = sm->waitForServiceManagerTerminalState(30);
    REQUIRE((readySnap.state == ServiceManagerState::Ready));

    auto meta = sm->getMetadataRepo();
    REQUIRE((meta != nullptr));

    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    metadata::DocumentInfo doc{};
    doc.fileName = "salvaged-auto-repair.txt";
    doc.filePath = (config_.dataDir / doc.fileName).string();
    doc.fileExtension = "txt";
    doc.fileSize = 26;
    doc.sha256Hash = std::string(64, 'a');
    doc.mimeType = "text/plain";
    doc.modifiedTime = now;
    doc.indexedTime = now;

    auto docId = meta->insertDocument(doc);
    REQUIRE(docId.has_value());

    metadata::DocumentContent content{};
    content.documentId = docId.value();
    content.contentText = "salvage auto repair content";
    content.contentLength = static_cast<int64_t>(content.contentText.size());
    content.extractionMethod = "test";
    content.language = "en";
    REQUIRE(meta->insertContent(content).has_value());
    REQUIRE(meta->updateDocumentExtractionStatus(docId.value(), true,
                                                 metadata::ExtractionStatus::Success)
                .has_value());

    {
        std::lock_guard<std::mutex> lk(state_.readiness.recoveryMutex);
        state_.readiness.databaseRecoveredFrom = "salvaged-auto-repair";
    }

    auto embedChannel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>("embed_jobs",
                                                                                       256);
    REQUIRE((embedChannel != nullptr));
    InternalEventBus::EmbedJob drained;
    while (embedChannel->try_pop(drained)) {
    }

    InternalEventBus::EmbedJob filler;
    filler.hashes = {std::string(64, 'f')};
    filler.batchSize = 1;
    filler.skipExisting = true;
    std::size_t filled = 0;
    while (embedChannel->try_push(filler)) {
        ++filled;
    }
    REQUIRE((filled > 0));

    sm->startRepairService([]() -> size_t { return 0; });
    auto repairService = sm->getRepairServiceShared();
    REQUIRE((repairService != nullptr));
    sm->startBackgroundTasks();

    REQUIRE(yams::test::wait_for_condition(std::chrono::seconds(5), std::chrono::milliseconds(10),
                                           [&]() { return repairService->isRepairInProgress(); }));

    std::atomic<bool> shutdownReturned{false};
    const auto shutdownStart = std::chrono::steady_clock::now();
    std::thread shutdownThread([&]() {
        sm->shutdown();
        shutdownReturned.store(true, std::memory_order_release);
    });

    REQUIRE(yams::test::wait_for_condition(
        std::chrono::seconds(3), std::chrono::milliseconds(10),
        [&]() { return shutdownReturned.load(std::memory_order_acquire); }));

    const auto shutdownElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - shutdownStart);
    shutdownThread.join();

    CHECK((shutdownElapsed < std::chrono::seconds(3)));
    CHECK((sm->getServiceManagerFsmSnapshot().state == ServiceManagerState::Stopped));
    CHECK_FALSE(state_.stats.repairInProgress.load(std::memory_order_acquire));
}

TEST_CASE_METHOD(ServiceManagerFixture,
                 "ServiceManager restart cycles recover freshly seeded stale WAL",
                 "[daemon][service_manager][wal][restart]") {
    config_.enableModelProvider = false;
    config_.useMockModelProvider = false;
    config_.autoLoadPlugins = false;

    const auto dbPath = metadataDbPath(config_);
    seedMetadataDb(dbPath, "restart_cycle_seed");

    for (int cycle = 0; cycle < 3; ++cycle) {
        StateComponent cycleState;
        DaemonLifecycleFsm cycleLifecycle;
        const std::string staleWalPayload = "wal-cycle-" + std::to_string(cycle);
        seedDummyWalSidecar(dbPath, staleWalPayload);

        auto sm = std::make_shared<ServiceManager>(config_, cycleState, cycleLifecycle);
        auto init = sm->initialize();
        REQUIRE(init);
        sm->startAsyncInit();
        auto smSnap = sm->waitForServiceManagerTerminalState(30);
        REQUIRE((smSnap.state == ServiceManagerState::Ready));

        INFO("cycle=" << cycle);
        requireReadyDatabaseState(cycleState);
        CHECK_FALSE(walSidecarStillMatchesPayload(dbPath, staleWalPayload));
        CHECK((startupProbeRowCount(dbPath) == 1U));

        sm->shutdown();
        CHECK(walSidecarCleared(dbPath));
    }
}

TEST_CASE("ServiceManager auto-vacuum requires material reclaimable pages or file tail",
          "[daemon][service_manager][vacuum]") {
    constexpr std::uint64_t kPageSize = 4096;
    constexpr std::uint64_t kLargeDatabaseBytes = 13ULL * 1024 * 1024 * 1024;

    CHECK_FALSE(
        ServiceManager::__test_shouldAutoVacuum(kLargeDatabaseBytes, 3'405'384, 182, kPageSize));
    CHECK_FALSE(
        ServiceManager::__test_shouldAutoVacuum(256ULL * 1024 * 1024, 65'536, 20'000, kPageSize));
    CHECK(ServiceManager::__test_shouldAutoVacuum(2ULL * 1024 * 1024 * 1024, 524'288, 131'072,
                                                  kPageSize));
    CHECK(ServiceManager::__test_shouldAutoVacuum(600ULL * 1024 * 1024, 8, 0, kPageSize));
}

} // namespace yams::daemon::test
