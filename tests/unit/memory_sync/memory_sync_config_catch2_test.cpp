// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include <yams/memory_sync/memory_sync_config.h>
#include <yams/memory_sync/memory_sync_service.h>

using namespace yams::memory_sync;

// Catch2 assertion decomposition intentionally resembles chained comparisons to clang-tidy.
// Standalone scanners use the no-op compatibility macros; Meson uses real Catch2.
// NOLINTBEGIN(bugprone-chained-comparison)

namespace {

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> out(text.size());
    std::memcpy(out.data(), text.data(), text.size());
    return out;
}

std::string text(const std::vector<std::byte>& data) {
    return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

void writeWriterKeyPair(const std::filesystem::path& directory, std::string_view name) {
    auto generated = generateWriterKeyPair();
    REQUIRE(generated.has_value());
    std::ofstream privateKey(directory / (std::string(name) + ".pem"), std::ios::binary);
    std::ofstream publicKey(directory / (std::string(name) + ".pub"), std::ios::binary);
    REQUIRE(privateKey.good());
    REQUIRE(publicKey.good());
    privateKey << generated.value().privateKeyPem;
    publicKey << generated.value().publicKeyPem;
    REQUIRE(privateKey.good());
    REQUIRE(publicKey.good());
}

} // namespace

TEST_CASE("MemorySyncDaemonConfig parses the [memory_sync] TOML section", "[memory-sync][config]") {
    const std::map<std::string, std::string> flat = {
        {"memory_sync.enabled", "true"},
        {"memory_sync.node_id", "123e4567-e89b-42d3-a456-426614174000"},
        {"memory_sync.corpus_id", "corpus-a"},
        {"memory_sync.corpus_epoch", "9"},
        {"memory_sync.max_index_objects_per_sync", "17"},
        {"memory_sync.max_envelope_bytes", "2048"},
        {"memory_sync.max_value_bytes", "4096"},
        {"memory_sync.max_merged_keys", "23"},
        {"memory_sync.max_cache_bytes", "8192"},
        {"memory_sync.max_tracked_identities", "31"},
        {"memory_sync.backend", "filesystem"},
        {"memory_sync.path", "/tmp/mem"},
        {"memory_sync.sync_interval_ms", "250"},
        {"memory_sync.temporary_session_ttl_ms", "900"},
        {"memory_sync.allow_legacy_unbound", "true"},
        {"memory_sync.writer_auth_required", "true"},
        {"memory_sync.writer_auth_manifest", "/secure/writers.json"},
    };
    const auto cfg = MemorySyncDaemonConfig::fromToml(flat);
    CHECK(cfg.enabled);
    CHECK(cfg.nodeId == "123e4567-e89b-42d3-a456-426614174000");
    CHECK(cfg.corpusId == "corpus-a");
    CHECK(cfg.corpusEpoch == 9);
    CHECK(cfg.limits.maxIndexObjectsPerSync == 17);
    CHECK(cfg.limits.maxEnvelopeBytes == 2048);
    CHECK(cfg.limits.maxValueBytes == 4096);
    CHECK(cfg.limits.maxMergedKeys == 23);
    CHECK(cfg.limits.maxCacheBytes == 8192);
    CHECK(cfg.limits.maxTrackedIdentities == 31);
    CHECK(cfg.backend == "filesystem");
    CHECK(cfg.path == "/tmp/mem");
    CHECK(cfg.syncIntervalMs == 250);
    CHECK(cfg.temporarySessionTtlMs == 900);
    CHECK(cfg.mode == "persistent");
    CHECK(cfg.allowLegacyUnbound);
    CHECK(cfg.writerAuthRequired);
    CHECK(cfg.writerAuthManifestPath == "/secure/writers.json");
}

TEST_CASE("MemorySyncDaemonConfig defaults to disabled", "[memory-sync][config]") {
    const auto cfg = MemorySyncDaemonConfig::fromToml({});
    CHECK_FALSE(cfg.enabled);
    CHECK(cfg.backend == "filesystem");
    CHECK(cfg.syncIntervalMs == 5000);
}

TEST_CASE("memory sync rejects personal or unknown corpus scope before opening a backend",
          "[memory-sync][config][sharing]") {
    for (const std::string scope : {"", "personal", "unknown"}) {
        const auto path =
            std::filesystem::temp_directory_path() /
            ("yams-personal-scope-" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::map<std::string, std::string> flat = {
            {"memory_sync.enabled", "true"},
            {"memory_sync.node_id", "123e4567-e89b-42d3-a456-426614174000"},
            {"memory_sync.corpus_id", "personal-corpus"},
            {"memory_sync.corpus_epoch", "1"},
            {"memory_sync.path", path.string()},
        };
        if (!scope.empty()) {
            flat["memory_sync.corpus_scope"] = scope;
        }
        auto service = createMemorySyncService(MemorySyncDaemonConfig::fromToml(flat));
        CHECK_FALSE(service.has_value());
        CHECK_FALSE(std::filesystem::exists(path));
        service = yams::Error{yams::ErrorCode::InvalidState, "test cleanup"};
        std::filesystem::remove_all(path);
    }
}

TEST_CASE("createMemorySyncService rejects omitted or placeholder writer identity",
          "[memory-sync][config][identity]") {
    for (const std::string nodeId : {"", "default"}) {
        MemorySyncDaemonConfig cfg;
        cfg.enabled = true;
        cfg.corpusScope = CorpusScope::Shared;
        cfg.nodeId = nodeId;
        cfg.corpusId = "corpus-a";
        cfg.corpusEpoch = 1;
        cfg.backend = "filesystem";
        cfg.path = std::filesystem::temp_directory_path().string();

        const auto svc = createMemorySyncService(cfg);
        REQUIRE_FALSE(svc.has_value());
        CHECK(svc.error().code == yams::ErrorCode::InvalidArgument);
    }
}

TEST_CASE("createMemorySyncService requires UUID writer and corpus identity",
          "[memory-sync][config][identity]") {
    MemorySyncDaemonConfig cfg;
    cfg.enabled = true;
    cfg.corpusScope = CorpusScope::Shared;
    cfg.nodeId = "node-a";
    cfg.backend = "filesystem";
    cfg.path = std::filesystem::temp_directory_path().string();

    CHECK_FALSE(createMemorySyncService(cfg).has_value());

    cfg.nodeId = "123e4567-e89b-42d3-a456-426614174000";
    CHECK_FALSE(createMemorySyncService(cfg).has_value());

    cfg.corpusId = "corpus-a";
    CHECK_FALSE(createMemorySyncService(cfg).has_value());

    cfg.corpusEpoch = 1;
    CHECK(createMemorySyncService(cfg).has_value());
}

TEST_CASE("authenticated writer manifest produces schema-v4 envelopes",
          "[memory-sync][config][auth]") {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("yams-memory-sync-auth-config-" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);
    writeWriterKeyPair(dir, "writer-v1");

    constexpr std::string_view writerId = "123e4567-e89b-42d3-a456-426614174000";
    const nlohmann::json manifest = {
        {"schema_version", 1},
        {"corpus_id", "corpus-a"},
        {"corpus_epoch", 7},
        {"local_key",
         {{"writer_id", writerId}, {"key_id", "writer-v1"}, {"private_key_path", "writer-v1.pem"}}},
        {"trusted_writers",
         {{{"writer_id", writerId},
           {"key_id", "writer-v1"},
           {"public_key_path", "writer-v1.pub"},
           {"revoked", false}}}},
    };
    const auto manifestPath = dir / "writers.json";
    {
        std::ofstream output(manifestPath);
        REQUIRE(output.good());
        output << manifest.dump(2);
    }

    MemorySyncDaemonConfig cfg;
    cfg.enabled = true;
    cfg.corpusScope = CorpusScope::Shared;
    cfg.nodeId = std::string(writerId);
    cfg.corpusId = "corpus-a";
    cfg.corpusEpoch = 7;
    cfg.path = (dir / "store").string();
    cfg.writerAuthRequired = true;
    cfg.writerAuthManifestPath = manifestPath.string();
    auto service = createMemorySyncService(cfg);
    REQUIRE(service.has_value());
    REQUIRE(service.value()->publish("signed", bytes("payload")).has_value());

    yams::storage::FilesystemBackend inspect;
    yams::storage::BackendConfig inspectConfig;
    inspectConfig.type = "filesystem";
    inspectConfig.localPath = cfg.path;
    REQUIRE(inspect.initialize(inspectConfig).has_value());
    const auto keys = inspect.list("index/signed/");
    REQUIRE(keys.has_value());
    REQUIRE(keys.value().size() == 1);
    const auto encoded = inspect.retrieve(keys.value().front());
    REQUIRE(encoded.has_value());
    const auto envelope = nlohmann::json::parse(text(encoded.value()));
    CHECK(envelope.at("schemaVersion") == kAuthenticatedMemoryIndexSchemaVersion);
    CHECK(envelope.at("signatureAlgorithm").get<std::string>() ==
          std::string(kMemoryIndexSignatureAlgorithm));
    CHECK(envelope.at("signingKeyId") == "writer-v1");
    CHECK(envelope.at("signature").get<std::string>().size() == 128);

    service.value().reset();
    const auto checkpoints = inspect.list("checkpoint/");
    REQUIRE(checkpoints.has_value());
    REQUIRE(checkpoints.value().size() == 1);
    auto checkpoint = inspect.retrieve(checkpoints.value().front());
    REQUIRE(checkpoint.has_value());
    auto forged = nlohmann::json::parse(text(checkpoint.value()));
    forged["commitments"] = nlohmann::json::array();
    REQUIRE(inspect.remove(checkpoints.value().front()).has_value());
    REQUIRE(inspect.store(checkpoints.value().front(), bytes(forged.dump())).has_value());
    auto rejected = createMemorySyncService(cfg);
    REQUIRE(rejected.has_value());
    auto recovered = rejected.value()->syncFully();
    REQUIRE_FALSE(recovered.has_value());
    CHECK(recovered.error().code == yams::ErrorCode::Unauthorized);

    rejected.value().reset();
    std::error_code error;
    std::filesystem::remove_all(dir, error);
}

TEST_CASE("authenticated writer recovery reports legacy unsigned history",
          "[memory-sync][config][auth][migration]") {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("yams-memory-sync-auth-migration-" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);
    writeWriterKeyPair(dir, "writer-v1");
    constexpr std::string_view writerId = "123e4567-e89b-42d3-a456-426614174000";

    MemorySyncDaemonConfig unsignedConfig;
    unsignedConfig.enabled = true;
    unsignedConfig.corpusScope = CorpusScope::Shared;
    unsignedConfig.nodeId = std::string(writerId);
    unsignedConfig.corpusId = "corpus-a";
    unsignedConfig.corpusEpoch = 7;
    unsignedConfig.path = (dir / "store").string();
    auto unsignedService = createMemorySyncService(unsignedConfig);
    REQUIRE(unsignedService.has_value());
    REQUIRE(unsignedService.value()->publish("legacy", bytes("payload")).has_value());
    unsignedService.value().reset();

    yams::storage::FilesystemBackend inspect;
    yams::storage::BackendConfig inspectConfig;
    inspectConfig.type = "filesystem";
    inspectConfig.localPath = unsignedConfig.path;
    REQUIRE(inspect.initialize(inspectConfig).has_value());
    auto checkpoints = inspect.list("checkpoint/");
    REQUIRE(checkpoints.has_value());
    REQUIRE_FALSE(checkpoints.value().empty());
    const auto checkpoint = inspect.retrieve(checkpoints.value().front());
    REQUIRE(checkpoint.has_value());
    auto forgedCheckpoint = nlohmann::json::parse(text(checkpoint.value()));
    forgedCheckpoint["authenticated_writers"] = true;
    REQUIRE(inspect.remove(checkpoints.value().front()).has_value());
    REQUIRE(inspect.store(checkpoints.value().front(), bytes(forgedCheckpoint.dump())).has_value());

    const nlohmann::json manifest = {
        {"schema_version", 1},
        {"corpus_id", "corpus-a"},
        {"corpus_epoch", 7},
        {"local_key",
         {{"writer_id", writerId}, {"key_id", "writer-v1"}, {"private_key_path", "writer-v1.pem"}}},
        {"trusted_writers",
         {{{"writer_id", writerId},
           {"key_id", "writer-v1"},
           {"public_key_path", "writer-v1.pub"},
           {"revoked", false}}}},
    };
    const auto manifestPath = dir / "writers.json";
    std::ofstream(manifestPath) << manifest.dump();

    auto authenticatedConfig = unsignedConfig;
    authenticatedConfig.writerAuthRequired = true;
    authenticatedConfig.writerAuthManifestPath = manifestPath.string();
    auto authenticatedService = createMemorySyncService(authenticatedConfig);
    REQUIRE(authenticatedService.has_value());
    const auto recovered = authenticatedService.value()->syncFully();
    REQUIRE_FALSE(recovered.has_value());
    CHECK(recovered.error().code == yams::ErrorCode::InvalidData);
    CHECK_FALSE(authenticatedService.value()->directP2pReady(writerId, "corpus-a", 7));

    for (const auto& key : checkpoints.value()) {
        REQUIRE(inspect.remove(key).has_value());
    }
    REQUIRE(authenticatedService.value()->syncFully().has_value());
    CHECK(authenticatedService.value()->legacyUnauthenticatedHistoryObserved());
    CHECK_FALSE(authenticatedService.value()->directP2pReady(writerId, "corpus-a", 7));
    CHECK_FALSE(authenticatedService.value()->readCached("legacy").has_value());

    authenticatedService.value().reset();
    std::error_code error;
    std::filesystem::remove_all(dir, error);
}

TEST_CASE("authenticated writer mode rejects legacy migration and manifest mismatch",
          "[memory-sync][config][auth][migration]") {
    MemorySyncDaemonConfig cfg;
    cfg.enabled = true;
    cfg.corpusScope = CorpusScope::Shared;
    cfg.nodeId = "123e4567-e89b-42d3-a456-426614174000";
    cfg.corpusId = "corpus-a";
    cfg.corpusEpoch = 1;
    cfg.path = std::filesystem::temp_directory_path().string();
    cfg.writerAuthRequired = true;
    cfg.allowLegacyUnbound = true;
    cfg.writerAuthManifestPath = "does-not-matter.json";
    const auto incompatible = createMemorySyncService(cfg);
    REQUIRE_FALSE(incompatible.has_value());
    CHECK(incompatible.error().code == yams::ErrorCode::InvalidArgument);
}

TEST_CASE("createMemorySyncService rejects disabled config", "[memory-sync][config]") {
    MemorySyncDaemonConfig cfg;
    cfg.enabled = false;
    const auto svc = createMemorySyncService(cfg);
    CHECK_FALSE(svc.has_value());
}

TEST_CASE("createMemorySyncService rejects unknown backend", "[memory-sync][config]") {
    MemorySyncDaemonConfig cfg;
    cfg.enabled = true;
    cfg.corpusScope = CorpusScope::Shared;
    cfg.backend = "ftp";
    cfg.path = "ftp://example.com";
    const auto svc = createMemorySyncService(cfg);
    CHECK_FALSE(svc.has_value());
}

TEST_CASE("memory sync S3 target overrides primary bucket while retaining credentials",
          "[memory-sync][config][s3]") {
    MemorySyncDaemonConfig cfg;
    cfg.backend = "s3";
    cfg.path = "s3://memory-sync-bucket/corpus";
    yams::storage::BackendConfig primary;
    primary.type = "s3";
    primary.url = "s3://primary-storage-bucket/content";
    primary.region = "auto";
    primary.credentials["access_key"] = "test-access";

    const auto resolved = resolveMemorySyncS3Config(cfg, primary);
    REQUIRE(resolved.has_value());
    CHECK(resolved.value().url == cfg.path);
    CHECK(resolved.value().url != primary.url);
    CHECK(resolved.value().credentials == primary.credentials);
    CHECK(resolved.value().region == primary.region);
}

TEST_CASE("createMemorySyncService rejects s3 with a non-s3 URL", "[memory-sync][config]") {
    MemorySyncDaemonConfig cfg;
    cfg.enabled = true;
    cfg.corpusScope = CorpusScope::Shared;
    cfg.backend = "s3";
    cfg.path = "/not/an/s3/url";
    const auto svc = createMemorySyncService(cfg);
    CHECK_FALSE(svc.has_value());
}

TEST_CASE("temporary memory sync owns and cleans only its session namespace",
          "[memory-sync][config][tombstone][temporary]") {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("yams-memory-sync-temporary-" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);

    MemorySyncDaemonConfig persistent;
    persistent.enabled = true;
    persistent.corpusScope = CorpusScope::Shared;
    persistent.nodeId = "123e4567-e89b-42d3-a456-426614174000";
    persistent.corpusId = "corpus-a";
    persistent.corpusEpoch = 1;
    persistent.path = dir.string();
    auto persistentService = createMemorySyncService(persistent);
    REQUIRE(persistentService.has_value());
    REQUIRE(persistentService.value()->publish("persistent", bytes("keep")).has_value());

    MemorySyncDaemonConfig temporary = persistent;
    temporary.nodeId = "123e4567-e89b-42d3-a456-426614174001";
    temporary.mode = "temporary";
    temporary.sessionId = "run-123";
    auto temporaryService = createMemorySyncService(temporary);
    REQUIRE(temporaryService.has_value());
    REQUIRE(temporaryService.value()->publish("ephemeral", bytes("discard")).has_value());
    REQUIRE(temporaryService.value()->erase("ephemeral").has_value());
    temporaryService.value()->stop();

    yams::storage::BackendConfig inspectConfig;
    inspectConfig.type = "filesystem";
    inspectConfig.localPath = dir;
    yams::storage::FilesystemBackend inspect;
    REQUIRE(inspect.initialize(inspectConfig).has_value());
    const auto persistentKeys = inspect.list("index/persistent/");
    REQUIRE(persistentKeys.has_value());
    CHECK_FALSE(persistentKeys.value().empty());
    const auto temporaryKeys = inspect.list(".sessions/run-123/");
    REQUIRE(temporaryKeys.has_value());
    const bool temporaryNamespaceIsEmpty = temporaryKeys.value().empty();
    CHECK(temporaryNamespaceIsEmpty);

    persistentService.value()->stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST_CASE("temporary memory sync collects bounded stale session leases",
          "[memory-sync][config][temporary][expiry]") {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("yams-memory-sync-expiry-" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);
    yams::storage::BackendConfig inspectConfig;
    inspectConfig.type = "filesystem";
    inspectConfig.localPath = dir;
    yams::storage::FilesystemBackend inspect;
    REQUIRE(inspect.initialize(inspectConfig).has_value());
    REQUIRE(inspect.store(".sessions/stale-session/orphan", bytes("orphan")).has_value());
    REQUIRE(inspect.store(".session-leases/stale-session", bytes("1")).has_value());
    REQUIRE(inspect.store("persistent-marker", bytes("keep")).has_value());

    MemorySyncDaemonConfig temporary;
    temporary.enabled = true;
    temporary.corpusScope = CorpusScope::Shared;
    temporary.nodeId = "123e4567-e89b-42d3-a456-426614174001";
    temporary.corpusId = "corpus-a";
    temporary.corpusEpoch = 1;
    temporary.path = dir.string();
    temporary.syncIntervalMs = 25;
    temporary.mode = "temporary";
    temporary.sessionId = "current-session";
    temporary.temporarySessionTtlMs = 75;
    auto service = createMemorySyncService(temporary);
    REQUIRE(service.has_value());
    REQUIRE(service.value()->start().has_value());

    REQUIRE(inspect.exists(".sessions/stale-session/orphan").has_value());
    CHECK_FALSE(inspect.exists(".sessions/stale-session/orphan").value());
    REQUIRE(inspect.exists(".session-leases/stale-session").has_value());
    CHECK_FALSE(inspect.exists(".session-leases/stale-session").value());
    REQUIRE(inspect.exists(".session-leases/current-session").has_value());
    CHECK(inspect.exists(".session-leases/current-session").value());
    REQUIRE(inspect.exists("persistent-marker").has_value());
    CHECK(inspect.exists("persistent-marker").value());

    service.value()->stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST_CASE("createMemorySyncService builds a working filesystem service", "[memory-sync][config]") {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("yams-memory-sync-config-" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);

    MemorySyncDaemonConfig cfg;
    cfg.enabled = true;
    cfg.corpusScope = CorpusScope::Shared;
    cfg.nodeId = "123e4567-e89b-42d3-a456-426614174000";
    cfg.corpusId = "corpus-a";
    cfg.corpusEpoch = 1;
    cfg.backend = "filesystem";
    cfg.path = dir.string();
    cfg.syncIntervalMs = 50;

    auto svc = createMemorySyncService(cfg);
    REQUIRE(svc.has_value());

    REQUIRE(svc.value()->publish("key", bytes("value")).has_value());
    const auto value = svc.value()->read("key");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "value");

    svc.value()->stop();

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

// NOLINTEND(bugprone-chained-comparison)
