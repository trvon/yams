// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <set>
#include <span>
#include <sstream>
#include <string>
#include <vector>

#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include <yams/memory_sync/memory_sync.h>
#include <yams/memory_sync/writer_auth.h>
#include <yams/storage/storage_backend.h>

using namespace yams::memory_sync;

namespace {

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> out(text.size());
    std::memcpy(out.data(), text.data(), text.size());
    return out;
}

std::string text(const std::vector<std::byte>& data) {
    return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

std::string digest(std::span<const std::byte> data) {
    yams::crypto::SHA256Hasher hasher;
    hasher.init();
    hasher.update(data);
    return hasher.finalize();
}

// The root coordination lease must persist: unlinking it could let another allocator
// lock a different inode while a live caller still holds the old one. Everything else
// must be a generation directory; never hide an unexpected file from cleanup assertions.
std::vector<std::filesystem::path> scratchGenerations(const std::filesystem::path& root) {
    std::vector<std::filesystem::path> generations;
    bool allocationLease = false;
    for (const auto& entry : std::filesystem::directory_iterator(root)) {
        const auto name = entry.path().filename().string();
        const auto status = entry.symlink_status();
        if (name == "allocation.lease") {
            REQUIRE(std::filesystem::is_regular_file(status));
            CHECK(std::filesystem::file_size(entry.path()) == 0);
            CHECK(std::filesystem::hard_link_count(entry.path()) == 1);
            allocationLease = true;
        } else {
            REQUIRE(std::filesystem::is_directory(status));
            REQUIRE(name.starts_with("sync-sweep-"));
            generations.push_back(entry.path());
        }
    }
    REQUIRE(allocationLease);
    return generations;
}

std::vector<std::byte> jsonBytes(const nlohmann::json& json) {
    return bytes(json.dump());
}

struct TestWriterKey {
    std::string privatePem;
    std::string publicPem;
};

TestWriterKey generateWriterKey() {
    auto generated = generateWriterKeyPair();
    REQUIRE(generated.has_value());
    return TestWriterKey{std::move(generated.value().privateKeyPem),
                         std::move(generated.value().publicKeyPem)};
}

std::shared_ptr<const WriterAuthenticator> writerAuth(std::string writerId, std::string keyId,
                                                      const TestWriterKey& localKey,
                                                      std::vector<TrustedWriterKey> trusted,
                                                      std::uint64_t epoch = 1) {
    WriterAuthConfig config;
    config.required = true;
    config.localWriterId = std::move(writerId);
    config.localKeyId = std::move(keyId);
    config.localPrivateKeyPem = localKey.privatePem;
    config.trustedKeys = std::move(trusted);
    auto auth = WriterAuthenticator::create(std::move(config), "auth-corpus", epoch);
    REQUIRE(auth.has_value());
    return auth.value();
}

class CountingFilesystemBackend final : public yams::storage::IStorageBackend {
public:
    yams::Result<void> initialize(const yams::storage::BackendConfig& config) override {
        return backend_.initialize(config);
    }

    yams::Result<void> store(std::string_view key, std::span<const std::byte> data) override {
        if (!failNextStorePrefix_.empty() && key.starts_with(failNextStorePrefix_)) {
            failNextStorePrefix_.clear();
            return yams::Error{yams::ErrorCode::IOError, "injected store failure"};
        }
        auto stored = backend_.store(key, data);
        if (stored && !failAfterStorePrefix_.empty() && key.starts_with(failAfterStorePrefix_)) {
            failAfterStorePrefix_.clear();
            return yams::Error{yams::ErrorCode::IOError, "injected error after store committed"};
        }
        return stored;
    }

    yams::Result<std::vector<std::byte>> retrieve(std::string_view key) const override {
        if (key.starts_with("blob/")) {
            ++blobRetrieveCalls_;
        }
        auto result = backend_.retrieve(key);
        if (key.starts_with("index/") && afterIndexRetrieve) {
            afterIndexRetrieve();
        }
        return result;
    }

    std::function<void()> afterIndexRetrieve;

    yams::Result<bool> exists(std::string_view key) const override {
        if (key.starts_with("blob/")) {
            ++blobExistsCalls_;
        }
        return backend_.exists(key);
    }

    yams::Result<void> remove(std::string_view key) override {
        if (!failNextRemovePrefix_.empty() && key.starts_with(failNextRemovePrefix_)) {
            failNextRemovePrefix_.clear();
            return yams::Error{yams::ErrorCode::IOError, "injected remove failure"};
        }
        return backend_.remove(key);
    }

    yams::Result<std::vector<std::string>> list(std::string_view prefix = "") const override {
        ++unboundedListCalls_;
        return backend_.list(prefix);
    }

    yams::Result<yams::storage::ObjectListPage> listPage(std::string_view prefix,
                                                         std::optional<std::string_view> cursor,
                                                         std::size_t limit) const override {
        ++listPageCalls_;
        largestPageRequested_ = std::max(largestPageRequested_, limit);
        if (!failNextListPrefix_.empty() && prefix.starts_with(failNextListPrefix_)) {
            failNextListPrefix_.clear();
            return yams::Error{yams::ErrorCode::IOError, "injected listing failure"};
        }
        return backend_.listPage(prefix, cursor, limit);
    }

    yams::Result<::yams::StorageStats> getStats() const override { return backend_.getStats(); }

    std::future<yams::Result<void>> storeAsync(std::string_view key,
                                               std::span<const std::byte> data) override {
        if (deferNextAsync_) {
            REQUIRE_FALSE(deferredStore_.has_value());
            deferNextAsync_ = false;
            deferredResult_ = std::promise<yams::Result<void>>{};
            deferredStore_ = DeferredStore{std::string(key), {data.begin(), data.end()}};
            return deferredResult_.get_future();
        }
        return backend_.storeAsync(key, data);
    }

    std::future<yams::Result<std::vector<std::byte>>>
    retrieveAsync(std::string_view key) const override {
        return backend_.retrieveAsync(key);
    }

    std::string getType() const override { return backend_.getType(); }
    bool isRemote() const override { return remote_; }
    void setRemote(bool remote) { remote_ = remote; }
    yams::Result<void> flush() override { return backend_.flush(); }

    std::size_t blobRetrieveCalls() const { return blobRetrieveCalls_; }
    std::size_t blobExistsCalls() const { return blobExistsCalls_; }
    std::size_t listPageCalls() const { return listPageCalls_; }
    std::size_t unboundedListCalls() const { return unboundedListCalls_; }
    std::size_t largestPageRequested() const { return largestPageRequested_; }
    void resetBlobCalls() const {
        blobRetrieveCalls_ = 0;
        blobExistsCalls_ = 0;
    }
    void resetListCalls() const {
        listPageCalls_ = 0;
        unboundedListCalls_ = 0;
        largestPageRequested_ = 0;
    }
    void failNextStore(std::string prefix) { failNextStorePrefix_ = std::move(prefix); }
    void failNextRemove(std::string prefix) { failNextRemovePrefix_ = std::move(prefix); }
    void failAfterStore(std::string prefix) { failAfterStorePrefix_ = std::move(prefix); }
    void failNextList(std::string prefix) { failNextListPrefix_ = std::move(prefix); }
    void deferNextStoreAsync() { deferNextAsync_ = true; }
    yams::Result<void> completeDeferredStore() {
        REQUIRE(deferredStore_.has_value());
        auto stored = backend_.store(deferredStore_->key, deferredStore_->value);
        deferredResult_.set_value(stored);
        deferredStore_.reset();
        return stored;
    }

private:
    struct DeferredStore {
        std::string key;
        std::vector<std::byte> value;
    };
    std::optional<DeferredStore> deferredStore_;
    std::promise<yams::Result<void>> deferredResult_;
    bool deferNextAsync_{false};
    yams::storage::FilesystemBackend backend_;
    mutable std::size_t blobRetrieveCalls_{0};
    mutable std::size_t blobExistsCalls_{0};
    mutable std::size_t listPageCalls_{0};
    mutable std::size_t unboundedListCalls_{0};
    mutable std::size_t largestPageRequested_{0};
    bool remote_{false};
    std::string failNextStorePrefix_;
    std::string failNextRemovePrefix_;
    std::string failAfterStorePrefix_;
    mutable std::string failNextListPrefix_;
};

struct CountingBackendFixture {
    explicit CountingBackendFixture(const std::string& name) {
        dir = std::filesystem::temp_directory_path() /
              ("yams-memory-sync-" + name + "-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(dir);
        yams::storage::BackendConfig config;
        config.type = "filesystem";
        config.localPath = dir;
        REQUIRE(backend.initialize(config).has_value());
    }
    ~CountingBackendFixture() {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    std::filesystem::path dir;
    CountingFilesystemBackend backend;
};

struct BackendFixture {
    explicit BackendFixture(const std::string& name) {
        dir = std::filesystem::temp_directory_path() /
              ("yams-memory-sync-" + name + "-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(dir);
        config.type = "filesystem";
        config.localPath = dir;
        REQUIRE(backend.initialize(config).has_value());
    }
    ~BackendFixture() {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    std::filesystem::path dir;
    yams::storage::BackendConfig config;
    yams::storage::FilesystemBackend backend;
};

std::string replaceEnvelope(BackendFixture& fixture, const std::string& oldKey,
                            const nlohmann::json& envelope) {
    const auto encoded = jsonBytes(envelope);
    const std::string slashPrefix = oldKey.substr(0, oldKey.rfind('/') + 1);
    const std::string newKey = slashPrefix + digest(encoded);
    REQUIRE(fixture.backend.store(newKey, encoded).has_value());
    return newKey;
}

} // namespace

TEST_CASE("MemorySyncLoop publish and read round-trips", "[memory-sync][sync-loop]") {
    BackendFixture fixture{"roundtrip"};
    MemorySyncLoop loop{fixture.backend, "A"};

    REQUIRE(loop.publish("slot", bytes("hello memory")).has_value());
    const auto value = loop.read("slot");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "hello memory");
}

TEST_CASE("MemorySyncLoop applyDeltas honors the direct-ingress admission gate",
          "[memory-sync][sync-loop][direct-delta][admission]") {
    BackendFixture writerFixture{"admission-writer"};
    BackendFixture readerFixture{"admission-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "admission-corpus", 1};
    MemorySyncLoop reader{readerFixture.backend,
                          "reader",
                          "admission-corpus",
                          1,
                          false,
                          {},
                          MemorySyncControl{.canAdmitRemoteWork = [] { return false; }}};

    REQUIRE(writer.publish("user/key", bytes("value")).has_value());
    auto deltas = writer.exportLocalDeltasAfter({});
    REQUIRE(deltas.has_value());
    REQUIRE_FALSE(deltas.value().deltas.empty());

    const auto applied = reader.applyDeltas(deltas.value().deltas);
    REQUIRE_FALSE(applied.has_value());
    CHECK(applied.error().code == yams::ErrorCode::ResourceExhausted);
}

TEST_CASE("MemorySyncLoop rejects unsafe logical keys", "[memory-sync][sync-loop][security]") {
    BackendFixture fixture{"unsafe-keys"};
    MemorySyncLoop loop{fixture.backend, "A"};
    const auto payload = bytes("must-stay-contained");

    const std::vector<std::string> invalidKeys = {
        "",
        "/absolute",
        "C:/absolute",
        "../escape",
        "nested/../escape",
        "./relative",
        "nested\\escape",
        "control\nkey",
        std::string(1025, 'a'),
    };
    for (const auto& key : invalidKeys) {
        CAPTURE(key);
        const auto result = loop.publish(key, payload);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == yams::ErrorCode::InvalidArgument);
    }

    // Typed adapter namespaces remain valid at the internal loop boundary.
    CHECK(loop.publish("document/abc123", payload).has_value());
}

TEST_CASE("MemorySyncLoop reads legacy envelopes alongside versioned records",
          "[memory-sync][sync-loop][compatibility]") {
    BackendFixture fixture{"legacy-envelope"};
    const auto legacyPayload = bytes("legacy payload");
    const auto payloadHash = digest(legacyPayload);
    REQUIRE(fixture.backend.store("blob/" + payloadHash, legacyPayload).has_value());

    const nlohmann::json legacy = {
        {"entryHash", payloadHash},
        {"ts", {{"physicalMs", 123}, {"logical", 4}, {"origin", "legacy-node"}}},
        {"version", {{"counters_", {{"legacy-node", 7}}}}},
    };
    const auto envelopeBytes = jsonBytes(legacy);
    REQUIRE(fixture.backend.store("index/legacy-key/" + digest(envelopeBytes), envelopeBytes)
                .has_value());

    MemorySyncLoop loop{fixture.backend, "modern-node", "local-test-corpus", 1, true};
    REQUIRE(loop.sync().has_value());
    const auto upgradedKeys = fixture.backend.list("index/legacy-key/");
    REQUIRE(upgradedKeys.has_value());
    CHECK(upgradedKeys.value().size() == 2);

    MemorySyncLoop strictReader{fixture.backend, "strict-node", "local-test-corpus", 1};
    const auto strictMerged = strictReader.sync();
    REQUIRE(strictMerged.has_value());
    CHECK(strictMerged.value().contains("legacy-key"));

    const auto legacyRead = loop.readCached("legacy-key");
    REQUIRE(legacyRead.has_value());
    CHECK(text(legacyRead.value()) == "legacy payload");

    REQUIRE(loop.publish("modern-key", bytes("modern payload")).has_value());
    const auto merged = loop.sync();
    REQUIRE(merged.has_value());
    CHECK(merged.value().contains("legacy-key"));
    CHECK(merged.value().contains("modern-key"));
}

TEST_CASE("incomplete migrated writer history rejects before creating a new operation",
          "[memory-sync][sync-loop][commitment][migration]") {
    BackendFixture fixture{"incomplete-history-migration"};
    const auto payload = bytes("retained frontier");
    const auto payloadHash = digest(payload);
    REQUIRE(fixture.backend.store("blob/" + payloadHash, payload).has_value());
    const nlohmann::json record = {
        {"schemaVersion", 3},
        {"entryHash", payloadHash},
        {"ts", {{"physicalMs", 100}, {"logical", 1}, {"origin", "writer"}}},
        {"origin", "writer"},
        {"vv", {{"counters_", {{"writer", 7}}}}},
        {"corpusId", "migration-corpus"},
        {"corpusEpoch", 1},
        {"logicalKey", "user/frontier"},
        {"recordKind", "value"},
        {"operationId", "writer:7"},
    };
    const auto envelope = jsonBytes(record);
    REQUIRE(fixture.backend.store("index/user/frontier/" + digest(envelope), envelope).has_value());

    MemorySyncLoop writer{fixture.backend, "writer", "migration-corpus", 1};
    REQUIRE(writer.syncFully().has_value());
    const auto before = fixture.backend.list("index/");
    REQUIRE(before.has_value());
    auto rejected = writer.publish("user/new", bytes("must-not-persist"));
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == yams::ErrorCode::InvalidState);
    const auto after = fixture.backend.list("index/");
    REQUIRE(after.has_value());
    CHECK(after.value() == before.value());
}

TEST_CASE("MemorySyncLoop quarantines malformed legacy causal identity without throwing",
          "[memory-sync][sync-loop][compatibility][security]") {
    BackendFixture fixture{"legacy-invalid-causal"};
    const auto payload = bytes("legacy payload");
    const auto payloadHash = digest(payload);
    REQUIRE(fixture.backend.store("blob/" + payloadHash, payload).has_value());
    const nlohmann::json legacy = {
        {"entryHash", payloadHash},
        {"ts", {{"physicalMs", 123}, {"logical", 4}, {"origin", "legacy-node"}}},
        {"version", {{"counters_", {{"other-node", 7}}}}},
    };
    const auto envelope = jsonBytes(legacy);
    REQUIRE(fixture.backend.store("index/legacy-key/" + digest(envelope), envelope).has_value());

    MemorySyncLoop loop{fixture.backend, "modern-node", "local-test-corpus", 1, true};
    const auto merged = loop.sync();
    REQUIRE(merged.has_value());
    CHECK_FALSE(merged.value().contains("legacy-key"));
    CHECK(loop.quarantinedRecordCount() == 1);
    const auto upgraded = fixture.backend.list("index/legacy-key/");
    REQUIRE(upgraded.has_value());
    CHECK(upgraded.value().size() == 1);
}

TEST_CASE("MemorySyncLoop quarantines corrupt envelopes and blobs",
          "[memory-sync][sync-loop][integrity]") {
    BackendFixture fixture{"corruption"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("good-key", bytes("good payload")).has_value());

    const auto claimedHash = std::string(64, 'a');
    const nlohmann::json wrongBlobRecord = {
        {"schemaVersion", 3},
        {"entryHash", claimedHash},
        {"ts", {{"physicalMs", 200}, {"logical", 1}}},
        {"origin", "attacker"},
        {"vv", {{"counters_", {{"attacker", 1}}}}},
        {"corpusId", "local-test-corpus"},
        {"corpusEpoch", 1},
        {"logicalKey", "wrong-blob"},
        {"recordKind", "value"},
        {"operationId", "attacker:1"},
    };
    const auto wrongBlobEnvelope = jsonBytes(wrongBlobRecord);
    REQUIRE(fixture.backend.store("blob/" + claimedHash, bytes("wrong bytes")).has_value());
    REQUIRE(
        fixture.backend.store("index/wrong-blob/" + digest(wrongBlobEnvelope), wrongBlobEnvelope)
            .has_value());

    const auto validEnvelope = jsonBytes(wrongBlobRecord);
    REQUIRE(fixture.backend.store("index/wrong-index/" + std::string(64, 'f'), validEnvelope)
                .has_value());

    const auto truncated = bytes("{\"schemaVersion\":2");
    REQUIRE(fixture.backend.store("index/truncated/" + digest(truncated), truncated).has_value());

    MemorySyncLoop reader{fixture.backend, "reader"};
    const auto merged = reader.sync();
    REQUIRE(merged.has_value());
    CHECK(merged.value().contains("good-key"));
    CHECK_FALSE(merged.value().contains("wrong-blob"));
    CHECK_FALSE(merged.value().contains("wrong-index"));
    CHECK_FALSE(merged.value().contains("truncated"));
    CHECK(reader.quarantinedRecordCount() == 3);
}

TEST_CASE("MemorySyncLoop isolates records by corpus and epoch",
          "[memory-sync][sync-loop][identity]") {
    BackendFixture fixture{"corpus-isolation"};
    MemorySyncLoop corpusA{fixture.backend, "writer-a", "corpus-a", 1};
    MemorySyncLoop corpusB{fixture.backend, "writer-b", "corpus-b", 1};

    REQUIRE(corpusA.publish("shared-key", bytes("from corpus a")).has_value());
    REQUIRE(corpusB.publish("shared-key", bytes("from corpus b")).has_value());

    const auto mergedA = corpusA.sync();
    const auto mergedB = corpusB.sync();
    REQUIRE(mergedA.has_value());
    REQUIRE(mergedB.has_value());
    REQUIRE(mergedA.value().contains("shared-key"));
    REQUIRE(mergedB.value().contains("shared-key"));
    CHECK(mergedA.value().at("shared-key").corpusId == "corpus-a");
    CHECK(mergedB.value().at("shared-key").corpusId == "corpus-b");
    CHECK(corpusA.quarantinedRecordCount() == 1);
    CHECK(corpusB.quarantinedRecordCount() == 1);
}

TEST_CASE("MemorySyncLoop fails closed on duplicate writer operation forks",
          "[memory-sync][sync-loop][identity]") {
    BackendFixture fixture{"writer-fork"};
    const bool exclusive = GENERATE(false, true);
    const auto storeFork = [&](std::string_view payload) {
        const auto payloadBytes = bytes(payload);
        const auto payloadHash = digest(payloadBytes);
        REQUIRE(fixture.backend.store("blob/" + payloadHash, payloadBytes).has_value());
        const nlohmann::json envelope = {
            {"schemaVersion", 3},
            {"entryHash", payloadHash},
            {"ts", {{"physicalMs", 100}, {"logical", 1}}},
            {"origin", "duplicate-writer"},
            {"vv", {{"counters_", {{"duplicate-writer", 1}}}}},
            {"corpusId", "corpus-a"},
            {"corpusEpoch", 1},
            {"logicalKey", "forked-key"},
            {"recordKind", "value"},
            {"operationId", "duplicate-writer:1"},
        };
        const auto envelopeBytes = jsonBytes(envelope);
        REQUIRE(fixture.backend.store("index/forked-key/" + digest(envelopeBytes), envelopeBytes)
                    .has_value());
    };
    storeFork("first payload");
    storeFork("second payload");

    MemorySyncLoop reader{fixture.backend,
                          "reader",
                          "corpus-a",
                          1,
                          false,
                          {},
                          MemorySyncControl{.exclusiveBackend = exclusive}};
    const auto merged = reader.sync();
    REQUIRE(merged.has_value());
    CHECK_FALSE(merged.value().contains("forked-key"));
    CHECK(reader.quarantinedRecordCount() == 2);
    CHECK(reader.writerQuarantined("duplicate-writer"));

    MemorySyncLoop restarted{fixture.backend,
                             "reader",
                             "corpus-a",
                             1,
                             false,
                             {},
                             MemorySyncControl{.exclusiveBackend = exclusive}};
    REQUIRE(restarted.syncFully().has_value());
    CHECK(restarted.writerQuarantined("duplicate-writer"));
    CHECK_FALSE(restarted.readCached("forked-key").has_value());
}

TEST_CASE("MemorySyncLoop keeps paged operation forks invisible until sweep completion",
          "[memory-sync][sync-loop][identity][pagination]") {
    BackendFixture fixture{"paged-writer-operation-fork"};
    const bool exclusive = GENERATE(false, true);
    const auto storeRecord = [&](std::string_view logicalKey, std::string_view payload) {
        const auto payloadBytes = bytes(payload);
        const auto payloadHash = digest(payloadBytes);
        REQUIRE(fixture.backend.store("blob/" + payloadHash, payloadBytes).has_value());
        const nlohmann::json record = {
            {"schemaVersion", 3},
            {"entryHash", payloadHash},
            {"ts", {{"physicalMs", 100}, {"logical", 1}, {"origin", "writer-a"}}},
            {"origin", "writer-a"},
            {"vv", {{"counters_", {{"writer-a", 1}}}}},
            {"corpusId", "corpus-a"},
            {"corpusEpoch", 1},
            {"logicalKey", logicalKey},
            {"recordKind", "value"},
            {"operationId", "writer-a:1"},
        };
        const auto envelope = jsonBytes(record);
        REQUIRE(fixture.backend
                    .store("index/" + std::string(logicalKey) + "/" + digest(envelope), envelope)
                    .has_value());
    };
    storeRecord("a-key", "first payload");
    storeRecord("z-key", "forked payload");

    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 1;
    MemorySyncLoop reader{fixture.backend,
                          "reader",
                          "corpus-a",
                          1,
                          false,
                          limits,
                          MemorySyncControl{.exclusiveBackend = exclusive}};

    const auto partial = reader.sync();
    REQUIRE(partial.has_value());
    CHECK_FALSE(partial.value().contains("a-key"));
    CHECK_FALSE(partial.value().contains("z-key"));

    const auto completed = reader.sync();
    REQUIRE(completed.has_value());
    CHECK_FALSE(completed.value().contains("a-key"));
    CHECK_FALSE(completed.value().contains("z-key"));
}

TEST_CASE("MemorySyncLoop rejects copied operation identity across logical keys",
          "[memory-sync][sync-loop][identity]") {
    BackendFixture fixture{"writer-operation-key-reuse"};
    MemorySyncLoop writer{fixture.backend, "writer-a", "corpus-a", 1};
    REQUIRE(writer.publish("original-key", bytes("shared payload")).has_value());

    const auto originalKeys = fixture.backend.list("index/original-key/");
    REQUIRE(originalKeys.has_value());
    REQUIRE(originalKeys.value().size() == 1);
    const auto envelope = fixture.backend.retrieve(originalKeys.value().front());
    REQUIRE(envelope.has_value());
    const auto envelopeHash = digest(envelope.value());
    REQUIRE(
        fixture.backend.store("index/copied-key/" + envelopeHash, envelope.value()).has_value());

    MemorySyncLoop reader{fixture.backend, "reader", "corpus-a", 1};
    const auto merged = reader.sync();
    REQUIRE(merged.has_value());
    CHECK_FALSE(merged.value().contains("original-key"));
    CHECK_FALSE(merged.value().contains("copied-key"));
    CHECK(reader.quarantinedRecordCount() == 2);
}

TEST_CASE("MemorySyncLoop replicates causal tombstones across restart",
          "[memory-sync][sync-loop][tombstone]") {
    BackendFixture fixture{"tombstone-restart"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    MemorySyncLoop peer{fixture.backend, "peer"};

    REQUIRE(writer.publish("deleted-key", bytes("value")).has_value());
    REQUIRE(peer.sync().has_value());
    REQUIRE(peer.readCached("deleted-key").has_value());

    REQUIRE(writer.erase("deleted-key").has_value());
    const auto merged = peer.sync();
    REQUIRE(merged.has_value());
    REQUIRE(merged.value().contains("deleted-key"));
    CHECK(merged.value().at("deleted-key").isTombstone());
    const auto deleted = peer.readCached("deleted-key");
    REQUIRE_FALSE(deleted.has_value());
    CHECK(deleted.error().code == yams::ErrorCode::NotFound);

    MemorySyncLoop restarted{fixture.backend, "restarted"};
    const auto afterRestart = restarted.sync();
    REQUIRE(afterRestart.has_value());
    REQUIRE(afterRestart.value().contains("deleted-key"));
    CHECK(afterRestart.value().at("deleted-key").isTombstone());
    CHECK(restarted.quarantinedRecordCount() == 0);

    const auto retained = fixture.backend.list("index/deleted-key/");
    REQUIRE(retained.has_value());
    CHECK(retained.value().size() == 2);
}

TEST_CASE("erase outbox retains an unready pre-delete intent across restart",
          "[memory-sync][tombstone][outbox]") {
    BackendFixture fixture{"erase-outbox-pending"};
    {
        MemorySyncLoop loop{fixture.backend, "writer"};
        REQUIRE(loop.stageErase("document/" + std::string(64, 'a'), std::string(64, 'a'), false,
                                EraseReadinessProbe::ContentAbsent)
                    .has_value());
        auto pending = loop.pendingErases();
        REQUIRE(pending.has_value());
        REQUIRE(pending.value().size() == 1);
        CHECK(pending.value().front().readinessProbe == EraseReadinessProbe::ContentAbsent);
        CHECK_FALSE(pending.value().front().ready);
        CHECK_FALSE(pending.value().front().prepared);
    }

    MemorySyncLoop restarted{fixture.backend, "writer"};
    auto pending = restarted.pendingErases();
    REQUIRE(pending.has_value());
    REQUIRE(pending.value().size() == 1);
    CHECK(pending.value().front().readinessProbe == EraseReadinessProbe::ContentAbsent);
    CHECK_FALSE(pending.value().front().ready);
    REQUIRE(restarted.syncFully().has_value());
    CHECK_FALSE(restarted.hasMergedRecord("document/" + std::string(64, 'a')));
}

TEST_CASE("erase staging batches reserve bounded outbox capacity atomically",
          "[memory-sync][tombstone][outbox][bounds]") {
    BackendFixture fixture{"erase-outbox-batch-bounds"};
    MemorySyncLimits limits;
    limits.maxMergedKeys = 2;
    MemorySyncLoop loop{fixture.backend, "writer", "local-test-corpus", 1, false, limits};
    REQUIRE(loop.stageErase("user/existing", "existing", false).has_value());

    const std::string hash(64, 'a');
    const std::array batch{
        EraseStageRequest{"content-blob/" + hash, hash, EraseReadinessProbe::ContentAbsent},
        EraseStageRequest{"document/" + hash, hash, EraseReadinessProbe::MetadataAbsent}};
    auto rejected = loop.stageErases(batch);
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == yams::ErrorCode::ResourceExhausted);

    auto pending = loop.pendingErases();
    REQUIRE(pending.has_value());
    REQUIRE(pending.value().size() == 1);
    CHECK(pending.value().front().logicalKey == "user/existing");

    limits.maxMergedKeys = 3;
    MemorySyncLoop expanded{fixture.backend, "writer", "local-test-corpus", 1, false, limits};
    REQUIRE(expanded.stageErases(batch).has_value());
    pending = expanded.pendingErases();
    REQUIRE(pending.has_value());
    CHECK(pending.value().size() == 3);
    auto singleOverflow = expanded.stageErase("user/overflow", "overflow", false);
    REQUIRE_FALSE(singleOverflow.has_value());
    CHECK(singleOverflow.error().code == yams::ErrorCode::ResourceExhausted);
    REQUIRE(expanded.pendingErases().value().size() == 3);
}

TEST_CASE("malformed erase outbox entries fail closed without deletion",
          "[memory-sync][tombstone][outbox][security]") {
    BackendFixture fixture{"erase-outbox-malformed"};
    MemorySyncLoop loop{fixture.backend, "writer"};
    REQUIRE(loop.stageErase("user/key", "key", false).has_value());
    auto keys = fixture.backend.list("outbox/erase-v1/");
    REQUIRE(keys.has_value());
    REQUIRE(keys.value().size() == 1);
    auto encoded = fixture.backend.retrieve(keys.value().front());
    REQUIRE(encoded.has_value());
    auto json = nlohmann::json::parse(text(encoded.value()));
    json["readiness_probe"] = "trust-me";
    REQUIRE(fixture.backend.store(keys.value().front(), jsonBytes(json)).has_value());

    auto pending = loop.pendingErases();
    REQUIRE_FALSE(pending.has_value());
    CHECK(pending.error().code == yams::ErrorCode::InvalidData);
    auto retained = fixture.backend.exists(keys.value().front());
    REQUIRE(retained.has_value());
    CHECK(retained.value());
}

TEST_CASE("authenticated prepared erase binds its typed readiness probe",
          "[memory-sync][tombstone][outbox][auth][security]") {
    CountingBackendFixture fixture{"erase-outbox-auth"};
    const auto key = generateWriterKey();
    auto auth = writerAuth("writer", "writer-key", key,
                           {TrustedWriterKey{"writer", "writer-key", key.publicPem, false}});
    MemorySyncLoop loop{fixture.backend, "writer", "auth-corpus", 1, false, {}, {}, {}, auth};
    REQUIRE(loop.stageErase("document/" + std::string(64, 'c'), std::string(64, 'c'), false,
                            EraseReadinessProbe::MetadataAbsent)
                .has_value());
    fixture.backend.failNextStore("index/");
    auto failed = loop.publishStagedErase("document/" + std::string(64, 'c'),
                                          [] { return yams::Result<bool>{true}; });
    REQUIRE_FALSE(failed.has_value());
    auto keys = fixture.backend.list("outbox/erase-v1/");
    REQUIRE(keys.has_value());
    REQUIRE(keys.value().size() == 1);
    auto encoded = fixture.backend.retrieve(keys.value().front());
    REQUIRE(encoded.has_value());
    auto json = nlohmann::json::parse(text(encoded.value()));
    json.erase("authorization");
    json["readiness_probe"] = "explicit";
    REQUIRE(fixture.backend.store(keys.value().front(), jsonBytes(json)).has_value());

    auto rejected = loop.pendingErases();
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == yams::ErrorCode::Unauthorized);
    auto retained = fixture.backend.exists(keys.value().front());
    REQUIRE(retained.has_value());
    CHECK(retained.value());
}

TEST_CASE("typed erase validator cancels stale intent before reserving a writer counter",
          "[memory-sync][tombstone][outbox][ordering]") {
    BackendFixture fixture{"erase-outbox-stale"};
    MemorySyncLoop loop{fixture.backend, "writer"};
    REQUIRE(loop.stageErase("document/" + std::string(64, 'b'), std::string(64, 'b'), false,
                            EraseReadinessProbe::MetadataAbsent)
                .has_value());
    bool sawUnprepared = false;
    auto cancelled = loop.publishStagedErase("document/" + std::string(64, 'b'), [&] {
        auto pending = loop.pendingErases();
        if (!pending) {
            return yams::Result<bool>{pending.error()};
        }
        if (pending.value().size() != 1) {
            return yams::Result<bool>{yams::Error{yams::ErrorCode::InvalidState,
                                                  "prepared intent missing during validation"}};
        }
        sawUnprepared = !pending.value().front().ready && !pending.value().front().prepared;
        return yams::Result<bool>{false};
    });
    REQUIRE(cancelled.has_value());
    CHECK(sawUnprepared);
    REQUIRE(loop.pendingErases().value().empty());
    REQUIRE(loop.syncFully().has_value());
    CHECK_FALSE(loop.hasMergedRecord("document/" + std::string(64, 'b')));
    CHECK(loop.currentVersion().get("writer") == 0);
}

TEST_CASE("earlier prepared deletion blocks later writer operations",
          "[memory-sync][tombstone][outbox][ordering]") {
    CountingBackendFixture fixture{"erase-outbox-ordering"};
    MemorySyncLoop loop{fixture.backend, "writer"};
    REQUIRE(loop.stageErase("document/" + std::string(64, 'a'), std::string(64, 'a'), false,
                            EraseReadinessProbe::MetadataAbsent)
                .has_value());
    fixture.backend.failNextStore("index/");
    auto firstFailed = loop.publishStagedErase("document/" + std::string(64, 'a'),
                                               [] { return yams::Result<bool>{true}; });
    REQUIRE_FALSE(firstFailed.has_value());
    REQUIRE(loop.stageErase("document/" + std::string(64, 'b'), std::string(64, 'b'), false,
                            EraseReadinessProbe::MetadataAbsent)
                .has_value());
    auto secondBlocked = loop.publishStagedErase("document/" + std::string(64, 'b'),
                                                 [] { return yams::Result<bool>{true}; });
    REQUIRE_FALSE(secondBlocked.has_value());
    CHECK(secondBlocked.error().code == yams::ErrorCode::InvalidState);
    auto writeBlocked = loop.publish("user/later", bytes("later"));
    REQUIRE_FALSE(writeBlocked.has_value());
    CHECK(writeBlocked.error().code == yams::ErrorCode::InvalidState);

    REQUIRE(loop.publishStagedErase("document/" + std::string(64, 'a'),
                                    [] { return yams::Result<bool>{true}; })
                .has_value());
    REQUIRE(loop.publishStagedErase("document/" + std::string(64, 'b'),
                                    [] { return yams::Result<bool>{true}; })
                .has_value());
    REQUIRE(loop.publish("user/later", bytes("later")).has_value());
    CHECK(loop.replicationState().commitments.at("writer").counter == 3);
}

TEST_CASE("checkpoint failure cannot expose a tombstone before revalidation",
          "[memory-sync][tombstone][outbox][ordering]") {
    CountingBackendFixture fixture{"erase-outbox-checkpoint-failure"};
    MemorySyncLoop loop{fixture.backend, "writer"};
    REQUIRE(loop.publish("document/" + std::string(64, 'd'), bytes("still-present")).has_value());
    REQUIRE(loop.stageErase("document/" + std::string(64, 'd'), std::string(64, 'd'), false,
                            EraseReadinessProbe::MetadataAbsent)
                .has_value());
    fixture.backend.failNextStore("checkpoint/replication-state-v1/");
    auto failed = loop.publishStagedErase("document/" + std::string(64, 'd'),
                                          [] { return yams::Result<bool>{true}; });
    REQUIRE_FALSE(failed.has_value());
    auto index = fixture.backend.list("index/document/" + std::string(64, 'd') + "/");
    REQUIRE(index.has_value());
    CHECK(index.value().size() == 1);
    CHECK(loop.replicationState().commitments.at("writer").counter == 1);

    auto cancelled = loop.publishStagedErase("document/" + std::string(64, 'd'),
                                             [] { return yams::Result<bool>{false}; });
    REQUIRE_FALSE(cancelled.has_value());
    CHECK(cancelled.error().code == yams::ErrorCode::ResourceBusy);
    REQUIRE(loop.syncFully().has_value());
    auto retained = loop.readCached("document/" + std::string(64, 'd'));
    REQUIRE(retained.has_value());
    CHECK(text(retained.value()) == "still-present");
    CHECK(loop.replicationState().commitments.at("writer").counter == 1);
    REQUIRE(loop.pendingErases().value().size() == 1);

    REQUIRE(loop.publishStagedErase("document/" + std::string(64, 'd'),
                                    [] { return yams::Result<bool>{true}; })
                .has_value());
    REQUIRE(loop.pendingErases().value().empty());
    CHECK_FALSE(loop.readCached("document/" + std::string(64, 'd')).has_value());
    REQUIRE(loop.publish("user/after-delete", bytes("next-counter")).has_value());
    CHECK(loop.replicationState().commitments.at("writer").counter == 3);

    MemorySyncLoop restarted{fixture.backend, "writer"};
    auto exported = restarted.exportLocalDeltasAfter({}, 3);
    REQUIRE(exported.has_value());
    CHECK(exported.value().deltas.size() == 3);
    CHECK(exported.value().deltas.back().logicalKey == "user/after-delete");
}

TEST_CASE("durable prepared commitment forces exact index completion",
          "[memory-sync][tombstone][outbox][ordering]") {
    CountingBackendFixture fixture{"erase-outbox-point-of-no-return"};
    MemorySyncLoop loop{fixture.backend, "writer"};
    REQUIRE(loop.publish("document/" + std::string(64, 'e'), bytes("value")).has_value());
    REQUIRE(loop.stageErase("document/" + std::string(64, 'e'), std::string(64, 'e'), false,
                            EraseReadinessProbe::MetadataAbsent)
                .has_value());
    fixture.backend.failNextStore("index/");
    auto failed = loop.publishStagedErase("document/" + std::string(64, 'e'),
                                          [] { return yams::Result<bool>{true}; });
    REQUIRE_FALSE(failed.has_value());
    CHECK(loop.replicationState().commitments.at("writer").counter == 2);
    auto index = fixture.backend.list("index/document/" + std::string(64, 'e') + "/");
    REQUIRE(index.has_value());
    CHECK(index.value().size() == 1);

    bool validatorCalled = false;
    auto completed = loop.publishStagedErase("document/" + std::string(64, 'e'), [&] {
        validatorCalled = true;
        return yams::Result<bool>{false};
    });
    REQUIRE(completed.has_value());
    CHECK_FALSE(validatorCalled);
    REQUIRE(loop.syncFully().has_value());
    CHECK_FALSE(loop.readCached("document/" + std::string(64, 'e')).has_value());
    REQUIRE(loop.pendingErases().value().empty());
}

TEST_CASE("prepared erase replays after an index-store failure",
          "[memory-sync][tombstone][outbox][replay]") {
    CountingBackendFixture fixture{"erase-outbox-index-failure"};
    MemorySyncLoop loop{fixture.backend, "writer"};
    REQUIRE(loop.publish("user/key", bytes("value")).has_value());
    REQUIRE(loop.stageErase("user/key", "key", true).has_value());
    fixture.backend.failNextStore("index/");
    auto failed = loop.publishStagedErase("user/key");
    REQUIRE_FALSE(failed.has_value());
    auto retained = loop.pendingErases();
    REQUIRE(retained.has_value());
    REQUIRE(retained.value().size() == 1);
    CHECK(retained.value().front().ready);
    CHECK(retained.value().front().prepared);

    MemorySyncLoop restarted{fixture.backend, "writer"};
    auto replayed = restarted.replayReadyErases();
    REQUIRE(replayed.has_value());
    CHECK(replayed.value() == 1);
    REQUIRE(restarted.pendingErases().value().empty());
    REQUIRE(restarted.syncFully().has_value());
    CHECK_FALSE(restarted.readCached("user/key").has_value());
    auto batch = restarted.exportLocalDeltasAfter({});
    REQUIRE(batch.has_value());
    CHECK(batch.value().deltas.size() == 2);
    CHECK(batch.value().deltas.back().record.isTombstone());
}

TEST_CASE("erase outbox cleanup failure cannot mint a later tombstone",
          "[memory-sync][tombstone][outbox][idempotent]") {
    CountingBackendFixture fixture{"erase-outbox-remove-failure"};
    MemorySyncLoop loop{fixture.backend, "writer"};
    REQUIRE(loop.publish("user/key", bytes("value")).has_value());
    REQUIRE(loop.stageErase("user/key", "key", true).has_value());
    fixture.backend.failNextRemove("outbox/erase-v1/");
    auto cleanupFailed = loop.publishStagedErase("user/key");
    REQUIRE_FALSE(cleanupFailed.has_value());
    auto retained = loop.pendingErases();
    REQUIRE(retained.has_value());
    REQUIRE(retained.value().size() == 1);
    CHECK(retained.value().front().prepared);

    MemorySyncLoop restarted{fixture.backend, "writer"};
    auto replayed = restarted.replayReadyErases();
    REQUIRE(replayed.has_value());
    CHECK(replayed.value() == 1);
    auto batch = restarted.exportLocalDeltasAfter({});
    REQUIRE(batch.has_value());
    REQUIRE(batch.value().deltas.size() == 2);
    CHECK(batch.value().deltas.back().record.vv.get("writer") == 2);
    CHECK(restarted.replicationState().commitments.at("writer").counter == 2);
    REQUIRE(restarted.pendingErases().value().empty());
}

TEST_CASE("MemorySyncLoop garbage collects tombstones only after every configured peer acks",
          "[memory-sync][sync-loop][tombstone][gc]") {
    BackendFixture fixture{"tombstone-gc-acks"};
    TombstoneGcPolicy policy{{"writer", "peer"}, std::chrono::milliseconds{0}};
    MemorySyncLoop writer{fixture.backend, "writer", "local-test-corpus", 1, false, {}, {}, policy};
    MemorySyncLoop peer{fixture.backend, "peer", "local-test-corpus", 1, false, {}, {}, policy};

    REQUIRE(writer.publish("deleted-key", bytes("value")).has_value());
    REQUIRE(writer.erase("deleted-key").has_value());
    REQUIRE(peer.sync().has_value());
    const auto peerAck = fixture.backend.exists("ack/writer%3A2/peer");
    REQUIRE(peerAck.has_value());
    CHECK(peerAck.value());
    REQUIRE(writer.sync().has_value());
    const auto collected =
        writer.collectTombstoneGarbage(std::numeric_limits<std::uint64_t>::max());
    REQUIRE(collected.has_value());
    CHECK(collected.value() == 1);
    const auto retained = fixture.backend.list("index/deleted-key/");
    REQUIRE(retained.has_value());
    const bool retainedHistoryIsEmpty = retained.value().empty();
    CHECK(retainedHistoryIsEmpty);
}

TEST_CASE("MemorySyncLoop retains tombstones while a configured peer is missing",
          "[memory-sync][sync-loop][tombstone][gc]") {
    BackendFixture fixture{"tombstone-gc-missing-peer"};
    TombstoneGcPolicy policy{{"writer", "peer", "offline"}, std::chrono::milliseconds{0}};
    MemorySyncLoop writer{fixture.backend, "writer", "local-test-corpus", 1, false, {}, {}, policy};
    MemorySyncLoop peer{fixture.backend, "peer", "local-test-corpus", 1, false, {}, {}, policy};

    REQUIRE(writer.publish("deleted-key", bytes("value")).has_value());
    REQUIRE(writer.erase("deleted-key").has_value());
    REQUIRE(peer.sync().has_value());
    REQUIRE(writer.sync().has_value());
    const auto collected =
        writer.collectTombstoneGarbage(std::numeric_limits<std::uint64_t>::max());
    REQUIRE(collected.has_value());
    CHECK(collected.value() == 0);
    const auto retained = fixture.backend.list("index/deleted-key/");
    REQUIRE(retained.has_value());
    CHECK(retained.value().size() == 2);
}

TEST_CASE("MemorySyncLoop converges across two nodes", "[memory-sync][sync-loop]") {
    BackendFixture fixture{"converge"};
    yams::storage::FilesystemBackend replicaBackend;
    REQUIRE(replicaBackend.initialize(fixture.config).has_value());

    MemorySyncLoop a{fixture.backend, "A"};
    MemorySyncLoop b{replicaBackend, "B"};

    REQUIRE(a.publish("a-key", bytes("from-a")).has_value());
    REQUIRE(b.publish("b-key", bytes("from-b")).has_value());

    const auto mergedA = a.sync();
    const auto mergedB = b.sync();
    REQUIRE(mergedA.has_value());
    REQUIRE(mergedB.has_value());

    CHECK(mergedA.value().size() == 2);
    CHECK(mergedB.value().size() == 2);
    CHECK(mergedA.value().count("a-key") == 1);
    CHECK(mergedA.value().count("b-key") == 1);
    CHECK(mergedB.value().count("a-key") == 1);
    CHECK(mergedB.value().count("b-key") == 1);

    // Both nodes see the same winning entry hashes (convergence).
    CHECK(mergedA.value().at("a-key").entryHash == mergedB.value().at("a-key").entryHash);
    CHECK(mergedA.value().at("b-key").entryHash == mergedB.value().at("b-key").entryHash);
}

TEST_CASE("MemorySyncLoop resolves concurrent writes deterministically",
          "[memory-sync][sync-loop]") {
    BackendFixture fixture{"concurrent"};
    MemorySyncLoop a{fixture.backend, "A"};
    MemorySyncLoop b{fixture.backend, "B"};

    REQUIRE(a.publish("contended", bytes("write-a")).has_value());
    REQUIRE(b.publish("contended", bytes("write-b")).has_value());

    const auto mergedA = a.sync();
    const auto mergedB = b.sync();
    REQUIRE(mergedA.has_value());
    REQUIRE(mergedB.has_value());

    // Exactly one winner, and both nodes agree on it (deterministic LWW).
    REQUIRE(mergedA.value().count("contended") == 1);
    REQUIRE(mergedB.value().count("contended") == 1);
    CHECK(mergedA.value().at("contended").entryHash == mergedB.value().at("contended").entryHash);
}

TEST_CASE("MemorySyncLoop caches unseen blobs after the first sync", "[memory-sync][sync-loop]") {
    BackendFixture fixture{"blob-cache"};
    CountingFilesystemBackend reader;
    REQUIRE(reader.initialize(fixture.config).has_value());

    MemorySyncLoop writer{fixture.backend, "A"};
    MemorySyncLoop replica{reader, "B"};

    REQUIRE(writer.publish("slot", bytes("cached memory")).has_value());
    REQUIRE(replica.sync().has_value());
    const auto blobRetrieveCalls = reader.blobRetrieveCalls();

    CHECK(blobRetrieveCalls == 1);
    CHECK(reader.blobExistsCalls() == 0);

    REQUIRE(replica.sync().has_value());
    const auto value = replica.read("slot");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "cached memory");
    CHECK(reader.blobRetrieveCalls() == blobRetrieveCalls);
}

TEST_CASE("MemorySyncLoop ignores a missing blob on an obsolete loser",
          "[memory-sync][sync-loop][bounds]") {
    BackendFixture fixture{"missing-loser-blob"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("history", bytes("obsolete")).has_value());
    const auto firstIndex = fixture.backend.list("index/history/");
    REQUIRE(firstIndex.has_value());
    REQUIRE(firstIndex.value().size() == 1);
    const auto firstEnvelope = fixture.backend.retrieve(firstIndex.value().front());
    REQUIRE(firstEnvelope.has_value());
    const auto firstRecord =
        nlohmann::json::parse(text(firstEnvelope.value())).get<MemoryIndexRecord>();

    REQUIRE(writer.publish("history", bytes("winner")).has_value());
    REQUIRE(fixture.backend.remove("blob/" + firstRecord.entryHash).has_value());

    CountingFilesystemBackend readerBackend;
    REQUIRE(readerBackend.initialize(fixture.config).has_value());
    MemorySyncLoop reader{readerBackend, "reader"};
    const auto merged = reader.sync();
    REQUIRE(merged.has_value());
    REQUIRE(merged.value().contains("history"));
    CHECK(readerBackend.blobRetrieveCalls() == 1);
    CHECK(reader.quarantinedRecordCount() == 0);
    const auto value = reader.readCached("history");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "winner");
}

TEST_CASE("MemorySyncLoop quarantines oversized envelopes without blocking unrelated keys",
          "[memory-sync][sync-loop][bounds]") {
    BackendFixture fixture{"oversized-envelope"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("good", bytes("good")).has_value());
    const auto oversized = bytes(std::string(2048, 'x'));
    REQUIRE(fixture.backend.store("index/bad/" + digest(oversized), oversized).has_value());

    MemorySyncLimits limits;
    limits.maxEnvelopeBytes = 1024;
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};
    const auto merged = reader.sync();
    REQUIRE(merged.has_value());
    CHECK(merged.value().contains("good"));
    CHECK_FALSE(merged.value().contains("bad"));
    CHECK(reader.quarantinedRecordCount() == 1);
}

TEST_CASE("MemorySyncLoop selects one same-key winner before blob hydration",
          "[memory-sync][sync-loop][bounds]") {
    BackendFixture fixture{"winner-before-hydration"};
    CountingFilesystemBackend readerBackend;
    REQUIRE(readerBackend.initialize(fixture.config).has_value());
    MemorySyncLoop writer{fixture.backend, "writer"};

    for (int i = 0; i < 12; ++i) {
        REQUIRE(writer.publish("long-history", bytes("value-" + std::to_string(i))).has_value());
    }

    readerBackend.resetBlobCalls();
    MemorySyncLoop reader{readerBackend, "reader"};
    const auto merged = reader.sync();
    REQUIRE(merged.has_value());
    CHECK(readerBackend.blobRetrieveCalls() == 1);
    CHECK(reader.testingCachedBlobCount() == 1);
    const auto value = reader.readCached("long-history");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "value-11");
}

TEST_CASE("MemorySyncLoop commits bounded index windows only after a complete sweep",
          "[memory-sync][sync-loop][bounds]") {
    BackendFixture fixture{"bounded-index-window"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    for (int i = 0; i < 5; ++i) {
        REQUIRE(writer.publish("key-" + std::to_string(i), bytes("value")).has_value());
    }

    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 2;
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};
    REQUIRE(reader.sync().has_value());
    CHECK(reader.mergedRecordCount() == 0);
    REQUIRE(reader.sync().has_value());
    CHECK(reader.mergedRecordCount() == 0);
    REQUIRE(reader.sync().has_value());
    CHECK(reader.mergedRecordCount() == 5);
    CHECK(reader.testingCachedBlobCount() == 1);
}

TEST_CASE("A sweep budget is aggregate across pages and fails without committing a prefix",
          "[memory-sync][scan-safety][bounds]") {
    BackendFixture fixture{"scan-byte-budget"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("old snapshot")).has_value());
    auto first = fixture.backend.list("index/");
    REQUIRE(first.has_value());
    REQUIRE(first.value().size() == 1);
    auto envelope = fixture.backend.retrieve(first.value().front());
    REQUIRE(envelope.has_value());

    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = GENERATE(std::size_t{1}, std::size_t{4096});
    limits.maxScanBytes = first.value().front().size() + envelope.value().size();
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};
    REQUIRE(reader.syncFully().has_value()); // the exact byte boundary is admitted
    const auto committed = reader.replicationState().commitments;
    REQUIRE(writer.publish("b", bytes("new snapshot")).has_value());

    const auto failed = reader.syncFully();
    REQUIRE_FALSE(failed.has_value());
    CHECK(failed.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(reader.mergedRecordCount() == 1);
    CHECK(reader.replicationState().commitments == committed);
    CHECK(reader.replicationState().version.counters().size() == 1);
    CHECK(reader.replicationState().version.get("writer") == 1);
    CHECK(reader.testingScanCandidateCount() == 0);
    CHECK(reader.testingScanBytes() == 0);
    CHECK(text(reader.readCached("a").value()) == "old snapshot");

    // Removing the over-budget object lets the same loop restart from the beginning.
    auto added = fixture.backend.list("index/b/");
    REQUIRE(added.has_value());
    REQUIRE(added.value().size() == 1);
    REQUIRE(fixture.backend.remove(added.value().front()).has_value());
    REQUIRE(reader.syncFully().has_value());
    CHECK(reader.mergedRecordCount() == 1);
}

TEST_CASE("Sweep byte admission handles zero and maximum size without overflow",
          "[memory-sync][scan-safety][bounds]") {
    BackendFixture fixture{"scan-byte-extremes"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("one")).has_value());
    MemorySyncLimits limits;
    limits.maxScanBytes = GENERATE(std::size_t{0}, std::numeric_limits<std::size_t>::max());
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};
    const auto result = reader.syncFully();
    if (limits.maxScanBytes == 0) {
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == yams::ErrorCode::ResourceExhausted);
        CHECK(reader.mergedRecordCount() == 0);
    } else {
        REQUIRE(result.has_value());
        CHECK(reader.hasMergedRecord("a"));
    }
    CHECK(reader.testingScanBytes() == 0);
}

TEST_CASE("Direct writer quarantine cannot exceed the invalidation window",
          "[memory-sync][recovery][review-regression]") {
    BackendFixture fixture{"quarantine-invalidation-window"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.erase("a").has_value());
    REQUIRE(writer.erase("b").has_value());
    MemorySyncLimits limits;
    limits.maxTrackedIdentities = GENERATE(std::size_t{1}, std::size_t{2});
    CAPTURE(limits.maxTrackedIdentities);
    MemorySyncControl control;
    control.exclusiveBackend = true;
    {
        MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false,
                              limits,          control};
        REQUIRE(reader.syncFully().has_value());
        REQUIRE(reader.mergedRecordCount() == 2);
        const auto generation = reader.durableQuarantineGeneration();
        const auto checkpointKeys = fixture.backend.list("checkpoint/");
        REQUIRE(checkpointKeys.has_value());
        REQUIRE_FALSE(checkpointKeys.value().empty());
        std::map<std::string, std::vector<std::byte>> checkpoints;
        for (const auto& key : checkpointKeys.value()) {
            auto value = fixture.backend.retrieve(key);
            REQUIRE(value.has_value());
            checkpoints.emplace(key, std::move(value.value()));
        }

        const auto quarantined =
            reader.quarantineWriter("writer", "reader", "bounded invalidations");
        if (limits.maxTrackedIdentities == 1) {
            REQUIRE_FALSE(quarantined.has_value());
            CHECK(quarantined.error().code == yams::ErrorCode::ResourceExhausted);
            CHECK_FALSE(reader.writerQuarantined("writer"));
            CHECK(reader.durableQuarantineGeneration() == generation);
            CHECK(reader.mergedRecordCount() == 2);
            const auto after = fixture.backend.list("checkpoint/");
            REQUIRE(after.has_value());
            CHECK(after.value() == checkpointKeys.value());
            for (const auto& [key, value] : checkpoints) {
                const auto actual = fixture.backend.retrieve(key);
                REQUIRE(actual.has_value());
                CHECK(actual.value() == value);
            }
            // A retry must not accumulate staged entries or publish a partial quarantine.
            const auto retried = reader.quarantineWriter("writer", "reader", "retry");
            REQUIRE_FALSE(retried.has_value());
            CHECK(retried.error().code == yams::ErrorCode::ResourceExhausted);
            CHECK(reader.durableQuarantineGeneration() == generation);
            // Re-verify durable state, not just the returned error: a retry that leaked its first
            // staged key would otherwise still satisfy the generation and status checks above.
            const auto afterRetry = fixture.backend.list("checkpoint/");
            REQUIRE(afterRetry.has_value());
            CHECK(afterRetry.value() == checkpointKeys.value());
            for (const auto& [key, value] : checkpoints) {
                const auto actual = fixture.backend.retrieve(key);
                REQUIRE(actual.has_value());
                CHECK(actual.value() == value);
            }
            CHECK_FALSE(reader.writerQuarantined("writer"));
        } else {
            REQUIRE(quarantined.has_value());
            CHECK(quarantined.value());
            CHECK(reader.writerQuarantined("writer"));
            const auto repeated =
                reader.quarantineWriter("writer", "reader", "already quarantined");
            REQUIRE(repeated.has_value());
            CHECK_FALSE(repeated.value());
        }
    }
    // Reopening sees exactly the committed outcome, never an over-budget attempt.
    MemorySyncLoop restarted{fixture.backend, "reader", "local-test-corpus", 1, false,
                             limits,          control};
    REQUIRE(restarted.syncFully().has_value());
    CHECK(restarted.writerQuarantined("writer") == (limits.maxTrackedIdentities == 2));
    CHECK(restarted.mergedRecordCount() == (limits.maxTrackedIdentities == 2 ? 0 : 2));
}

TEST_CASE("Sweep count exhaustion is not a successful truncated security decision",
          "[memory-sync][scan-safety][bounds]") {
    BackendFixture fixture{"scan-count-budget"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("one")).has_value());
    REQUIRE(writer.publish("b", bytes("two")).has_value());
    MemorySyncLimits limits;
    limits.maxTrackedIdentities = 1;
    limits.maxIndexObjectsPerSync = GENERATE(std::size_t{1}, std::size_t{4096});
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};
    const auto result = reader.syncFully();
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(reader.mergedRecordCount() == 0);
    CHECK(reader.replicationState().commitments.empty());
    CHECK(reader.testingScanCandidateCount() == 0);
    CHECK(reader.testingScanBytes() == 0);
}

TEST_CASE("An unsatisfiable sweep backs off instead of re-walking the store",
          "[memory-sync][scan-safety][bounds][backoff]") {
    BackendFixture fixture{"scan-budget-backoff"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("one")).has_value());
    REQUIRE(writer.publish("b", bytes("two")).has_value());

    MemorySyncLimits limits;
    limits.maxTrackedIdentities = 1;
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};

    const auto first = reader.syncFully();
    REQUIRE_FALSE(first.has_value());
    CHECK(first.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(reader.testingSweepAttempts() == 1);
    CHECK(reader.testingSweepDegraded());
    CHECK(reader.mergedRecordCount() == 0);

    // A single exhaustion may be recoverable, so one retry is still admitted...
    const auto second = reader.syncFully();
    REQUIRE_FALSE(second.has_value());
    CHECK(second.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(reader.testingSweepAttempts() == 2);

    // ...but a repeated failure disproves convergence and stops the store walk.
    const auto third = reader.syncFully();
    REQUIRE_FALSE(third.has_value());
    CHECK(third.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(reader.testingSweepAttempts() == 2);

    // The backoff expiry is the only gate: the retry lists the store again.
    reader.testingExpireSweepBackoff();
    const auto fourth = reader.syncFully();
    REQUIRE_FALSE(fourth.has_value());
    CHECK(fourth.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(reader.testingSweepAttempts() == 3);
    CHECK(reader.testingScanBytes() == 0);
}

TEST_CASE("A sweep that fits its budget does not back off",
          "[memory-sync][scan-safety][bounds][backoff]") {
    BackendFixture fixture{"scan-budget-no-backoff"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("one")).has_value());

    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus"};
    REQUIRE(reader.syncFully().has_value());
    CHECK_FALSE(reader.testingSweepDegraded());
    CHECK(reader.testingSweepAttempts() == 1);

    REQUIRE(reader.syncFully().has_value());
    CHECK(reader.testingSweepAttempts() == 2);
}

TEST_CASE("Cancellation takes precedence over a cached sweep failure",
          "[memory-sync][scan-safety][backoff][governor]") {
    BackendFixture fixture{"cancel-budget-backoff"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("one")).has_value());
    REQUIRE(writer.publish("b", bytes("two")).has_value());
    MemorySyncLimits limits;
    limits.maxTrackedIdentities = 1;
    bool cancelled = false;
    MemorySyncLoop reader{fixture.backend,
                          "reader",
                          "local-test-corpus",
                          1,
                          false,
                          limits,
                          MemorySyncControl{[&] { return cancelled; }, {}}};
    REQUIRE_FALSE(reader.syncFully().has_value());
    REQUIRE_FALSE(reader.syncFully().has_value());
    cancelled = true;
    const auto result = reader.sync();
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::OperationCancelled);
}

TEST_CASE("An exclusive local sweep can recover more records than its resident identity budget",
          "[memory-sync][scan-safety][exclusive-recovery]") {
    BackendFixture fixture{"exclusive-large-sweep"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    for (int i = 0; i < 6; ++i) {
        REQUIRE(writer.publish("key-" + std::to_string(i), bytes("value")).has_value());
    }
    MemorySyncLimits limits;
    limits.maxTrackedIdentities = 2;
    limits.maxIndexObjectsPerSync = 2;
    MemorySyncLoop reader{fixture.backend,
                          "reader",
                          "local-test-corpus",
                          1,
                          false,
                          limits,
                          MemorySyncControl{.exclusiveBackend = true}};
    REQUIRE(reader.syncFully().has_value());
    CHECK(reader.mergedRecordCount() == 6);
    CHECK(reader.replicationState().version.get("writer") == 6);
    CHECK(reader.testingScanCandidateCount() == 0);
    CHECK(reader.testingScanBytes() == 0);
}

TEST_CASE("Exclusive local recovery serves every winner within resident budgets",
          "[memory-sync][exclusive-recovery][disk-backed-serving]") {
    BackendFixture fixture{"disk-backed-serving"};
    constexpr std::size_t kRecords = 6;
    std::vector<std::pair<std::string, std::string>> expected;
    MemorySyncLoop writer{fixture.backend, "writer"};
    for (std::size_t i = 0; i < kRecords; ++i) {
        expected.emplace_back("key-" + std::to_string(i), "payload-" + std::to_string(i));
        REQUIRE(writer.publish(expected.back().first, bytes(expected.back().second)).has_value());
    }

    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 2;
    limits.maxMergedKeys = kRecords;
    limits.maxCacheBytes = kRecords * expected.front().second.size();
    SECTION("more logical winners than resident key slots") {
        limits.maxMergedKeys = 2;
    }
    SECTION("more distinct values than the resident byte cache can hold") {
        limits.maxCacheBytes = 2 * expected.front().second.size();
    }

    // Resident budgets are working-set bounds, not a cap on the exclusive local corpus.
    // Repeat with a fresh reader to require recovery after the previous reader closes.
    for (int recovery = 0; recovery < 2; ++recovery) {
        CAPTURE(recovery, limits.maxMergedKeys, limits.maxCacheBytes);
        MemorySyncLoop reader{fixture.backend,
                              "reader",
                              "local-test-corpus",
                              1,
                              false,
                              limits,
                              MemorySyncControl{.exclusiveBackend = true}};
        const auto checkResidentBounds = [&] {
            const auto state = reader.committedState();
            CHECK(state.merged.size() <= limits.maxMergedKeys);
            std::size_t residentBytes = 0;
            for (const auto& [hash, value] : state.cachedBlobs) {
                (void)hash;
                residentBytes += value.size();
            }
            CHECK(residentBytes <= limits.maxCacheBytes);
        };

        const auto recovered = reader.syncFully();
        INFO("recovery result: " << (recovered ? std::string{"success"}
                                               : recovered.error().message));
        REQUIRE(recovered.has_value());
        CHECK(reader.mergedRecordCount() == kRecords);
        checkResidentBounds();
        // Revisit values in reverse order as well, so evicted values must remain readable.
        for (int pass = 0; pass < 2; ++pass) {
            for (std::size_t i = 0; i < kRecords; ++i) {
                const auto index = pass == 0 ? i : kRecords - 1 - i;
                const auto& [key, payload] = expected[index];
                CAPTURE(key, pass);
                const auto value = reader.read(key);
                REQUIRE(value.has_value());
                CHECK(text(value.value()) == payload);
                checkResidentBounds();
            }
        }
    }
}

TEST_CASE("Exclusive winner workspaces clean abandoned state without removing pinned generations",
          "[memory-sync][exclusive-recovery][disk-backed-lifecycle]") {
    BackendFixture fixture{"disk-backed-leases"};
    {
        MemorySyncLoop seed{fixture.backend, "writer"};
        REQUIRE(seed.publish("key", bytes("original")).has_value());
    }
    MemorySyncLimits limits;
    limits.maxMergedKeys = 1;
    limits.maxCacheBytes = 16;
    std::shared_ptr<const MemorySyncLoop::WinnerView> pinned;
    std::filesystem::path live;
    {
        MemorySyncLoop owner{fixture.backend,
                             "writer",
                             "local-test-corpus",
                             1,
                             false,
                             limits,
                             MemorySyncControl{.exclusiveBackend = true}};
        REQUIRE(owner.syncFully().has_value());
        pinned = owner.committedState().winners;
        REQUIRE(pinned);
        const auto generations = scratchGenerations(fixture.dir / "temp");
        REQUIRE(generations.size() == 1);
        live = generations.front();
        REQUIRE(owner.publish("key", bytes("changed")).has_value());
        REQUIRE(pinned->find("key").has_value());
        CHECK(pinned->find("key").value().entryHash == digest(bytes("original")));
        CHECK(owner.committedState().winners->find("key").value().entryHash ==
              digest(bytes("changed")));
    }
    REQUIRE(std::filesystem::exists(live));
    const auto abandoned = fixture.dir / "temp" / ("sync-sweep-1-0" + live.extension().string());
    // Build the abandoned workspace by hand. Copying `live` reads its lease, which the pinned
    // generation holds under a byte-range lock; Windows locks are mandatory, so the copy fails.
    REQUIRE(std::filesystem::create_directory(abandoned));
    {
        std::ofstream lease{abandoned / "lease", std::ios::binary};
        lease << "YAMS disposable memory-sync workspace v1\n";
        std::ofstream db{abandoned / "winners.sqlite", std::ios::binary};
        db << "abandoned";
        REQUIRE((lease && db));
    }
    {
        MemorySyncLoop restarted{fixture.backend,
                                 "reader",
                                 "local-test-corpus",
                                 1,
                                 false,
                                 limits,
                                 MemorySyncControl{.exclusiveBackend = true}};
        REQUIRE(restarted.syncFully().has_value());
        CHECK_FALSE(std::filesystem::exists(abandoned));
        CHECK(std::filesystem::exists(live));
        const auto value = restarted.readCommitted("key");
        REQUIRE(value.has_value());
        CHECK(text(value.value()) == "changed");
        REQUIRE(pinned->find("key").has_value());
        CHECK(pinned->find("key").value().entryHash == digest(bytes("original")));
    }
    pinned.reset();
    CHECK(scratchGenerations(fixture.dir / "temp").empty());
}

TEST_CASE("Exclusive winner workspaces bound simultaneously pinned generations",
          "[memory-sync][exclusive-recovery][disk-backed-lifecycle][bounds]") {
    BackendFixture fixture{"disk-backed-generation-limit"};
    {
        MemorySyncLoop seed{fixture.backend, "writer"};
        REQUIRE(seed.publish("key", bytes("value")).has_value());
    }
    std::vector<std::shared_ptr<const MemorySyncLoop::WinnerView>> pins;
    for (int i = 0; i < 4; ++i) {
        MemorySyncLoop reader{fixture.backend,
                              "reader",
                              "local-test-corpus",
                              1,
                              false,
                              {},
                              MemorySyncControl{.exclusiveBackend = true}};
        REQUIRE(reader.syncFully().has_value());
        pins.push_back(reader.committedState().winners);
    }
    MemorySyncLoop next{fixture.backend,
                        "reader",
                        "local-test-corpus",
                        1,
                        false,
                        {},
                        MemorySyncControl{.exclusiveBackend = true}};
    const auto denied = next.syncFully();
    REQUIRE_FALSE(denied.has_value());
    CHECK(denied.error().code == yams::ErrorCode::ResourceExhausted);
    for (const auto& pin : pins)
        REQUIRE(pin->find("key").has_value());
    pins.front().reset();
    REQUIRE(next.syncFully().has_value());
    CHECK(next.mergedRecordCount() == 1);
}

TEST_CASE("Exclusive deletion updates cold winners and collects off-resident tombstones",
          "[memory-sync][exclusive-recovery][disk-backed-deletion]") {
    BackendFixture fixture{"disk-backed-deletion"};
    {
        MemorySyncLoop seed{fixture.backend, "writer"};
        for (int i = 0; i < 6; ++i)
            REQUIRE(seed.publish("key-" + std::to_string(i), bytes("value")).has_value());
    }
    MemorySyncLimits limits;
    limits.maxMergedKeys = 2;
    limits.maxCacheBytes = 16;
    TombstoneGcPolicy gc;
    gc.minRetention = std::chrono::milliseconds{0};
    gc.requiredPeers = {"writer"};
    MemorySyncLoop owner{fixture.backend,
                         "writer",
                         "local-test-corpus",
                         1,
                         false,
                         limits,
                         MemorySyncControl{.exclusiveBackend = true},
                         gc};
    REQUIRE(owner.syncFully().has_value());
    REQUIRE(owner.erase("key-5").has_value());
    CHECK_FALSE(owner.readCommitted("key-5").has_value());
    {
        const auto state = owner.committedState();
        REQUIRE(state.winners);
        REQUIRE(state.winners->find("key-5").has_value());
        CHECK(state.winners->find("key-5").value().isTombstone());
        CHECK(state.merged.size() <= limits.maxMergedKeys);
    }
    // A completed sweep emits the local acknowledgement before GC; keep the tombstone
    // outside the resident metadata slots so this exercises full-view enumeration.
    REQUIRE(owner.syncFully().has_value());
    const auto collected = owner.collectTombstoneGarbage(std::numeric_limits<std::uint64_t>::max());
    REQUIRE(collected.has_value());
    CHECK(collected.value() == 1);
    CHECK(owner.mergedRecordCount() == 5);
    REQUIRE(owner.syncFully().has_value());
    CHECK(owner.mergedRecordCount() == 5);
    CHECK_FALSE(owner.readCommitted("key-5").has_value());
}

TEST_CASE("Exclusive authenticated bootstrap exports every disk-backed winner or fails closed",
          "[memory-sync][exclusive-recovery][disk-backed-export][auth]") {
    BackendFixture fixture{"disk-backed-export"};
    const auto key = generateWriterKey();
    const std::vector<TrustedWriterKey> trust{{"writer", "v1", key.publicPem, false}};
    const auto auth = writerAuth("writer", "v1", key, trust);
    {
        MemorySyncLoop seed{fixture.backend, "writer", "auth-corpus", 1, false, {}, {}, {}, auth};
        for (int i = 0; i < 6; ++i)
            REQUIRE(seed.publish("user/key-" + std::to_string(i), bytes("payload")).has_value());
    }
    MemorySyncLimits limits;
    limits.maxMergedKeys = 2;
    limits.maxCacheBytes = 16;
    MemorySyncLoop owner{fixture.backend,
                         "writer",
                         "auth-corpus",
                         1,
                         false,
                         limits,
                         MemorySyncControl{.exclusiveBackend = true},
                         {},
                         auth};
    REQUIRE(owner.syncFully().has_value());
    const auto frozen = owner.replicationState();
    const auto snapshot = owner.exportColdBootstrap(frozen, 8, 1024);
    REQUIRE(snapshot.has_value());
    REQUIRE(snapshot.value().winners.size() == 6);
    for (int i = 0; i < 6; ++i) {
        const auto& delta = snapshot.value().winners[i];
        CHECK(delta.logicalKey == "user/key-" + std::to_string(i));
        CHECK(text(delta.payload) == "payload");
        CHECK(auth->verify(delta.record));
    }
    CHECK(auth->verifyDigest(snapshot.value().witnessSignature, "cold-bootstrap-v1",
                             snapshot.value().rootDigest));
    const auto tooSmall = owner.exportColdBootstrap(frozen, 5, 1024);
    REQUIRE_FALSE(tooSmall.has_value());
    CHECK(owner.committedState().merged.size() <= limits.maxMergedKeys);
}

TEST_CASE("Exclusive disk lookup cooperates even when its value is resident",
          "[memory-sync][exclusive-recovery][disk-backed-cooperation][governor]") {
    BackendFixture fixture{"disk-backed-cooperation"};
    {
        MemorySyncLoop seed{fixture.backend, "writer"};
        REQUIRE(seed.publish("key", bytes("value")).has_value());
    }
    bool cancel = false;
    std::size_t calls = 0;
    MemorySyncControl control;
    control.exclusiveBackend = true;
    control.cooperate = [&]() -> yams::Result<void> {
        ++calls;
        return cancel
                   ? yams::Result<void>{yams::Error{yams::ErrorCode::OperationCancelled, "cancel"}}
                   : yams::Result<void>{};
    };
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, {}, control};
    REQUIRE(reader.syncFully().has_value());
    calls = 0;
    cancel = true;
    REQUIRE(reader.readCached("key").has_value());
    CHECK(calls == 0); // the explicitly cache-only API remains nonblocking
    SECTION("no-op backfill lookup") {
        const auto result = reader.matchesCommittedValue("key", bytes("value"));
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == yams::ErrorCode::OperationCancelled);
    }
    SECTION("explicit on-demand read") {
        const auto result = reader.readCommitted("key");
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == yams::ErrorCode::OperationCancelled);
    }
    CHECK(calls > 0);
}

TEST_CASE("Trusted exclusive publications update disk winners without a new index sweep",
          "[memory-sync][exclusive-recovery][disk-backed-writes]") {
    BackendFixture fixture{"disk-backed-local-writes"};
    {
        MemorySyncLoop writer{fixture.backend, "writer"};
        for (int i = 0; i < 6; ++i) {
            REQUIRE(
                writer.publish("key-" + std::to_string(i), bytes("payload-" + std::to_string(i)))
                    .has_value());
        }
    }
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 2;
    limits.maxMergedKeys = 2;
    limits.maxCacheBytes = 18;
    MemorySyncLoop writer{fixture.backend,
                          "writer",
                          "local-test-corpus",
                          1,
                          false,
                          limits,
                          MemorySyncControl{.exclusiveBackend = true}};
    REQUIRE(writer.syncFully().has_value());
    // Exact built-in backend: derived wrappers deliberately disable scan reuse.
    const auto attempts = writer.testingSweepAttempts();
    for (int i = 0; i < 3; ++i) {
        const auto key = "new-" + std::to_string(i);
        REQUIRE(writer.publish(key, bytes("new-value")).has_value());
        const auto value = writer.readCommitted(key);
        REQUIRE(value.has_value());
        CHECK(text(value.value()) == "new-value");
        CHECK(writer.mergedRecordCount() == static_cast<std::size_t>(7 + i));
        const auto resident = writer.committedState();
        CHECK(resident.merged.size() <= limits.maxMergedKeys);
        CHECK(resident.cachedBlobs.size() <= limits.maxMergedKeys);
        std::size_t residentBytes = 0;
        for (const auto& [hash, value] : resident.cachedBlobs) {
            (void)hash;
            residentBytes += value.size();
        }
        CHECK(residentBytes <= limits.maxCacheBytes);
    }
    CHECK(writer.testingSweepAttempts() == attempts);
}

TEST_CASE("Exclusive direct deltas spill winners instead of exhausting resident cache",
          "[memory-sync][exclusive-recovery][disk-backed-writes][direct]") {
    BackendFixture source{"disk-backed-direct-source"};
    BackendFixture target{"disk-backed-direct-target"};
    MemorySyncLoop writer{source.backend, "writer"};
    for (int i = 0; i < 6; ++i) {
        REQUIRE(writer.publish("key-" + std::to_string(i), bytes("payload-" + std::to_string(i)))
                    .has_value());
    }
    const auto batch = writer.exportLocalDeltasAfter(VersionVector{});
    REQUIRE(batch.has_value());
    REQUIRE(batch.value().deltas.size() == 6);
    MemorySyncLimits limits;
    limits.maxMergedKeys = 2;
    limits.maxCacheBytes = 18;
    MemorySyncLoop reader{target.backend,
                          "reader",
                          "local-test-corpus",
                          1,
                          false,
                          limits,
                          MemorySyncControl{.exclusiveBackend = true}};
    const bool recoverFirst = GENERATE(false, true);
    if (recoverFirst)
        REQUIRE(reader.syncFully().has_value());
    const auto applied = reader.applyDeltas(batch.value().deltas);
    REQUIRE(applied.has_value());
    CHECK(applied.value().merged == 6);
    CHECK(reader.mergedRecordCount() == 6);
    CHECK(reader.currentVersion().get("writer") == 6);
    CHECK(reader.committedState().merged.size() <= limits.maxMergedKeys);
    for (int i = 0; i < 6; ++i) {
        const auto value = reader.readCommitted("key-" + std::to_string(i));
        REQUIRE(value.has_value());
        CHECK(text(value.value()) == "payload-" + std::to_string(i));
    }
}

TEST_CASE("Exclusive recovery reports hard working-set failures instead of truncated success",
          "[memory-sync][exclusive-recovery][bounds]") {
    BackendFixture fixture{"exclusive-serving-capacity"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("value")).has_value());
    REQUIRE(writer.publish("b", bytes("value")).has_value());
    MemorySyncLimits limits;
    SECTION("no metadata slot is available") {
        // One slot now permits eviction/spill; zero cannot hold even one working winner.
        limits.maxMergedKeys = 0;
    }
    SECTION("value-cache capacity") {
        limits.maxCacheBytes = 1;
    }
    MemorySyncLoop reader{fixture.backend,
                          "reader",
                          "local-test-corpus",
                          1,
                          false,
                          limits,
                          MemorySyncControl{.exclusiveBackend = true}};
    const auto result = reader.syncFully();
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(reader.mergedRecordCount() == 0);
    CHECK(reader.replicationState().version.empty());
}

TEST_CASE("Exclusive sweeps retain fork decisions and chained history across chunks",
          "[memory-sync][exclusive-recovery][identity][pagination]") {
    BackendFixture fixture{"exclusive-gap-fork"};
    const auto value = bytes("payload");
    const auto hash = digest(value);
    REQUIRE(fixture.backend.store("blob/" + hash, value).has_value());
    const auto store = [&](std::string key, std::uint64_t counter) {
        const auto encoded = jsonBytes(
            nlohmann::json{{"schemaVersion", 3},
                           {"entryHash", hash},
                           {"ts", {{"physicalMs", counter}, {"logical", 1}, {"origin", "writer"}}},
                           {"origin", "writer"},
                           {"vv", {{"counters_", {{"writer", counter}}}}},
                           {"corpusId", "corpus"},
                           {"corpusEpoch", 1},
                           {"logicalKey", key},
                           {"recordKind", "value"},
                           {"operationId", "writer:" + std::to_string(counter)}});
        REQUIRE(fixture.backend.store("index/" + key + "/" + digest(encoded), encoded).has_value());
    };
    const auto first = GENERATE(std::uint64_t{1}, std::uint64_t{9});
    const bool forked = GENERATE(false, true);
    for (std::uint64_t i = 0; i < 260; ++i)
        store("a-" + std::to_string(i), first + i);
    if (forked)
        store("z-fork", first);
    MemorySyncLimits limits;
    limits.maxTrackedIdentities = 2;
    limits.maxIndexObjectsPerSync = 32;
    {
        MemorySyncLoop reader{fixture.backend,
                              "reader",
                              "corpus",
                              1,
                              false,
                              limits,
                              MemorySyncControl{.exclusiveBackend = true}};
        REQUIRE(reader.syncFully().has_value());
        CHECK(reader.mergedRecordCount() == (forked ? 0 : 260));
        CHECK(reader.writerQuarantined("writer") == forked);
        CHECK(reader.replicationState().version.get("writer") == (forked ? 0 : first + 259));
        if (!forked && first == 1) {
            CHECK(reader.replicationState().commitments.at("writer").counter == 260);
        } else {
            CHECK_FALSE(reader.replicationState().commitments.contains("writer"));
        }
        MemorySyncLoop restarted{fixture.backend,
                                 "reader",
                                 "corpus",
                                 1,
                                 false,
                                 limits,
                                 MemorySyncControl{.exclusiveBackend = true}};
        REQUIRE(restarted.syncFully().has_value());
        CHECK(restarted.writerQuarantined("writer") == forked);
        CHECK(restarted.mergedRecordCount() == (forked ? 0 : 260));
        if (!forked && first == 1) {
            CHECK(restarted.replicationState().commitments.at("writer").counter == 260);
        }
        // Both complete generations are leased while their loops are alive, alongside
        // the single verified empty root coordination lease.
        CHECK(scratchGenerations(fixture.dir / "temp").size() == 2);
    }
    CHECK(scratchGenerations(fixture.dir / "temp").empty());
}

TEST_CASE("Filesystem listing cooperates while visiting unrelated keys",
          "[memory-sync][governor][filesystem]") {
    BackendFixture fixture{"cooperative-listing"};
    REQUIRE(fixture.backend.store("unrelated/a", bytes("one")).has_value());
    REQUIRE(fixture.backend.store("unrelated/b", bytes("two")).has_value());
    std::size_t checks = 0;
    fixture.backend.setListingWorkCheck([&]() -> yams::Result<void> {
        if (++checks == 3) {
            return yams::Error{yams::ErrorCode::OperationCancelled, "test checkpoint"};
        }
        return {};
    });
    const auto page = fixture.backend.listPage("absent/", std::nullopt, 1);
    REQUIRE_FALSE(page.has_value());
    CHECK(page.error().code == yams::ErrorCode::OperationCancelled);
    CHECK(checks == 3);
    fixture.backend.setListingWorkCheck({});
    const auto intact = fixture.backend.list("unrelated/");
    REQUIRE(intact.has_value());
    CHECK(intact.value().size() == 2);
}

TEST_CASE("Cancellation discards an incomplete sweep before a retry",
          "[memory-sync][scan-safety][cancellation]") {
    BackendFixture fixture{"scan-cancel-restart"};
    const bool exclusive = GENERATE(false, true);
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("removed before retry")).has_value());
    REQUIRE(writer.publish("b", bytes("retained")).has_value());
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 1;
    bool cancelled = false;
    MemorySyncLoop reader{
        fixture.backend,
        "reader",
        "local-test-corpus",
        1,
        false,
        limits,
        MemorySyncControl{.isCancelled = [&] { return cancelled; }, .exclusiveBackend = exclusive}};
    REQUIRE(reader.sync().has_value());
    CHECK(reader.mergedRecordCount() == 0);
    cancelled = true;
    const auto stopped = reader.sync();
    REQUIRE_FALSE(stopped.has_value());
    CHECK(stopped.error().code == yams::ErrorCode::OperationCancelled);
    CHECK(reader.testingScanCandidateCount() == 0);
    CHECK(reader.testingScanBytes() == 0);
    // Both the in-memory path and the disk-backed path must discard the first page.
    const auto removed = fixture.backend.list("index/a/");
    REQUIRE(removed.has_value());
    REQUIRE(fixture.backend.remove(removed.value().front()).has_value());
    cancelled = false;
    REQUIRE(reader.syncFully().has_value());
    CHECK_FALSE(reader.hasMergedRecord("a"));
    CHECK(reader.hasMergedRecord("b"));
}

TEST_CASE("Misplaced causal observations are charged even when they cannot be winners",
          "[memory-sync][scan-safety][bounds][identity]") {
    BackendFixture fixture{"misplaced-scan-budget"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("correct", bytes("value")).has_value());
    const auto original = fixture.backend.list("index/");
    REQUIRE(original.has_value());
    const auto envelope = fixture.backend.retrieve(original.value().front());
    REQUIRE(envelope.has_value());
    const auto misplaced = "index/misplaced/" + digest(envelope.value());
    REQUIRE(fixture.backend.store(misplaced, envelope.value()).has_value());
    REQUIRE(fixture.backend.remove(original.value().front()).has_value());
    MemorySyncLimits limits;
    limits.maxScanBytes = misplaced.size() + envelope.value().size() - 1;
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};
    const auto result = reader.syncFully();
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(reader.mergedRecordCount() == 0);
    CHECK(reader.replicationState().version.empty());
    CHECK(reader.testingScanBytes() == 0);
}

TEST_CASE("Fork variants cannot bypass the candidate count budget by reusing identities",
          "[memory-sync][scan-safety][bounds][identity]") {
    BackendFixture fixture{"fork-variant-scan-budget"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("same", bytes("value")).has_value());
    const auto keys = fixture.backend.list("index/");
    REQUIRE(keys.has_value());
    const auto envelope = fixture.backend.retrieve(keys.value().front());
    REQUIRE(envelope.has_value());
    auto record = nlohmann::json::parse(text(envelope.value())).get<MemoryIndexRecord>();
    ++record.ts.logical; // same operation and version vector, different envelope digest
    const auto variant = jsonBytes(nlohmann::json(record));
    REQUIRE(fixture.backend.store("index/same/" + digest(variant), variant).has_value());
    MemorySyncLimits limits;
    limits.maxTrackedIdentities = GENERATE(std::size_t{1}, std::size_t{2});
    limits.maxIndexObjectsPerSync = 1;
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};
    const auto result = reader.syncFully();
    if (limits.maxTrackedIdentities == 1) {
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == yams::ErrorCode::ResourceExhausted);
    } else {
        REQUIRE(result.has_value());
        CHECK(reader.writerQuarantined("writer"));
    }
    CHECK(reader.mergedRecordCount() == 0);
    CHECK(reader.replicationState().commitments.empty());
    CHECK(reader.testingScanCandidateCount() == 0);
}

TEST_CASE("Listing failure discards an incomplete sweep before a retry",
          "[memory-sync][scan-safety][retry]") {
    CountingBackendFixture fixture{"scan-list-failure"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("removed before retry")).has_value());
    REQUIRE(writer.publish("b", bytes("retained")).has_value());
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 1;
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};
    REQUIRE(reader.sync().has_value());
    fixture.backend.failNextList("index/");
    const auto failed = reader.sync();
    REQUIRE_FALSE(failed.has_value());
    CHECK(failed.error().code == yams::ErrorCode::IOError);
    CHECK(reader.testingScanCandidateCount() == 0);
    CHECK(reader.testingScanBytes() == 0);
    auto first = fixture.backend.list("index/a/");
    REQUIRE(first.has_value());
    REQUIRE(fixture.backend.remove(first.value().front()).has_value());
    REQUIRE(reader.syncFully().has_value());
    CHECK_FALSE(reader.hasMergedRecord("a"));
    CHECK(reader.hasMergedRecord("b"));
}

TEST_CASE("An exception while fetching a page discards the incomplete sweep",
          "[memory-sync][scan-safety][retry][exception]") {
    CountingBackendFixture fixture{"scan-fetch-exception"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("removed before retry")).has_value());
    REQUIRE(writer.publish("b", bytes("retained")).has_value());
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 1;
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};
    REQUIRE(reader.sync().has_value());
    fixture.backend.afterIndexRetrieve = [] { throw std::bad_alloc{}; };
    CHECK_THROWS_AS(reader.sync(), std::bad_alloc);
    CHECK(reader.testingScanCandidateCount() == 0);
    CHECK(reader.testingScanBytes() == 0);
    auto first = fixture.backend.list("index/a/");
    REQUIRE(first.has_value());
    REQUIRE(fixture.backend.remove(first.value().front()).has_value());
    fixture.backend.afterIndexRetrieve = {};
    REQUIRE(reader.syncFully().has_value());
    CHECK_FALSE(reader.hasMergedRecord("a"));
    CHECK(reader.hasMergedRecord("b"));
}

TEST_CASE("Cancellation during the final fetch is checked before history finalization",
          "[memory-sync][scan-safety][cancellation]") {
    CountingBackendFixture fixture{"cancel-last-fetch"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("a", bytes("value")).has_value());
    bool cancelled = false;
    fixture.backend.afterIndexRetrieve = [&] { cancelled = true; };
    MemorySyncLoop reader{fixture.backend,
                          "reader",
                          "local-test-corpus",
                          1,
                          false,
                          {},
                          MemorySyncControl{[&] { return cancelled; }, {}}};
    const auto stopped = reader.syncFully();
    REQUIRE_FALSE(stopped.has_value());
    CHECK(stopped.error().code == yams::ErrorCode::OperationCancelled);
    CHECK(reader.mergedRecordCount() == 0);
    CHECK(reader.replicationState().commitments.empty());
    CHECK(reader.replicationState().version.empty());
    CHECK(reader.testingScanCandidateCount() == 0);
    fixture.backend.afterIndexRetrieve = {};
    cancelled = false;
    REQUIRE(reader.syncFully().has_value());
    CHECK(reader.hasMergedRecord("a"));
}

TEST_CASE("Bounded filesystem snapshots certify only complete traversals",
          "[memory-sync][listing-cache][snapshot]") {
    BackendFixture fixture{"bounded-snapshot"};
    for (const auto* key : {"index/c", "index/a", "index/b", "blob/unrelated"}) {
        REQUIRE(fixture.backend.store(key, bytes("value")).has_value());
    }
    const auto full = fixture.backend.listBoundedSnapshot("index/", 3, 21);
    REQUIRE(full.has_value());
    REQUIRE(full.value().has_value());
    CHECK(*full.value() == std::vector<std::string>{"index/a", "index/b", "index/c"});

    const auto tooMany = fixture.backend.listBoundedSnapshot("index/", 2, 21);
    REQUIRE(tooMany.has_value());
    CHECK_FALSE(tooMany.value().has_value());
    const auto tooLarge = fixture.backend.listBoundedSnapshot("index/", 3, 20);
    REQUIRE(tooLarge.has_value());
    CHECK_FALSE(tooLarge.value().has_value());

    yams::storage::FilesystemListingStats stats;
    stats.prefix = "index/";
    stats.onEntry = [&] { fixture.backend.requestCancel(); };
    struct RestoreObservation {
        yams::storage::FilesystemListingStats* previous;
        ~RestoreObservation() { yams::storage::testing_setFilesystemListingStats(previous); }
    } restore{yams::storage::testing_setFilesystemListingStats(&stats)};
    const auto interrupted = fixture.backend.listBoundedSnapshot("index/", 3, 21);
    REQUIRE_FALSE(interrupted.has_value());
    CHECK(interrupted.error().code == yams::ErrorCode::OperationCancelled);
    fixture.backend.resetCancel();
}

TEST_CASE("Exclusive filesystem listing falls back to pages above the snapshot budget",
          "[memory-sync][listing-cache][budget]") {
    BackendFixture fixture{"snapshot-fallback"};
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 1; // 64 retained key slots, fewer than the 65 objects.
    auto cached = MemorySyncLoop::testing_countedBackend(fixture.backend, true, limits);
    for (std::size_t i = 0; i < 65; ++i) {
        REQUIRE(cached->store("index/" + std::to_string(10'000 + i), bytes("value")).has_value());
    }
    std::vector<std::string> keys;
    std::optional<std::string> cursor;
    do {
        auto page = cached->listPage("index/", cursor, 1);
        REQUIRE(page.has_value());
        keys.insert(keys.end(), page.value().keys.begin(), page.value().keys.end());
        cursor = page.value().nextCursor;
    } while (cursor);
    REQUIRE(keys.size() == 65);
    CHECK(keys.front() == "index/10000");
    CHECK(keys.back() == "index/10064");
}

TEST_CASE("Exclusive filesystem listing snapshots follow committed key mutations",
          "[memory-sync][listing-cache][mutations]") {
    CountingBackendFixture fixture{"listing-mutations"};
    auto cached = MemorySyncLoop::testing_countedBackend(fixture.backend, true, {}, {}, true);
    const auto value = bytes("value");
    for (const auto* key : {"index/b", "index/d", "index/f"}) {
        REQUIRE(cached->store(key, value).has_value());
    }
    const auto warm = cached->listPage("index/", std::nullopt, 4);
    REQUIRE(warm.has_value());
    REQUIRE(warm.value().keys.size() == 3);
    fixture.backend.resetListCalls();

    SECTION("unrelated writes and ordered insertions preserve complete listings") {
        REQUIRE(cached->store("blob/unrelated", value).has_value());
        REQUIRE(cached->store("index/a", value).has_value());
        REQUIRE(cached->store("index/d", bytes("replacement")).has_value());
        REQUIRE(cached->remove("index/b").has_value());
        const auto listed = cached->listPage("index/", std::nullopt, 4);
        REQUIRE(listed.has_value());
        CHECK(listed.value().keys == std::vector<std::string>{"index/a", "index/d", "index/f"});
        const auto afterDeletedCursor = cached->listPage("index/", std::string_view{"index/b"}, 2);
        REQUIRE(afterDeletedCursor.has_value());
        CHECK(afterDeletedCursor.value().keys == std::vector<std::string>{"index/d", "index/f"});
        CHECK_FALSE(afterDeletedCursor.value().nextCursor.has_value());
        const auto replaced = cached->retrieve("index/d");
        REQUIRE(replaced.has_value());
        CHECK(text(replaced.value()) == "replacement");
        CHECK(fixture.backend.listPageCalls() == 0);
    }
    SECTION("a failed store can have committed its key") {
        fixture.backend.failAfterStore("index/");
        CHECK_FALSE(cached->store("index/a", value).has_value());
        const auto listed = cached->listPage("index/", std::nullopt, 4);
        REQUIRE(listed.has_value());
        CHECK(listed.value().keys ==
              std::vector<std::string>{"index/a", "index/b", "index/d", "index/f"});
        CHECK(fixture.backend.listPageCalls() > 0);
    }
    SECTION("failed store or remove must not invent a key-set change") {
        fixture.backend.failNextStore("index/");
        CHECK_FALSE(cached->store("index/a", value).has_value());
        fixture.backend.failNextRemove("index/");
        CHECK_FALSE(cached->remove("index/b").has_value());
        const auto listed = cached->listPage("index/", std::nullopt, 4);
        REQUIRE(listed.has_value());
        CHECK(listed.value().keys == std::vector<std::string>{"index/b", "index/d", "index/f"});
        CHECK(fixture.backend.listPageCalls() > 0);
    }
    SECTION("clear invalidates snapshots") {
        REQUIRE(cached->clear().has_value());
        const auto listed = cached->listPage("index/", std::nullopt, 4);
        REQUIRE(listed.has_value());
        CHECK(listed.value().keys.empty());
        CHECK_FALSE(listed.value().nextCursor.has_value());
        CHECK(fixture.backend.listPageCalls() > 0);
    }
    SECTION("zero limit still fails after a snapshot is complete") {
        const auto listed = cached->listPage("index/", std::nullopt, 0);
        REQUIRE_FALSE(listed.has_value());
        CHECK(listed.error().code == yams::ErrorCode::InvalidArgument);
    }
}

TEST_CASE("Exclusive filesystem listing snapshots never certify a cursor gap",
          "[memory-sync][listing-cache][cursor]") {
    CountingBackendFixture fixture{"listing-gap"};
    auto cached = MemorySyncLoop::testing_countedBackend(fixture.backend, true, {}, {}, true);
    const auto value = bytes("value");
    for (const auto* key : {"index/b", "index/d", "index/f"}) {
        REQUIRE(cached->store(key, value).has_value());
    }
    auto page = cached->listPage("index/", std::nullopt, 1);
    REQUIRE(page.has_value());
    REQUIRE(page.value().nextCursor == "index/b");
    SECTION("insertion behind a partially collected cursor") {
        REQUIRE(cached->store("index/a", value).has_value());
    }
    SECTION("failed listing followed by a non-contiguous request") {
        fixture.backend.failNextList("index/");
        const auto failed = cached->listPage("index/", std::string_view{"index/b"}, 1);
        REQUIRE_FALSE(failed.has_value());
        CHECK(failed.error().code == yams::ErrorCode::IOError);
        REQUIRE(cached->store("index/a", value).has_value());
    }
    // End a suffix scan, without re-collecting its missing initial keys.
    page = cached->listPage("index/", std::string_view{"index/d"}, 1);
    REQUIRE(page.has_value());
    CHECK(page.value().keys == std::vector<std::string>{"index/f"});
    CHECK_FALSE(page.value().nextCursor.has_value());
    const auto full = cached->listPage("index/", std::nullopt, 4);
    REQUIRE(full.has_value());
    CHECK(full.value().keys ==
          std::vector<std::string>{"index/a", "index/b", "index/d", "index/f"});
}

TEST_CASE("Exclusive filesystem listing snapshots respect key and byte budgets",
          "[memory-sync][listing-cache][budget]") {
    CountingBackendFixture fixture{"listing-budget"};
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 1;
    std::size_t records = 0;
    bool fits = false;
    SECTION("key count boundary") {
        records = GENERATE(std::size_t{64}, std::size_t{65});
        fits = records == 64;
    }
    SECTION("key byte boundary") {
        records = 4;
        // Each index/10000-style key has eleven bytes; metadata overhead is separate.
        limits.maxCacheBytes = GENERATE(std::size_t{43}, std::size_t{44});
        fits = limits.maxCacheBytes == 44;
    }
    auto cached = MemorySyncLoop::testing_countedBackend(fixture.backend, true, limits, {}, true);
    for (std::size_t i = 0; i < records; ++i) {
        REQUIRE(cached->store("index/" + std::to_string(10'000 + i), bytes("value")).has_value());
    }
    std::optional<std::string> cursor;
    std::size_t observed = 0;
    do {
        auto page = cached->listPage("index/", cursor, 1);
        REQUIRE(page.has_value());
        observed += page.value().keys.size();
        cursor = page.value().nextCursor;
    } while (cursor);
    REQUIRE(observed == records);
    CHECK(fixture.backend.largestPageRequested() == 1);
    fixture.backend.resetListCalls();
    const auto first = cached->listPage("index/", std::nullopt, 1);
    REQUIRE(first.has_value());
    CHECK(first.value().keys == std::vector<std::string>{"index/10000"});
    CHECK(fixture.backend.listPageCalls() == (fits ? 0 : 1));
}

TEST_CASE("Optional listing cache allocation failure cannot fail a committed store",
          "[memory-sync][listing-cache][allocation]") {
    CountingBackendFixture fixture{"listing-allocation-store"};
    bool armed = false;
    int insertions = 0;
    auto hook = [&](std::string_view stage) {
        if (armed && stage == "store-insert" && ++insertions == 2) {
            throw std::bad_alloc{};
        }
    };
    auto cached = MemorySyncLoop::testing_countedBackend(fixture.backend, true, {}, hook, true);
    REQUIRE(cached->store("index/b", bytes("one")).has_value());
    REQUIRE(cached->listPage("", std::nullopt, 4).has_value());
    REQUIRE(cached->listPage("index/", std::nullopt, 4).has_value());
    armed = true;
    REQUIRE(cached->store("index/a", bytes("two")).has_value());
    CHECK(insertions == 2); // fail after the first of two overlapping snapshots was updated
    for (const auto* prefix : {"", "index/"}) {
        const auto listed = cached->listPage(prefix, std::nullopt, 4);
        REQUIRE(listed.has_value());
        CHECK(listed.value().keys == std::vector<std::string>{"index/a", "index/b"});
    }
}

TEST_CASE("Optional listing cache allocation failure releases its key budget",
          "[memory-sync][listing-cache][allocation]") {
    CountingBackendFixture fixture{"listing-allocation-prefix"};
    MemorySyncLimits limits;
    limits.maxCacheBytes = 7; // exactly the byte contents of index/b
    bool fail = true;
    auto hook = [&](std::string_view stage) {
        if (fail && stage == "prefix-insert") {
            fail = false;
            throw std::bad_alloc{};
        }
    };
    auto cached = MemorySyncLoop::testing_countedBackend(fixture.backend, true, limits, hook, true);
    REQUIRE(cached->store("index/b", bytes("one")).has_value());
    const auto uncached = cached->listPage("index/", std::nullopt, 1);
    REQUIRE(uncached.has_value());
    CHECK(uncached.value().keys == std::vector<std::string>{"index/b"});
    REQUIRE(cached->listPage("index/", std::nullopt, 1).has_value());
    fixture.backend.resetListCalls();
    REQUIRE(cached->listPage("index/", std::nullopt, 1).has_value());
    CHECK(fixture.backend.listPageCalls() == 0);
}

TEST_CASE("Optional listing cache append allocation failure cannot certify a suffix",
          "[memory-sync][listing-cache][allocation]") {
    CountingBackendFixture fixture{"listing-allocation-append"};
    bool fail = true;
    auto hook = [&](std::string_view stage) {
        if (fail && stage == "page-append") {
            fail = false;
            throw std::bad_alloc{};
        }
    };
    auto cached = MemorySyncLoop::testing_countedBackend(fixture.backend, true, {}, hook, true);
    for (const auto* key : {"index/b", "index/d", "index/f"}) {
        REQUIRE(cached->store(key, bytes("value")).has_value());
    }
    REQUIRE(cached->listPage("index/", std::nullopt, 1).has_value());
    REQUIRE(cached->listPage("index/", std::string_view{"index/b"}, 1).has_value());
    REQUIRE(cached->listPage("index/", std::string_view{"index/d"}, 1).has_value());
    const auto full = cached->listPage("index/", std::nullopt, 4);
    REQUIRE(full.has_value());
    CHECK(full.value().keys == std::vector<std::string>{"index/b", "index/d", "index/f"});
}

TEST_CASE("Exclusive non-filesystem listings preserve opaque cursors",
          "[memory-sync][listing-cache][opaque]") {
    BackendFixture fixture{"opaque-listing"};
    struct OpaqueBackend final : yams::storage::FilesystemBackend {
        bool remote{false};
        bool reportsFilesystem{false};
        mutable std::size_t calls{0};
        std::string getType() const override {
            return reportsFilesystem ? "filesystem" : "native-opaque";
        }
        bool isRemote() const override { return remote; }
        yams::Result<yams::storage::ObjectListPage> listPage(std::string_view,
                                                             std::optional<std::string_view> cursor,
                                                             std::size_t limit) const override {
            CHECK(limit == 1);
            ++calls;
            if (!cursor) {
                return yams::storage::ObjectListPage{{"index/a"}, "opaque-token"};
            }
            if (*cursor == "opaque-token") {
                return yams::storage::ObjectListPage{{"index/z"}, std::nullopt};
            }
            return yams::Error{yams::ErrorCode::InvalidArgument, "unexpected cursor"};
        }
    } backend;
    const auto mode = GENERATE(0, 1, 2);
    backend.remote = mode == 1;
    backend.reportsFilesystem = mode != 0; // mode 2 is a local filesystem-typed decorator
    REQUIRE(backend.initialize(fixture.config).has_value());
    bool failCacheCopy = false;
    auto hook = [&](std::string_view stage) {
        if (failCacheCopy && stage == "exact-page-copy") {
            failCacheCopy = false;
            throw std::bad_alloc{};
        }
    };
    auto cached = MemorySyncLoop::testing_countedBackend(backend, true, {}, hook);
    const auto first = cached->listPage("index/", std::nullopt, 1);
    REQUIRE(first.has_value());
    REQUIRE(first.value().nextCursor == "opaque-token");
    const auto last = cached->listPage("index/", first.value().nextCursor, 1);
    REQUIRE(last.has_value());
    CHECK(last.value().keys == std::vector<std::string>{"index/z"});
    CHECK_FALSE(last.value().nextCursor.has_value());
    const auto repeated = cached->listPage("index/", std::nullopt, 1);
    REQUIRE(repeated.has_value());
    CHECK(repeated.value().nextCursor == "opaque-token");
    CHECK(backend.calls == 2);
    failCacheCopy = true;
    const auto afterAllocationFailure = cached->listPage("index/", std::nullopt, 1);
    REQUIRE(afterAllocationFailure.has_value());
    CHECK(afterAllocationFailure.value().nextCursor == "opaque-token");
    CHECK(backend.calls == 3); // discard failed cache copy and retry the backend once
}

TEST_CASE("Asynchronous mutations permanently bypass exclusive listing snapshots",
          "[memory-sync][listing-cache][async]") {
    CountingBackendFixture fixture{"listing-async"};
    auto cached = MemorySyncLoop::testing_countedBackend(fixture.backend, true, {}, {}, true);
    REQUIRE(cached->store("index/a", bytes("one")).has_value());
    REQUIRE(cached->listPage("index/", std::nullopt, 4).has_value());
    fixture.backend.deferNextStoreAsync();
    const auto payload = bytes("two");
    auto pending = cached->storeAsync("index/b", payload);
    const auto beforeCompletion = cached->listPage("index/", std::nullopt, 4);
    REQUIRE(beforeCompletion.has_value());
    CHECK(beforeCompletion.value().keys == std::vector<std::string>{"index/a"});
    REQUIRE(fixture.backend.completeDeferredStore().has_value());
    REQUIRE(pending.get().has_value());
    fixture.backend.resetListCalls();
    for (int cycle = 0; cycle < 2; ++cycle) {
        const auto afterCompletion = cached->listPage("index/", std::nullopt, 4);
        REQUIRE(afterCompletion.has_value());
        CHECK(afterCompletion.value().keys == std::vector<std::string>{"index/a", "index/b"});
    }
    CHECK(fixture.backend.listPageCalls() == 2);
}

TEST_CASE("MemorySyncLoop uses bounded backend pages instead of full listings",
          "[memory-sync][sync-loop][bounds]") {
    BackendFixture fixture{"native-pages"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    for (int i = 0; i < 5; ++i) {
        REQUIRE(writer.publish("page-key-" + std::to_string(i), bytes("value")).has_value());
    }

    CountingFilesystemBackend readerBackend;
    REQUIRE(readerBackend.initialize(fixture.config).has_value());
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 2;
    MemorySyncLoop reader{readerBackend, "reader", "local-test-corpus", 1, false, limits};

    REQUIRE(reader.sync().has_value());
    CHECK(readerBackend.listPageCalls() == 1);
    CHECK(readerBackend.unboundedListCalls() == 0);
    CHECK(readerBackend.largestPageRequested() == 2);
    CHECK(reader.mergedRecordCount() == 0);
}

TEST_CASE("MemorySyncLoop checks cancellation and remote admission before listing",
          "[memory-sync][sync-loop][bounds]") {
    BackendFixture fixture{"remote-control"};
    CountingFilesystemBackend backend;
    REQUIRE(backend.initialize(fixture.config).has_value());
    backend.setRemote(true);

    bool cancelled = true;
    bool admitted = true;
    MemorySyncLoop loop{backend,
                        "reader",
                        "local-test-corpus",
                        1,
                        false,
                        {},
                        MemorySyncControl{[&] { return cancelled; }, [&] { return admitted; }}};
    auto result = loop.sync();
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::OperationCancelled);
    CHECK(backend.listPageCalls() == 0);

    cancelled = false;
    admitted = false;
    result = loop.sync();
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(backend.listPageCalls() == 0);

    admitted = true;
    REQUIRE(loop.sync().has_value());
    CHECK(backend.listPageCalls() == 1);
}

TEST_CASE("A resource-governor denial is not latched as an unsatisfiable sweep",
          "[memory-sync][sync-loop][bounds][backoff]") {
    BackendFixture fixture{"governor-not-latched"};
    CountingFilesystemBackend backend;
    REQUIRE(backend.initialize(fixture.config).has_value());
    backend.setRemote(true);

    bool admitted = false;
    MemorySyncLoop loop{backend,
                        "reader",
                        "local-test-corpus",
                        1,
                        false,
                        {},
                        MemorySyncControl{[] { return false; }, [&] { return admitted; }}};

    // Repeated governed denials are backpressure, not proof that the sweep cannot fit.
    for (int attempt = 0; attempt < 3; ++attempt) {
        const auto denied = loop.sync();
        REQUIRE_FALSE(denied.has_value());
        CHECK(denied.error().code == yams::ErrorCode::ResourceExhausted);
    }
    CHECK_FALSE(loop.testingSweepDegraded());
    CHECK(backend.listPageCalls() == 0);

    // Pressure lifted: the very next call must converge, with no backoff window.
    admitted = true;
    REQUIRE(loop.sync().has_value());
    CHECK(backend.listPageCalls() == 1);
}

TEST_CASE("MemorySyncLoop reconciles exclusive mutations and reuses settled state",
          "[memory-sync][sync-loop][bounds]") {
    BackendFixture writerFixture{"exclusive-writer"};
    CountingBackendFixture readerFixture{"exclusive-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer"};
    MemorySyncLoop reader{readerFixture.backend,
                          "reader",
                          "local-test-corpus",
                          1,
                          false,
                          {},
                          MemorySyncControl{.exclusiveBackend = true}};

    REQUIRE(reader.publish("local", bytes("one")).has_value());
    REQUIRE(reader.sync().has_value());
    readerFixture.backend.resetListCalls();
    readerFixture.backend.resetBlobCalls();

    for (int cycle = 0; cycle < 3; ++cycle) {
        auto idle = reader.sync();
        REQUIRE(idle.has_value());
        CHECK(idle.value().contains("local"));
    }
    CHECK(readerFixture.backend.listPageCalls() == 0);
    CHECK(readerFixture.backend.blobRetrieveCalls() == 0);

    REQUIRE(writer.publish("remote", bytes("two")).has_value());
    auto deltas = writer.exportLocalDeltasAfter({});
    REQUIRE(deltas.has_value());
    REQUIRE(reader.applyDeltas(deltas.value().deltas).has_value());
    auto afterIngress = reader.sync();
    REQUIRE(afterIngress.has_value());
    CHECK(afterIngress.value().contains("remote"));
    const auto remote = reader.read("remote");
    REQUIRE(remote.has_value());
    CHECK(text(remote.value()) == "two");

    readerFixture.backend.resetListCalls();
    REQUIRE(reader.sync().has_value());
    CHECK(readerFixture.backend.listPageCalls() == 0);

    REQUIRE(reader.publish("local", bytes("three")).has_value());
    auto afterPublish = reader.sync();
    REQUIRE(afterPublish.has_value());
    CHECK(afterPublish.value().contains("remote"));
    const auto value = reader.read("local");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "three");
}

TEST_CASE("MemorySyncLoop reuses exclusive-backend listings until it writes",
          "[memory-sync][sync-loop][bounds]") {
    CountingBackendFixture fixture{"exclusive-listings"};
    MemorySyncLoop loop{fixture.backend,
                        "reader",
                        "local-test-corpus",
                        1,
                        false,
                        {},
                        MemorySyncControl{.exclusiveBackend = true}};
    REQUIRE(loop.publish("slot", bytes("value")).has_value());
    REQUIRE(loop.sync().has_value());
    REQUIRE(loop.replayReadyErases().has_value());
    REQUIRE(loop.pendingErases().has_value());
    fixture.backend.resetListCalls();

    // One idle worker cycle per iteration: reconcile, replay ready erases, inspect the outbox.
    for (int cycle = 0; cycle < 3; ++cycle) {
        REQUIRE(loop.sync().has_value());
        REQUIRE(loop.replayReadyErases().has_value());
        auto pending = loop.pendingErases();
        REQUIRE(pending.has_value());
        CHECK(pending.value().empty());
    }
    CHECK(fixture.backend.listPageCalls() == 0);

    REQUIRE(loop.stageErase("slot", "gone").has_value());
    auto pending = loop.pendingErases();
    REQUIRE(pending.has_value());
    REQUIRE(pending.value().size() == 1);
    CHECK(pending.value().front().logicalKey == "slot");
}

TEST_CASE("MemorySyncLoop rescans a shared backend every cycle",
          "[memory-sync][sync-loop][bounds]") {
    CountingBackendFixture fixture{"shared-rescan"};
    MemorySyncLoop loop{fixture.backend, "reader"};
    REQUIRE(loop.publish("slot", bytes("value")).has_value());
    REQUIRE(loop.sync().has_value());
    fixture.backend.resetListCalls();
    REQUIRE(loop.sync().has_value());
    REQUIRE(loop.sync().has_value());
    CHECK(fixture.backend.listPageCalls() == 2);
}

TEST_CASE("MemorySyncLoop revisits direct ingress behind an active scan cursor",
          "[memory-sync][sync-loop][bounds][active-cursor]") {
    BackendFixture writerFixture{"active-cursor-writer"};
    BackendFixture readerFixture{"active-cursor-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer"};
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 1;
    MemorySyncLoop reader{readerFixture.backend,
                          "reader",
                          "local-test-corpus",
                          1,
                          false,
                          limits,
                          MemorySyncControl{.exclusiveBackend = true}};
    REQUIRE(writer.publish("m", bytes("middle")).has_value());
    REQUIRE(writer.publish("z", bytes("last")).has_value());
    auto initial = writer.exportLocalDeltasAfter({});
    REQUIRE(initial.has_value());
    REQUIRE(reader.applyDeltas(initial.value().deltas).has_value());
    REQUIRE(reader.sync().has_value()); // The one-record page leaves its cursor at index/m/....

    REQUIRE(writer.publish("a", bytes("before cursor")).has_value());
    auto changed = writer.exportLocalDeltasAfter({});
    REQUIRE(changed.has_value());
    REQUIRE(reader.applyDeltas(changed.value().deltas).has_value());
    for (int cycle = 0; cycle < 3; ++cycle) {
        const auto reconciled = reader.syncFully();
        REQUIRE(reconciled.has_value());
        CHECK(reconciled.value().contains("a"));
        CHECK(reconciled.value().contains("m"));
        CHECK(reconciled.value().contains("z"));
    }
}

TEST_CASE("MemorySyncLoop retains complete exclusive listings during active publication",
          "[.listing-cost][memory-sync][sync-loop][bounds][slow][active-listing]") {
    const auto records = GENERATE(std::size_t{511}, std::size_t{513}, std::size_t{1025});
    BackendFixture fixture{"active-listing"};
    const auto payload = bytes("seed payload");
    const auto payloadHash = digest(payload);
    REQUIRE(fixture.backend.store("blob/" + payloadHash, payload).has_value());
    // Seed valid bound envelopes before giving the loop exclusive ownership. This avoids
    // quadratic publication/reconciliation setup in a traversal characterization test.
    for (std::size_t i = 0; i < records; ++i) {
        MemoryIndexRecord record;
        record.entryHash = payloadHash;
        record.ts = {100, 0};
        record.origin = "seed";
        record.vv.observe(record.origin, i + 1);
        record.corpusId = "local-test-corpus";
        record.corpusEpoch = 1;
        record.operationId = makeOperationId(record.origin, i + 1);
        record.logicalKey = "seed/" + std::to_string(10'000 + i);
        record.recordKind = std::string(kMemoryValueRecordKind);
        const auto encoded = jsonBytes(nlohmann::json(record));
        REQUIRE(fixture.backend.store("index/" + record.logicalKey + "/" + digest(encoded), encoded)
                    .has_value());
    }
    MemorySyncLoop loop{fixture.backend,
                        "reader",
                        "local-test-corpus",
                        1,
                        false,
                        {},
                        MemorySyncControl{.exclusiveBackend = true}};
    yams::storage::FilesystemListingStats stats;
    stats.prefix = "index/";
    struct RestoreObservation {
        yams::storage::FilesystemListingStats* previous;
        ~RestoreObservation() { yams::storage::testing_setFilesystemListingStats(previous); }
    } restore{yams::storage::testing_setFilesystemListingStats(&stats)};

    const auto warm = loop.syncFully();
    REQUIRE(warm.has_value());
    REQUIRE(warm.value().size() == records);
    const auto warmWalks = stats.walks;
    // A certified complete filesystem listing should cost one traversal, regardless of pages.
    CHECK(warmWalks == 1);
    for (const auto* key : {"active/a", "active/z"}) {
        REQUIRE(loop.publish(key, bytes("live value")).has_value());
        const auto reconciled = loop.syncFully();
        REQUIRE(reconciled.has_value());
        CHECK(reconciled.value().contains(key));
        CHECK(reconciled.value().contains("seed/10000"));
    }
    CAPTURE(records, warmWalks, stats.walks, stats.entriesVisited);
    // Initial traversal stays paged. Subsequent key mutations must keep a completed,
    // bounded listing coherent rather than forcing another whole-store walk per page.
    CHECK(stats.walks == warmWalks);
    CHECK(loop.quarantinedRecordCount() == 0);
}

TEST_CASE("MemorySyncLoop replaces a same-key winner within a tight cache budget",
          "[memory-sync][sync-loop][bounds]") {
    BackendFixture fixture{"cache-replacement"};
    MemorySyncLoop writer{fixture.backend, "writer"};
    REQUIRE(writer.publish("slot", bytes("aaaa")).has_value());

    MemorySyncLimits limits;
    limits.maxCacheBytes = 4;
    limits.maxValueBytes = 4;
    MemorySyncLoop reader{fixture.backend, "reader", "local-test-corpus", 1, false, limits};
    REQUIRE(reader.sync().has_value());
    REQUIRE(writer.publish("slot", bytes("bbbb")).has_value());
    REQUIRE(reader.sync().has_value());

    const auto value = reader.readCached("slot");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "bbbb");
    CHECK(reader.testingCachedBlobCount() == 1);
}

TEST_CASE("MemorySyncLoop rejects values above its typed resident limit",
          "[memory-sync][sync-loop][bounds]") {
    BackendFixture fixture{"oversized-value"};
    MemorySyncLimits limits;
    limits.maxValueBytes = 4;
    MemorySyncLoop loop{fixture.backend, "writer", "local-test-corpus", 1, false, limits};

    const auto published = loop.publish("oversized", bytes("12345"));
    REQUIRE_FALSE(published.has_value());
    CHECK(published.error().code == yams::ErrorCode::InvalidArgument);
    const auto keys = fixture.backend.list("index/");
    REQUIRE(keys.has_value());
    const bool indexIsEmpty = keys.value().empty();
    CHECK(indexIsEmpty);
}

TEST_CASE("MemorySyncLoop writes a distinct index record for repeated content",
          "[memory-sync][sync-loop]") {
    BackendFixture fixture{"repeated-content"};
    MemorySyncLoop loop{fixture.backend, "A"};

    REQUIRE(loop.publish("same-key", bytes("same content")).has_value());
    REQUIRE(loop.publish("same-key", bytes("same content")).has_value());

    const auto indexKeys = fixture.backend.list("index/same-key/");
    REQUIRE(indexKeys.has_value());
    CHECK(indexKeys.value().size() == 2);

    const auto merged = loop.sync();
    REQUIRE(merged.has_value());
    CHECK(merged.value().at("same-key").vv.get("A") == 2);
}

TEST_CASE("MemorySyncLoop prefers causally-later writes", "[memory-sync][sync-loop]") {
    BackendFixture fixture{"causal"};
    MemorySyncLoop a{fixture.backend, "A"};
    MemorySyncLoop b{fixture.backend, "B"};

    // A writes first.
    REQUIRE(a.publish("causal-key", bytes("first")).has_value());

    // B observes A's write, then writes a causally-later value.
    REQUIRE(b.sync().has_value());
    REQUIRE(b.publish("causal-key", bytes("second")).has_value());

    // A syncs and must observe B's causally-later value (dominance beats timestamp).
    const auto mergedA = a.sync();
    REQUIRE(mergedA.has_value());
    REQUIRE(mergedA.value().count("causal-key") == 1);

    const auto value = a.read("causal-key");
    REQUIRE(value.has_value());
    CHECK(text(value.value()) == "second");
}

TEST_CASE("authenticated memory sync rejects forged writer IDs before blob hydration",
          "[memory-sync][auth][security]") {
    const bool exclusive = GENERATE(false, true);
    BackendFixture fixture{"auth-forged-writer"};
    const auto writerKey = generateWriterKey();
    const auto readerKey = generateWriterKey();
    const std::string writerId = "123e4567-e89b-42d3-a456-426614174000";
    const std::string readerId = "123e4567-e89b-42d3-a456-426614174001";
    const std::vector<TrustedWriterKey> trust = {
        {writerId, "writer-v1", writerKey.publicPem, false},
        {readerId, "reader-v1", readerKey.publicPem, false},
    };
    MemorySyncLoop writer{fixture.backend,
                          writerId,
                          "auth-corpus",
                          1,
                          false,
                          {},
                          {},
                          {},
                          writerAuth(writerId, "writer-v1", writerKey, trust)};
    REQUIRE(writer.publish("secret", bytes("authenticated payload")).has_value());

    auto keys = fixture.backend.list("index/secret/");
    REQUIRE(keys.has_value());
    REQUIRE(keys.value().size() == 1);
    const auto encoded = fixture.backend.retrieve(keys.value().front());
    REQUIRE(encoded.has_value());
    auto forged = nlohmann::json::parse(text(encoded.value()));
    forged["origin"] = "123e4567-e89b-42d3-a456-426614174099";
    forged["operationId"] = "123e4567-e89b-42d3-a456-426614174099:1";
    forged["vv"] = {{"counters_", {{"123e4567-e89b-42d3-a456-426614174099", 1}}}};
    (void)replaceEnvelope(fixture, keys.value().front(), forged);

    CountingFilesystemBackend readerBackend;
    REQUIRE(readerBackend.initialize(fixture.config).has_value());
    auto& readerStore = exclusive ? static_cast<yams::storage::IStorageBackend&>(fixture.backend)
                                  : static_cast<yams::storage::IStorageBackend&>(readerBackend);
    MemorySyncLoop reader{readerStore,
                          readerId,
                          "auth-corpus",
                          1,
                          false,
                          {},
                          MemorySyncControl{.exclusiveBackend = exclusive},
                          {},
                          writerAuth(readerId, "reader-v1", readerKey, trust)};
    REQUIRE(reader.sync().has_value());
    CHECK(reader.authFailureCount() == 1);
    if (!exclusive) {
        CHECK(readerBackend.blobRetrieveCalls() == 1); // only the valid sibling hydrates
    }
    CHECK(text(reader.readCached("secret").value()) == "authenticated payload");
    REQUIRE(reader.publish("local/cache-write", bytes("trusted local value")).has_value());
    const auto afterWrite = reader.syncFully();
    REQUIRE(afterWrite.has_value());
    CHECK(afterWrite.value().contains("secret"));
    CHECK(afterWrite.value().contains("local/cache-write"));
    CHECK(reader.quarantinedRecordCount() == 1);
    CHECK(text(reader.readCached("secret").value()) == "authenticated payload");
}

TEST_CASE("authenticated memory sync rejects invalid and revoked signatures",
          "[memory-sync][auth][security]") {
    const std::string writerId = "123e4567-e89b-42d3-a456-426614174000";
    const std::string readerId = "123e4567-e89b-42d3-a456-426614174001";
    const auto writerKey = generateWriterKey();
    const auto readerKey = generateWriterKey();

    SECTION("invalid signature") {
        BackendFixture fixture{"auth-invalid-signature"};
        const std::vector<TrustedWriterKey> trust = {
            {writerId, "writer-v1", writerKey.publicPem, false},
            {readerId, "reader-v1", readerKey.publicPem, false},
        };
        MemorySyncLoop writer{fixture.backend,
                              writerId,
                              "auth-corpus",
                              1,
                              false,
                              {},
                              {},
                              {},
                              writerAuth(writerId, "writer-v1", writerKey, trust)};
        REQUIRE(writer.publish("slot", bytes("value")).has_value());
        const auto keys = fixture.backend.list("index/slot/");
        REQUIRE(keys.has_value());
        REQUIRE(keys.value().size() == 1);
        const auto encoded = fixture.backend.retrieve(keys.value().front());
        REQUIRE(encoded.has_value());
        auto tampered = nlohmann::json::parse(text(encoded.value()));
        auto signature = tampered.at("signature").get<std::string>();
        signature.front() = signature.front() == '0' ? '1' : '0';
        tampered["signature"] = signature;
        REQUIRE(fixture.backend.remove(keys.value().front()).has_value());
        (void)replaceEnvelope(fixture, keys.value().front(), tampered);
        auto secondTampered = tampered;
        auto secondSignature = secondTampered.at("signature").get<std::string>();
        secondSignature.back() = secondSignature.back() == '0' ? '1' : '0';
        secondTampered["signature"] = secondSignature;
        (void)replaceEnvelope(fixture, keys.value().front(), secondTampered);

        CountingFilesystemBackend readerBackend;
        REQUIRE(readerBackend.initialize(fixture.config).has_value());
        MemorySyncLimits limits;
        limits.maxTrackedIdentities = 1;
        MemorySyncLoop reader{readerBackend,
                              readerId,
                              "auth-corpus",
                              1,
                              false,
                              limits,
                              {},
                              {},
                              writerAuth(readerId, "reader-v1", readerKey, trust)};
        REQUIRE(reader.sync().has_value());
        CHECK(reader.mergedRecordCount() == 0);
        CHECK(reader.quarantinedRecordCount() == 2);
        CHECK(reader.authFailureCount() == 1);
        CHECK(readerBackend.blobRetrieveCalls() == 0);
    }

    SECTION("revoked key") {
        BackendFixture fixture{"auth-revoked-key"};
        const std::vector<TrustedWriterKey> writerTrust = {
            {writerId, "writer-v1", writerKey.publicPem, false},
        };
        MemorySyncLoop writer{fixture.backend,
                              writerId,
                              "auth-corpus",
                              1,
                              false,
                              {},
                              {},
                              {},
                              writerAuth(writerId, "writer-v1", writerKey, writerTrust)};
        REQUIRE(writer.publish("slot", bytes("value")).has_value());

        const std::vector<TrustedWriterKey> readerTrust = {
            {writerId, "writer-v1", writerKey.publicPem, true},
            {readerId, "reader-v1", readerKey.publicPem, false},
        };
        MemorySyncLoop reader{fixture.backend,
                              readerId,
                              "auth-corpus",
                              1,
                              false,
                              {},
                              {},
                              {},
                              writerAuth(readerId, "reader-v1", readerKey, readerTrust)};
        REQUIRE(reader.sync().has_value());
        CHECK(reader.mergedRecordCount() == 0);
        CHECK(reader.authFailureCount() == 1);
    }
}

TEST_CASE("direct memory deltas apply, replay idempotently, and propagate tombstones",
          "[memory-sync][direct-delta]") {
    BackendFixture writerFixture{"direct-delta-writer"};
    BackendFixture readerFixture{"direct-delta-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "direct-corpus", 1};
    MemorySyncLoop reader{readerFixture.backend, "reader", "direct-corpus", 1};

    REQUIRE(writer.publish("user/key", bytes("v1")).has_value());
    auto firstBatch = writer.exportLocalDeltasAfter({});
    REQUIRE(firstBatch.has_value());
    REQUIRE(firstBatch.value().deltas.size() == 1);
    CHECK_FALSE(firstBatch.value().hasMore);

    auto firstApply = reader.applyDeltas(firstBatch.value().deltas);
    REQUIRE(firstApply.has_value());
    CHECK(firstApply.value().merged == 1);
    CHECK(firstApply.value().replayed == 0);
    REQUIRE(reader.readCached("user/key").has_value());
    CHECK(text(reader.readCached("user/key").value()) == "v1");

    auto replay = reader.applyDeltas(firstBatch.value().deltas);
    REQUIRE(replay.has_value());
    CHECK(replay.value().merged == 0);
    CHECK(replay.value().replayed == 1);

    REQUIRE(writer.publish("user/key", bytes("v2")).has_value());
    auto updateBatch = writer.exportLocalDeltasAfter(reader.currentVersion());
    REQUIRE(updateBatch.has_value());
    REQUIRE(updateBatch.value().deltas.size() == 1);
    auto updated = reader.applyDeltas(updateBatch.value().deltas);
    REQUIRE(updated.has_value());
    CHECK(updated.value().merged == 1);
    CHECK(text(reader.readCached("user/key").value()) == "v2");

    REQUIRE(writer.publish("user/shared", bytes("v2")).has_value());
    auto sharedBatch = writer.exportLocalDeltasAfter(reader.currentVersion());
    REQUIRE(sharedBatch.has_value());
    REQUIRE(reader.applyDeltas(sharedBatch.value().deltas).has_value());
    CHECK(text(reader.readCached("user/shared").value()) == "v2");

    REQUIRE(writer.erase("user/key", "key").has_value());
    auto deleteBatch = writer.exportLocalDeltasAfter(reader.currentVersion());
    REQUIRE(deleteBatch.has_value());
    REQUIRE(deleteBatch.value().deltas.size() == 1);
    auto deleted = reader.applyDeltas(deleteBatch.value().deltas);
    REQUIRE(deleted.has_value());
    CHECK(deleted.value().merged == 1);
    CHECK_FALSE(reader.readCached("user/key").has_value());
    REQUIRE(reader.readCached("user/shared").has_value());
    CHECK(text(reader.readCached("user/shared").value()) == "v2");
}

TEST_CASE("direct local writer restores its causal counter across paged restart",
          "[memory-sync][direct-delta][restart]") {
    BackendFixture fixture{"direct-delta-restart"};
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 2;
    {
        MemorySyncLoop writer{fixture.backend, "writer", "restart-corpus", 1, false, limits};
        REQUIRE(writer.publish("user/one", bytes("one")).has_value());
        REQUIRE(writer.publish("user/two", bytes("two")).has_value());
        REQUIRE(writer.publish("user/three", bytes("three")).has_value());
    }
    MemorySyncLoop restarted{fixture.backend, "writer", "restart-corpus", 1, false, limits};
    REQUIRE(restarted.publish("user/four", bytes("four")).has_value());
    auto exported = restarted.exportLocalDeltasAfter({}, 8);
    REQUIRE(exported.has_value());
    REQUIRE(exported.value().deltas.size() == 4);
    std::set<std::string> operationIds;
    for (const auto& delta : exported.value().deltas) {
        operationIds.insert(delta.record.operationId);
    }
    CHECK(operationIds.size() == 4);
    CHECK(exported.value().deltas.back().record.vv.get("writer") == 4);
}

TEST_CASE("direct memory delta export resumes from causal watermark",
          "[memory-sync][direct-delta]") {
    BackendFixture writerFixture{"direct-delta-gap-writer"};
    BackendFixture readerFixture{"direct-delta-gap-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "direct-gap-corpus", 1};
    MemorySyncLoop reader{readerFixture.backend, "reader", "direct-gap-corpus", 1};

    REQUIRE(writer.publish("user/one", bytes("one")).has_value());
    REQUIRE(writer.publish("user/two", bytes("two")).has_value());
    REQUIRE(writer.publish("user/three", bytes("three")).has_value());

    auto first = writer.exportLocalDeltasAfter({}, 2);
    REQUIRE(first.has_value());
    CHECK(first.value().deltas.size() == 2);
    CHECK(first.value().hasMore);
    REQUIRE(reader.applyDeltas(first.value().deltas).has_value());
    CHECK(reader.currentVersion().get("writer") == 2);

    auto second = writer.exportLocalDeltasAfter(reader.currentVersion(), 2);
    REQUIRE(second.has_value());
    CHECK(second.value().deltas.size() == 1);
    CHECK_FALSE(second.value().hasMore);
    REQUIRE(reader.applyDeltas(second.value().deltas).has_value());
    CHECK(reader.currentVersion().get("writer") == 3);
    CHECK(reader.readCached("user/one").has_value());
    CHECK(reader.readCached("user/two").has_value());
    CHECK(reader.readCached("user/three").has_value());
}

TEST_CASE("bounded direct export reads only the requested writer-counter window",
          "[memory-sync][direct-delta][limits][lazy-export]") {
    CountingBackendFixture fixture{"direct-delta-lazy-export"};
    MemorySyncLoop writer{fixture.backend, "writer", "direct-lazy-corpus", 1};
    for (int index = 0; index < 64; ++index) {
        REQUIRE(writer
                    .publish("user/key-" + std::to_string(index),
                             bytes("value-" + std::to_string(index)))
                    .has_value());
    }

    fixture.backend.resetListCalls();
    fixture.backend.resetBlobCalls();
    auto first = writer.exportLocalDeltasAfter({}, 2);
    REQUIRE(first.has_value());
    CHECK(first.value().deltas.size() == 2);
    CHECK(first.value().hasMore);
    CHECK(fixture.backend.listPageCalls() == 0);
    CHECK(fixture.backend.unboundedListCalls() == 0);
    CHECK(fixture.backend.blobRetrieveCalls() == 2);
}

TEST_CASE("writer window selection hydrates only the wire-bounded prefix and one lookahead",
          "[memory-sync][direct-delta][limits][wire-window]") {
    CountingBackendFixture fixture{"direct-delta-wire-window"};
    MemorySyncLoop writer{fixture.backend, "writer", "direct-wire-corpus", 1};
    const std::string value(1200, 'x');
    REQUIRE(writer.publish("user/one", bytes(value)).has_value());
    REQUIRE(writer.publish("user/two", bytes(value)).has_value());
    REQUIRE(writer.publish("user/three", bytes(value)).has_value());

    fixture.backend.resetBlobCalls();
    auto window = writer.localHistoryWindowAfter(0, 3, 2200);
    REQUIRE(window.has_value());
    CHECK(window.value().counter == 1);
    CHECK(fixture.backend.blobRetrieveCalls() == 2);
}

TEST_CASE("historical writer commitments persist independently of identity limits",
          "[memory-sync][direct-delta][commitment][restart][limits]") {
    CountingBackendFixture fixture{"direct-history-prefix-index"};
    MemorySyncLimits limits;
    limits.maxTrackedIdentities = 1;
    WriterHistoryCommitment middle;
    {
        // Build complete durable history under normal scan admission. The restricted
        // restart below must resolve its historical commitment without scanning at all.
        MemorySyncLoop writer{fixture.backend, "writer", "prefix-index-corpus", 1};
        REQUIRE(writer.publish("user/one", bytes("one")).has_value());
        REQUIRE(writer.publish("user/two", bytes("two")).has_value());
        middle = writer.replicationState().commitments.at("writer");
        REQUIRE(writer.publish("user/three", bytes("three")).has_value());
    }

    MemorySyncLoop restarted{fixture.backend, "writer", "prefix-index-corpus", 1, false, limits};
    fixture.backend.resetListCalls();
    auto resolved = restarted.localHistoryCommitmentAt(2);
    REQUIRE(resolved.has_value());
    CHECK(resolved.value() == middle);
    CHECK(fixture.backend.listPageCalls() == 0);
    CHECK(fixture.backend.unboundedListCalls() == 0);
}

TEST_CASE("legacy writer history migrates once into bounded counter entries",
          "[memory-sync][direct-delta][commitment][migration][restart]") {
    CountingBackendFixture fixture{"direct-history-prefix-migration"};
    WriterHistoryCommitment firstPrefix;
    {
        MemorySyncLoop writer{fixture.backend, "writer", "prefix-migration-corpus", 1};
        REQUIRE(writer.publish("user/one", bytes("one")).has_value());
        firstPrefix = writer.replicationState().commitments.at("writer");
        REQUIRE(writer.publish("user/two", bytes("two")).has_value());
        REQUIRE(writer.publish("user/three", bytes("three")).has_value());
    }
    auto entries = fixture.backend.list("history/counter-v1/");
    REQUIRE(entries.has_value());
    REQUIRE(entries.value().size() == 3);
    for (const auto& key : entries.value()) {
        REQUIRE(fixture.backend.remove(key).has_value());
    }

    {
        MemorySyncLoop upgraded{fixture.backend, "writer", "prefix-migration-corpus", 1};
        REQUIRE(upgraded.publish("user/four", bytes("four")).has_value());
    }

    MemorySyncLoop restarted{fixture.backend, "writer", "prefix-migration-corpus", 1};
    fixture.backend.resetListCalls();
    auto exported = restarted.exportLocalDeltasAfter({}, 4);
    REQUIRE(exported.has_value());
    CHECK(exported.value().deltas.size() == 4);
    CHECK(fixture.backend.listPageCalls() > 0);

    fixture.backend.resetListCalls();
    auto persisted = restarted.localHistoryCommitmentAt(1);
    REQUIRE(persisted.has_value());
    CHECK(persisted.value() == firstPrefix);
    CHECK(fixture.backend.listPageCalls() == 0);
}

TEST_CASE("legacy writer history migration resumes across bounded restart attempts",
          "[memory-sync][direct-delta][commitment][migration][restart][limits]") {
    CountingBackendFixture fixture{"direct-history-resumable-migration"};
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 2;
    limits.maxHistoryMigrationObjects = 2;
    {
        MemorySyncLoop writer{fixture.backend, "writer", "resumable-migration-corpus", 1,
                              false,           limits};
        for (int index = 0; index < 5; ++index) {
            REQUIRE(writer
                        .publish("user/key-" + std::to_string(index),
                                 bytes("value-" + std::to_string(index)))
                        .has_value());
        }
    }
    auto entries = fixture.backend.list("history/counter-v1/");
    REQUIRE(entries.has_value());
    for (const auto& key : entries.value()) {
        REQUIRE(fixture.backend.remove(key).has_value());
    }

    bool completed = false;
    std::size_t attempts = 0;
    for (; attempts < 10 && !completed; ++attempts) {
        MemorySyncLoop restarted{fixture.backend, "writer", "resumable-migration-corpus", 1,
                                 false,           limits};
        auto exported = restarted.exportLocalDeltasAfter({}, 5);
        if (exported) {
            CHECK(exported.value().deltas.size() == 5);
            completed = true;
        } else {
            CHECK(exported.error().code == yams::ErrorCode::OperationInProgress);
        }
    }
    CHECK(completed);
    CHECK(attempts > 1);
}

TEST_CASE("legacy migration rejects an individually oversized envelope without livelock",
          "[memory-sync][direct-delta][commitment][migration][limits]") {
    CountingBackendFixture fixture{"direct-history-oversized-migration"};
    {
        MemorySyncLoop writer{fixture.backend, "writer", "oversized-migration-corpus", 1};
        REQUIRE(writer.publish("user/one", bytes("one")).has_value());
    }
    auto entries = fixture.backend.list("history/counter-v1/");
    REQUIRE(entries.has_value());
    for (const auto& key : entries.value()) {
        REQUIRE(fixture.backend.remove(key).has_value());
    }

    MemorySyncLimits limits;
    limits.maxHistoryMigrationBytes = 1;
    for (int attempt = 0; attempt < 2; ++attempt) {
        MemorySyncLoop restarted{fixture.backend, "writer", "oversized-migration-corpus", 1,
                                 false,           limits};
        auto exported = restarted.exportLocalDeltasAfter({}, 1);
        REQUIRE_FALSE(exported.has_value());
        CHECK(exported.error().code == yams::ErrorCode::ResourceExhausted);
    }
}

TEST_CASE("direct memory deltas enforce the merged-key limit atomically",
          "[memory-sync][direct-delta][limits]") {
    BackendFixture writerFixture{"direct-delta-key-limit-writer"};
    BackendFixture readerFixture{"direct-delta-key-limit-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "direct-limit-corpus", 1};
    MemorySyncLimits limits;
    limits.maxMergedKeys = 1;
    MemorySyncLoop reader{readerFixture.backend, "reader", "direct-limit-corpus", 1, false, limits};

    REQUIRE(writer.publish("user/one", bytes("one")).has_value());
    REQUIRE(writer.publish("user/two", bytes("two")).has_value());
    auto batch = writer.exportLocalDeltasAfter({}, 2);
    REQUIRE(batch.has_value());
    REQUIRE(batch.value().deltas.size() == 2);

    auto applied = reader.applyDeltas(batch.value().deltas);
    REQUIRE_FALSE(applied.has_value());
    CHECK(applied.error().code == yams::ErrorCode::ResourceExhausted);
    CHECK(reader.currentVersion().empty());
    CHECK(reader.mergedRecordCount() == 0);
    CHECK_FALSE(reader.readCached("user/one").has_value());
    CHECK_FALSE(reader.readCached("user/two").has_value());
}

TEST_CASE("direct memory deltas quarantine tampering and writer-operation forks",
          "[memory-sync][direct-delta][security]") {
    BackendFixture writerFixture{"direct-delta-security-writer"};
    BackendFixture readerFixture{"direct-delta-security-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "direct-security-corpus", 1};
    MemorySyncLoop reader{readerFixture.backend, "reader", "direct-security-corpus", 1};

    REQUIRE(writer.publish("user/key", bytes("original")).has_value());
    auto batch = writer.exportLocalDeltasAfter({});
    REQUIRE(batch.has_value());
    REQUIRE(batch.value().deltas.size() == 1);

    auto tampered = batch.value().deltas;
    tampered.front().payload.front() = std::byte{'x'};
    auto rejected = reader.applyDeltas(tampered);
    REQUIRE(rejected.has_value());
    CHECK(rejected.value().merged == 0);
    CHECK(rejected.value().quarantined.size() == 1);
    CHECK(reader.currentVersion().empty());

    auto applied = reader.applyDeltas(batch.value().deltas);
    REQUIRE(applied.has_value());
    CHECK(applied.value().merged == 1);

    auto fork = batch.value().deltas;
    fork.front().payload = bytes("forked");
    fork.front().record.entryHash = digest(fork.front().payload);
    auto forked = reader.applyDeltas(fork);
    REQUIRE(forked.has_value());
    CHECK(forked.value().merged == 0);
    CHECK(forked.value().quarantined.size() == 1);
    CHECK(reader.writerQuarantined("writer"));
    CHECK_FALSE(reader.readCached("user/key").has_value());

    MemorySyncLoop restarted{readerFixture.backend, "reader", "direct-security-corpus", 1};
    REQUIRE(restarted.syncFully().has_value());
    CHECK(restarted.writerQuarantined("writer"));
    CHECK_FALSE(restarted.readCached("user/key").has_value());
}

TEST_CASE("writer history commitments cover complete ordered history and survive restart",
          "[memory-sync][direct-delta][commitment][restart]") {
    BackendFixture firstFixture{"direct-commitment-first"};
    MemorySyncLoop first{firstFixture.backend, "writer", "commitment-corpus", 1};
    REQUIRE(first.publish("user/history", bytes("first-history")).has_value());
    const auto prefix = first.replicationState().commitments.at("writer");
    REQUIRE(prefix.counter == 1);
    REQUIRE(first.publish("user/frontier", bytes("same-frontier-value")).has_value());
    REQUIRE(first.syncFully().has_value());
    const auto firstState = first.replicationState();
    REQUIRE(firstState.commitments.contains("writer"));
    CHECK(firstState.commitments.at("writer").counter == 2);
    CHECK(isSha256Digest(firstState.commitments.at("writer").digest));

    MemorySyncLoop restarted{firstFixture.backend, "writer", "commitment-corpus", 1};
    REQUIRE(restarted.syncFully().has_value());
    CHECK(restarted.replicationState().commitments == firstState.commitments);
    auto resolvedPrefix = restarted.localHistoryCommitmentAt(1);
    REQUIRE(resolvedPrefix.has_value());
    CHECK(resolvedPrefix.value() == prefix);

    BackendFixture secondFixture{"direct-commitment-second"};
    MemorySyncLoop second{secondFixture.backend, "writer", "commitment-corpus", 1};
    REQUIRE(second.publish("user/history", bytes("different-history")).has_value());
    REQUIRE(second.publish("user/frontier", bytes("same-frontier-value")).has_value());
    REQUIRE(second.syncFully().has_value());
    const auto secondState = second.replicationState();
    REQUIRE(secondState.commitments.contains("writer"));
    CHECK(secondState.commitments.at("writer").counter == 2);
    CHECK(secondState.commitments.at("writer").digest !=
          firstState.commitments.at("writer").digest);
}

TEST_CASE("authenticated cold bootstrap installs a bounded multiwriter snapshot and restarts",
          "[memory-sync][cold-bootstrap][restart][security]") {
    BackendFixture sourceFixture{"cold-bootstrap-source"};
    BackendFixture targetFixture{"cold-bootstrap-target"};
    const auto keyA = generateWriterKey();
    const auto keyB = generateWriterKey();
    const auto keyC = generateWriterKey();
    const std::vector<TrustedWriterKey> trust = {
        {"writer-a", "a-v1", keyA.publicPem, false},
        {"writer-b", "b-v1", keyB.publicPem, false},
        {"writer-c", "c-v1", keyC.publicPem, false},
    };
    MemorySyncLoop writerA{sourceFixture.backend,
                           "writer-a",
                           "auth-corpus",
                           1,
                           false,
                           {},
                           {},
                           {},
                           writerAuth("writer-a", "a-v1", keyA, trust)};
    MemorySyncLoop writerB{sourceFixture.backend,
                           "writer-b",
                           "auth-corpus",
                           1,
                           false,
                           {},
                           {},
                           {},
                           writerAuth("writer-b", "b-v1", keyB, trust)};
    REQUIRE(writerA.publish("user/a", bytes("from-a")).has_value());
    REQUIRE(writerB.syncFully().has_value());
    REQUIRE(writerB.publish("user/b", bytes("from-b")).has_value());
    REQUIRE(writerA.syncFully().has_value());
    const auto frozen = writerA.replicationState();
    REQUIRE(frozen.version.get("writer-a") == 1);
    REQUIRE(frozen.version.get("writer-b") == 1);
    auto snapshot = writerA.exportColdBootstrap(frozen, 8, 1024);
    REQUIRE(snapshot.has_value());
    REQUIRE(snapshot.value().winners.size() == 2);

    MemorySyncLoop target{targetFixture.backend,
                          "writer-c",
                          "auth-corpus",
                          1,
                          false,
                          {},
                          {},
                          {},
                          writerAuth("writer-c", "c-v1", keyC, trust)};
    auto tampered = snapshot.value();
    tampered.winners.front().payload.front() = std::byte{'x'};
    auto rejected = target.applyColdBootstrap(tampered, "writer-a", 8, 1024);
    REQUIRE_FALSE(rejected.has_value());
    CHECK(rejected.error().code == yams::ErrorCode::InvalidData);
    CHECK(target.currentVersion().empty());
    CHECK(target.mergedRecordCount() == 0);

    auto imported = target.applyColdBootstrap(snapshot.value(), "writer-a", 8, 1024);
    REQUIRE(imported.has_value());
    CHECK(imported.value().merged == 2);
    CHECK(target.currentVersion().counters() == frozen.version.counters());
    REQUIRE(target.readCached("user/a").has_value());
    REQUIRE(target.readCached("user/b").has_value());

    MemorySyncLoop restarted{targetFixture.backend,
                             "writer-c",
                             "auth-corpus",
                             1,
                             false,
                             {},
                             {},
                             {},
                             writerAuth("writer-c", "c-v1", keyC, trust)};
    REQUIRE(restarted.syncFully().has_value());
    CHECK(restarted.currentVersion().counters() == frozen.version.counters());
    CHECK(restarted.replicationState().commitments == frozen.commitments);
    REQUIRE(restarted.readCached("user/a").has_value());
    REQUIRE(restarted.readCached("user/b").has_value());

    auto second = restarted.applyColdBootstrap(snapshot.value(), "writer-a", 8, 1024);
    REQUIRE_FALSE(second.has_value());
    CHECK(second.error().code == yams::ErrorCode::InvalidState);
}

TEST_CASE("cold bootstrap journal completes after checkpoint failure and restart",
          "[memory-sync][cold-bootstrap][restart][failure-injection]") {
    BackendFixture sourceFixture{"cold-bootstrap-recovery-source"};
    CountingBackendFixture targetFixture{"cold-bootstrap-recovery-target"};
    const auto sourceKey = generateWriterKey();
    const auto targetKey = generateWriterKey();
    const std::vector<TrustedWriterKey> trust = {
        {"source", "source-v1", sourceKey.publicPem, false},
        {"target", "target-v1", targetKey.publicPem, false},
    };
    MemorySyncLoop source{sourceFixture.backend,
                          "source",
                          "auth-corpus",
                          1,
                          false,
                          {},
                          {},
                          {},
                          writerAuth("source", "source-v1", sourceKey, trust)};
    REQUIRE(source.publish("user/recover", bytes("recover-me")).has_value());
    REQUIRE(source.syncFully().has_value());
    auto snapshot = source.exportColdBootstrap(source.replicationState(), 4, 1024);
    REQUIRE(snapshot.has_value());

    targetFixture.backend.failNextStore("checkpoint/replication-state-v1/");
    {
        MemorySyncLoop target{targetFixture.backend,
                              "target",
                              "auth-corpus",
                              1,
                              false,
                              {},
                              {},
                              {},
                              writerAuth("target", "target-v1", targetKey, trust)};
        auto failed = target.applyColdBootstrap(snapshot.value(), "source", 4, 1024);
        REQUIRE_FALSE(failed.has_value());
        CHECK(failed.error().code == yams::ErrorCode::IOError);
        CHECK(target.currentVersion().empty());
        CHECK(target.mergedRecordCount() == 0);
    }

    MemorySyncLoop restarted{targetFixture.backend,
                             "target",
                             "auth-corpus",
                             1,
                             false,
                             {},
                             {},
                             {},
                             writerAuth("target", "target-v1", targetKey, trust)};
    REQUIRE(restarted.syncFully().has_value());
    CHECK(restarted.currentVersion().counters() == snapshot.value().frontier.counters());
    REQUIRE(restarted.readCached("user/recover").has_value());
    CHECK(text(restarted.readCached("user/recover").value()) == "recover-me");
}

TEST_CASE("staged deltas cannot exceed the handshake-frozen writer frontier",
          "[memory-sync][direct-delta][commitment][security]") {
    BackendFixture writerFixture{"direct-frozen-frontier-writer"};
    BackendFixture readerFixture{"direct-frozen-frontier-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "frozen-frontier-corpus", 1};
    MemorySyncLoop reader{readerFixture.backend, "reader", "frozen-frontier-corpus", 1};
    REQUIRE(writer.publish("user/one", bytes("one")).has_value());
    const auto frozen = writer.replicationState().commitments.at("writer");
    auto first = writer.exportLocalDeltasAfter({});
    REQUIRE(first.has_value());
    REQUIRE(reader.applyDeltas(first.value().deltas).has_value());
    REQUIRE(writer.publish("user/two", bytes("two")).has_value());
    auto later = writer.exportLocalDeltasAfter(reader.currentVersion());
    REQUIRE(later.has_value());
    REQUIRE(later.value().deltas.size() == 1);

    auto rejected = reader.validateHistoryExtension("writer", later.value().deltas, frozen);
    REQUIRE(rejected.has_value());
    REQUIRE(rejected.value().has_value());
    CHECK(rejected.value()->code == yams::ErrorCode::InvalidData);
    CHECK_FALSE(reader.readCached("user/two").has_value());
}

TEST_CASE("a staged window may overlap a durable prefix advanced after its handshake",
          "[memory-sync][direct-delta][commitment][concurrency]") {
    BackendFixture writerFixture{"direct-overlap-writer"};
    BackendFixture readerFixture{"direct-overlap-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "overlap-corpus", 1};
    MemorySyncLoop reader{readerFixture.backend, "reader", "overlap-corpus", 1};
    for (const auto* key : {"user/one", "user/two", "user/three"}) {
        REQUIRE(writer.publish(key, bytes(key)).has_value());
    }
    const auto frontier = writer.replicationState().commitments.at("writer");
    auto window = writer.exportLocalDeltasAfter({}, 8);
    REQUIRE(window.has_value());
    REQUIRE(window.value().deltas.size() == 3);

    // A concurrent session applied the first operation after this window was frozen.
    REQUIRE(reader.applyDeltas(std::span(window.value().deltas).first(1)).has_value());
    auto overlapping = reader.validateHistoryExtension("writer", window.value().deltas, frontier);
    REQUIRE(overlapping.has_value());
    CHECK_FALSE(overlapping.value().has_value());

    // Replayed operations are not applied again, but they must still be valid records.
    auto tamperedReplay = window.value().deltas;
    tamperedReplay.front().payload = bytes("tampered");
    auto invalidReplay = reader.validateHistoryExtension("writer", tamperedReplay, frontier);
    REQUIRE(invalidReplay.has_value());
    REQUIRE(invalidReplay.value().has_value());
    CHECK(invalidReplay.value()->code == yams::ErrorCode::HashMismatch);

    // ... and one that already reached the frontier leaves an empty window to verify.
    REQUIRE(reader.applyDeltas(window.value().deltas).has_value());
    auto empty = reader.validateHistoryExtension("writer", {}, frontier);
    REQUIRE(empty.has_value());
    CHECK_FALSE(empty.value().has_value());
    auto replayedInvalid = reader.validateHistoryExtension("writer", tamperedReplay, frontier);
    REQUIRE(replayedInvalid.has_value());
    CHECK(replayedInvalid.value().has_value());

    auto forged = frontier;
    forged.digest = std::string(64, '0');
    auto contradicted = reader.validateHistoryExtension("writer", {}, forged);
    REQUIRE(contradicted.has_value());
    REQUIRE(contradicted.value().has_value());
    CHECK(contradicted.value()->code == yams::ErrorCode::HashMismatch);
}

TEST_CASE("a staged window that rewrites history above the durable prefix is a violation",
          "[memory-sync][direct-delta][commitment][security]") {
    BackendFixture writerFixture{"direct-rewrite-writer"};
    BackendFixture forkFixture{"direct-rewrite-fork"};
    BackendFixture readerFixture{"direct-rewrite-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "rewrite-corpus", 1};
    MemorySyncLoop fork{forkFixture.backend, "writer", "rewrite-corpus", 1};
    MemorySyncLoop reader{readerFixture.backend, "reader", "rewrite-corpus", 1};
    REQUIRE(writer.publish("user/one", bytes("one")).has_value());
    REQUIRE(writer.publish("user/two", bytes("two")).has_value());
    REQUIRE(fork.publish("user/other", bytes("other")).has_value());
    REQUIRE(fork.publish("user/forked", bytes("forked")).has_value());
    auto prefix = writer.exportLocalDeltasAfter({}, 1);
    REQUIRE(prefix.has_value());
    REQUIRE(reader.applyDeltas(prefix.value().deltas).has_value());

    // The fork's window overlaps the reader's prefix with a different first operation. That
    // replayed counter is skipped, but the fork's second operation cannot link the reader's
    // durable prefix to the fork's frontier.
    auto forkWindow = fork.exportLocalDeltasAfter({}, 8);
    REQUIRE(forkWindow.has_value());
    const auto forkFrontier = fork.replicationState().commitments.at("writer");
    auto forked =
        reader.validateHistoryExtension("writer", forkWindow.value().deltas, forkFrontier);
    REQUIRE(forked.has_value());
    REQUIRE(forked.value().has_value());
    CHECK(forked.value()->code == yams::ErrorCode::HashMismatch);

    auto honestWindow = writer.exportLocalDeltasAfter({}, 8);
    REQUIRE(honestWindow.has_value());
    const auto frontier = writer.replicationState().commitments.at("writer");
    auto tampered = honestWindow.value().deltas;
    tampered.back().payload = bytes("tampered");
    auto rejected = reader.validateHistoryExtension("writer", tampered, frontier);
    REQUIRE(rejected.has_value());
    REQUIRE(rejected.value().has_value());
    CHECK(rejected.value()->code == yams::ErrorCode::HashMismatch);

    auto foreign =
        reader.validateHistoryExtension("someone-else", honestWindow.value().deltas, frontier);
    REQUIRE(foreign.has_value());
    REQUIRE(foreign.value().has_value());
    CHECK(foreign.value()->code == yams::ErrorCode::Unauthorized);

    auto honest = reader.validateHistoryExtension("writer", honestWindow.value().deltas, frontier);
    REQUIRE(honest.has_value());
    CHECK_FALSE(honest.value().has_value());
}

TEST_CASE("a replayed counter that differs from durable history is a violation after restart",
          "[memory-sync][direct-delta][commitment][restart][security]") {
    BackendFixture writerFixture{"direct-replay-fork-writer"};
    BackendFixture readerFixture{"direct-replay-fork-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "replay-fork-corpus", 1};
    REQUIRE(writer.publish("user/one", bytes("one")).has_value());
    REQUIRE(writer.publish("user/two", bytes("two")).has_value());
    const auto frontier = writer.replicationState().commitments.at("writer");
    auto window = writer.exportLocalDeltasAfter({}, 8);
    REQUIRE(window.has_value());
    REQUIRE(window.value().deltas.size() == 2);

    // A different, internally valid record for the writer's first counter: same operation id,
    // different content, so a different record hash.
    auto forkedFirst = window.value().deltas.front();
    forkedFirst.payload = bytes("forked-one");
    forkedFirst.record.entryHash = digest(forkedFirst.payload);

    SECTION("a window that only replays durable counters") {
        {
            MemorySyncLoop reader{readerFixture.backend, "reader", "replay-fork-corpus", 1};
            REQUIRE(reader.applyDeltas(window.value().deltas).has_value());
        }
        // After a restart the reader no longer remembers the applied operation ids, so durable
        // history is the only record of what each counter committed to.
        MemorySyncLoop restarted{readerFixture.backend, "reader", "replay-fork-corpus", 1};
        const std::vector<MemoryDelta> forkedWindow{forkedFirst};
        auto forked = restarted.validateHistoryExtension("writer", forkedWindow, frontier);
        REQUIRE(forked.has_value());
        REQUIRE(forked.value().has_value());
        CHECK(forked.value()->code == yams::ErrorCode::HashMismatch);

        // An identical replay is still a harmless no-op.
        auto replay = restarted.validateHistoryExtension("writer", window.value().deltas, frontier);
        REQUIRE(replay.has_value());
        CHECK_FALSE(replay.value().has_value());
        auto applied = restarted.applyDeltas(window.value().deltas);
        REQUIRE(applied.has_value());
        CHECK(applied.value().replayed == 2);
        CHECK(applied.value().merged == 0);
        CHECK(applied.value().quarantined.empty());
        CHECK_FALSE(restarted.writerQuarantined("writer"));
    }

    SECTION("a window that overlaps the durable prefix and then extends it") {
        {
            MemorySyncLoop reader{readerFixture.backend, "reader", "replay-fork-corpus", 1};
            REQUIRE(reader.applyDeltas(std::span(window.value().deltas).first(1)).has_value());
        }
        MemorySyncLoop restarted{readerFixture.backend, "reader", "replay-fork-corpus", 1};
        // The fork rides along an honest extension that links the durable prefix to the
        // frontier, so only the replayed counter betrays it.
        const std::vector<MemoryDelta> forkedWindow{forkedFirst, window.value().deltas.back()};
        auto forked = restarted.validateHistoryExtension("writer", forkedWindow, frontier);
        REQUIRE(forked.has_value());
        REQUIRE(forked.value().has_value());
        CHECK(forked.value()->code == yams::ErrorCode::HashMismatch);

        auto honest = restarted.validateHistoryExtension("writer", window.value().deltas, frontier);
        REQUIRE(honest.has_value());
        CHECK_FALSE(honest.value().has_value());
    }
}

TEST_CASE("every durable writer quarantine logs its reason and counters exactly once",
          "[memory-sync][direct-delta][quarantine][logging]") {
    BackendFixture writerFixture{"quarantine-log-writer"};
    BackendFixture readerFixture{"quarantine-log-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "quarantine-log-corpus", 1};
    MemorySyncLoop reader{readerFixture.backend, "reader", "quarantine-log-corpus", 1};
    REQUIRE(writer.publish("user/key", bytes("original")).has_value());
    auto batch = writer.exportLocalDeltasAfter({});
    REQUIRE(batch.has_value());
    REQUIRE(reader.applyDeltas(batch.value().deltas).has_value());

    std::ostringstream captured;
    auto previous = spdlog::default_logger();
    auto logger = std::make_shared<spdlog::logger>(
        "quarantine_log_capture", std::make_shared<spdlog::sinks::ostream_sink_mt>(captured));
    logger->set_level(spdlog::level::info);
    spdlog::set_default_logger(logger);
    struct RestoreLogger {
        std::shared_ptr<spdlog::logger> previous;
        ~RestoreLogger() { spdlog::set_default_logger(previous); }
    } restore{previous};

    // The duplicate-operation fork path inside applyDeltas quarantines without any caller log.
    auto fork = batch.value().deltas;
    fork.front().payload = bytes("forked");
    fork.front().record.entryHash = digest(fork.front().payload);
    auto forked = reader.applyDeltas(fork);
    REQUIRE(forked.has_value());
    REQUIRE(reader.writerQuarantined("writer"));

    // Quarantining an already-quarantined writer is not a new quarantine and is not logged.
    auto repeated = reader.quarantineWriter("writer", "reader", "repeated request");
    REQUIRE(repeated.has_value());
    CHECK_FALSE(repeated.value());

    logger->flush();
    const auto out = captured.str();
    INFO(out);
    const std::string needle = "durably quarantined writer writer";
    const auto first = out.find(needle);
    REQUIRE(first != std::string::npos);
    CHECK(out.find(needle, first + needle.size()) == std::string::npos);
    CHECK(out.find("duplicate writer operation fork") != std::string::npos);
    CHECK(out.find("source=reader") != std::string::npos);
    CHECK(out.find("history_counter=1") != std::string::npos);
    CHECK(out.find("version_counter=1") != std::string::npos);
    CHECK(out.find("repeated request") == std::string::npos);
}

TEST_CASE("durable quarantine is not blocked by malformed adapter payload",
          "[memory-sync][direct-delta][quarantine][security]") {
    BackendFixture writerFixture{"direct-malformed-adapter-writer"};
    BackendFixture readerFixture{"direct-malformed-adapter-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "malformed-adapter-corpus", 1};
    MemorySyncLoop reader{readerFixture.backend, "reader", "malformed-adapter-corpus", 1};
    const std::string hash(64, 'a');
    REQUIRE(writer.publish("document/" + hash, bytes("not-json")).has_value());
    auto batch = writer.exportLocalDeltasAfter({});
    REQUIRE(batch.has_value());
    REQUIRE(reader.applyDeltas(batch.value().deltas).has_value());

    auto quarantined = reader.quarantineWriter("writer", "reader", "test request");
    REQUIRE(quarantined.has_value());
    CHECK(quarantined.value());
    CHECK(reader.writerQuarantined("writer"));
    CHECK_FALSE(reader.readCached("document/" + hash).has_value());

    MemorySyncLoop restarted{readerFixture.backend, "reader", "malformed-adapter-corpus", 1};
    REQUIRE(restarted.syncFully().has_value());
    CHECK(restarted.writerQuarantined("writer"));
    CHECK_FALSE(restarted.readCached("document/" + hash).has_value());
}

TEST_CASE("direct writer quarantine removes winners and survives restart",
          "[memory-sync][direct-delta][quarantine][restart]") {
    const bool exclusive = GENERATE(false, true);
    BackendFixture writerFixture{"direct-quarantine-writer"};
    BackendFixture readerFixture{"direct-quarantine-reader"};
    MemorySyncLoop writer{writerFixture.backend, "writer", "quarantine-corpus", 1};
    MemorySyncLoop reader{readerFixture.backend,
                          "reader",
                          "quarantine-corpus",
                          1,
                          false,
                          {},
                          MemorySyncControl{.exclusiveBackend = exclusive}};

    REQUIRE(writer.publish("user/one", bytes("one")).has_value());
    REQUIRE(writer.publish("user/two", bytes("two")).has_value());
    auto batch = writer.exportLocalDeltasAfter({}, 8);
    REQUIRE(batch.has_value());
    REQUIRE(reader.applyDeltas(batch.value().deltas).has_value());
    REQUIRE(reader.readCached("user/one").has_value());
    REQUIRE(reader.readCached("user/two").has_value());
    REQUIRE(reader.syncFully().has_value()); // warm the exclusive key snapshot before quarantine

    auto quarantined = reader.quarantineWriter("writer", "reader", "test request");
    REQUIRE(quarantined.has_value());
    CHECK(quarantined.value());
    CHECK(reader.writerQuarantined("writer"));
    CHECK_FALSE(reader.readCached("user/one").has_value());
    CHECK_FALSE(reader.readCached("user/two").has_value());

    const auto filtered = reader.syncFully();
    REQUIRE(filtered.has_value());
    CHECK_FALSE(filtered.value().contains("user/one"));
    CHECK_FALSE(filtered.value().contains("user/two"));
    MemorySyncLoop restarted{readerFixture.backend,
                             "reader",
                             "quarantine-corpus",
                             1,
                             false,
                             {},
                             MemorySyncControl{.exclusiveBackend = exclusive}};
    REQUIRE(restarted.syncFully().has_value());
    CHECK(restarted.writerQuarantined("writer"));
    CHECK_FALSE(restarted.readCached("user/one").has_value());
    CHECK_FALSE(restarted.readCached("user/two").has_value());
    auto replayed = restarted.applyDeltas(batch.value().deltas);
    REQUIRE(replayed.has_value());
    CHECK(replayed.value().merged == 0);
    CHECK(restarted.writerQuarantined("writer"));
}

TEST_CASE("replication checkpoints isolate concurrent local node state",
          "[memory-sync][direct-delta][quarantine][commitment][restart]") {
    BackendFixture fixture{"direct-checkpoint-node-isolation"};
    MemorySyncLoop nodeA{fixture.backend, "node-a", "checkpoint-corpus", 1};
    MemorySyncLoop nodeB{fixture.backend, "node-b", "checkpoint-corpus", 1};
    REQUIRE(nodeA.quarantineWriter("forked-writer", "node-a", "test request").has_value());
    REQUIRE(nodeB.publish("user/from-b", bytes("b")).has_value());

    MemorySyncLoop restartedA{fixture.backend, "node-a", "checkpoint-corpus", 1};
    REQUIRE(restartedA.syncFully().has_value());
    CHECK(restartedA.writerQuarantined("forked-writer"));
    MemorySyncLoop restartedB{fixture.backend, "node-b", "checkpoint-corpus", 1};
    REQUIRE(restartedB.syncFully().has_value());
    CHECK_FALSE(restartedB.writerQuarantined("forked-writer"));
    CHECK(restartedB.replicationState().commitments.contains("node-b"));
}

TEST_CASE("replication checkpoint corruption fails closed",
          "[memory-sync][direct-delta][quarantine][commitment][security]") {
    BackendFixture fixture{"direct-checkpoint-corrupt"};
    const auto checkpoint = jsonBytes(
        {{"schema_version", 1},
         {"corpus_id", "checkpoint-corpus"},
         {"corpus_epoch", 1},
         {"quarantined_writers", nlohmann::json::array()},
         {"commitments", {{{"writer_id", "writer"}, {"counter", 1}, {"digest", "not-a-digest"}}}}});
    const auto identity =
        nlohmann::json::array({"yams-replication-state-v1", "checkpoint-corpus", 1, "reader"})
            .dump();
    const auto checkpointKey = "checkpoint/replication-state-v1/" + digest(bytes(identity));
    REQUIRE(fixture.backend.store(checkpointKey, checkpoint).has_value());

    MemorySyncLoop loop{fixture.backend, "reader", "checkpoint-corpus", 1};
    auto synced = loop.syncFully();
    REQUIRE_FALSE(synced.has_value());
    CHECK(synced.error().code == yams::ErrorCode::InvalidData);
    CHECK(loop.currentVersion().empty());
    CHECK(loop.mergedRecordCount() == 0);
}

TEST_CASE("quarantined local writer cannot write or export",
          "[memory-sync][direct-delta][quarantine][security]") {
    BackendFixture fixture{"direct-own-quarantine"};
    MemorySyncLoop writer{fixture.backend, "writer", "quarantine-corpus", 1};
    REQUIRE(writer.publish("user/before", bytes("before")).has_value());

    auto quarantined = writer.quarantineWriter("writer", "peer", "test request");
    REQUIRE(quarantined.has_value());
    CHECK(quarantined.value());
    CHECK(writer.writerQuarantined("writer"));

    auto published = writer.publish("user/after", bytes("after"));
    REQUIRE_FALSE(published.has_value());
    CHECK(published.error().code == yams::ErrorCode::InvalidState);
    auto erased = writer.erase("user/before", "before");
    REQUIRE_FALSE(erased.has_value());
    CHECK(erased.error().code == yams::ErrorCode::InvalidState);
    auto exported = writer.exportLocalDeltasAfter({});
    REQUIRE_FALSE(exported.has_value());
    CHECK(exported.error().code == yams::ErrorCode::InvalidState);
}

TEST_CASE("authenticated memory sync accepts explicit key rotation and rejects epoch replay",
          "[memory-sync][auth][rotation][replay]") {
    BackendFixture fixture{"auth-rotation"};
    const std::string writerId = "123e4567-e89b-42d3-a456-426614174000";
    const std::string readerId = "123e4567-e89b-42d3-a456-426614174001";
    const auto oldKey = generateWriterKey();
    const auto newKey = generateWriterKey();
    const auto readerKey = generateWriterKey();
    const std::vector<TrustedWriterKey> trust = {
        {writerId, "writer-2026-q1", oldKey.publicPem, false},
        {writerId, "writer-2026-q2", newKey.publicPem, false},
        {readerId, "reader-v1", readerKey.publicPem, false},
    };

    MemorySyncLoop oldWriter{fixture.backend,
                             writerId,
                             "auth-corpus",
                             1,
                             false,
                             {},
                             {},
                             {},
                             writerAuth(writerId, "writer-2026-q1", oldKey, trust)};
    REQUIRE(oldWriter.publish("before-rotation", bytes("old key")).has_value());
    MemorySyncLoop newWriter{fixture.backend,
                             writerId,
                             "auth-corpus",
                             1,
                             false,
                             {},
                             {},
                             {},
                             writerAuth(writerId, "writer-2026-q2", newKey, trust)};
    REQUIRE(newWriter.publish("after-rotation", bytes("new key")).has_value());

    MemorySyncLoop reader{fixture.backend,
                          readerId,
                          "auth-corpus",
                          1,
                          false,
                          {},
                          {},
                          {},
                          writerAuth(readerId, "reader-v1", readerKey, trust)};
    REQUIRE(reader.sync().has_value());
    CHECK(reader.authFailureCount() == 0);
    CHECK(text(reader.readCached("before-rotation").value()) == "old key");
    CHECK(text(reader.readCached("after-rotation").value()) == "new key");

    const auto epochTwoAuth = writerAuth(readerId, "reader-v1", readerKey, trust, 2);
    const auto oldKeys = fixture.backend.list("index/before-rotation/");
    REQUIRE(oldKeys.has_value());
    const auto encoded = fixture.backend.retrieve(oldKeys.value().front());
    REQUIRE(encoded.has_value());
    const auto oldRecord = nlohmann::json::parse(text(encoded.value())).get<MemoryIndexRecord>();
    CHECK_FALSE(epochTwoAuth->verify(oldRecord));
}

TEST_CASE("Signatures are verified once per distinct envelope", "[memory-sync][auth][cache]") {
    BackendFixture fixture{"auth-cache-hit"};
    const auto writerKey = generateWriterKey();
    const auto readerKey = generateWriterKey();
    const std::string writerId = "123e4567-e89b-42d3-a456-426614174010";
    const std::string readerId = "123e4567-e89b-42d3-a456-426614174011";
    const std::vector<TrustedWriterKey> trust = {
        {writerId, "writer-v1", writerKey.publicPem, false},
        {readerId, "reader-v1", readerKey.publicPem, false},
    };
    MemorySyncLoop writer{fixture.backend,
                          writerId,
                          "auth-corpus",
                          1,
                          false,
                          {},
                          {},
                          {},
                          writerAuth(writerId, "writer-v1", writerKey, trust)};
    for (const auto* logical : {"alpha", "beta", "gamma"}) {
        REQUIRE(writer.publish(logical, bytes("payload for cache")).has_value());
    }

    MemorySyncLoop reader{fixture.backend,
                          readerId,
                          "auth-corpus",
                          1,
                          false,
                          {},
                          MemorySyncControl{.exclusiveBackend = false},
                          {},
                          writerAuth(readerId, "reader-v1", readerKey, trust)};
    REQUIRE(reader.syncFully().has_value());
    const auto firstPass = reader.testingVerifyAttempts();
    REQUIRE(firstPass == 3);
    CHECK(reader.testingVerifiedEnvelopeCount() == 3);

    REQUIRE(reader.syncFully().has_value());
    CHECK(reader.testingVerifyAttempts() == firstPass); // nothing re-verified
    CHECK(reader.testingVerifiedEnvelopeCount() == 3);
}

TEST_CASE("A replaced signature cannot reuse a verified envelope",
          "[memory-sync][auth][cache][security]") {
    BackendFixture fixture{"auth-cache-forged"};
    const auto writerKey = generateWriterKey();
    const auto readerKey = generateWriterKey();
    const std::string writerId = "123e4567-e89b-42d3-a456-426614174012";
    const std::string readerId = "123e4567-e89b-42d3-a456-426614174013";
    const std::vector<TrustedWriterKey> trust = {
        {writerId, "writer-v1", writerKey.publicPem, false},
        {readerId, "reader-v1", readerKey.publicPem, false},
    };
    MemorySyncLoop writer{fixture.backend,
                          writerId,
                          "auth-corpus",
                          1,
                          false,
                          {},
                          {},
                          {},
                          writerAuth(writerId, "writer-v1", writerKey, trust)};
    REQUIRE(writer.publish("delta", bytes("cache forgery payload")).has_value());

    MemorySyncLoop reader{fixture.backend,
                          readerId,
                          "auth-corpus",
                          1,
                          false,
                          {},
                          MemorySyncControl{.exclusiveBackend = false},
                          {},
                          writerAuth(readerId, "reader-v1", readerKey, trust)};
    REQUIRE(reader.syncFully().has_value());
    REQUIRE(reader.testingVerifiedEnvelopeCount() == 1);

    auto keys = fixture.backend.list("index/delta/");
    REQUIRE(keys.has_value());
    REQUIRE(keys.value().size() == 1);
    const auto encoded = fixture.backend.retrieve(keys.value().front());
    REQUIRE(encoded.has_value());
    auto forged = nlohmann::json::parse(text(encoded.value()));
    auto signature = forged["signature"].get<std::string>();
    signature[0] = signature[0] == 'a' ? 'b' : 'a';
    forged["signature"] = signature;
    // Remove the original first: otherwise the intact record wins the merge and the
    // corrupted signature is never examined.
    REQUIRE(fixture.backend.remove(keys.value().front()).has_value());
    // The old key is only used to derive the index prefix; the object is already gone.
    (void)replaceEnvelope(fixture, keys.value().front(), forged);

    REQUIRE(reader.syncFully().has_value());
    // The envelope digest changed, so this is a new entry and a real verification that fails.
    CHECK(reader.authFailureCount() == 1);
    CHECK(reader.testingVerifiedEnvelopeCount() == 1);
}

TEST_CASE("Verification cache stays bounded and misses verify again",
          "[memory-sync][auth][cache]") {
    BackendFixture fixture{"auth-cache-bound"};
    const auto writerKey = generateWriterKey();
    const auto readerKey = generateWriterKey();
    const std::string writerId = "123e4567-e89b-42d3-a456-426614174014";
    const std::string readerId = "123e4567-e89b-42d3-a456-426614174015";
    const std::vector<TrustedWriterKey> trust = {
        {writerId, "writer-v1", writerKey.publicPem, false},
        {readerId, "reader-v1", readerKey.publicPem, false},
    };
    MemorySyncLoop writer{fixture.backend,
                          writerId,
                          "auth-corpus",
                          1,
                          false,
                          {},
                          {},
                          {},
                          writerAuth(writerId, "writer-v1", writerKey, trust)};
    for (const auto* logical : {"one", "two"}) {
        REQUIRE(writer.publish(logical, bytes("bounded cache payload")).has_value());
    }

    MemorySyncLimits limits;
    limits.maxVerifiedEnvelopes = 1;
    MemorySyncLoop reader{fixture.backend,
                          readerId,
                          "auth-corpus",
                          1,
                          false,
                          limits,
                          MemorySyncControl{.exclusiveBackend = false},
                          {},
                          writerAuth(readerId, "reader-v1", readerKey, trust)};
    REQUIRE(reader.syncFully().has_value());
    const auto firstPass = reader.testingVerifyAttempts();
    CHECK(firstPass == 2);
    CHECK(reader.testingVerifiedEnvelopeCount() <= 1);

    REQUIRE(reader.syncFully().has_value());
    // Capacity 1 cannot hold both, so the second pass verifies again rather than trusting.
    CHECK(reader.testingVerifyAttempts() == firstPass + 2);
}
