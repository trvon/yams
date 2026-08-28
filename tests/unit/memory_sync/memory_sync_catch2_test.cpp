// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <cstring>
#include <filesystem>
#include <set>
#include <string>
#include <vector>

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
        return backend_.store(key, data);
    }

    yams::Result<std::vector<std::byte>> retrieve(std::string_view key) const override {
        if (key.starts_with("blob/")) {
            ++blobRetrieveCalls_;
        }
        return backend_.retrieve(key);
    }

    yams::Result<bool> exists(std::string_view key) const override {
        if (key.starts_with("blob/")) {
            ++blobExistsCalls_;
        }
        return backend_.exists(key);
    }

    yams::Result<void> remove(std::string_view key) override { return backend_.remove(key); }

    yams::Result<std::vector<std::string>> list(std::string_view prefix = "") const override {
        ++unboundedListCalls_;
        return backend_.list(prefix);
    }

    yams::Result<yams::storage::ObjectListPage> listPage(std::string_view prefix,
                                                         std::optional<std::string_view> cursor,
                                                         std::size_t limit) const override {
        ++listPageCalls_;
        largestPageRequested_ = std::max(largestPageRequested_, limit);
        return backend_.listPage(prefix, cursor, limit);
    }

    yams::Result<::yams::StorageStats> getStats() const override { return backend_.getStats(); }

    std::future<yams::Result<void>> storeAsync(std::string_view key,
                                               std::span<const std::byte> data) override {
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

private:
    yams::storage::FilesystemBackend backend_;
    mutable std::size_t blobRetrieveCalls_{0};
    mutable std::size_t blobExistsCalls_{0};
    mutable std::size_t listPageCalls_{0};
    mutable std::size_t unboundedListCalls_{0};
    mutable std::size_t largestPageRequested_{0};
    bool remote_{false};
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

    MemorySyncLoop reader{fixture.backend, "reader", "corpus-a", 1};
    const auto merged = reader.sync();
    REQUIRE(merged.has_value());
    CHECK_FALSE(merged.value().contains("forked-key"));
    CHECK(reader.quarantinedRecordCount() == 2);
}

TEST_CASE("MemorySyncLoop keeps paged operation forks invisible until sweep completion",
          "[memory-sync][sync-loop][identity][pagination]") {
    BackendFixture fixture{"paged-writer-operation-fork"};
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
    MemorySyncLoop reader{fixture.backend, "reader", "corpus-a", 1, false, limits};

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
    MemorySyncLoop reader{readerBackend,
                          readerId,
                          "auth-corpus",
                          1,
                          false,
                          {},
                          {},
                          {},
                          writerAuth(readerId, "reader-v1", readerKey, trust)};
    REQUIRE(reader.sync().has_value());
    CHECK(reader.authFailureCount() == 1);
    CHECK(readerBackend.blobRetrieveCalls() == 1); // only the valid sibling hydrates
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
    CHECK(text(reader.readCached("user/key").value()) == "original");
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
