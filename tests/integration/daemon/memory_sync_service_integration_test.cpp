// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <nlohmann/json.hpp>
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <yams/api/content_store.h>
#include <yams/crypto/hasher.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/memory_sync/records.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/storage/storage_backend.h>

#include "test_async_helpers.h"
#include "test_daemon_harness.h"

using namespace std::chrono_literals;

namespace {

#ifndef YAMS_TEST_TIMEOUT_SCALE
#define YAMS_TEST_TIMEOUT_SCALE 1
#endif
constexpr int kTestTimeoutScale = YAMS_TEST_TIMEOUT_SCALE;

template <typename Rep, typename Period>
constexpr auto scaledTimeout(std::chrono::duration<Rep, Period> timeout) {
    return timeout * kTestTimeoutScale;
}

std::vector<std::byte> bytes(std::string_view value) {
    std::vector<std::byte> result(value.size());
    std::memcpy(result.data(), value.data(), value.size());
    return result;
}

std::string digest(std::span<const std::byte> data) {
    yams::crypto::SHA256Hasher hasher;
    hasher.init();
    hasher.update(data);
    return hasher.finalize();
}

/// Delegates every operation to the real daemon content store except
/// `retrieveBytes` for one poisoned hash, which fails with the same
/// manifest-corruption error the live backfill wedge produced. Used to prove
/// the backfill skips an unrecoverable blob instead of wedging the document
/// domain.
class ManifestPoisonedContentStore final : public yams::api::IContentStore {
public:
    ManifestPoisonedContentStore(std::shared_ptr<yams::api::IContentStore> inner,
                                 std::string poisonedHash)
        : inner_(std::move(inner)), poisonedHash_(std::move(poisonedHash)) {}

    yams::Result<yams::api::StoreResult> store(const std::filesystem::path& path,
                                               const yams::api::ContentMetadata& metadata,
                                               yams::api::ProgressCallback progress) override {
        return inner_->store(path, metadata, std::move(progress));
    }
    yams::Result<yams::api::RetrieveResult>
    retrieve(const std::string& hash, const std::filesystem::path& outputPath,
             yams::api::ProgressCallback progress) override {
        return inner_->retrieve(hash, outputPath, std::move(progress));
    }
    yams::Result<yams::api::StoreResult>
    storeStream(std::istream& stream, const yams::api::ContentMetadata& metadata,
                yams::api::ProgressCallback progress) override {
        return inner_->storeStream(stream, metadata, std::move(progress));
    }
    yams::Result<yams::api::RetrieveResult>
    retrieveStream(const std::string& hash, std::ostream& output,
                   yams::api::ProgressCallback progress) override {
        return inner_->retrieveStream(hash, output, std::move(progress));
    }
    yams::Result<yams::api::StoreResult>
    storeBytes(std::span<const std::byte> data,
               const yams::api::ContentMetadata& metadata) override {
        return inner_->storeBytes(data, metadata);
    }
    yams::Result<std::vector<std::byte>> retrieveBytes(const std::string& hash) override {
        if (hash == poisonedHash_) {
            return yams::Error{yams::ErrorCode::ManifestInvalid, "test: poisoned content manifest"};
        }
        return inner_->retrieveBytes(hash);
    }
    yams::Result<std::vector<std::byte>> retrieveBytesPrefix(const std::string& hash,
                                                             std::size_t maxBytes) override {
        if (hash == poisonedHash_) {
            return yams::Error{yams::ErrorCode::ManifestInvalid, "test: poisoned content manifest"};
        }
        return inner_->retrieveBytesPrefix(hash, maxBytes);
    }
    yams::Result<yams::api::IContentStore::RawContent>
    retrieveRaw(const std::string& hash) override {
        return inner_->retrieveRaw(hash);
    }
    std::future<yams::Result<yams::api::IContentStore::RawContent>>
    retrieveRawAsync(const std::string& hash) override {
        return inner_->retrieveRawAsync(hash);
    }
    yams::Result<bool> exists(const std::string& hash) const override {
        return inner_->exists(hash);
    }
    yams::Result<bool> remove(const std::string& hash) override { return inner_->remove(hash); }
    yams::Result<yams::api::ContentMetadata> getMetadata(const std::string& hash) const override {
        return inner_->getMetadata(hash);
    }
    yams::Result<void> updateMetadata(const std::string& hash,
                                      const yams::api::ContentMetadata& metadata) override {
        return inner_->updateMetadata(hash, metadata);
    }
    std::vector<yams::Result<yams::api::StoreResult>>
    storeBatch(const std::vector<std::filesystem::path>& paths,
               const std::vector<yams::api::ContentMetadata>& metadata) override {
        return inner_->storeBatch(paths, metadata);
    }
    std::vector<yams::Result<bool>> removeBatch(const std::vector<std::string>& hashes) override {
        return inner_->removeBatch(hashes);
    }
    yams::api::ContentStoreStats getStats() const override { return inner_->getStats(); }
    yams::api::HealthStatus checkHealth() const override { return inner_->checkHealth(); }
    yams::Result<void> verify(yams::api::ProgressCallback progress) override {
        return inner_->verify(std::move(progress));
    }
    yams::Result<void> compact(yams::api::ProgressCallback progress) override {
        return inner_->compact(std::move(progress));
    }
    yams::Result<void> garbageCollect(yams::api::ProgressCallback progress) override {
        return inner_->garbageCollect(std::move(progress));
    }

private:
    std::shared_ptr<yams::api::IContentStore> inner_;
    std::string poisonedHash_;
};

yams::test::DaemonHarness::Options makeMemorySyncHarnessOptions(std::string nodeId,
                                                                std::string corpusId) {
    yams::test::DaemonHarness::Options options;
    options.enableModelProvider = false;
    options.useMockModelProvider = false;
    options.autoLoadPlugins = false;
    options.enableAutoRepair = false;
    options.isolateConfig = true;
    options.isolateState = true;
    options.configureDaemon = [nodeId, corpusId](yams::daemon::DaemonConfig& config) {
        config.memorySync.enabled = true;
        config.memorySync.corpusScope = yams::memory_sync::CorpusScope::Shared;
        config.memorySync.nodeId = nodeId;
        config.memorySync.corpusId = corpusId;
        config.memorySync.corpusEpoch = 1;
        config.memorySync.transport = "shared-store";
        config.memorySync.backend = "filesystem";
        config.memorySync.path = "shared-memory";
        config.memorySync.syncIntervalMs = 60'000;
    };
    std::ostringstream toml;
    toml << "[daemon]\n"
         << "auto_load_plugins = false\n"
         << "auto_repair = false\n\n"
         << "[memory_sync]\n"
         << "enabled = true\n"
         << "corpus_scope = \"shared\"\n"
         << "node_id = \"" << nodeId << "\"\n"
         << "corpus_id = \"" << corpusId << "\"\n"
         << "corpus_epoch = 1\n"
         << "backend = \"filesystem\"\n"
         << "path = \"shared-memory\"\n"
         << "sync_interval_ms = 60000\n";
    options.isolatedConfigContents = toml.str();
    return options;
}

std::unique_ptr<yams::storage::FilesystemBackend>
makeFilesystemBackend(const std::filesystem::path& path) {
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = path;

    auto backend = std::make_unique<yams::storage::FilesystemBackend>();
    REQUIRE(backend->initialize(config).has_value());
    return backend;
}

} // namespace

TEST_CASE("Daemon refuses enabled memory sync when writer authentication cannot initialize",
          "[integration][daemon][memory-sync][auth][startup]") {
    auto options = makeMemorySyncHarnessOptions("123e4567-e89b-42d3-a456-426614174099",
                                                "startup-failure-corpus");
    options.configureDaemon = [](yams::daemon::DaemonConfig& config) {
        config.memorySync.enabled = true;
        config.memorySync.corpusScope = yams::memory_sync::CorpusScope::Shared;
        config.memorySync.nodeId = "123e4567-e89b-42d3-a456-426614174099";
        config.memorySync.corpusId = "startup-failure-corpus";
        config.memorySync.corpusEpoch = 1;
        config.memorySync.transport = "shared-store";
        config.memorySync.backend = "filesystem";
        config.memorySync.path = "shared-memory";
        config.memorySync.writerAuthRequired = true;
        config.memorySync.writerAuthManifestPath =
            "/definitely-missing-yams-writer-auth-manifest.json";
    };

    yams::test::DaemonHarness harness(std::move(options));
    CHECK_FALSE(harness.start(2s));
}

TEST_CASE("Daemon memory sync periodically converges and stops cleanly",
          "[integration][daemon][memory-sync]") {
    yams::test::DaemonHarness::Options options;
    options.enableModelProvider = false;
    options.useMockModelProvider = false;
    options.autoLoadPlugins = false;
    options.enableAutoRepair = false;
    options.isolateConfig = true;
    options.isolateState = true;
    options.configureDaemon = [](yams::daemon::DaemonConfig& config) {
        config.memorySync.enabled = true;
        config.memorySync.corpusScope = yams::memory_sync::CorpusScope::Shared;
        config.memorySync.nodeId = "123e4567-e89b-42d3-a456-426614174000";
        config.memorySync.corpusId = "integration-corpus";
        config.memorySync.corpusEpoch = 1;
        config.memorySync.transport = "shared-store";
        config.memorySync.backend = "filesystem";
        config.memorySync.path = "shared-memory";
        config.memorySync.syncIntervalMs = 25;
    };
    options.isolatedConfigContents = R"(
[daemon]
auto_load_plugins = false
auto_repair = false

[memory_sync]
enabled = true
corpus_scope = "shared"
node_id = "123e4567-e89b-42d3-a456-426614174000"
corpus_id = "integration-corpus"
corpus_epoch = 1
backend = "filesystem"
path = "shared-memory"
sync_interval_ms = 25
)";

    yams::test::DaemonHarness harness{std::move(options)};
    REQUIRE(harness.start(30s));
    REQUIRE(harness.daemon() != nullptr);

    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto* daemonSync = serviceManager->testingMemorySyncService();
    REQUIRE(daemonSync != nullptr);
    REQUIRE(daemonSync->started());

    const auto sharedPath = harness.dataDir() / "shared-memory";
    yams::memory_sync::MemorySyncService peer{
        makeFilesystemBackend(sharedPath),
        yams::memory_sync::MemorySyncConfig{"peer-node", 25, "integration-corpus", 1}};
    REQUIRE(peer.publish("convergence-key", bytes("from-peer")).has_value());

    const auto deadline = std::chrono::steady_clock::now() + scaledTimeout(3s);
    while (!daemonSync->testingHasMergedRecord("convergence-key") &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
    }
    CHECK(daemonSync->testingHasMergedRecord("convergence-key"));

    // Production daemon application uses ContentBlobSyncAdapter, including its
    // digest check, rather than a parallel unchecked content-store path.
    auto contentStore = serviceManager->getContentStore();
    REQUIRE(contentStore != nullptr);
    const auto peerBlob = bytes("peer-content-blob");
    const auto peerBlobHash = digest(peerBlob);
    REQUIRE(peer.publish("content-blob/" + peerBlobHash, peerBlob).has_value());
    bool peerBlobApplied = false;
    const auto blobDeadline = std::chrono::steady_clock::now() + scaledTimeout(3s);
    while (!peerBlobApplied && std::chrono::steady_clock::now() < blobDeadline) {
        const auto exists = contentStore->exists(peerBlobHash);
        REQUIRE(exists.has_value());
        peerBlobApplied = exists.value();
        if (!peerBlobApplied) {
            std::this_thread::sleep_for(10ms);
        }
    }
    REQUIRE(peerBlobApplied);
    const auto hydratedBlob = contentStore->retrieveBytes(peerBlobHash);
    REQUIRE(hydratedBlob.has_value());
    CHECK(hydratedBlob.value() == peerBlob);

    const std::string& kDocumentHash = peerBlobHash;
    yams::memory_sync::MetadataDocumentRecord record;
    record.documentId = kDocumentHash;
    record.contentHash = kDocumentHash;
    record.filePath = "/peer/corpus/daemon-applied.md";
    record.fileName = "daemon-applied.md";
    record.fileExtension = ".md";
    record.fileSize = 42;
    record.mimeType = "text/markdown";
    record.createdTime = 1000;
    record.modifiedTime = 1001;
    record.indexedTime = 1002;
    record.contentExtracted = true;
    record.metadata["source"] = {.value = "peer", .type = 0};
    const auto serialized = nlohmann::json(record).dump();
    REQUIRE(peer.publish("document/" + std::string(kDocumentHash), bytes(serialized)).has_value());

    auto repository = serviceManager->getMetadataRepo();
    REQUIRE(repository != nullptr);
    std::optional<yams::metadata::DocumentInfo> replicated;
    const auto applyDeadline = std::chrono::steady_clock::now() + scaledTimeout(3s);
    while (!replicated && std::chrono::steady_clock::now() < applyDeadline) {
        auto found = repository->getDocumentByHash(std::string(kDocumentHash));
        REQUIRE(found.has_value());
        replicated = found.value();
        if (!replicated) {
            std::this_thread::sleep_for(10ms);
        }
    }
    REQUIRE(replicated.has_value());
    CHECK(replicated->filePath == record.filePath);

    const auto source = repository->getMetadata(replicated->id, "source");
    REQUIRE(source.has_value());
    REQUIRE(source.value().has_value());
    CHECK(source.value()->value == "peer");

    // A document committed after daemon startup is discovered by bounded periodic backfill.
    const auto localPayload = bytes("periodic-backfill-search-needle");
    auto stored = contentStore->storeBytes(localPayload);
    REQUIRE(stored.has_value());

    yams::metadata::BatchDocumentInsert localInsert;
    localInsert.info.filePath = "/local/corpus/backfill.md";
    localInsert.info.fileName = "backfill.md";
    localInsert.info.fileExtension = ".md";
    localInsert.info.fileSize = static_cast<std::int64_t>(localPayload.size());
    localInsert.info.sha256Hash = stored.value().contentHash;
    localInsert.info.mimeType = "text/markdown";
    localInsert.tags.emplace_back("origin", yams::metadata::MetadataValue{"daemon"});
    std::vector<yams::metadata::BatchDocumentInsert> localInserts;
    localInserts.push_back(std::move(localInsert));
    REQUIRE(repository->batchInsertDocumentsWithMetadata(localInserts).has_value());

    auto kgStore = serviceManager->getKgStore();
    REQUIRE(kgStore != nullptr);
    yams::metadata::KGNode fileNode;
    fileNode.nodeKey = "file:/local/corpus/backfill.md";
    fileNode.type = "file";
    fileNode.label = "backfill.md";
    auto fileNodeId = kgStore->upsertNode(fileNode);
    REQUIRE(fileNodeId.has_value());
    yams::metadata::KGNode symbolNode;
    symbolNode.nodeKey = "fn:backfill";
    symbolNode.type = "function";
    symbolNode.label = "backfill";
    auto symbolNodeId = kgStore->upsertNode(symbolNode);
    REQUIRE(symbolNodeId.has_value());
    yams::metadata::KGEdge edge;
    edge.srcNodeId = fileNodeId.value();
    edge.dstNodeId = symbolNodeId.value();
    edge.relation = "defines";
    REQUIRE(kgStore->addEdge(edge).has_value());

    const std::string documentKey = "document/" + stored.value().contentHash;
    const std::string blobKey = "content-blob/" + stored.value().contentHash;
    const std::string topologyNodeKey = "topology-node/file%3A%2Flocal%2Fcorpus%2Fbackfill.md";
    const std::string topologyEdgeKey =
        "topology-edge/file%3A%2Flocal%2Fcorpus%2Fbackfill.md/defines/fn%3Abackfill";
    bool documentPublished = false;
    bool blobPublished = false;
    bool topologyNodePublished = false;
    bool topologyEdgePublished = false;
    const auto backfillDeadline = std::chrono::steady_clock::now() + scaledTimeout(8s);
    while ((!documentPublished || !blobPublished || !topologyNodePublished ||
            !topologyEdgePublished) &&
           std::chrono::steady_clock::now() < backfillDeadline) {
        documentPublished = peer.read(documentKey).has_value();
        auto replicatedBlob = peer.read(blobKey);
        blobPublished = replicatedBlob.has_value() && replicatedBlob.value() == localPayload;
        topologyNodePublished = peer.read(topologyNodeKey).has_value();
        topologyEdgePublished = peer.read(topologyEdgeKey).has_value();
        if (!documentPublished || !blobPublished || !topologyNodePublished ||
            !topologyEdgePublished) {
            std::this_thread::sleep_for(25ms);
        }
    }
    CHECK(documentPublished);
    CHECK(blobPublished);
    CHECK(topologyNodePublished);
    CHECK(topologyEdgePublished);

    // Persistent-mode tombstones converge through the daemon's production adapters.
    REQUIRE(peer.erase("document/" + std::string(kDocumentHash), std::string(kDocumentHash))
                .has_value());
    REQUIRE(peer.erase("content-blob/" + peerBlobHash, peerBlobHash).has_value());
    bool documentDeleted = false;
    bool blobDeleted = false;
    const auto deleteDeadline = std::chrono::steady_clock::now() + scaledTimeout(3s);
    while ((!documentDeleted || !blobDeleted) &&
           std::chrono::steady_clock::now() < deleteDeadline) {
        auto found = repository->getDocumentByHash(std::string(kDocumentHash));
        REQUIRE(found.has_value());
        documentDeleted = !found.value().has_value();
        const auto exists = contentStore->exists(peerBlobHash);
        REQUIRE(exists.has_value());
        blobDeleted = !exists.value();
        if (!documentDeleted || !blobDeleted) {
            std::this_thread::sleep_for(10ms);
        }
    }
    CHECK(documentDeleted);
    CHECK(blobDeleted);

    REQUIRE(serviceManager->publishMemorySync("user-delete", "value").has_value());
    REQUIRE(peer.syncOnce().has_value());
    REQUIRE(peer.readCached("user/user-delete").has_value());
    REQUIRE(serviceManager->deleteMemorySync("user-delete").has_value());
    REQUIRE(peer.syncOnce().has_value());
    CHECK_FALSE(peer.readCached("user/user-delete").has_value());

    harness.stop();
    CHECK(harness.shutdownSucceeded());
}

TEST_CASE("Production document deletion also replicates content-byte deletion",
          "[integration][daemon][memory-sync][deletion]") {
    auto options = makeMemorySyncHarnessOptions("123e4567-e89b-42d3-a456-426614174020",
                                                "delete-integrity-corpus");
    yams::test::DaemonHarness harness{std::move(options)};
    REQUIRE(harness.start(30s));

    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto* daemonSync = serviceManager->testingMemorySyncService();
    REQUIRE(daemonSync != nullptr);
    auto contentStore = serviceManager->getContentStore();
    REQUIRE(contentStore != nullptr);
    auto repository = serviceManager->getMetadataRepo();
    REQUIRE(repository != nullptr);

    const auto payload = bytes("production-delete-content");
    auto stored = contentStore->storeBytes(payload);
    REQUIRE(stored.has_value());
    const auto& hash = stored.value().contentHash;

    yams::metadata::BatchDocumentInsert insert;
    insert.info.filePath = "/local/corpus/delete-me.md";
    insert.info.fileName = "delete-me.md";
    insert.info.fileExtension = ".md";
    insert.info.fileSize = static_cast<std::int64_t>(payload.size());
    insert.info.sha256Hash = hash;
    insert.info.mimeType = "text/markdown";
    std::vector<yams::metadata::BatchDocumentInsert> inserts;
    inserts.push_back(std::move(insert));
    REQUIRE(repository->batchInsertDocumentsWithMetadata(inserts).has_value());

    yams::memory_sync::MetadataDocumentRecord record;
    record.documentId = hash;
    record.contentHash = hash;
    record.filePath = "/local/corpus/delete-me.md";
    record.fileName = "delete-me.md";
    record.fileExtension = ".md";
    record.fileSize = static_cast<std::int64_t>(payload.size());
    record.mimeType = "text/markdown";
    const std::string documentKey = "document/" + hash;
    const std::string blobKey = "content-blob/" + hash;
    const auto serialized = nlohmann::json(record).dump();
    REQUIRE(daemonSync->publish(documentKey, bytes(serialized)).has_value());
    REQUIRE(daemonSync->publish(blobKey, payload).has_value());

    yams::memory_sync::MemorySyncService peer{
        makeFilesystemBackend(harness.dataDir() / "shared-memory"),
        yams::memory_sync::MemorySyncConfig{"delete-peer", 60'000, "delete-integrity-corpus", 1}};
    REQUIRE(peer.syncOnce().has_value());
    REQUIRE(peer.readCached(documentKey).has_value());
    REQUIRE(peer.readCached(blobKey).has_value());

    // Exercise the real IPC dispatcher, including its pre-delete staging and post-delete
    // publication hooks.
    yams::daemon::ClientConfig clientConfig;
    clientConfig.socketPath = harness.socketPath();
    clientConfig.connectTimeout = 5s;
    clientConfig.requestTimeout = 10s;
    clientConfig.autoStart = false;
    yams::daemon::DaemonClient client{clientConfig};
    REQUIRE(yams::cli::run_sync(client.connect(), 5s).has_value());
    yams::daemon::DeleteRequest deleteRequest;
    deleteRequest.hash = hash;
    const auto writerCounterBeforeDelete =
        peer.currentVersion().get("123e4567-e89b-42d3-a456-426614174020");
    std::promise<void> publisherLockedPromise;
    auto publisherLocked = publisherLockedPromise.get_future();
    std::promise<void> drainerWaitingPromise;
    auto drainerWaiting = drainerWaitingPromise.get_future();
    std::promise<void> releasePublisherPromise;
    auto releasePublisher = releasePublisherPromise.get_future().share();
    std::atomic<bool> publisherSignalled{false};
    std::atomic<bool> drainerSignalled{false};
    serviceManager->testingSetMemorySyncDeleteOutboxObserver([&](std::string_view stage) {
        if (stage == "delete_publish_locked" && !publisherSignalled.exchange(true)) {
            publisherLockedPromise.set_value();
            releasePublisher.wait();
        } else if (stage == "delete_drain_waiting" && !drainerSignalled.exchange(true)) {
            drainerWaitingPromise.set_value();
        }
    });
    auto deleteCall = std::async(std::launch::async, [&] {
        return yams::cli::run_sync(client.call<yams::daemon::DeleteRequest>(deleteRequest), 10s);
    });
    const auto publisherStatus = publisherLocked.wait_for(5s);
    if (publisherStatus != std::future_status::ready) {
        releasePublisherPromise.set_value();
        FAIL("delete publisher did not reach the serialized outbox section");
    }
    auto competingDrainer = std::async(
        std::launch::async, [serviceManager] { serviceManager->testingApplyMemorySyncWinners(); });
    const auto drainerStatus = drainerWaiting.wait_for(5s);
    releasePublisherPromise.set_value();
    REQUIRE(drainerStatus == std::future_status::ready);
    auto deleted = deleteCall.get();
    competingDrainer.get();
    serviceManager->testingSetMemorySyncDeleteOutboxObserver({});
    REQUIRE(deleted.has_value());
    CHECK(deleted.value().successCount == 1);
    auto pendingErases = daemonSync->pendingErases();
    REQUIRE(pendingErases.has_value());
    CHECK(pendingErases.value().empty());
    auto firstPublisher = std::async(std::launch::async, [serviceManager, hash] {
        return serviceManager->publishMemorySyncDocumentDelete(hash);
    });
    auto secondPublisher = std::async(std::launch::async, [serviceManager, hash] {
        return serviceManager->publishMemorySyncDocumentDelete(hash);
    });
    CHECK(firstPublisher.get().has_value());
    CHECK(secondPublisher.get().has_value());

    REQUIRE(peer.syncOnce().has_value());
    CHECK_FALSE(peer.readCached(documentKey).has_value());
    CHECK_FALSE(peer.readCached(blobKey).has_value());
    CHECK(peer.currentVersion().get("123e4567-e89b-42d3-a456-426614174020") ==
          writerCounterBeforeDelete + 2);

    // --keep-refs carries its retain-content mode through durable staging and publication. Its
    // document-only completion remains idempotent without inventing a content tombstone.
    const auto retainedPayload = bytes("retained-production-content");
    auto retainedStored = contentStore->storeBytes(retainedPayload);
    REQUIRE(retainedStored.has_value());
    const auto& retainedHash = retainedStored.value().contentHash;
    yams::metadata::BatchDocumentInsert retainedInsert;
    retainedInsert.info.filePath = "/local/corpus/retained.md";
    retainedInsert.info.fileName = "retained.md";
    retainedInsert.info.fileExtension = ".md";
    retainedInsert.info.fileSize = static_cast<std::int64_t>(retainedPayload.size());
    retainedInsert.info.sha256Hash = retainedHash;
    retainedInsert.info.mimeType = "text/markdown";
    std::vector<yams::metadata::BatchDocumentInsert> retainedInserts;
    retainedInserts.push_back(std::move(retainedInsert));
    REQUIRE(repository->batchInsertDocumentsWithMetadata(retainedInserts).has_value());

    record.documentId = retainedHash;
    record.contentHash = retainedHash;
    record.filePath = "/local/corpus/retained.md";
    record.fileName = "retained.md";
    record.fileSize = static_cast<std::int64_t>(retainedPayload.size());
    const std::string retainedDocumentKey = "document/" + retainedHash;
    const std::string retainedBlobKey = "content-blob/" + retainedHash;
    REQUIRE(
        daemonSync->publish(retainedDocumentKey, bytes(nlohmann::json(record).dump())).has_value());
    REQUIRE(daemonSync->publish(retainedBlobKey, retainedPayload).has_value());
    REQUIRE(peer.syncOnce().has_value());
    const auto writerCounterBeforeRetainedDelete =
        peer.currentVersion().get("123e4567-e89b-42d3-a456-426614174020");

    yams::daemon::DeleteRequest retainDeleteRequest;
    retainDeleteRequest.hash = retainedHash;
    retainDeleteRequest.keepRefs = true;
    auto retainedDelete =
        yams::cli::run_sync(client.call<yams::daemon::DeleteRequest>(retainDeleteRequest), 10s);
    REQUIRE(retainedDelete.has_value());
    CHECK(retainedDelete.value().successCount == 1);
    auto retainedLocally = contentStore->exists(retainedHash);
    REQUIRE(retainedLocally.has_value());
    CHECK(retainedLocally.value());
    REQUIRE(serviceManager->publishMemorySyncDocumentDelete(retainedHash, true).has_value());
    REQUIRE(peer.syncOnce().has_value());
    CHECK_FALSE(peer.readCached(retainedDocumentKey).has_value());
    CHECK(peer.readCached(retainedBlobKey).has_value());
    CHECK(peer.currentVersion().get("123e4567-e89b-42d3-a456-426614174020") ==
          writerCounterBeforeRetainedDelete + 1);

    // If local deletion is interrupted after removing bytes but before removing metadata, drain
    // only the ready blob tombstone. The document intent must survive for a later metadata repair.
    const auto interruptedPayload = bytes("interrupted-production-delete");
    auto interruptedStored = contentStore->storeBytes(interruptedPayload);
    REQUIRE(interruptedStored.has_value());
    const auto& interruptedHash = interruptedStored.value().contentHash;
    yams::metadata::BatchDocumentInsert interruptedInsert;
    interruptedInsert.info.filePath = "/local/corpus/interrupted.md";
    interruptedInsert.info.fileName = "interrupted.md";
    interruptedInsert.info.fileExtension = ".md";
    interruptedInsert.info.fileSize = static_cast<std::int64_t>(interruptedPayload.size());
    interruptedInsert.info.sha256Hash = interruptedHash;
    interruptedInsert.info.mimeType = "text/markdown";
    std::vector<yams::metadata::BatchDocumentInsert> interruptedInserts;
    interruptedInserts.push_back(std::move(interruptedInsert));
    REQUIRE(repository->batchInsertDocumentsWithMetadata(interruptedInserts).has_value());

    record.documentId = interruptedHash;
    record.contentHash = interruptedHash;
    record.filePath = "/local/corpus/interrupted.md";
    record.fileName = "interrupted.md";
    record.fileSize = static_cast<std::int64_t>(interruptedPayload.size());
    const std::string interruptedDocumentKey = "document/" + interruptedHash;
    const std::string interruptedBlobKey = "content-blob/" + interruptedHash;
    REQUIRE(daemonSync->publish(interruptedDocumentKey, bytes(nlohmann::json(record).dump()))
                .has_value());
    REQUIRE(daemonSync->publish(interruptedBlobKey, interruptedPayload).has_value());
    REQUIRE(peer.syncOnce().has_value());
    REQUIRE(peer.readCached(interruptedDocumentKey).has_value());
    REQUIRE(peer.readCached(interruptedBlobKey).has_value());

    REQUIRE(serviceManager->stageMemorySyncDocumentDelete(interruptedHash).has_value());
    auto removedBytes = contentStore->remove(interruptedHash);
    REQUIRE(removedBytes.has_value());
    REQUIRE(removedBytes.value());
    serviceManager->testingApplyMemorySyncWinners();
    REQUIRE(peer.syncOnce().has_value());
    CHECK(peer.readCached(interruptedDocumentKey).has_value());
    CHECK_FALSE(peer.readCached(interruptedBlobKey).has_value());
    pendingErases = daemonSync->pendingErases();
    REQUIRE(pendingErases.has_value());
    CHECK(std::ranges::any_of(pendingErases.value(), [&](const auto& intent) {
        return intent.logicalKey == interruptedDocumentKey;
    }));
    CHECK_FALSE(std::ranges::any_of(pendingErases.value(), [&](const auto& intent) {
        return intent.logicalKey == interruptedBlobKey;
    }));

    auto interruptedDocument = repository->getDocumentByHash(interruptedHash);
    REQUIRE(interruptedDocument.has_value());
    REQUIRE(interruptedDocument.value().has_value());
    REQUIRE(repository->deleteDocument(interruptedDocument.value()->id).has_value());
    serviceManager->testingApplyMemorySyncWinners();
    REQUIRE(peer.syncOnce().has_value());
    CHECK_FALSE(peer.readCached(interruptedDocumentKey).has_value());
    CHECK(daemonSync->pendingErases().value().empty());

    harness.stop();
    CHECK(harness.shutdownSucceeded());
}

TEST_CASE("Daemon memory sync stop during production apply prevents later adapters",
          "[integration][daemon][memory-sync][lifecycle]") {
    auto options = makeMemorySyncHarnessOptions("123e4567-e89b-42d3-a456-426614174010",
                                                "apply-cancellation-corpus");
    yams::test::DaemonHarness harness{std::move(options)};
    REQUIRE(harness.start(30s));

    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto* daemonSync = serviceManager->testingMemorySyncService();
    REQUIRE(daemonSync != nullptr);

    yams::memory_sync::MemorySyncService peer{
        makeFilesystemBackend(harness.dataDir() / "shared-memory"),
        yams::memory_sync::MemorySyncConfig{"apply-peer", 60'000, "apply-cancellation-corpus", 1}};
    const auto payload = bytes("apply-cancellation-content");
    const auto hash = digest(payload);
    REQUIRE(peer.publish("content-blob/" + hash, payload).has_value());

    yams::memory_sync::MetadataDocumentRecord record;
    record.documentId = hash;
    record.contentHash = hash;
    record.filePath = "/peer/apply-cancelled.md";
    record.fileName = "apply-cancelled.md";
    record.fileExtension = ".md";
    record.fileSize = static_cast<std::int64_t>(payload.size());
    record.mimeType = "text/markdown";
    REQUIRE(peer.publish("document/" + hash, bytes(nlohmann::json(record).dump())).has_value());
    REQUIRE(daemonSync->syncOnce().has_value());

    std::atomic<bool> afterContent{false};
    std::atomic<bool> afterMetadata{false};
    serviceManager->testingSetMemorySyncStageObserver([&](std::string_view stage) {
        if (stage == "apply.after_content") {
            afterContent.store(true, std::memory_order_release);
            daemonSync->stop();
        } else if (stage == "apply.after_metadata") {
            afterMetadata.store(true, std::memory_order_release);
        }
    });
    serviceManager->testingApplyMemorySyncWinners();

    CHECK(afterContent.load(std::memory_order_acquire));
    CHECK_FALSE(afterMetadata.load(std::memory_order_acquire));
    auto contentStore = serviceManager->getContentStore();
    REQUIRE(contentStore != nullptr);
    const auto contentExists = contentStore->exists(hash);
    REQUIRE(contentExists.has_value());
    CHECK(contentExists.value());
    auto repository = serviceManager->getMetadataRepo();
    REQUIRE(repository != nullptr);
    const auto document = repository->getDocumentByHash(hash);
    REQUIRE(document.has_value());
    CHECK_FALSE(document.value().has_value());

    harness.stop();
    CHECK(harness.shutdownSucceeded());
}

TEST_CASE("Daemon serializes concurrent memory sync apply callbacks",
          "[integration][daemon][memory-sync][apply-serialization]") {
    auto options = makeMemorySyncHarnessOptions("123e4567-e89b-42d3-a456-426614174021",
                                                "apply-serialization-corpus");
    yams::test::DaemonHarness harness{std::move(options)};
    REQUIRE(harness.start(30s));

    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto* daemonSync = serviceManager->testingMemorySyncService();
    REQUIRE(daemonSync != nullptr);
    std::atomic<std::size_t> contentStages{0};
    std::promise<void> firstEnteredPromise;
    auto firstEntered = firstEnteredPromise.get_future();
    std::promise<void> secondEnteredPromise;
    auto secondEntered = secondEnteredPromise.get_future();
    std::promise<void> releaseFirstPromise;
    auto releaseFirst = releaseFirstPromise.get_future().share();
    serviceManager->testingSetMemorySyncStageObserver([&](std::string_view stage) {
        if (stage != "apply.after_content") {
            return;
        }
        if (contentStages.fetch_add(1, std::memory_order_acq_rel) == 0) {
            firstEnteredPromise.set_value();
            releaseFirst.wait();
        } else {
            secondEnteredPromise.set_value();
        }
    });

    const auto applyAttemptsBefore = serviceManager->testingMemorySyncApplyAttempts();
    auto first = std::async(std::launch::async,
                            [serviceManager] { serviceManager->testingApplyMemorySyncWinners(); });
    REQUIRE(firstEntered.wait_for(5s) == std::future_status::ready);
    CHECK(serviceManager->testingMemorySyncApplyLockHeld());
    auto second = std::async(std::launch::async,
                             [serviceManager] { serviceManager->testingApplyMemorySyncWinners(); });
    const auto secondApplyDeadline = std::chrono::steady_clock::now() + 5s;
    while (serviceManager->testingMemorySyncApplyAttempts() < applyAttemptsBefore + 2 &&
           std::chrono::steady_clock::now() < secondApplyDeadline) {
        std::this_thread::yield();
    }
    REQUIRE(serviceManager->testingMemorySyncApplyAttempts() == applyAttemptsBefore + 2);
    CHECK(contentStages.load(std::memory_order_acquire) == 1);

    releaseFirstPromise.set_value();
    first.get();
    second.get();
    CHECK(secondEntered.wait_for(5s) == std::future_status::ready);
    CHECK(contentStages.load(std::memory_order_acquire) == 2);

    auto repository = serviceManager->getMetadataRepo();
    auto contentStore = serviceManager->getContentStore();
    REQUIRE(repository != nullptr);
    REQUIRE(contentStore != nullptr);
    std::vector<std::string> hashes;
    std::vector<yams::metadata::BatchDocumentInsert> inserts;
    for (int index = 0; index < 2; ++index) {
        const auto payload = bytes("serialized-backfill-" + std::to_string(index));
        auto stored = contentStore->storeBytes(payload);
        REQUIRE(stored.has_value());
        hashes.push_back(stored.value().contentHash);
        yams::metadata::BatchDocumentInsert item;
        item.info.filePath = "/local/serialized-" + std::to_string(index) + ".md";
        item.info.fileName = "serialized-" + std::to_string(index) + ".md";
        item.info.fileExtension = ".md";
        item.info.fileSize = static_cast<std::int64_t>(payload.size());
        item.info.sha256Hash = stored.value().contentHash;
        item.info.mimeType = "text/markdown";
        inserts.push_back(std::move(item));
    }
    REQUIRE(repository->batchInsertDocumentsWithMetadata(inserts).has_value());
    const auto writerCounterBeforeBackfill =
        daemonSync->currentVersion().get("123e4567-e89b-42d3-a456-426614174021");
    serviceManager->testingSetMemorySyncBackfillItemBudget(1);

    std::atomic<std::size_t> backfillStages{0};
    std::promise<void> firstBackfillEnteredPromise;
    auto firstBackfillEntered = firstBackfillEnteredPromise.get_future();
    std::promise<void> secondBackfillEnteredPromise;
    auto secondBackfillEntered = secondBackfillEnteredPromise.get_future();
    std::promise<void> releaseFirstBackfillPromise;
    auto releaseFirstBackfill = releaseFirstBackfillPromise.get_future().share();
    serviceManager->testingSetMemorySyncStageObserver([&](std::string_view stage) {
        if (!stage.starts_with("backfill.")) {
            return;
        }
        if (backfillStages.fetch_add(1, std::memory_order_acq_rel) == 0) {
            firstBackfillEnteredPromise.set_value();
            releaseFirstBackfill.wait();
        } else {
            secondBackfillEnteredPromise.set_value();
        }
    });
    const auto backfillAttemptsBefore = serviceManager->testingMemorySyncBackfillAttempts();
    auto firstBackfill = std::async(std::launch::async, [serviceManager] {
        serviceManager->testingPublishMemorySyncBackfill();
    });
    REQUIRE(firstBackfillEntered.wait_for(5s) == std::future_status::ready);
    CHECK(serviceManager->testingMemorySyncBackfillLockHeld());
    auto secondBackfill = std::async(std::launch::async, [serviceManager] {
        serviceManager->testingPublishMemorySyncBackfill();
    });
    const auto secondBackfillDeadline = std::chrono::steady_clock::now() + 5s;
    while (serviceManager->testingMemorySyncBackfillAttempts() < backfillAttemptsBefore + 2 &&
           std::chrono::steady_clock::now() < secondBackfillDeadline) {
        std::this_thread::yield();
    }
    REQUIRE(serviceManager->testingMemorySyncBackfillAttempts() == backfillAttemptsBefore + 2);
    CHECK(backfillStages.load(std::memory_order_acquire) == 1);

    releaseFirstBackfillPromise.set_value();
    firstBackfill.get();
    secondBackfill.get();
    CHECK(secondBackfillEntered.wait_for(5s) == std::future_status::ready);
    CHECK(backfillStages.load(std::memory_order_acquire) == 2);
    serviceManager->testingSetMemorySyncStageObserver({});

    yams::memory_sync::MemorySyncService peer{
        makeFilesystemBackend(harness.dataDir() / "shared-memory"),
        yams::memory_sync::MemorySyncConfig{"serialization-peer", 60'000,
                                            "apply-serialization-corpus", 1}};
    REQUIRE(peer.syncFully().has_value());
    for (const auto& hash : hashes) {
        CHECK(peer.readCached("document/" + hash).has_value());
    }
    CHECK(peer.currentVersion().get("123e4567-e89b-42d3-a456-426614174021") ==
          writerCounterBeforeBackfill + 4);

    harness.stop();
    CHECK(harness.shutdownSucceeded());
}

TEST_CASE("Daemon memory sync prerequisite failures stop dependent adapters and retry",
          "[integration][daemon][memory-sync][prerequisite]") {
    auto options = makeMemorySyncHarnessOptions("123e4567-e89b-42d3-a456-426614174012",
                                                "apply-prerequisite-corpus");
    yams::test::DaemonHarness harness{std::move(options)};
    REQUIRE(harness.start(30s));

    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto* daemonSync = serviceManager->testingMemorySyncService();
    REQUIRE(daemonSync != nullptr);
    yams::memory_sync::MemorySyncService peer{
        makeFilesystemBackend(harness.dataDir() / "shared-memory"),
        yams::memory_sync::MemorySyncConfig{"apply-peer", 60'000, "apply-prerequisite-corpus", 1}};
    std::vector<std::string> stages;
    serviceManager->testingSetMemorySyncStageObserver(
        [&](std::string_view stage) { stages.emplace_back(stage); });

    const auto payload = bytes("prerequisite-content");
    const auto hash = digest(payload);
    REQUIRE(peer.publish("content-blob/" + hash, bytes("wrong-content")).has_value());
    REQUIRE(daemonSync->syncOnce().has_value());
    serviceManager->testingApplyMemorySyncWinners();
    CHECK(stages.empty());

    auto contentStore = serviceManager->getContentStore();
    REQUIRE(contentStore != nullptr);
    REQUIRE(contentStore->exists(hash).has_value());
    CHECK_FALSE(contentStore->exists(hash).value());

    REQUIRE(peer.publish("content-blob/" + hash, payload).has_value());
    REQUIRE(daemonSync->syncOnce().has_value());
    stages.clear();
    serviceManager->testingApplyMemorySyncWinners();
    CHECK(stages == std::vector<std::string>{"apply.after_content", "apply.after_metadata",
                                             "apply.after_vector", "apply.after_topology"});
    REQUIRE(contentStore->exists(hash).has_value());
    CHECK(contentStore->exists(hash).value());

    const auto metadataPayload = bytes("metadata-prerequisite-content");
    const auto metadataHash = digest(metadataPayload);
    REQUIRE(peer.publish("content-blob/" + metadataHash, metadataPayload).has_value());
    REQUIRE(peer.publish("document/" + metadataHash, bytes("not-json")).has_value());
    REQUIRE(daemonSync->syncOnce().has_value());
    stages.clear();
    serviceManager->testingApplyMemorySyncWinners();
    CHECK(stages == std::vector<std::string>{"apply.after_content"});

    yams::memory_sync::MetadataDocumentRecord record;
    record.documentId = metadataHash;
    record.contentHash = metadataHash;
    record.filePath = "/peer/prerequisite.md";
    record.fileName = "prerequisite.md";
    record.fileExtension = ".md";
    record.fileSize = static_cast<std::int64_t>(metadataPayload.size());
    record.mimeType = "text/markdown";
    REQUIRE(
        peer.publish("document/" + metadataHash, bytes(nlohmann::json(record).dump())).has_value());
    REQUIRE(daemonSync->syncOnce().has_value());
    stages.clear();
    serviceManager->testingApplyMemorySyncWinners();
    CHECK(stages == std::vector<std::string>{"apply.after_content", "apply.after_metadata",
                                             "apply.after_vector", "apply.after_topology"});
    auto repository = serviceManager->getMetadataRepo();
    REQUIRE(repository != nullptr);
    const auto document = repository->getDocumentByHash(metadataHash);
    REQUIRE(document.has_value());
    CHECK(document.value().has_value());

    yams::memory_sync::TopologyNodeRecord topologyRecord;
    topologyRecord.nodeKey = "payload-node";
    topologyRecord.type = "symbol";
    topologyRecord.label = "payload";
    REQUIRE(
        peer.publish("topology-node/envelope-node", bytes(nlohmann::json(topologyRecord).dump()))
            .has_value());
    REQUIRE(daemonSync->syncOnce().has_value());
    stages.clear();
    serviceManager->testingApplyMemorySyncWinners();
    CHECK(stages == std::vector<std::string>{"apply.after_content", "apply.after_metadata",
                                             "apply.after_vector"});

    topologyRecord.nodeKey = "envelope-node";
    topologyRecord.label = "repaired";
    REQUIRE(
        peer.publish("topology-node/envelope-node", bytes(nlohmann::json(topologyRecord).dump()))
            .has_value());
    REQUIRE(daemonSync->syncOnce().has_value());
    stages.clear();
    serviceManager->testingApplyMemorySyncWinners();
    CHECK(stages == std::vector<std::string>{"apply.after_content", "apply.after_metadata",
                                             "apply.after_vector", "apply.after_topology"});
    auto kgStore = serviceManager->getKgStore();
    REQUIRE(kgStore != nullptr);
    const auto repairedNode = kgStore->getNodeByKey("envelope-node");
    REQUIRE(repairedNode.has_value());
    CHECK(repairedNode.value().has_value());

    harness.stop();
    CHECK(harness.shutdownSucceeded());
}

TEST_CASE("Daemon memory sync requires content before metadata visibility",
          "[integration][daemon][memory-sync][prerequisite][content]") {
    auto options = makeMemorySyncHarnessOptions("123e4567-e89b-42d3-a456-426614174013",
                                                "content-prerequisite-corpus");
    yams::test::DaemonHarness harness{std::move(options)};
    REQUIRE(harness.start(30s));

    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto* daemonSync = serviceManager->testingMemorySyncService();
    REQUIRE(daemonSync != nullptr);
    auto repository = serviceManager->getMetadataRepo();
    REQUIRE(repository != nullptr);

    const auto localHash = digest(bytes("missing-local-content"));
    yams::metadata::BatchDocumentInsert local;
    local.info.filePath = "/local/missing.md";
    local.info.fileName = "missing.md";
    local.info.fileExtension = ".md";
    local.info.sha256Hash = localHash;
    local.info.mimeType = "text/markdown";
    std::vector<yams::metadata::BatchDocumentInsert> localInserts;
    localInserts.push_back(std::move(local));
    REQUIRE(repository->batchInsertDocumentsWithMetadata(localInserts).has_value());

    serviceManager->testingPublishMemorySyncBackfill();
    yams::memory_sync::MemorySyncService peer{
        makeFilesystemBackend(harness.dataDir() / "shared-memory"),
        yams::memory_sync::MemorySyncConfig{"content-peer", 60'000, "content-prerequisite-corpus",
                                            1}};
    REQUIRE(peer.syncOnce().has_value());
    CHECK_FALSE(peer.readCached("document/" + localHash).has_value());

    const auto remoteHash = digest(bytes("missing-remote-content"));
    yams::memory_sync::MetadataDocumentRecord remote;
    remote.documentId = remoteHash;
    remote.contentHash = remoteHash;
    remote.filePath = "/peer/missing.md";
    remote.fileName = "missing.md";
    remote.fileExtension = ".md";
    remote.mimeType = "text/markdown";
    REQUIRE(
        peer.publish("document/" + remoteHash, bytes(nlohmann::json(remote).dump())).has_value());
    REQUIRE(daemonSync->syncOnce().has_value());
    serviceManager->testingApplyMemorySyncWinners();
    const auto imported = repository->getDocumentByHash(remoteHash);
    REQUIRE(imported.has_value());
    CHECK_FALSE(imported.value().has_value());

    harness.stop();
    CHECK(harness.shutdownSucceeded());
}

TEST_CASE("Daemon memory sync stop during production backfill bounds published documents",
          "[integration][daemon][memory-sync][lifecycle]") {
    auto options = makeMemorySyncHarnessOptions("123e4567-e89b-42d3-a456-426614174011",
                                                "backfill-cancellation-corpus");
    yams::test::DaemonHarness harness{std::move(options)};
    REQUIRE(harness.start(30s));

    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto* daemonSync = serviceManager->testingMemorySyncService();
    REQUIRE(daemonSync != nullptr);
    auto repository = serviceManager->getMetadataRepo();
    auto contentStore = serviceManager->getContentStore();
    REQUIRE(repository != nullptr);
    REQUIRE(contentStore != nullptr);

    std::vector<std::string> hashes;
    std::vector<yams::metadata::BatchDocumentInsert> inserts;
    for (int index = 0; index < 2; ++index) {
        const auto payload = bytes("backfill-cancellation-" + std::to_string(index));
        auto stored = contentStore->storeBytes(payload);
        REQUIRE(stored.has_value());
        hashes.push_back(stored.value().contentHash);
        yams::metadata::BatchDocumentInsert item;
        item.info.filePath = "/local/backfill-" + std::to_string(index) + ".md";
        item.info.fileName = "backfill-" + std::to_string(index) + ".md";
        item.info.fileExtension = ".md";
        item.info.fileSize = static_cast<std::int64_t>(payload.size());
        item.info.sha256Hash = stored.value().contentHash;
        item.info.mimeType = "text/markdown";
        inserts.push_back(std::move(item));
    }
    REQUIRE(repository->batchInsertDocumentsWithMetadata(inserts).has_value());

    std::atomic<std::size_t> completedDocuments{0};
    serviceManager->testingSetMemorySyncStageObserver([&](std::string_view stage) {
        if (stage == "backfill.after_document") {
            completedDocuments.fetch_add(1, std::memory_order_relaxed);
            daemonSync->stop();
        }
    });
    serviceManager->testingPublishMemorySyncBackfill();
    CHECK(completedDocuments.load(std::memory_order_relaxed) == 1);

    yams::memory_sync::MemorySyncService peer{
        makeFilesystemBackend(harness.dataDir() / "shared-memory"),
        yams::memory_sync::MemorySyncConfig{"backfill-peer", 60'000, "backfill-cancellation-corpus",
                                            1}};
    REQUIRE(peer.syncOnce().has_value());
    std::size_t publishedDocuments = 0;
    for (const auto& hash : hashes) {
        if (peer.readCached("document/" + hash).has_value()) {
            ++publishedDocuments;
        }
    }
    CHECK(publishedDocuments == 1);

    harness.stop();
    CHECK(harness.shutdownSucceeded());
}

TEST_CASE("Daemon memory sync backfill budget resumes document cursor",
          "[integration][daemon][memory-sync][backfill]") {
    auto options = makeMemorySyncHarnessOptions("123e4567-e89b-42d3-a456-426614174012",
                                                "backfill-budget-corpus");
    yams::test::DaemonHarness harness{std::move(options)};
    REQUIRE(harness.start(30s));

    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto repository = serviceManager->getMetadataRepo();
    auto contentStore = serviceManager->getContentStore();
    auto kgStore = serviceManager->getKgStore();
    REQUIRE(repository != nullptr);
    REQUIRE(contentStore != nullptr);
    REQUIRE(kgStore != nullptr);

    yams::metadata::KGNode topologyNode;
    topologyNode.nodeKey = "entity:backfill-budget-source";
    topologyNode.type = "backfill-budget";
    auto sourceNodeId = kgStore->upsertNode(topologyNode);
    REQUIRE(sourceNodeId.has_value());
    topologyNode.nodeKey = "entity:backfill-budget-target";
    auto targetNodeId = kgStore->upsertNode(topologyNode);
    REQUIRE(targetNodeId.has_value());
    yams::metadata::KGEdge topologyEdge;
    topologyEdge.srcNodeId = sourceNodeId.value();
    topologyEdge.dstNodeId = targetNodeId.value();
    topologyEdge.relation = "backfill-budget-edge";
    REQUIRE(kgStore->addEdge(topologyEdge).has_value());

    std::vector<std::string> hashes;
    std::vector<yams::metadata::BatchDocumentInsert> inserts;
    for (int index = 0; index < 3; ++index) {
        const auto payload = bytes("backfill-budget-" + std::to_string(index));
        auto stored = contentStore->storeBytes(payload);
        REQUIRE(stored.has_value());
        hashes.push_back(stored.value().contentHash);
        yams::metadata::BatchDocumentInsert item;
        item.info.filePath = "/local/budget-" + std::to_string(index) + ".md";
        item.info.fileName = "budget-" + std::to_string(index) + ".md";
        item.info.fileExtension = ".md";
        item.info.fileSize = static_cast<std::int64_t>(payload.size());
        item.info.sha256Hash = stored.value().contentHash;
        item.info.mimeType = "text/markdown";
        inserts.push_back(std::move(item));
    }
    REQUIRE(repository->batchInsertDocumentsWithMetadata(inserts).has_value());

    serviceManager->testingSetMemorySyncBackfillItemBudget(1);
    std::vector<std::string> stages;
    serviceManager->testingSetMemorySyncStageObserver(
        [&](std::string_view stage) { stages.emplace_back(stage); });
    yams::memory_sync::MemorySyncService peer{
        makeFilesystemBackend(harness.dataDir() / "shared-memory"),
        yams::memory_sync::MemorySyncConfig{"backfill-peer", 60'000, "backfill-budget-corpus", 1}};

    serviceManager->testingPublishMemorySyncBackfill();
    REQUIRE(peer.syncOnce().has_value());
    std::size_t firstCycle = 0;
    for (const auto& hash : hashes) {
        firstCycle += peer.readCached("document/" + hash).has_value() ? 1U : 0U;
    }
    CHECK(firstCycle == 1);

    stages.clear();
    serviceManager->testingPublishMemorySyncBackfill();
    REQUIRE(peer.syncOnce().has_value());
    CHECK(std::find(stages.begin(), stages.end(), "backfill.after_node") != stages.end());
    std::size_t secondCycle = 0;
    for (const auto& hash : hashes) {
        secondCycle += peer.readCached("document/" + hash).has_value() ? 1U : 0U;
    }
    CHECK(secondCycle == 1);

    serviceManager->testingPublishMemorySyncBackfill();
    REQUIRE(peer.syncOnce().has_value());
    std::size_t thirdCycle = 0;
    for (const auto& hash : hashes) {
        thirdCycle += peer.readCached("document/" + hash).has_value() ? 1U : 0U;
    }
    CHECK(thirdCycle == 2);

    stages.clear();
    serviceManager->testingPublishMemorySyncBackfill();
    REQUIRE(peer.syncOnce().has_value());
    CHECK(std::find(stages.begin(), stages.end(), "backfill.after_edge") != stages.end());

    serviceManager->testingSetMemorySyncStageObserver({});
    harness.stop();
    CHECK(harness.shutdownSucceeded());
}

TEST_CASE("Daemon memory sync backfill skips a poisoned blob and continues",
          "[integration][daemon][memory-sync][backfill]") {
    auto options = makeMemorySyncHarnessOptions("123e4567-e89b-42d3-a456-426614174014",
                                                "backfill-skip-corpus");
    yams::test::DaemonHarness harness{std::move(options)};
    REQUIRE(harness.start(30s));

    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto repository = serviceManager->getMetadataRepo();
    REQUIRE(repository != nullptr);

    // Two documents; the first's blob is poisoned before the backfill runs.
    std::vector<std::string> hashes;
    std::vector<yams::metadata::BatchDocumentInsert> inserts;
    for (int index = 0; index < 2; ++index) {
        const auto payload = bytes("backfill-skip-" + std::to_string(index));
        auto stored = serviceManager->getContentStore()->storeBytes(payload);
        REQUIRE(stored.has_value());
        hashes.push_back(stored.value().contentHash);
        yams::metadata::BatchDocumentInsert item;
        item.info.filePath = "/local/skip-" + std::to_string(index) + ".md";
        item.info.fileName = "skip-" + std::to_string(index) + ".md";
        item.info.fileExtension = ".md";
        item.info.fileSize = static_cast<std::int64_t>(payload.size());
        item.info.sha256Hash = stored.value().contentHash;
        item.info.mimeType = "text/markdown";
        inserts.push_back(std::move(item));
    }
    REQUIRE(repository->batchInsertDocumentsWithMetadata(inserts).has_value());

    // Poison the first blob so its manifest is unrecoverable, then run backfill.
    serviceManager->__test_setContentStore(std::make_shared<ManifestPoisonedContentStore>(
        serviceManager->getContentStore(), hashes.front()));
    std::vector<std::string> stages;
    serviceManager->testingSetMemorySyncStageObserver(
        [&](std::string_view stage) { stages.emplace_back(stage); });
    serviceManager->testingPublishMemorySyncBackfill();

    CHECK(std::find(stages.begin(), stages.end(), "backfill.skip_document") != stages.end());

    // The poisoned document is never replicated; the backfill advances past it
    // and publishes the next document instead of wedging the domain.
    yams::memory_sync::MemorySyncService peer{
        makeFilesystemBackend(harness.dataDir() / "shared-memory"),
        yams::memory_sync::MemorySyncConfig{"backfill-skip-peer", 60'000, "backfill-skip-corpus",
                                            1}};
    REQUIRE(peer.syncOnce().has_value());
    CHECK_FALSE(peer.readCached("document/" + hashes.front()).has_value());
    CHECK(peer.readCached("document/" + hashes.back()).has_value());

    serviceManager->testingSetMemorySyncStageObserver({});
    harness.stop();
    CHECK(harness.shutdownSucceeded());
}

TEST_CASE("Daemon direct deltas immediately hydrate content and metadata",
          "[integration][daemon][memory-sync][direct-delta]") {
    auto options =
        makeMemorySyncHarnessOptions("123e4567-e89b-42d3-a456-426614174015", "direct-apply-corpus");
    yams::test::DaemonHarness harness{std::move(options)};
    REQUIRE(harness.start(30s));

    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto* daemonSync = serviceManager->testingMemorySyncService();
    REQUIRE(daemonSync != nullptr);
    auto repository = serviceManager->getMetadataRepo();
    auto contentStore = serviceManager->getContentStore();
    REQUIRE(repository != nullptr);
    REQUIRE(contentStore != nullptr);

    yams::memory_sync::MemorySyncService peer{
        makeFilesystemBackend(harness.dataDir() / "direct-peer-memory"),
        yams::memory_sync::MemorySyncConfig{"direct-peer", 60'000, "direct-apply-corpus", 1}};
    const auto payload = bytes("direct-delta-searchable-content");
    const auto hash = digest(payload);
    REQUIRE(peer.publish("content-blob/" + hash, payload).has_value());

    yams::memory_sync::MetadataDocumentRecord remote;
    remote.documentId = hash;
    remote.filePath = "/peer/direct.md";
    remote.fileName = "direct.md";
    remote.fileExtension = ".md";
    remote.fileSize = static_cast<std::int64_t>(payload.size());
    remote.contentHash = hash;
    remote.mimeType = "text/markdown";
    remote.contentExtracted = true;
    REQUIRE(peer.publish("document/" + hash, bytes(nlohmann::json(remote).dump())).has_value());

    auto deltas = peer.exportLocalDeltasAfter({});
    REQUIRE(deltas.has_value());
    REQUIRE(deltas.value().deltas.size() == 2);
    auto applied = daemonSync->applyDeltas(deltas.value().deltas);
    REQUIRE(applied.has_value());
    CHECK(applied.value().merged == 2);

    auto contentExists = contentStore->exists(hash);
    REQUIRE(contentExists.has_value());
    CHECK(contentExists.value());
    auto replicatedBytes = contentStore->retrieveBytes(hash);
    REQUIRE(replicatedBytes.has_value());
    CHECK(replicatedBytes.value() == payload);
    auto imported = repository->getDocumentByHash(hash);
    REQUIRE(imported.has_value());
    REQUIRE(imported.value().has_value());
    CHECK(imported.value()->fileName == "direct.md");
    CHECK(imported.value()->sha256Hash == hash);

    // Remote metadata is not enough for retrieval parity: applying a direct delta must enqueue
    // the same extraction/FTS path used by a local add.
    std::optional<yams::metadata::DocumentContent> indexedContent;
    const auto searchDeadline = std::chrono::steady_clock::now() + scaledTimeout(3s);
    while (std::chrono::steady_clock::now() < searchDeadline) {
        auto content = repository->getContent(imported.value()->id);
        REQUIRE(content.has_value());
        if (content.value()) {
            indexedContent = std::move(*content.value());
            break;
        }
        std::this_thread::sleep_for(10ms);
    }
    REQUIRE(indexedContent.has_value());
    CHECK(indexedContent->contentText.find("direct-delta-searchable-content") != std::string::npos);

    harness.stop();
    CHECK(harness.shutdownSucceeded());
}

TEST_CASE("Daemon temporary memory sync converges inside one session and cleans only its namespace",
          "[integration][daemon][memory-sync][temporary]") {
    yams::test::DaemonHarness::Options options;
    options.enableModelProvider = false;
    options.useMockModelProvider = false;
    options.autoLoadPlugins = false;
    options.enableAutoRepair = false;
    options.isolateConfig = true;
    options.isolateState = true;
    options.configureDaemon = [](yams::daemon::DaemonConfig& config) {
        config.memorySync.enabled = true;
        config.memorySync.corpusScope = yams::memory_sync::CorpusScope::Shared;
        config.memorySync.nodeId = "123e4567-e89b-42d3-a456-426614174001";
        config.memorySync.corpusId = "temporary-integration-corpus";
        config.memorySync.corpusEpoch = 1;
        config.memorySync.transport = "shared-store";
        config.memorySync.backend = "filesystem";
        config.memorySync.path = "shared-memory";
        config.memorySync.syncIntervalMs = 25;
        config.memorySync.mode = "temporary";
        config.memorySync.sessionId = "integration-session";
    };
    // DaemonHarness constructs YamsDaemon directly, so this case injects memory-sync policy
    // through configureDaemon. ConfigResolver's normal TOML path is covered separately.
    options.isolatedConfigContents = R"(
[daemon]
auto_load_plugins = false
auto_repair = false
)";

    yams::test::DaemonHarness harness{std::move(options)};
    const auto sharedRoot = harness.dataDir() / "shared-memory";
    const auto sessionPath = sharedRoot / ".sessions" / "integration-session";
    const auto otherSessionPath = sharedRoot / ".sessions" / "other-session";
    std::filesystem::create_directories(sharedRoot);
    const auto persistentMarker = sharedRoot / "persistent-marker";
    {
        std::ofstream marker(persistentMarker);
        REQUIRE(marker.good());
        marker << "keep";
    }

    REQUIRE(harness.start(30s));
    auto* serviceManager = harness.daemon()->getServiceManager();
    REQUIRE(serviceManager != nullptr);
    auto* daemonSync = serviceManager->testingMemorySyncService();
    REQUIRE(daemonSync != nullptr);

    yams::memory_sync::MemorySyncService sameSession{
        makeFilesystemBackend(sessionPath),
        yams::memory_sync::MemorySyncConfig{"peer-session", 25, "temporary-integration-corpus", 1}};
    REQUIRE(sameSession.publish("temporary-key", bytes("same-session")).has_value());
    const auto convergenceDeadline = std::chrono::steady_clock::now() + scaledTimeout(3s);
    while (!daemonSync->testingHasMergedRecord("temporary-key") &&
           std::chrono::steady_clock::now() < convergenceDeadline) {
        std::this_thread::sleep_for(10ms);
    }
    REQUIRE(daemonSync->testingHasMergedRecord("temporary-key"));

    yams::memory_sync::MemorySyncService otherSession{
        makeFilesystemBackend(otherSessionPath),
        yams::memory_sync::MemorySyncConfig{"other-session", 25, "temporary-integration-corpus",
                                            1}};
    REQUIRE(otherSession.publish("isolated-key", bytes("other-session")).has_value());
    std::this_thread::sleep_for(100ms);
    CHECK_FALSE(daemonSync->testingHasMergedRecord("isolated-key"));

    harness.stop();
    CHECK(harness.shutdownSucceeded());
    CHECK(std::filesystem::exists(persistentMarker));
    CHECK(std::filesystem::exists(otherSessionPath));
    CHECK((!std::filesystem::exists(sessionPath) || std::filesystem::is_empty(sessionPath)));
}
