// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <yams/memory_sync/content_blob_sync.h>
#include <yams/storage/storage_backend.h>
#include <yams/storage/storage_engine.h>

using namespace yams::memory_sync;

namespace {

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> result(text.size());
    std::memcpy(result.data(), text.data(), text.size());
    return result;
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

std::unique_ptr<yams::storage::FilesystemBackend> makeBackend(const std::filesystem::path& path) {
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = path;
    auto backend = std::make_unique<yams::storage::FilesystemBackend>();
    REQUIRE(backend->initialize(config).has_value());
    return backend;
}

std::unique_ptr<yams::storage::StorageEngine> makeContentStore(const std::filesystem::path& path) {
    yams::storage::StorageConfig config;
    config.basePath = path;
    return std::make_unique<yams::storage::StorageEngine>(config);
}

struct TempDirGuard {
    TempDirGuard() {
        path = std::filesystem::temp_directory_path() /
               ("yams-content-blob-sync-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    ~TempDirGuard() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }

    std::filesystem::path path;
};

} // namespace

TEST_CASE("content blob adapter mirrors a local write and hydrates a missing peer blob",
          "[memory-sync][content-blob]") {
    TempDirGuard temp;
    auto storeA = makeContentStore(temp.path / "content-a");
    auto storeB = makeContentStore(temp.path / "content-b");
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService syncB{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    ContentBlobSyncAdapter a{*storeA, syncA};
    ContentBlobSyncAdapter b{*storeB, syncB};

    const auto payload = bytes("content addressed memory");
    const auto blobHash = digest(payload);
    REQUIRE(a.store(blobHash, payload).has_value());

    const auto before = storeB->exists(blobHash);
    REQUIRE(before.has_value());
    CHECK_FALSE(before.value());

    REQUIRE(syncB.syncOnce().has_value());
    REQUIRE(b.applyCached(blobHash).has_value());
    const auto copied = storeB->retrieve(blobHash);
    REQUIRE(copied.has_value());
    CHECK(text(copied.value()) == "content addressed memory");

    REQUIRE(a.publishDelete(blobHash).has_value());
    const auto tombstones = syncB.syncOnce();
    REQUIRE(tombstones.has_value());
    REQUIRE(tombstones.value().at("content-blob/" + blobHash).isTombstone());
    const auto removed = b.applyDelete(blobHash);
    REQUIRE(removed.has_value());
    CHECK(removed.value());
    const auto afterDelete = storeB->exists(blobHash);
    REQUIRE(afterDelete.has_value());
    CHECK_FALSE(afterDelete.value());
}

TEST_CASE("daemon content pass hydrates every exclusive winner beyond resident budgets",
          "[memory-sync][content-blob][paged-adapter][disk-backed-serving]") {
    TempDirGuard temp;
    auto target = makeContentStore(temp.path / "target");
    std::vector<std::vector<std::byte>> payloads;
    std::vector<std::string> hashes;
    {
        MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"writer"}};
        for (int i = 0; i < 6; ++i) {
            payloads.push_back(bytes("blob-" + std::to_string(i)));
            hashes.push_back(digest(payloads.back()));
            REQUIRE(writer.publish("content-blob/" + hashes.back(), payloads.back()).has_value());
        }
    }
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 2;
    SECTION("metadata slots") {
        limits.maxMergedKeys = 2;
    }
    SECTION("payload cache bytes") {
        limits.maxCacheBytes = 2 * payloads.front().size();
    }
    MemorySyncConfig config{"reader", 60'000, "local-test-corpus", 1, limits};
    config.exclusiveBackend = true;
    MemorySyncService reader{makeBackend(temp.path / "sync"), config};
    REQUIRE(reader.syncFully().has_value());
    REQUIRE(reader.mergedRecordCount() == hashes.size());
    ContentBlobSyncAdapter adapter{*target, reader};
    const auto applied = adapter.applyCommitted();
    REQUIRE(applied.has_value());
    REQUIRE_FALSE(applied.value().failure.has_value());
    CHECK(applied.value().applied == hashes.size());
    CHECK(applied.value().deferredKeys.empty());
    for (std::size_t i = 0; i < hashes.size(); ++i) {
        const auto actual = target->retrieve(hashes[i]);
        CHECK(actual.has_value());
        if (actual) {
            CHECK(actual.value() == payloads[i]);
        }
    }
    const auto repeated = adapter.applyCommitted();
    REQUIRE(repeated.has_value());
    CHECK_FALSE(repeated.value().failure.has_value());
    CHECK(repeated.value().deferredKeys.empty());
    CHECK(repeated.value().applied == 0);
}

TEST_CASE("daemon content pass reports cold faults without starving later pages",
          "[memory-sync][content-blob][paged-adapter][fault]") {
    TempDirGuard temp;
    auto target = makeContentStore(temp.path / "target");
    std::vector<std::pair<std::string, std::vector<std::byte>>> payloads;
    {
        MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"writer"}};
        for (int i = 0; i < 6; ++i) {
            auto payload = bytes("cold-" + std::to_string(i));
            auto hash = digest(payload);
            REQUIRE(writer.publish("content-blob/" + hash, payload).has_value());
            payloads.emplace_back(std::move(hash), std::move(payload));
        }
    }
    std::sort(payloads.begin(), payloads.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    MemorySyncLimits limits;
    limits.maxIndexObjectsPerSync = 1;
    limits.maxMergedKeys = 1;
    MemorySyncConfig config{"reader", 60'000, "local-test-corpus", 1, limits};
    config.exclusiveBackend = true;
    MemorySyncService reader{makeBackend(temp.path / "sync"), config};
    REQUIRE(reader.syncFully().has_value());
    REQUIRE(reader.mergedRecordCount() == payloads.size());
    // Make the sole resident payload explicit, so all injected faults are cold even if cache
    // eviction order changes. This refresh occurs once, before the daemon-stage capture.
    REQUIRE(reader.readCommitted("content-blob/" + payloads.front().first).has_value());
    REQUIRE(reader.syncFully().has_value());
    REQUIRE(reader.readCached("content-blob/" + payloads.front().first).has_value());
    const auto& corrupt = payloads[1];
    const auto& missing = payloads[2];
    const auto& laterGood = payloads.back();
    // Fault the authoritative payload objects after successful recovery; envelopes and the
    // pinned winner generation are unchanged. Other backend handles model storage faults,
    // not concurrent writers participating in the exclusive-owner protocol.
    auto source = makeBackend(temp.path / "sync");
    REQUIRE(source->store("blob/" + corrupt.first, bytes("wrong bytes")).has_value());
    REQUIRE(source->remove("blob/" + missing.first).has_value());
    ContentBlobSyncAdapter adapter{*target, reader};
    const auto pass = adapter.applyCommitted();
    REQUIRE(pass.has_value());
    REQUIRE(pass.value().failure.has_value());
    CHECK(pass.value().failure->code == yams::ErrorCode::HashMismatch);
    CHECK(pass.value().applied == payloads.size() - 2);
    REQUIRE(pass.value().deferredKeys.size() == 1);
    CHECK(pass.value().deferredKeys.front() == "content-blob/" + missing.first);
    const auto later = target->retrieve(laterGood.first);
    REQUIRE(later.has_value());
    CHECK(later.value() == laterGood.second);
    for (const auto& hash : {corrupt.first, missing.first}) {
        const auto exists = target->exists(hash);
        REQUIRE(exists.has_value());
        CHECK_FALSE(exists.value());
    }
    REQUIRE(source->store("blob/" + missing.first, missing.second).has_value());
    const auto retried = adapter.applyCommitted();
    REQUIRE(retried.has_value());
    CHECK(retried.value().applied == 1);
    CHECK(retried.value().deferredKeys.empty());
    REQUIRE(retried.value().failure.has_value());
    CHECK(retried.value().failure->code == yams::ErrorCode::HashMismatch);
    REQUIRE(source->store("blob/" + corrupt.first, corrupt.second).has_value());
    const auto repaired = adapter.applyCommitted();
    REQUIRE(repaired.has_value());
    CHECK(repaired.value().applied == 1);
    CHECK_FALSE(repaired.value().failure.has_value());
    CHECK(repaired.value().deferredKeys.empty());
}

TEST_CASE("content blob adapter rejects a valid-looking hash for different bytes",
          "[memory-sync][content-blob][integrity]") {
    TempDirGuard temp;
    auto store = makeContentStore(temp.path / "content");
    MemorySyncService sync{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    ContentBlobSyncAdapter adapter{*store, sync};

    const auto payload = bytes("payload");
    const auto stored = adapter.store(std::string(64, 'a'), payload);
    REQUIRE_FALSE(stored.has_value());
    CHECK(stored.error().code == yams::ErrorCode::HashMismatch);
    const auto exists = store->exists(std::string(64, 'a'));
    REQUIRE(exists.has_value());
    CHECK_FALSE(exists.value());
}

TEST_CASE("content blob adapter rejects non-content-addressed identifiers",
          "[memory-sync][content-blob]") {
    TempDirGuard temp;
    auto store = makeContentStore(temp.path / "content");
    MemorySyncService sync{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    ContentBlobSyncAdapter adapter{*store, sync};

    const auto stored = adapter.store("not-a-sha256", bytes("payload"));
    REQUIRE_FALSE(stored.has_value());
    CHECK(stored.error().code == yams::ErrorCode::InvalidArgument);
}

TEST_CASE("content blob winner pass reports a failure and a deferral from the same scan",
          "[memory-sync][content-blob][defer]") {
    TempDirGuard temp;
    auto writerStore = makeContentStore(temp.path / "content-writer");
    auto readerStore = makeContentStore(temp.path / "content-reader");
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"W", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"R", 50}};

    // A corrupt blob: its key claims one hash while the replicated bytes hash to another.
    const auto claimed = digest(bytes("claimed-bytes"));
    REQUIRE(writer.publish("content-blob/" + claimed, bytes("different-bytes")).has_value());
    auto winners = reader.syncOnce();
    REQUIRE(winners.has_value());
    REQUIRE(winners.value().contains("content-blob/" + claimed));

    // A winner whose bytes have not reached the reader's sync cache yet.
    const auto late = bytes("late-bytes");
    const auto lateHash = digest(late);
    ContentBlobSyncAdapter writerAdapter{*writerStore, writer};
    REQUIRE(writerAdapter.store(lateHash, late).has_value());
    const auto writerView = writer.syncOnce();
    REQUIRE(writerView.has_value());
    const auto lateKey = "content-blob/" + lateHash;
    winners.value().emplace(lateKey, writerView.value().at(lateKey));

    ContentBlobSyncAdapter readerAdapter{*readerStore, reader};
    const auto pass = readerAdapter.applyWinners(winners.value());
    REQUIRE(pass.failure.has_value());
    CHECK(pass.failure->code == yams::ErrorCode::HashMismatch);
    REQUIRE(pass.deferredKeys.size() == 1);
    CHECK(pass.deferredKeys.front() == lateKey);
    CHECK(pass.applied == 0);
}
