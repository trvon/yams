// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

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
