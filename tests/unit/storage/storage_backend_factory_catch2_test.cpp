// SPDX-License-Identifier: GPL-3.0-or-later
// StorageBackendFactory unit tests — cover parseURL() scheme handling and
// create() dispatch for each built-in backend type.

#include <catch2/catch_test_macros.hpp>
#include <yams/storage/storage_backend.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

using namespace yams::storage;

TEST_CASE("StorageBackendFactory::parseURL: bare path becomes filesystem backend",
          "[storage][factory][parseURL]") {
    auto cfg = StorageBackendFactory::parseURL("/tmp/yams-test-store");
    CHECK(cfg.type == "filesystem");
    CHECK(cfg.localPath == std::filesystem::path("/tmp/yams-test-store"));
    CHECK(cfg.url.empty());
}

TEST_CASE("StorageBackendFactory::parseURL: file:// scheme strips prefix",
          "[storage][factory][parseURL]") {
    auto cfg = StorageBackendFactory::parseURL("file:///var/lib/yams");
    CHECK(cfg.type == "filesystem");
    CHECK(cfg.localPath == std::filesystem::path("/var/lib/yams"));
}

TEST_CASE("StorageBackendFactory::parseURL: s3:// scheme sets type and preserves URL",
          "[storage][factory][parseURL]") {
    auto cfg = StorageBackendFactory::parseURL("s3://my-bucket/prefix");
    CHECK(cfg.type == "s3");
    CHECK(cfg.url == "s3://my-bucket/prefix");
}

TEST_CASE("StorageBackendFactory::parseURL: http and https map to their schemes",
          "[storage][factory][parseURL]") {
    auto http = StorageBackendFactory::parseURL("http://example.com/data");
    CHECK(http.type == "http");
    CHECK(http.url == "http://example.com/data");

    auto https = StorageBackendFactory::parseURL("https://example.com/data");
    CHECK(https.type == "https");
    CHECK(https.url == "https://example.com/data");
}

TEST_CASE("StorageBackendFactory::parseURL: ftp and ftps fold to ftp",
          "[storage][factory][parseURL]") {
    auto ftp = StorageBackendFactory::parseURL("ftp://host/path");
    CHECK(ftp.type == "ftp");

    auto ftps = StorageBackendFactory::parseURL("ftps://host/path");
    CHECK(ftps.type == "ftp");
}

TEST_CASE("StorageBackendFactory::parseURL: unknown scheme falls back to filesystem",
          "[storage][factory][parseURL]") {
    auto cfg = StorageBackendFactory::parseURL("gopher://old.example.com/x");
    CHECK(cfg.type == "filesystem");
    CHECK(cfg.localPath == std::filesystem::path("gopher://old.example.com/x"));
}

TEST_CASE("StorageBackendFactory::parseURL: query parameters populate cache/timeout",
          "[storage][factory][parseURL]") {
    auto cfg = StorageBackendFactory::parseURL("s3://b/p?cache_size=1024&cache_ttl=60&timeout=5");
    CHECK(cfg.type == "s3");
    CHECK(cfg.cacheSize == 1024u);
    CHECK(cfg.cacheTTL == 60u);
    CHECK(cfg.requestTimeout == 5u);
}

TEST_CASE("StorageBackendFactory::parseURL: invalid numeric params are ignored",
          "[storage][factory][parseURL]") {
    auto cfg = StorageBackendFactory::parseURL("s3://b/p?cache_size=not-a-number");
    CHECK(cfg.type == "s3");
    // Default is preserved when parse fails.
    CHECK(cfg.cacheSize == BackendConfig{}.cacheSize);
}

TEST_CASE("StorageBackendFactory::create: filesystem backend initializes in temp dir",
          "[storage][factory][create]") {
    static std::atomic<uint64_t> counter{0};
    auto dir = std::filesystem::temp_directory_path() /
               ("yams_sbf_test_" + std::to_string(reinterpret_cast<std::uintptr_t>(&counter)) +
                "_" + std::to_string(counter.fetch_add(1)));

    BackendConfig cfg;
    cfg.type = "filesystem";
    cfg.localPath = dir;

    auto backend = StorageBackendFactory::create(cfg);
    REQUIRE(backend != nullptr);
    CHECK(std::filesystem::exists(dir / "objects"));
    CHECK(std::filesystem::exists(dir / "temp"));

    std::filesystem::remove_all(dir);
}

TEST_CASE("StorageBackendFactory::create: type aliases 'local' and 'file' work",
          "[storage][factory][create]") {
    static std::atomic<uint64_t> counter{0};
    for (auto alias : {"local", "file", "FileSystem"}) {
        auto dir = std::filesystem::temp_directory_path() /
                   ("yams_sbf_alias_" + std::to_string(reinterpret_cast<std::uintptr_t>(&counter)) +
                    "_" + std::to_string(counter.fetch_add(1)));

        BackendConfig cfg;
        cfg.type = alias;
        cfg.localPath = dir;
        auto backend = StorageBackendFactory::create(cfg);
        CHECK(backend != nullptr);
        std::filesystem::remove_all(dir);
    }
}

TEST_CASE("StorageBackendFactory::create: unknown backend type returns nullptr",
          "[storage][factory][create]") {
    BackendConfig cfg;
    cfg.type = "does-not-exist";
    auto backend = StorageBackendFactory::create(cfg);
    CHECK(backend == nullptr);
}

TEST_CASE("StorageBackendFactory::create: missing explicit plugin returns nullptr",
          "[storage][factory][create][plugin]") {
    BackendConfig cfg;
    cfg.type = "plugin:yams_missing_plugin_for_test";
    cfg.url = "s3://bucket/prefix";
    auto backend = StorageBackendFactory::create(cfg);
    CHECK(backend == nullptr);
}

TEST_CASE("StorageBackendFactory::create: registered backend init failure returns nullptr",
          "[storage][factory][create]") {
    struct FailingInitBackend : IStorageBackend {
        yams::Result<void> initialize(const BackendConfig&) override {
            return yams::Error{yams::ErrorCode::InvalidState, "init failed"};
        }
        yams::Result<void> store(std::string_view, std::span<const std::byte>) override {
            return yams::Error{yams::ErrorCode::InvalidState, "not initialized"};
        }
        yams::Result<std::vector<std::byte>> retrieve(std::string_view) const override {
            return yams::Error{yams::ErrorCode::InvalidState, "not initialized"};
        }
        yams::Result<bool> exists(std::string_view) const override {
            return yams::Error{yams::ErrorCode::InvalidState, "not initialized"};
        }
        yams::Result<void> remove(std::string_view) override {
            return yams::Error{yams::ErrorCode::InvalidState, "not initialized"};
        }
        yams::Result<std::vector<std::string>> list(std::string_view) const override {
            return yams::Error{yams::ErrorCode::InvalidState, "not initialized"};
        }
        yams::Result<yams::StorageStats> getStats() const override {
            return yams::Error{yams::ErrorCode::InvalidState, "not initialized"};
        }
        std::future<yams::Result<void>> storeAsync(std::string_view,
                                                   std::span<const std::byte>) override {
            std::promise<yams::Result<void>> p;
            p.set_value(yams::Error{yams::ErrorCode::InvalidState, "not initialized"});
            return p.get_future();
        }
        std::future<yams::Result<std::vector<std::byte>>>
        retrieveAsync(std::string_view) const override {
            std::promise<yams::Result<std::vector<std::byte>>> p;
            p.set_value(yams::Error{yams::ErrorCode::InvalidState, "not initialized"});
            return p.get_future();
        }
        std::string getType() const override { return "yams_test_failing_init"; }
        bool isRemote() const override { return true; }
        yams::Result<void> flush() override {
            return yams::Error{yams::ErrorCode::InvalidState, "not initialized"};
        }
    };

    StorageBackendFactory::registerBackendType<FailingInitBackend>("yams_test_failing_init");

    BackendConfig cfg;
    cfg.type = "yams_test_failing_init";
    auto backend = StorageBackendFactory::create(cfg);
    CHECK(backend == nullptr);
}

TEST_CASE("StorageBackendFactory::create: URL and S3 backends initialize without network I/O",
          "[storage][factory][remote][url][s3]") {
    for (const auto& [type, url] : {
             std::pair{"http", "http://127.0.0.1:9/yams-test"},
             std::pair{"https", "https://example.invalid/yams-test"},
             std::pair{"ftp", "ftp://example.invalid/yams-test"},
             std::pair{"s3", "s3://yams-test-bucket/prefix"},
         }) {
        BackendConfig cfg;
        cfg.type = type;
        cfg.url = url;
        cfg.requestTimeout = 1;
        cfg.maxRetries = 0;

        auto backend = StorageBackendFactory::create(cfg);
        REQUIRE(backend != nullptr);
        CHECK(backend->isRemote());
        if (std::string_view(type) == "s3") {
            // S3 may be served by an optional plugin or by the URLBackend fallback.
            CHECK_FALSE(backend->getType().empty());
        } else {
            CHECK(backend->getType() == type);
        }
        CHECK(backend->flush().has_value());
    }
}

TEST_CASE("StorageBackendFactory::registerBackend: custom type can be created",
          "[storage][factory][create]") {
    // A minimal fake backend that always initializes successfully.
    struct FakeBackend : IStorageBackend {
        yams::Result<void> initialize(const BackendConfig&) override { return {}; }
        yams::Result<void> store(std::string_view, std::span<const std::byte>) override {
            return yams::Error{yams::ErrorCode::NotImplemented, "fake"};
        }
        yams::Result<std::vector<std::byte>> retrieve(std::string_view) const override {
            return yams::Error{yams::ErrorCode::NotImplemented, "fake"};
        }
        yams::Result<bool> exists(std::string_view) const override { return false; }
        yams::Result<void> remove(std::string_view) override {
            return yams::Error{yams::ErrorCode::NotImplemented, "fake"};
        }
        yams::Result<std::vector<std::string>> list(std::string_view) const override {
            return std::vector<std::string>{};
        }
        yams::Result<yams::StorageStats> getStats() const override { return yams::StorageStats{}; }
        std::future<yams::Result<void>> storeAsync(std::string_view,
                                                   std::span<const std::byte>) override {
            std::promise<yams::Result<void>> p;
            p.set_value(yams::Error{yams::ErrorCode::NotImplemented, "fake"});
            return p.get_future();
        }
        std::future<yams::Result<std::vector<std::byte>>>
        retrieveAsync(std::string_view) const override {
            std::promise<yams::Result<std::vector<std::byte>>> p;
            p.set_value(yams::Error{yams::ErrorCode::NotImplemented, "fake"});
            return p.get_future();
        }
        std::string getType() const override { return "yams_test_fake"; }
        bool isRemote() const override { return false; }
        yams::Result<void> flush() override { return {}; }
    };

    StorageBackendFactory::registerBackendType<FakeBackend>("yams_test_fake");

    BackendConfig cfg;
    cfg.type = "yams_test_fake";
    auto backend = StorageBackendFactory::create(cfg);
    CHECK(backend != nullptr);
}

TEST_CASE("StorageBackendFactory::registerBackend: mocked remote backend supports CRUD",
          "[storage][factory][remote][mock][plugin]") {
    struct MockRemoteBackend : IStorageBackend {
        mutable std::mutex mutex;
        std::unordered_map<std::string, std::vector<std::byte>> objects;

        yams::Result<void> initialize(const BackendConfig&) override { return {}; }

        yams::Result<void> store(std::string_view key, std::span<const std::byte> data) override {
            std::lock_guard lock(mutex);
            objects[std::string(key)] = std::vector<std::byte>(data.begin(), data.end());
            return {};
        }

        yams::Result<std::vector<std::byte>> retrieve(std::string_view key) const override {
            std::lock_guard lock(mutex);
            auto it = objects.find(std::string(key));
            if (it == objects.end()) {
                return yams::Error{yams::ErrorCode::ChunkNotFound, "mock key not found"};
            }
            return it->second;
        }

        yams::Result<bool> exists(std::string_view key) const override {
            std::lock_guard lock(mutex);
            return objects.contains(std::string(key));
        }

        yams::Result<void> remove(std::string_view key) override {
            std::lock_guard lock(mutex);
            objects.erase(std::string(key));
            return {};
        }

        yams::Result<std::vector<std::string>> list(std::string_view prefix) const override {
            std::lock_guard lock(mutex);
            std::vector<std::string> keys;
            for (const auto& [key, _] : objects) {
                if (key.starts_with(prefix)) {
                    keys.push_back(key);
                }
            }
            std::ranges::sort(keys);
            return keys;
        }

        yams::Result<yams::StorageStats> getStats() const override {
            std::lock_guard lock(mutex);
            yams::StorageStats stats;
            stats.totalObjects = objects.size();
            size_t totalBytes = 0;
            for (const auto& [_, value] : objects) {
                totalBytes += value.size();
            }
            stats.totalBytes = totalBytes;
            return stats;
        }

        std::future<yams::Result<void>> storeAsync(std::string_view key,
                                                   std::span<const std::byte> data) override {
            return std::async(std::launch::async,
                              [this, key = std::string(key),
                               copy = std::vector<std::byte>(data.begin(), data.end())]() {
                                  return store(key, copy);
                              });
        }

        std::future<yams::Result<std::vector<std::byte>>>
        retrieveAsync(std::string_view key) const override {
            return std::async(std::launch::async,
                              [this, key = std::string(key)]() { return retrieve(key); });
        }

        std::string getType() const override { return "yams_test_remote"; }
        bool isRemote() const override { return true; }
        yams::Result<void> flush() override { return {}; }
    };

    StorageBackendFactory::registerBackendType<MockRemoteBackend>("yams_test_remote");

    BackendConfig cfg;
    cfg.type = "yams_test_remote";
    auto backend = StorageBackendFactory::create(cfg);
    REQUIRE(backend != nullptr);
    CHECK(backend->isRemote());

    const std::vector<std::byte> payload{std::byte{static_cast<unsigned char>('r')},
                                         std::byte{static_cast<unsigned char>('e')},
                                         std::byte{static_cast<unsigned char>('m')}};
    REQUIRE(backend->store("docs/a", payload).has_value());
    REQUIRE(backend->storeAsync("docs/b", payload).get().has_value());

    auto exists = backend->exists("docs/a");
    REQUIRE(exists.has_value());
    CHECK(exists.value());

    auto readBack = backend->retrieveAsync("docs/a").get();
    REQUIRE(readBack.has_value());
    CHECK(readBack.value() == payload);

    auto listed = backend->list("docs/");
    REQUIRE(listed.has_value());
    CHECK(listed.value() == std::vector<std::string>{"docs/a", "docs/b"});

    auto stats = backend->getStats();
    REQUIRE(stats.has_value());
    CHECK(stats.value().totalObjects == 2u);
    CHECK(stats.value().totalBytes == payload.size() * 2u);

    REQUIRE(backend->remove("docs/a").has_value());
    auto existsAfterRemove = backend->exists("docs/a");
    REQUIRE(existsAfterRemove.has_value());
    CHECK_FALSE(existsAfterRemove.value());
}
