// SPDX-License-Identifier: GPL-3.0-or-later
// StorageBackendFactory unit tests — cover parseURL() scheme handling and
// create() dispatch for each built-in backend type.

#include <catch2/catch_test_macros.hpp>
#include <yams/storage/storage_backend.h>

#include <atomic>
#include <filesystem>
#include <future>
#include <memory>
#include <span>
#include <string>
#include <string_view>
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
