#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <future>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <yams/storage/storage_backend.h>
#include <yams/storage/storage_backend_engine_adapter.h>

namespace {

class TempDir {
public:
    TempDir() {
        path_ = std::filesystem::temp_directory_path() /
                ("yams_storage_adapter_test_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

std::vector<std::byte> bytesFromString(std::string_view input) {
    std::vector<std::byte> out(input.size());
    std::transform(input.begin(), input.end(), out.begin(),
                   [](char c) { return std::byte{static_cast<unsigned char>(c)}; });
    return out;
}

class ScriptedBackend final : public yams::storage::IStorageBackend {
public:
    bool failStore{false};
    bool failRetrieve{false};
    bool failRemove{false};
    bool failList{false};
    bool failStats{false};
    bool throwExists{false};
    bool remote{false};
    mutable size_t retrieveCalls{0};
    mutable size_t listCalls{0};
    mutable size_t statsCalls{0};

    yams::Result<void> initialize(const yams::storage::BackendConfig&) override { return {}; }

    yams::Result<void> store(std::string_view key, std::span<const std::byte> data) override {
        if (failStore) {
            return yams::Error{yams::ErrorCode::WriteError, "scripted store failure"};
        }
        objects_[std::string(key)] = std::vector<std::byte>(data.begin(), data.end());
        return {};
    }

    yams::Result<std::vector<std::byte>> retrieve(std::string_view key) const override {
        ++retrieveCalls;
        if (failRetrieve) {
            return yams::Error{yams::ErrorCode::NotFound, "scripted retrieve failure"};
        }
        auto it = objects_.find(std::string(key));
        if (it == objects_.end()) {
            return yams::Error{yams::ErrorCode::NotFound, "scripted key missing"};
        }
        return it->second;
    }

    yams::Result<bool> exists(std::string_view key) const override {
        if (throwExists) {
            throw std::runtime_error("scripted exists failure");
        }
        return objects_.contains(std::string(key));
    }

    yams::Result<void> remove(std::string_view key) override {
        if (failRemove) {
            return yams::Error{yams::ErrorCode::IOError, "scripted remove failure"};
        }
        objects_.erase(std::string(key));
        return {};
    }

    yams::Result<std::vector<std::string>> list(std::string_view prefix = "") const override {
        ++listCalls;
        if (failList) {
            return yams::Error{yams::ErrorCode::NetworkError, "scripted list failure"};
        }

        std::vector<std::string> keys;
        for (const auto& [key, _] : objects_) {
            if (prefix.empty() || key.starts_with(prefix)) {
                keys.push_back(key);
            }
        }
        return keys;
    }

    yams::Result<::yams::StorageStats> getStats() const override {
        ++statsCalls;
        if (failStats) {
            return yams::Error{yams::ErrorCode::NetworkError, "scripted stats failure"};
        }
        ::yams::StorageStats stats;
        stats.totalObjects = objects_.size();
        for (const auto& [_, value] : objects_) {
            stats.totalBytes += value.size();
        }
        return stats;
    }

    std::future<yams::Result<void>> storeAsync(std::string_view key,
                                               std::span<const std::byte> data) override {
        return std::async(std::launch::deferred,
                          [this, key = std::string(key),
                           data = std::vector<std::byte>(data.begin(), data.end())]() {
                              return store(key, data);
                          });
    }

    std::future<yams::Result<std::vector<std::byte>>>
    retrieveAsync(std::string_view key) const override {
        return std::async(std::launch::deferred,
                          [this, key = std::string(key)]() { return retrieve(key); });
    }

    std::string getType() const override { return "scripted"; }
    bool isRemote() const override { return remote; }
    yams::Result<void> flush() override { return {}; }

private:
    std::unordered_map<std::string, std::vector<std::byte>> objects_;
};

} // namespace

TEST_CASE("Storage backend engine adapter supports basic CRUD", "[storage][adapter][catch2]") {
    TempDir td;

    yams::storage::BackendConfig backendCfg;
    backendCfg.type = "local";
    backendCfg.localPath = td.path() / "backend";

    auto backend = yams::storage::StorageBackendFactory::create(backendCfg);
    REQUIRE(backend != nullptr);

    auto engine = yams::storage::createStorageEngineFromBackend(std::move(backend));
    REQUIRE(engine != nullptr);

    auto payload = bytesFromString("hello-adapter");
    REQUIRE(engine->store("abc123", payload).has_value());

    auto exists = engine->exists("abc123");
    REQUIRE(exists.has_value());
    CHECK(exists.value());

    auto readBack = engine->retrieve("abc123");
    REQUIRE(readBack.has_value());
    CHECK(readBack.value() == payload);

    auto raw = engine->retrieveRaw("abc123");
    REQUIRE(raw.has_value());
    CHECK(raw.value().data == payload);
    CHECK_FALSE(raw.value().header.has_value());

    auto blockSize = engine->getBlockSize("abc123");
    REQUIRE(blockSize.has_value());
    CHECK(blockSize.value() == payload.size());

    REQUIRE(engine->remove("abc123").has_value());
    auto existsAfterDelete = engine->exists("abc123");
    REQUIRE(existsAfterDelete.has_value());
    CHECK_FALSE(existsAfterDelete.value());
}

TEST_CASE("Storage backend engine adapter reports aggregate storage size",
          "[storage][adapter][catch2]") {
    TempDir td;

    yams::storage::BackendConfig backendCfg;
    backendCfg.type = "local";
    backendCfg.localPath = td.path() / "backend-size";

    auto backend = yams::storage::StorageBackendFactory::create(backendCfg);
    REQUIRE(backend != nullptr);

    auto engine = yams::storage::createStorageEngineFromBackend(std::move(backend));
    REQUIRE(engine != nullptr);

    auto one = bytesFromString("1111");
    auto two = bytesFromString("222222");
    REQUIRE(engine->store("one", one).has_value());
    REQUIRE(engine->store("two", two).has_value());

    auto total = engine->getStorageSize();
    REQUIRE(total.has_value());
    CHECK(total.value() == one.size() + two.size());
}

TEST_CASE("Storage backend engine adapter rejects null backend", "[storage][adapter][catch2]") {
    CHECK(yams::storage::createStorageEngineFromBackend(nullptr) == nullptr);
}

TEST_CASE("Storage backend engine adapter accounts for backend failures",
          "[storage][adapter][catch2]") {
    auto backend = std::make_unique<ScriptedBackend>();
    auto* scripted = backend.get();
    auto engine = yams::storage::createStorageEngineFromBackend(std::move(backend));
    REQUIRE(engine != nullptr);

    auto payload = bytesFromString("adapter-failure-paths");

    scripted->failStore = true;
    auto failedStore = engine->store("store-fails", payload);
    REQUIRE_FALSE(failedStore.has_value());
    CHECK(failedStore.error().code == yams::ErrorCode::WriteError);

    auto statsAfterStoreFailure = engine->getStats();
    CHECK(statsAfterStoreFailure.writeOperations.load() == 0);
    CHECK(statsAfterStoreFailure.totalObjects.load() == 0);
    CHECK(statsAfterStoreFailure.failedOperations.load() == 1);

    scripted->failStore = false;
    REQUIRE(engine->store("stored", payload).has_value());

    auto statsAfterStore = engine->getStats();
    CHECK(statsAfterStore.writeOperations.load() == 1);
    CHECK(statsAfterStore.totalObjects.load() == 1);
    CHECK(statsAfterStore.totalBytes.load() == payload.size());

    scripted->failRetrieve = true;
    auto failedRetrieve = engine->retrieve("stored");
    REQUIRE_FALSE(failedRetrieve.has_value());
    CHECK(failedRetrieve.error().code == yams::ErrorCode::NotFound);

    auto statsAfterRetrieveFailure = engine->getStats();
    CHECK(statsAfterRetrieveFailure.readOperations.load() == 0);
    CHECK(statsAfterRetrieveFailure.failedOperations.load() == 2);

    scripted->failRetrieve = false;
    REQUIRE(engine->retrieve("stored").has_value());

    auto statsAfterRetrieve = engine->getStats();
    CHECK(statsAfterRetrieve.readOperations.load() == 1);

    scripted->failRemove = true;
    auto failedRemove = engine->remove("stored");
    REQUIRE_FALSE(failedRemove.has_value());
    CHECK(failedRemove.error().code == yams::ErrorCode::IOError);

    auto statsAfterRemoveFailure = engine->getStats();
    CHECK(statsAfterRemoveFailure.deleteOperations.load() == 0);
    CHECK(statsAfterRemoveFailure.failedOperations.load() == 3);

    scripted->failRemove = false;
    REQUIRE(engine->remove("stored").has_value());

    auto statsAfterRemove = engine->getStats();
    CHECK(statsAfterRemove.deleteOperations.load() == 1);
}

TEST_CASE("Storage backend engine adapter propagates size aggregation failures",
          "[storage][adapter][catch2]") {
    auto backend = std::make_unique<ScriptedBackend>();
    auto* scripted = backend.get();
    auto engine = yams::storage::createStorageEngineFromBackend(std::move(backend));
    REQUIRE(engine != nullptr);

    auto payload = bytesFromString("size-failure");
    REQUIRE(engine->store("stored", payload).has_value());

    scripted->failList = true;
    auto listFailure = engine->getStorageSize();
    REQUIRE_FALSE(listFailure.has_value());
    CHECK(listFailure.error().code == yams::ErrorCode::NetworkError);

    scripted->failList = false;
    scripted->failRetrieve = true;
    auto retrieveFailure = engine->getStorageSize();
    REQUIRE_FALSE(retrieveFailure.has_value());
    CHECK(retrieveFailure.error().code == yams::ErrorCode::NotFound);
}

TEST_CASE("Storage backend engine adapter uses remote stats for aggregate size",
          "[storage][adapter][remote][catch2]") {
    auto backend = std::make_unique<ScriptedBackend>();
    auto* scripted = backend.get();
    scripted->remote = true;
    auto engine = yams::storage::createStorageEngineFromBackend(std::move(backend));
    REQUIRE(engine != nullptr);

    auto payload = bytesFromString("remote-size-from-stats");
    REQUIRE(engine->store("stored", payload).has_value());

    scripted->failList = true;
    scripted->failRetrieve = true;
    auto size = engine->getStorageSize();
    REQUIRE(size.has_value());
    CHECK(size.value() == payload.size());
    CHECK(scripted->statsCalls == 1);
    CHECK(scripted->listCalls == 0);
    CHECK(scripted->retrieveCalls == 0);

    scripted->failStats = true;
    auto statsFailure = engine->getStorageSize();
    REQUIRE_FALSE(statsFailure.has_value());
    CHECK(statsFailure.error().code == yams::ErrorCode::NetworkError);
}

TEST_CASE("Storage backend engine adapter converts exists exceptions to typed errors",
          "[storage][adapter][catch2]") {
    auto backend = std::make_unique<ScriptedBackend>();
    auto* scripted = backend.get();
    auto engine = yams::storage::createStorageEngineFromBackend(std::move(backend));
    REQUIRE(engine != nullptr);

    scripted->throwExists = true;
    auto exists = engine->exists("boom");
    REQUIRE_FALSE(exists.has_value());
    CHECK(exists.error().code == yams::ErrorCode::InternalError);
    CHECK(exists.error().message.find("scripted exists failure") != std::string::npos);
}
