#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>

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
