// Catch2 tests for object storage adapter
// Migrated from GTest: object_storage_adapter_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <yams/plugins/object_storage_adapter.hpp>
#include <yams/plugins/object_storage_iface.hpp>

#include <cstdlib>
#include <cstring>

using namespace yams::plugins;
using yams::plugins::adapter::expose_as_c_abi_with_state;

namespace {

struct FakeBackend : public IObjectStorageBackend {
    Capabilities capabilities() const override { return {}; }
    std::variant<PutResult, ErrorInfo> put(std::string_view key, const void* data, std::size_t len,
                                           const PutOptions&) override {
        lastKey = std::string(key);
        const auto* bytes = static_cast<const std::uint8_t*>(data);
        stored.assign(bytes, bytes + len);
        return PutResult{};
    }
    std::variant<ObjectMetadata, ErrorInfo> head(std::string_view key, const GetOptions&) override {
        lastKey = std::string(key);
        ObjectMetadata md;
        md.size = stored.size();
        md.etag = "fake-etag";
        return md;
    }
    std::variant<std::vector<std::uint8_t>, ErrorInfo>
    get(std::string_view key, std::optional<Range>, const GetOptions&) override {
        lastKey = std::string(key);
        return stored;
    }
    std::optional<ErrorInfo> remove(std::string_view key) override {
        lastKey = std::string(key);
        removed = true;
        return std::nullopt;
    }
    std::variant<Page<ObjectSummary>, ErrorInfo> list(std::string_view prefix,
                                                      std::optional<std::string> /*delimiter*/,
                                                      std::optional<std::string> /*pageToken*/,
                                                      std::optional<int> /*maxKeys*/) override {
        Page<ObjectSummary> p;
        // Return items under prefix "a/"
        p.items.push_back(ObjectSummary{.key = std::string(prefix) + "b/c.txt", .size = 0});
        p.items.push_back(ObjectSummary{.key = std::string(prefix) + "d/e.txt", .size = 0});
        p.items.push_back(ObjectSummary{.key = std::string(prefix) + "f.txt", .size = 0});
        return p;
    }
    VerifyResult verifyObject(std::string_view, std::optional<ChecksumAlgo>) override { return {}; }
    HealthStatus health() const override { return {}; }

    std::string lastKey;
    std::vector<std::uint8_t> stored;
    bool removed{false};
};

} // namespace

TEST_CASE("ObjectStorageAdapter list produces prefixes when delimiter provided",
          "[storage][object-storage][adapter][catch2]") {
    auto impl = std::make_shared<FakeBackend>();
    auto [table, handle] = expose_as_c_abi_with_state(impl);
    REQUIRE(table != nullptr);
    REQUIRE(handle != nullptr);

    void* created = reinterpret_cast<void*>(0x1);
    CHECK(table->create("{}", &created) != 0);
    CHECK(created == nullptr);

    const char* prefix = "a/";
    const char* opts = R"({"delimiter":"/","maxKeys":1000})";
    char* out = nullptr;
    int rc = table->list(handle, prefix, &out, opts);
    REQUIRE(rc == 0);
    REQUIRE(out != nullptr);

    std::string json(out);
    // Expect prefixes a/b/ and a/d/
    CHECK(json.find("\"prefixes\":[") != std::string::npos);
    CHECK(json.find("a/b/") != std::string::npos);
    CHECK(json.find("a/d/") != std::string::npos);

    std::free(out);
    table->destroy(handle);
    std::free(table);
}

TEST_CASE("ObjectStorageAdapter forwards C ABI operations and rejects missing state",
          "[storage][object-storage][adapter][catch2]") {
    auto impl = std::make_shared<FakeBackend>();
    auto [table, handle] = expose_as_c_abi_with_state(impl);
    REQUIRE(table != nullptr);
    REQUIRE(handle != nullptr);

    const unsigned char payload[] = {1, 2, 3, 4};
    CHECK(table->put(nullptr, "k", payload, sizeof(payload), nullptr) != 0);
    CHECK(table->put(handle, "k", payload, sizeof(payload), nullptr) == 0);
    CHECK(impl->lastKey == "k");
    CHECK(impl->stored == std::vector<std::uint8_t>{1, 2, 3, 4});

    void* outBuf = nullptr;
    std::size_t outLen = 0;
    CHECK(table->get(nullptr, "k", &outBuf, &outLen, nullptr) != 0);
    REQUIRE(table->get(handle, "k", &outBuf, &outLen, nullptr) == 0);
    REQUIRE(outBuf != nullptr);
    CHECK(outLen == sizeof(payload));
    CHECK(std::memcmp(outBuf, payload, sizeof(payload)) == 0);
    std::free(outBuf);

    char* metadata = nullptr;
    CHECK(table->head(nullptr, "k", &metadata, nullptr) != 0);
    REQUIRE(table->head(handle, "k", &metadata, nullptr) == 0);
    REQUIRE(metadata != nullptr);
    std::string metadataJson(metadata);
    CHECK(metadataJson.find("\"size\":4") != std::string::npos);
    CHECK(metadataJson.find("fake-etag") != std::string::npos);
    std::free(metadata);

    CHECK(table->del(nullptr, "k", nullptr) != 0);
    CHECK(table->del(handle, "k", nullptr) == 0);
    CHECK(impl->removed);

    table->destroy(handle);
    std::free(table);
}
