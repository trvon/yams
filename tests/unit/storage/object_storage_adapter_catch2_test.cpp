// Catch2 tests for object storage adapter
// Migrated from GTest: object_storage_adapter_test.cpp

#include <nlohmann/json.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <memory>

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
        // A second member of the same prefix also exercises JSON string escaping.
        p.items.push_back(ObjectSummary{.key = std::string(prefix) + "b/sibling\".txt", .size = 0});
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

struct CAbiOwner {
    yams_object_storage_v1* table;
    void* handle;

    ~CAbiOwner() {
        if (table && handle) {
            table->destroy(handle);
        }
        std::free(table);
    }
};

} // namespace

TEST_CASE("ObjectStorageAdapter list produces prefixes when delimiter provided",
          "[storage][object-storage][adapter][catch2]") {
    auto impl = std::make_shared<FakeBackend>();
    auto [table, handle] = expose_as_c_abi_with_state(impl);
    CAbiOwner owner{table, handle};
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

    std::unique_ptr<char, decltype(&std::free)> ownedOutput(out, &std::free);
    const auto json = nlohmann::json::parse(ownedOutput.get());
    auto items = json.at("items").get<std::vector<std::string>>();
    auto prefixes = json.at("prefixes").get<std::vector<std::string>>();
    std::sort(items.begin(), items.end());
    std::sort(prefixes.begin(), prefixes.end());
    CHECK(items ==
          std::vector<std::string>{"a/b/c.txt", "a/b/sibling\".txt", "a/d/e.txt", "a/f.txt"});
    // Exact arrays prove both prefixes exist once; a substring check could accept duplicates.
    CHECK(prefixes == std::vector<std::string>{"a/b/", "a/d/"});
}

TEST_CASE("ObjectStorageAdapter forwards C ABI operations and rejects missing state",
          "[storage][object-storage][adapter][catch2]") {
    auto impl = std::make_shared<FakeBackend>();
    auto [table, handle] = expose_as_c_abi_with_state(impl);
    CAbiOwner owner{table, handle};
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
    std::unique_ptr<void, decltype(&std::free)> ownedBuffer(outBuf, &std::free);
    REQUIRE(outLen == sizeof(payload));
    CHECK(std::memcmp(outBuf, payload, sizeof(payload)) == 0);

    char* metadata = nullptr;
    CHECK(table->head(nullptr, "k", &metadata, nullptr) != 0);
    REQUIRE(table->head(handle, "k", &metadata, nullptr) == 0);
    REQUIRE(metadata != nullptr);
    std::unique_ptr<char, decltype(&std::free)> ownedMetadata(metadata, &std::free);
    const auto metadataJson = nlohmann::json::parse(ownedMetadata.get());
    CHECK(metadataJson.at("size").get<std::size_t>() == sizeof(payload));
    CHECK(metadataJson.at("etag").get<std::string>() == "fake-etag");

    CHECK(table->del(nullptr, "k", nullptr) != 0);
    CHECK(table->del(handle, "k", nullptr) == 0);
    CHECK(impl->removed);
}

TEST_CASE("ObjectStorageAdapter represents an empty object without an allocation",
          "[storage][object-storage][adapter][catch2]") {
    auto impl = std::make_shared<FakeBackend>();
    auto [table, handle] = expose_as_c_abi_with_state(impl);
    CAbiOwner owner{table, handle};
    REQUIRE(table != nullptr);
    REQUIRE(handle != nullptr);

    void* outBuf = reinterpret_cast<void*>(0x1);
    std::size_t outLen = 1;
    REQUIRE(table->get(handle, "empty", &outBuf, &outLen, nullptr) == 0);
    CHECK(outBuf == nullptr);
    CHECK(outLen == 0);
}
