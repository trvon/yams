// Catch2 tests for object storage adapter
// Migrated from GTest: object_storage_adapter_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <yams/plugins/object_storage_adapter.hpp>
#include <yams/plugins/object_storage_iface.hpp>

using namespace yams::plugins;
using yams::plugins::adapter::expose_as_c_abi_with_state;

namespace {

struct FakeBackend : public IObjectStorageBackend {
    Capabilities capabilities() const override { return {}; }
    std::variant<PutResult, ErrorInfo> put(std::string_view, const void*, std::size_t,
                                           const PutOptions&) override {
        return PutResult{};
    }
    std::variant<ObjectMetadata, ErrorInfo> head(std::string_view, const GetOptions&) override {
        ObjectMetadata md;
        md.size = 0;
        return md;
    }
    std::variant<std::vector<std::uint8_t>, ErrorInfo> get(std::string_view, std::optional<Range>,
                                                           const GetOptions&) override {
        return std::vector<std::uint8_t>{};
    }
    std::optional<ErrorInfo> remove(std::string_view) override { return std::nullopt; }
    std::variant<Page<ObjectSummary>, ErrorInfo> list(std::string_view prefix,
                                                      std::optional<std::string> /*delimiter*/,
                                                      std::optional<std::string> /*pageToken*/,
                                                      std::optional<int> /*maxKeys*/) override {
        Page<ObjectSummary> p;
        // Return items under prefix "a/"
        p.items.push_back(ObjectSummary{std::string(prefix) + "b/c.txt", 0});
        p.items.push_back(ObjectSummary{std::string(prefix) + "d/e.txt", 0});
        p.items.push_back(ObjectSummary{std::string(prefix) + "f.txt", 0});
        return p;
    }
    VerifyResult verifyObject(std::string_view, std::optional<ChecksumAlgo>) override { return {}; }
    HealthStatus health() const override { return {}; }
};

} // namespace

TEST_CASE("ObjectStorageAdapter list produces prefixes when delimiter provided",
          "[storage][object-storage][adapter][catch2]") {
    auto impl = std::make_shared<FakeBackend>();
    auto [table, handle] = expose_as_c_abi_with_state(impl);
    REQUIRE(table != nullptr);
    REQUIRE(handle != nullptr);

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
}
