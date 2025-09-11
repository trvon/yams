#include <gtest/gtest.h>

#include <yams/plugins/object_storage_adapter.hpp>
#include <yams/plugins/object_storage_iface.hpp>

using namespace yams::plugins;
using yams::plugins::adapter::expose_as_c_abi_with_state;

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

TEST(ObjectStorageAdapterTest, ListProducesPrefixesWhenDelimiterProvided) {
    auto impl = std::make_shared<FakeBackend>();
    auto [table, handle] = expose_as_c_abi_with_state(impl);
    ASSERT_NE(table, nullptr);
    ASSERT_NE(handle, nullptr);

    const char* prefix = "a/";
    const char* opts = "{\"delimiter\":\"/\",\"maxKeys\":1000}";
    char* out = nullptr;
    int rc = table->list(handle, prefix, &out, opts);
    ASSERT_EQ(rc, 0);
    ASSERT_NE(out, nullptr);
    std::string json(out);
    // Expect prefixes a/b/ and a/d/
    EXPECT_NE(json.find("\"prefixes\":["), std::string::npos);
    EXPECT_NE(json.find("a/b/"), std::string::npos);
    EXPECT_NE(json.find("a/d/"), std::string::npos);
    std::free(out);

    table->destroy(handle);
}
