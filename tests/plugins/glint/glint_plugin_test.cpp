/**
 * @file glint_plugin_test.cpp
 * @brief Unit tests for Glint (GLiNER-based) NL entity extractor plugin
 *
 * Tests the entity_extractor_v2 interface implementation without requiring
 * actual ONNX model inference (skeleton returns empty results).
 */

#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include <yams/compat/dlfcn.h>

extern "C" {
#include <yams/plugins/abi.h>
#include <yams/plugins/entity_extractor_v2.h>
}

namespace {

struct GlintPluginAPI {
    void* handle{};
    yams_entity_extractor_v2* api{};

    GlintPluginAPI() = default;
    GlintPluginAPI(const GlintPluginAPI&) = delete;
    GlintPluginAPI& operator=(const GlintPluginAPI&) = delete;

    GlintPluginAPI(GlintPluginAPI&& other) noexcept : handle(other.handle), api(other.api) {
        other.handle = nullptr;
        other.api = nullptr;
    }

    GlintPluginAPI& operator=(GlintPluginAPI&& other) noexcept {
        if (this != &other) {
            handle = other.handle;
            api = other.api;
            other.handle = nullptr;
            other.api = nullptr;
        }
        return *this;
    }

    ~GlintPluginAPI() { /* avoid dlclose in tests to prevent teardown crashes */ }
};

std::optional<GlintPluginAPI> load_glint_plugin() {
    GlintPluginAPI p;

#ifdef GLINT_PLUGIN_PATH
    const char* plugin_path = GLINT_PLUGIN_PATH;
#else
#ifdef __APPLE__
    const char* plugin_path = "builddir/plugins/glint/yams_glint.dylib";
#elif defined(_WIN32)
    const char* plugin_path = "builddir/plugins/glint/yams_glint.dll";
#else
    const char* plugin_path = "builddir/plugins/glint/yams_glint.so";
#endif
#endif

    p.handle = dlopen(plugin_path, RTLD_LAZY | RTLD_LOCAL);
    if (!p.handle) {
        fprintf(stderr, "dlopen failed for %s: %s\n", plugin_path, dlerror());
        return std::nullopt;
    }

    auto getabi = reinterpret_cast<int (*)()>(dlsym(p.handle, "yams_plugin_get_abi_version"));
    auto getiface = reinterpret_cast<int (*)(const char*, uint32_t, void**)>(
        dlsym(p.handle, "yams_plugin_get_interface"));
    auto init =
        reinterpret_cast<int (*)(const char*, const void*)>(dlsym(p.handle, "yams_plugin_init"));
    auto getname = reinterpret_cast<const char* (*)()>(dlsym(p.handle, "yams_plugin_get_name"));
    auto getmanifest =
        reinterpret_cast<const char* (*)()>(dlsym(p.handle, "yams_plugin_get_manifest_json"));

    if (!getabi || !getiface || !init || !getname || !getmanifest) {
        fprintf(stderr, "Symbol lookup failed in glint plugin\n");
        return std::nullopt;
    }

    EXPECT_GT(getabi(), 0);
    EXPECT_STREQ(getname(), "glint");
    EXPECT_EQ(0, init(nullptr, nullptr));

    void* ptr = nullptr;
    int rc = getiface(YAMS_IFACE_ENTITY_EXTRACTOR_V2, YAMS_IFACE_ENTITY_EXTRACTOR_V2_VERSION, &ptr);
    if (rc != 0 || !ptr) {
        fprintf(stderr, "get_interface failed: rc=%d ptr=%p\n", rc, ptr);
        return std::nullopt;
    }

    p.api = reinterpret_cast<yams_entity_extractor_v2*>(ptr);
    return p.api ? std::optional<GlintPluginAPI>(std::move(p)) : std::nullopt;
}

} // namespace

class GlintPluginTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto loaded = load_glint_plugin();
        if (!loaded) {
            GTEST_SKIP() << "Glint plugin not available";
        }
        plugin_ = std::move(*loaded);
    }

    GlintPluginAPI plugin_;
};

TEST_F(GlintPluginTest, ABIVersionValid) {
    ASSERT_NE(plugin_.api, nullptr);
    EXPECT_EQ(plugin_.api->abi_version, YAMS_IFACE_ENTITY_EXTRACTOR_V2_VERSION);
}

TEST_F(GlintPluginTest, SupportsTextPlain) {
    ASSERT_NE(plugin_.api, nullptr);
    EXPECT_TRUE(plugin_.api->supports(plugin_.api->self, "text/plain"));
}

TEST_F(GlintPluginTest, SupportsTextMarkdown) {
    ASSERT_NE(plugin_.api, nullptr);
    EXPECT_TRUE(plugin_.api->supports(plugin_.api->self, "text/markdown"));
}

TEST_F(GlintPluginTest, SupportsApplicationJson) {
    ASSERT_NE(plugin_.api, nullptr);
    EXPECT_TRUE(plugin_.api->supports(plugin_.api->self, "application/json"));
}

TEST_F(GlintPluginTest, DoesNotSupportBinary) {
    ASSERT_NE(plugin_.api, nullptr);
    EXPECT_FALSE(plugin_.api->supports(plugin_.api->self, "application/octet-stream"));
    EXPECT_FALSE(plugin_.api->supports(plugin_.api->self, "image/png"));
    EXPECT_FALSE(plugin_.api->supports(plugin_.api->self, nullptr));
}

TEST_F(GlintPluginTest, ExtractReturnsValidResult) {
    ASSERT_NE(plugin_.api, nullptr);

    const char* text = "Albert Einstein developed the theory of relativity at Princeton.";
    yams_entity_extraction_result_v2* result = nullptr;

    int rc = plugin_.api->extract(plugin_.api->self, text, strlen(text), nullptr, &result);
    EXPECT_EQ(rc, YAMS_PLUGIN_OK);
    ASSERT_NE(result, nullptr);

    // Skeleton returns empty results, but structure should be valid
    EXPECT_EQ(result->error, nullptr);
    // entity_count can be 0 for skeleton implementation
    EXPECT_GE(result->entity_count, 0u);

    plugin_.api->free_result(plugin_.api->self, result);
}

TEST_F(GlintPluginTest, ExtractWithOptions) {
    ASSERT_NE(plugin_.api, nullptr);

    const char* text = "Google was founded by Larry Page and Sergey Brin in 1998.";
    const char* types[] = {"person", "organization", "date"};
    yams_entity_extraction_options_v2 opts = {
        .entity_types = types,
        .entity_type_count = 3,
        .language = "en",
        .file_path = nullptr,
        .extract_relations = false,
    };

    yams_entity_extraction_result_v2* result = nullptr;
    int rc = plugin_.api->extract(plugin_.api->self, text, strlen(text), &opts, &result);
    EXPECT_EQ(rc, YAMS_PLUGIN_OK);
    ASSERT_NE(result, nullptr);

    plugin_.api->free_result(plugin_.api->self, result);
}

TEST_F(GlintPluginTest, ExtractNullOutputReturnsError) {
    ASSERT_NE(plugin_.api, nullptr);

    const char* text = "Test text";
    int rc = plugin_.api->extract(plugin_.api->self, text, strlen(text), nullptr, nullptr);
    EXPECT_EQ(rc, YAMS_PLUGIN_ERR_INVALID);
}

TEST_F(GlintPluginTest, GetCapabilitiesJson) {
    ASSERT_NE(plugin_.api, nullptr);

    char* json_str = nullptr;
    int rc = plugin_.api->get_capabilities_json(plugin_.api->self, &json_str);
    EXPECT_EQ(rc, YAMS_PLUGIN_OK);
    ASSERT_NE(json_str, nullptr);

    auto j = nlohmann::json::parse(json_str, nullptr, false);
    EXPECT_FALSE(j.is_discarded());

    // Verify expected capability fields
    EXPECT_TRUE(j.contains("content_types"));
    EXPECT_TRUE(j.contains("entity_types"));
    EXPECT_TRUE(j["content_types"].is_array());
    EXPECT_TRUE(j["entity_types"].is_array());

    // Check expected entity types
    auto entity_types = j["entity_types"].get<std::vector<std::string>>();
    EXPECT_GE(entity_types.size(), 1u);

    plugin_.api->free_string(plugin_.api->self, json_str);
}

TEST_F(GlintPluginTest, GetCapabilitiesNullOutputReturnsError) {
    ASSERT_NE(plugin_.api, nullptr);

    int rc = plugin_.api->get_capabilities_json(plugin_.api->self, nullptr);
    EXPECT_EQ(rc, YAMS_PLUGIN_ERR_INVALID);
}

TEST_F(GlintPluginTest, FreeResultHandlesNull) {
    ASSERT_NE(plugin_.api, nullptr);
    // Should not crash
    plugin_.api->free_result(plugin_.api->self, nullptr);
}

TEST_F(GlintPluginTest, FreeStringHandlesNull) {
    ASSERT_NE(plugin_.api, nullptr);
    // Should not crash
    plugin_.api->free_string(plugin_.api->self, nullptr);
}

TEST_F(GlintPluginTest, ManifestJsonValid) {
    ASSERT_NE(plugin_.handle, nullptr);

    auto getmanifest =
        reinterpret_cast<const char* (*)()>(dlsym(plugin_.handle, "yams_plugin_get_manifest_json"));
    ASSERT_NE(getmanifest, nullptr);

    const char* manifest = getmanifest();
    ASSERT_NE(manifest, nullptr);

    auto j = nlohmann::json::parse(manifest, nullptr, false);
    EXPECT_FALSE(j.is_discarded());
    EXPECT_EQ(j.value("name", ""), "glint");
    EXPECT_TRUE(j.contains("interfaces"));
    EXPECT_TRUE(j.contains("version"));

    // Check that entity_extractor_v2 is listed
    // Interfaces can be either array of strings or array of objects with "id" field
    bool has_v2 = false;
    if (j["interfaces"].is_array()) {
        for (const auto& iface : j["interfaces"]) {
            std::string iface_id;
            if (iface.is_string()) {
                iface_id = iface.get<std::string>();
            } else if (iface.is_object() && iface.contains("id")) {
                iface_id = iface["id"].get<std::string>();
            }
            if (iface_id == "entity_extractor_v2") {
                has_v2 = true;
                break;
            }
        }
    }
    EXPECT_TRUE(has_v2);
}

TEST_F(GlintPluginTest, EmptyTextExtraction) {
    ASSERT_NE(plugin_.api, nullptr);

    yams_entity_extraction_result_v2* result = nullptr;
    int rc = plugin_.api->extract(plugin_.api->self, "", 0, nullptr, &result);
    EXPECT_EQ(rc, YAMS_PLUGIN_OK);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->entity_count, 0u);

    plugin_.api->free_result(plugin_.api->self, result);
}

TEST_F(GlintPluginTest, LargeTextExtraction) {
    ASSERT_NE(plugin_.api, nullptr);

    // Generate large text (10KB)
    std::string large_text;
    large_text.reserve(10240);
    for (int i = 0; i < 100; ++i) {
        large_text += "The quick brown fox jumps over the lazy dog. "
                      "Microsoft and Apple are technology companies. "
                      "New York is a city in the United States. ";
    }

    yams_entity_extraction_result_v2* result = nullptr;
    int rc = plugin_.api->extract(plugin_.api->self, large_text.c_str(), large_text.size(), nullptr,
                                  &result);
    EXPECT_EQ(rc, YAMS_PLUGIN_OK);
    ASSERT_NE(result, nullptr);

    plugin_.api->free_result(plugin_.api->self, result);
}
