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
#include <catch2/catch_test_macros.hpp>
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

    CHECK(getabi() > 0);
    CHECK(std::string(getname()) == "glint");
    CHECK(0 == init(nullptr, nullptr));

    void* ptr = nullptr;
    int rc = getiface(YAMS_IFACE_ENTITY_EXTRACTOR_V2, YAMS_IFACE_ENTITY_EXTRACTOR_V2_VERSION, &ptr);
    if (rc != 0 || !ptr) {
        fprintf(stderr, "get_interface failed: rc=%d ptr=%p\n", rc, ptr);
        return std::nullopt;
    }

    p.api = reinterpret_cast<yams_entity_extractor_v2*>(ptr);
    return p.api ? std::optional<GlintPluginAPI>(std::move(p)) : std::nullopt;
}

struct GlintPluginTest {
    GlintPluginAPI plugin_;

    GlintPluginTest() {
        auto loaded = load_glint_plugin();
        if (!loaded) {
            SKIP("Glint plugin not available");
        }
        plugin_ = std::move(*loaded);
    }
};

} // namespace

TEST_CASE_METHOD(GlintPluginTest, "ABIVersionValid", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);
    CHECK(plugin_.api->abi_version == YAMS_IFACE_ENTITY_EXTRACTOR_V2_VERSION);
}

TEST_CASE_METHOD(GlintPluginTest, "SupportsTextPlain", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);
    CHECK(plugin_.api->supports(plugin_.api->self, "text/plain"));
}

TEST_CASE_METHOD(GlintPluginTest, "SupportsTextMarkdown", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);
    CHECK(plugin_.api->supports(plugin_.api->self, "text/markdown"));
}

TEST_CASE_METHOD(GlintPluginTest, "SupportsApplicationJson", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);
    CHECK(plugin_.api->supports(plugin_.api->self, "application/json"));
}

TEST_CASE_METHOD(GlintPluginTest, "DoesNotSupportBinary", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);
    CHECK_FALSE(plugin_.api->supports(plugin_.api->self, "application/octet-stream"));
    CHECK_FALSE(plugin_.api->supports(plugin_.api->self, "image/png"));
    CHECK_FALSE(plugin_.api->supports(plugin_.api->self, nullptr));
}

TEST_CASE_METHOD(GlintPluginTest, "ExtractReturnsValidResult", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);

    const char* text = "Albert Einstein developed the theory of relativity at Princeton.";
    yams_entity_extraction_result_v2* result = nullptr;

    int rc = plugin_.api->extract(plugin_.api->self, text, strlen(text), nullptr, &result);
    CHECK(rc == YAMS_PLUGIN_OK);
    REQUIRE(result != nullptr);

    // Skeleton returns empty results, but structure should be valid
    CHECK(result->error == nullptr);
    // entity_count can be 0 for skeleton implementation
    CHECK(result->entity_count >= 0u);

    plugin_.api->free_result(plugin_.api->self, result);
}

TEST_CASE_METHOD(GlintPluginTest, "ExtractWithOptions", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);

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
    CHECK(rc == YAMS_PLUGIN_OK);
    REQUIRE(result != nullptr);

    plugin_.api->free_result(plugin_.api->self, result);
}

TEST_CASE_METHOD(GlintPluginTest, "ExtractNullOutputReturnsError", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);

    const char* text = "Test text";
    int rc = plugin_.api->extract(plugin_.api->self, text, strlen(text), nullptr, nullptr);
    CHECK(rc == YAMS_PLUGIN_ERR_INVALID);
}

TEST_CASE_METHOD(GlintPluginTest, "GetCapabilitiesJson", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);

    char* json_str = nullptr;
    int rc = plugin_.api->get_capabilities_json(plugin_.api->self, &json_str);
    CHECK(rc == YAMS_PLUGIN_OK);
    REQUIRE(json_str != nullptr);

    auto j = nlohmann::json::parse(json_str, nullptr, false);
    CHECK_FALSE(j.is_discarded());

    // Verify expected capability fields
    CHECK(j.contains("content_types"));
    CHECK(j.contains("entity_types"));
    CHECK(j["content_types"].is_array());
    CHECK(j["entity_types"].is_array());

    // Check expected entity types
    auto entity_types = j["entity_types"].get<std::vector<std::string>>();
    CHECK(entity_types.size() >= 1u);

    plugin_.api->free_string(plugin_.api->self, json_str);
}

TEST_CASE_METHOD(GlintPluginTest, "GetCapabilitiesNullOutputReturnsError", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);

    int rc = plugin_.api->get_capabilities_json(plugin_.api->self, nullptr);
    CHECK(rc == YAMS_PLUGIN_ERR_INVALID);
}

TEST_CASE_METHOD(GlintPluginTest, "FreeResultHandlesNull", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);
    // Should not crash
    plugin_.api->free_result(plugin_.api->self, nullptr);
}

TEST_CASE_METHOD(GlintPluginTest, "FreeStringHandlesNull", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);
    // Should not crash
    plugin_.api->free_string(plugin_.api->self, nullptr);
}

TEST_CASE_METHOD(GlintPluginTest, "ManifestJsonValid", "[plugin][glint]") {
    REQUIRE(plugin_.handle != nullptr);

    auto getmanifest =
        reinterpret_cast<const char* (*)()>(dlsym(plugin_.handle, "yams_plugin_get_manifest_json"));
    REQUIRE(getmanifest != nullptr);

    const char* manifest = getmanifest();
    REQUIRE(manifest != nullptr);

    auto j = nlohmann::json::parse(manifest, nullptr, false);
    CHECK_FALSE(j.is_discarded());
    CHECK(j.value("name", "") == "glint");
    CHECK(j.contains("interfaces"));
    CHECK(j.contains("version"));

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
    CHECK(has_v2);
}

TEST_CASE_METHOD(GlintPluginTest, "EmptyTextExtraction", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);

    yams_entity_extraction_result_v2* result = nullptr;
    int rc = plugin_.api->extract(plugin_.api->self, "", 0, nullptr, &result);
    CHECK(rc == YAMS_PLUGIN_OK);
    REQUIRE(result != nullptr);
    CHECK(result->entity_count == 0u);

    plugin_.api->free_result(plugin_.api->self, result);
}

TEST_CASE_METHOD(GlintPluginTest, "LargeTextExtraction", "[plugin][glint]") {
    REQUIRE(plugin_.api != nullptr);

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
    CHECK(rc == YAMS_PLUGIN_OK);
    REQUIRE(result != nullptr);

    plugin_.api->free_result(plugin_.api->self, result);
}
