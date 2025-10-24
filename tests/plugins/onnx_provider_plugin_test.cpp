#include <nlohmann/json.hpp>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <string>
#include <gtest/gtest.h>
extern "C" {
#include <yams/plugins/abi.h>
#include <yams/plugins/model_provider_v1.h>
}

static yams_model_provider_v1* load_provider(void** handle) {
#ifdef __APPLE__
    const char* so = "builddir/plugins/onnx/libyams_onnx_plugin.dylib";
#else
    const char* so = "builddir/plugins/onnx/libyams_onnx_plugin.so";
#endif
    *handle = dlopen(so, RTLD_LAZY | RTLD_LOCAL);
    if (!*handle)
        return nullptr;
    auto getiface =
        (int (*)(const char*, uint32_t, void**))dlsym(*handle, "yams_plugin_get_interface");
    if (!getiface)
        return nullptr;
    void* p = nullptr;
    int rc = getiface(YAMS_IFACE_MODEL_PROVIDER_V1, 2, &p);
    if (rc != 0 || !p)
        return nullptr;
    return reinterpret_cast<yams_model_provider_v1*>(p);
}

static int get_health_json(void* handle, std::string& out_json) {
    if (!handle)
        return -1;
    auto get_health = (int (*)(char**))dlsym(handle, "yams_plugin_get_health_json");
    if (!get_health)
        return -2;
    char* s = nullptr;
    int rc = get_health(&s);
    if (rc == 0 && s) {
        out_json.assign(s);
        std::free(s);
    }
    return rc;
}

TEST(OnnxProviderPluginTest, RuntimeInfoIncludesHints) {
    setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);
    void* h = nullptr;
    auto* prov = load_provider(&h);
    if (!prov)
        GTEST_SKIP() << "onnx plugin not available";
    char* js = nullptr;
    ASSERT_EQ(0, prov->get_runtime_info_json(prov->self, "e5-small", &js));
    ASSERT_NE(js, nullptr);
    auto j = nlohmann::json::parse(js, nullptr, false);
    prov->free_string(prov->self, js);
    ASSERT_FALSE(j.is_discarded());
    EXPECT_EQ(j.value("model", ""), "e5-small");
    EXPECT_TRUE(j.contains("graph_optimization"));
    EXPECT_TRUE(j.contains("execution_provider"));
}

TEST(OnnxProviderPluginTest, ConfigPreloadParsingAndReady) {
    // Create temp XDG config with preload sections
    namespace fs = std::filesystem;
    fs::path tmp = fs::temp_directory_path() / "yams_test_cfg";
    fs::create_directories(tmp / "yams");
    std::ofstream(tmp / "yams" / "config.toml")
        << "[plugins.onnx]\npreload=\"a,b\"\n\n[plugins.onnx.models.sentiment]\n"
        << "task=\"sentiment\"\n";
    setenv("XDG_CONFIG_HOME", tmp.c_str(), 1);
    setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);

    void* h = nullptr;
    auto* prov = load_provider(&h);
    if (!prov)
        GTEST_SKIP() << "onnx plugin not available";
    std::string health;
    ASSERT_EQ(0, get_health_json(h, health));
    auto j = nlohmann::json::parse(health, nullptr, false);
    ASSERT_FALSE(j.is_discarded());
    EXPECT_NE(j.value("status", "unknown"), "unavailable");
    // Prefer ok status
    EXPECT_EQ(j.value("status", "ok"), "ok");
}

TEST(OnnxProviderPluginTest, GenerateEmbeddingBatchErrors) {
    setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);
    void* h = nullptr;
    auto* prov = load_provider(&h);
    if (!prov)
        GTEST_SKIP() << "onnx plugin not available";

    // Invalid args: null inputs
    float* out = nullptr;
    size_t b = 0, d = 0;
    EXPECT_NE(0, prov->generate_embedding_batch(prov->self, "bad-model", nullptr, nullptr, 0, &out,
                                                &b, &d));

    // Bad model id with empty batch arrays
    const uint8_t* inputs_dummy = nullptr;
    size_t lens_dummy = 0; // not dereferenced for batch_size=0
    EXPECT_NE(0, prov->generate_embedding_batch(prov->self, "bad-model", &inputs_dummy, &lens_dummy,
                                                0, &out, &b, &d));
}
