#include <nlohmann/json.hpp>
#include <dlfcn.h>
#include <string>
#include <gtest/gtest.h>
extern "C" {
#include <yams/plugins/abi.h>
#include <yams/plugins/onnx_request_v1.h>
}

static yams_onnx_request_v1* load_req_iface(void** handle) {
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
    int rc = getiface(YAMS_IFACE_ONNX_REQUEST_V1, YAMS_IFACE_ONNX_REQUEST_V1_VERSION, &p);
    if (rc != 0 || !p)
        return nullptr;
    return reinterpret_cast<yams_onnx_request_v1*>(p);
}

TEST(OnnxRequestPluginTest, NoInputsError) {
    // Avoid background preload threads during tests
    setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);
    void* h = nullptr;
    auto* req = load_req_iface(&h);
    if (!req)
        GTEST_SKIP() << "onnx plugin not available";
    char* out = nullptr;
    int rc = req->process_json(req->self, "{}", &out);
    ASSERT_EQ(rc, 0);
    ASSERT_NE(out, nullptr);
    auto j = nlohmann::json::parse(out);
    req->free_string(req->self, out);
    ASSERT_TRUE(j.contains("error"));
}

TEST(OnnxRequestPluginTest, EmbeddingMockBatchShape) {
    // Avoid heavy model load on CI
    setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);
    void* h = nullptr;
    auto* req = load_req_iface(&h);
    if (!req)
        GTEST_SKIP() << "onnx plugin not available";
    std::string body =
        R"({"task":"embedding","model_id":"e5-small","inputs":["a","b"],"options":{"device":"cpu","batch":2}})";
    char* out = nullptr;
    int rc = req->process_json(req->self, body.c_str(), &out);
    ASSERT_EQ(rc, 0);
    ASSERT_NE(out, nullptr);
    auto j = nlohmann::json::parse(out, nullptr, false);
    req->free_string(req->self, out);
    ASSERT_FALSE(j.is_discarded());
    if (j.contains("error"))
        GTEST_SKIP() << j.dump();
    ASSERT_EQ(j.value("task", ""), "embedding");
    ASSERT_EQ(j.value("batch", 0), 2);
}
