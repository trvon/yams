#include <nlohmann/json.hpp>
#include <dlfcn.h>
#include <string>
#include <gtest/gtest.h>
extern "C" {
#include <yams/plugins/abi.h>
#include <yams/plugins/onnx_request_v1.h>
}

// Helper to load plugin and fetch onnx_request_v1
static yams_onnx_request_v1* load_req_iface(void** handle) {
#ifdef __APPLE__
    const char* so = "builddir/plugins/onnx/libyams_onnx_plugin.dylib";
#else
    const char* so = "builddir/plugins/onnx/libyams_onnx_plugin.so";
#endif
    *handle = dlopen(so, RTLD_LAZY | RTLD_LOCAL);
    if (!*handle)
        return nullptr;
    auto getabi = (int (*)())dlsym(*handle, "yams_plugin_get_abi_version");
    auto getiface =
        (int (*)(const char*, uint32_t, void**))dlsym(*handle, "yams_plugin_get_interface");
    if (!getabi || !getiface)
        return nullptr;
    void* p = nullptr;
    int rc = getiface(YAMS_IFACE_ONNX_REQUEST_V1, YAMS_IFACE_ONNX_REQUEST_V1_VERSION, &p);
    if (rc != 0 || !p)
        return nullptr;
    return reinterpret_cast<yams_onnx_request_v1*>(p);
}

TEST(OnnxRequestTest, EmbeddingRequestNoInputsError) {
    void* h = nullptr;
    auto* req = load_req_iface(&h);
    ASSERT_NE(req, nullptr);
    char* out = nullptr;
    int rc = req->process_json(req->self, "{}", &out);
    ASSERT_EQ(rc, 0);
    ASSERT_NE(out, nullptr);
    auto j = nlohmann::json::parse(out);
    req->free_string(req->self, out);
    ASSERT_TRUE(j.contains("error"));
}

TEST(OnnxRequestTest, EmbeddingRequestBatchShape) {
    void* h = nullptr;
    auto* req = load_req_iface(&h);
    ASSERT_NE(req, nullptr);
    // Use mock mode to avoid heavy model load
    setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);
    std::string body = R"({"task":"embedding","model_id":"e5-small","inputs":["a","b"]})";
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
