#include <nlohmann/json.hpp>
#include <string>
#include "../common/env_compat.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/compat/dlfcn.h>
#include <yams/compat/unistd.h>

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

TEST_CASE("OnnxRequestPluginTest.NoInputsError", "[plugin][onnxrequestplugintest]") {
    // Avoid background preload threads during tests
    setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);
    void* h = nullptr;
    auto* req = load_req_iface(&h);
    if (!req)
        SKIP("onnx plugin not available");
    char* out = nullptr;
    int rc = req->process_json(req->self, "{}", &out);
    REQUIRE(rc == 0);
    REQUIRE(out != nullptr);
    auto j = nlohmann::json::parse(out);
    req->free_string(req->self, out);
    REQUIRE(j.contains("error"));
}

TEST_CASE("OnnxRequestPluginTest.EmbeddingMockBatchShape", "[plugin][onnxrequestplugintest]") {
    // Avoid heavy model load on CI
    setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);
    void* h = nullptr;
    auto* req = load_req_iface(&h);
    if (!req)
        SKIP("onnx plugin not available");
    std::string body =
        R"({"task":"embedding","model_id":"e5-small","inputs":["a","b"],"options":{"device":"cpu","batch":2}})";
    char* out = nullptr;
    int rc = req->process_json(req->self, body.c_str(), &out);
    REQUIRE(rc == 0);
    REQUIRE(out != nullptr);
    auto j = nlohmann::json::parse(out, nullptr, false);
    req->free_string(req->self, out);
    REQUIRE_FALSE(j.is_discarded());
    if (j.contains("error"))
        SKIP(j.dump());
    REQUIRE(j.value("task", "") == "embedding");
    REQUIRE(j.value("batch", 0) == 2);
}
