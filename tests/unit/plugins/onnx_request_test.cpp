#include <nlohmann/json.hpp>
#include <array>
#include <cstddef>
#include <cstdlib>
#include <string>
#include <catch2/catch_test_macros.hpp>
#include <yams/compat/dlfcn.h>

extern "C" {
#include <yams/plugins/abi.h>
#include <yams/plugins/onnx_request_v1.h>
}

namespace {

/**
 * @brief RAII wrapper for plugin handle
 */
struct PluginHandle {
    void* handle{nullptr};

    PluginHandle() = default;
    PluginHandle(const PluginHandle&) = delete;
    PluginHandle& operator=(const PluginHandle&) = delete;
    PluginHandle(PluginHandle&& o) noexcept : handle(o.handle) { o.handle = nullptr; }
    PluginHandle& operator=(PluginHandle&& o) noexcept {
        if (this != &o) {
            if (handle)
                dlclose(handle);
            handle = o.handle;
            o.handle = nullptr;
        }
        return *this;
    }
    ~PluginHandle() {
        if (handle)
            dlclose(handle);
    }
};

/**
 * @brief Helper to load plugin and fetch onnx_request_v1 interface
 */
yams_onnx_request_v1* load_req_iface(PluginHandle& ph) {
#if defined(_WIN32)
    const char* so = "builddir/plugins/onnx/yams_onnx_plugin.dll";
#elif defined(__APPLE__)
    const char* so = "builddir/plugins/onnx/libyams_onnx_plugin.dylib";
#else
    const char* so = "builddir/plugins/onnx/libyams_onnx_plugin.so";
#endif
    ph.handle = dlopen(so, RTLD_LAZY | RTLD_LOCAL);
    if (!ph.handle)
        return nullptr;
    auto getabi = (int (*)())dlsym(ph.handle, "yams_plugin_get_abi_version");
    auto getiface =
        (int (*)(const char*, uint32_t, void**))dlsym(ph.handle, "yams_plugin_get_interface");
    if (!getabi || !getiface)
        return nullptr;
    void* p = nullptr;
    int rc = getiface(YAMS_IFACE_ONNX_REQUEST_V1, YAMS_IFACE_ONNX_REQUEST_V1_VERSION, &p);
    if (rc != 0 || !p)
        return nullptr;
    return reinterpret_cast<yams_onnx_request_v1*>(p);
}

} // namespace

TEST_CASE("ONNX Request plugin interface", "[plugins][onnx][request]") {
    PluginHandle ph;
    auto* req = load_req_iface(ph);

    if (!req) {
        SKIP("ONNX plugin not available - skipping test");
    }

    SECTION("embedding request without inputs returns error") {
        char* out = nullptr;
        int rc = req->process_json(req->self, "{}", &out);
        REQUIRE(rc == 0);
        REQUIRE(out != nullptr);

        auto j = nlohmann::json::parse(out);
        req->free_string(req->self, out);
        CHECK(j.contains("error"));
    }

    SECTION("embedding request with batch returns correct shape") {
        // Use mock mode to avoid heavy model load
#ifdef _WIN32
        _putenv_s("YAMS_SKIP_MODEL_LOADING", "1");
#else
        setenv("YAMS_SKIP_MODEL_LOADING", "1", 1);
#endif
        std::string body = R"({"task":"embedding","model_id":"e5-small","inputs":["a","b"]})";
        char* out = nullptr;
        int rc = req->process_json(req->self, body.c_str(), &out);
        REQUIRE(rc == 0);
        REQUIRE(out != nullptr);

        auto j = nlohmann::json::parse(out, nullptr, false);
        req->free_string(req->self, out);
        REQUIRE_FALSE(j.is_discarded());

        if (j.contains("error")) {
            SKIP("Model not available: " + j.dump());
        }

        CHECK(j.value("task", "") == "embedding");
        CHECK(j.value("batch", 0) == 2);
    }
}
