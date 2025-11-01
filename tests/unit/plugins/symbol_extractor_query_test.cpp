#include <cstring>
#include <dlfcn.h>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include "../../common/plugins.h"
#include <yams/plugins/abi.h>
#include <yams/plugins/symbol_extractor_v1.h>

static void free_result(yams_symbol_extractor_v1* api, yams_symbol_extraction_result_v1* res) {
    if (!api || !res)
        return;
    api->free_result(api->self, res);
}

struct PluginHandle {
    void* h{};
    PluginHandle() = default;
    PluginHandle(const PluginHandle&) = delete;
    PluginHandle& operator=(const PluginHandle&) = delete;
    PluginHandle(PluginHandle&& o) noexcept : h(o.h) { o.h = nullptr; }
    PluginHandle& operator=(PluginHandle&& o) noexcept {
        if (this != &o) {
            if (h)
                dlclose(h);
            h = o.h;
            o.h = nullptr;
        }
        return *this;
    }
    ~PluginHandle() {
        if (h)
            dlclose(h);
    }
};

static yams_symbol_extractor_v1* load_plugin(const char* so_path) {
    PluginHandle ph;
    ph.h = dlopen(so_path, RTLD_LAZY | RTLD_LOCAL);
    if (!ph.h)
        return nullptr;
    auto getabi = (int (*)())dlsym(ph.h, "yams_plugin_get_abi_version");
    auto getiface =
        (int (*)(const char*, uint32_t, void**))dlsym(ph.h, "yams_plugin_get_interface");
    auto init = (int (*)(const char*, const void*))dlsym(ph.h, "yams_plugin_init");
    if (!getabi || !getiface || !init)
        return nullptr;
    EXPECT_GT(getabi(), 0);
    EXPECT_EQ(0, init("{\n  \"languages\": [\"cpp\", \"python\"]\n}", nullptr));
    void* ptr = nullptr;
    int rc = getiface(YAMS_IFACE_SYMBOL_EXTRACTOR_V1, YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION, &ptr);
    if (rc != 0 || !ptr)
        return nullptr;
    // Intentionally leak handle for test lifetime; process exit cleans it.
    ph.h = nullptr;
    return reinterpret_cast<yams_symbol_extractor_v1*>(ptr);
}

TEST(SymbolExtractorQueryTest, CppDetectsFunctionAndClass) {
#ifdef __APPLE__
    const char* so = "/usr/local/lib/yams/plugins/yams_symbol_extractor.dylib";
#else
    const char* so = "/usr/local/lib/yams/plugins/yams_symbol_extractor.so";
#endif
    auto* api = load_plugin(so);
    ASSERT_NE(api, nullptr);

    const char* code = R"CPP(
        class Foo { public: void bar(); };
        int baz(int x) { return x+1; }
    )CPP";

    yams_symbol_extraction_result_v1* out = nullptr;
    int rc = api->extract_symbols(api->self, code, std::strlen(code), "/tmp/test.cpp", "cpp", &out);
    PLUGIN_MISSING_SKIP(rc, out, "symbol grammar not available");
    ASSERT_EQ(rc, 0);
    ASSERT_NE(out, nullptr);

    size_t funcs = 0, classes = 0;
    for (size_t i = 0; i < out->symbol_count; ++i) {
        if (out->symbols[i].kind && std::strcmp(out->symbols[i].kind, "function") == 0)
            ++funcs;
        if (out->symbols[i].kind && (std::strcmp(out->symbols[i].kind, "class") == 0 ||
                                     std::strcmp(out->symbols[i].kind, "struct") == 0))
            ++classes;
    }
    EXPECT_GE(funcs, 1u);
    EXPECT_GE(classes, 1u);
    free_result(api, out);
}

TEST(SymbolExtractorQueryTest, PythonDetectsFunction) {
#ifdef __APPLE__
    const char* so = "/usr/local/lib/yams/plugins/yams_symbol_extractor.dylib";
#else
    const char* so = "/usr/local/lib/yams/plugins/yams_symbol_extractor.so";
#endif
    auto* api = load_plugin(so);
    ASSERT_NE(api, nullptr);

    const char* code = R"PY(
        def foo(x):
            return x+1
    )PY";

    yams_symbol_extraction_result_v1* out = nullptr;
    int rc =
        api->extract_symbols(api->self, code, std::strlen(code), "/tmp/test.py", "python", &out);
    PLUGIN_MISSING_SKIP(rc, out, "symbol grammar not available");
    ASSERT_EQ(rc, 0);
    ASSERT_NE(out, nullptr);

    size_t funcs = 0;
    for (size_t i = 0; i < out->symbol_count; ++i) {
        if (out->symbols[i].kind && std::strcmp(out->symbols[i].kind, "function") == 0)
            ++funcs;
    }
    EXPECT_GE(funcs, 1u);
    free_result(api, out);
}
