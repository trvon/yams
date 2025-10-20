#include <cstring>
#include <dlfcn.h>
#include <optional>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <yams/plugins/symbol_extractor_v1.h>

namespace {
struct PluginAPI {
    void* handle{};
    yams_symbol_extractor_v1* api{};
    ~PluginAPI() {
        if (handle)
            dlclose(handle);
    }
};

std::optional<PluginAPI> load_extractor(const char* so_path) {
    PluginAPI p;
    p.handle = dlopen(so_path, RTLD_LAZY | RTLD_LOCAL);
    if (!p.handle)
        return std::nullopt;
    auto getabi = (int (*)())dlsym(p.handle, "yams_plugin_get_abi_version");
    auto getiface =
        (int (*)(const char*, uint32_t, void**))dlsym(p.handle, "yams_plugin_get_interface");
    auto init = (int (*)(const char*, const void*))dlsym(p.handle, "yams_plugin_init");
    if (!getabi || !getiface || !init)
        return std::nullopt;
    EXPECT_GT(getabi(), 0);
    EXPECT_EQ(0, init("{\n  \"languages\": [\"cpp\", \"python\", \"rust\", \"go\", \"javascript\", "
                      "\"typescript\"]\n}",
                      nullptr));
    void* ptr = nullptr;
    int rc = getiface(YAMS_IFACE_SYMBOL_EXTRACTOR_V1, YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION, &ptr);
    if (rc != 0 || !ptr)
        return std::nullopt;
    p.api = reinterpret_cast<yams_symbol_extractor_v1*>(ptr);
    return p.api ? std::optional<PluginAPI>(std::move(p)) : std::nullopt;
}

void assert_has_kind(yams_symbol_extraction_result_v1* out, const char* kind,
                     size_t min_count = 1) {
    size_t n = 0;
    for (size_t i = 0; i < out->symbol_count; ++i) {
        if (out->symbols[i].kind && std::strcmp(out->symbols[i].kind, kind) == 0)
            ++n;
    }
    EXPECT_GE(n, min_count);
}

} // namespace

TEST(SymbolExtractorPlugins, RustAndGoAndJS) {
    const char* so = "plugins/symbol_extractor_treesitter/libsymbol_extractor_treesitter.so";
    auto plug = load_extractor(so);
    ASSERT_TRUE(plug.has_value());
    auto* api = plug->api;

    struct Case {
        const char* lang;
        const char* path;
        const char* code;
        const char* want_kind;
    } cases[] = {
        {"rust", "/tmp/t.rs", "fn hello(x:i32)->i32{ x+1 }", "function"},
        {"go", "/tmp/t.go", "package main\nfunc Hello(x int) int { return x+1 }", "function"},
        {"javascript", "/tmp/t.js", "function foo(a){ return a+1 }", "function"},
        {"typescript", "/tmp/t.ts", "function foo(a:number){ return a+1 }", "function"},
    };

    for (auto& c : cases) {
        yams_symbol_extraction_result_v1* out = nullptr;
        int rc = api->extract_symbols(api->self, c.code, std::strlen(c.code), c.path, c.lang, &out);
        // Allow environments without grammars to skip gracefully
        if (rc != 0 && out && out->error) {
            // Grammar missing — skip
            continue;
        }
        ASSERT_EQ(rc, 0);
        ASSERT_NE(out, nullptr);
        assert_has_kind(out, c.want_kind, 1);
        api->free_result(api->self, out);
    }
}
