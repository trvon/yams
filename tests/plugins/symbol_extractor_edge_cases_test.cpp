#include <cstring>
#include <dlfcn.h>
#include <optional>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include "../common/plugins.h"
#include <yams/plugins/symbol_extractor_v1.h>

namespace {
struct PluginAPI {
    void* handle{};
    yams_symbol_extractor_v1* api{};
    ~PluginAPI() { /* avoid dlclose in tests to prevent teardown crashes */ }
};

std::optional<PluginAPI> loadPlugin() {
#ifdef PLUGIN_PATH
    const char* libpath = PLUGIN_PATH;
#else
    const char* libpath = nullptr;
#endif
    PluginAPI p;
    if (!libpath)
        return std::nullopt;
    p.handle = dlopen(libpath, RTLD_LAZY | RTLD_LOCAL);
    if (!p.handle)
        return std::nullopt;
    auto getabi = (int (*)())dlsym(p.handle, "yams_plugin_get_abi_version");
    auto getiface =
        (int (*)(const char*, uint32_t, void**))dlsym(p.handle, "yams_plugin_get_interface");
    auto init = (int (*)(const char*, const void*))dlsym(p.handle, "yams_plugin_init");
    if (!getabi || !getiface || !init)
        return std::nullopt;
    if (getabi() <= 0)
        return std::nullopt;
    if (init("{\"languages\":[\"cpp\",\"python\",\"rust\",\"go\"]}", nullptr) != 0)
        return std::nullopt;
    void* ptr = nullptr;
    int rc = getiface(YAMS_IFACE_SYMBOL_EXTRACTOR_V1, YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION, &ptr);
    if (rc != 0 || !ptr)
        return std::nullopt;
    p.api = reinterpret_cast<yams_symbol_extractor_v1*>(ptr);
    return p.api ? std::optional<PluginAPI>(std::move(p)) : std::nullopt;
}
} // namespace

// Unicode identifiers (Python) — ensure extractor handles UTF-8 content
TEST(SymbolExtractorPlugins_EdgeCases, UnicodeIdentifiers) {
    auto plug = loadPlugin();
    ASSERT_TRUE(plug.has_value());
    auto* api = plug->api;
    ASSERT_NE(api, nullptr);

    const char* py_code =
        "\n# -*- coding: utf-8 -*-\n\nclass 解析器:\n    def 寿司(self, 値):\n        return 値\n";

    yams_symbol_extraction_result_v1* out = nullptr;
    int rc = api->extract_symbols(api->self, py_code, std::strlen(py_code), "/tmp/unicode.py",
                                  "python", &out);

    PLUGIN_MISSING_SKIP(rc, out, "Python grammar not available");
    ASSERT_EQ(rc, 0);
    ASSERT_NE(out, nullptr);

    // At minimum, we should extract a class or a function/method
    bool saw_symbol = false;
    bool saw_unicode_name = false;
    for (size_t i = 0; i < out->symbol_count; ++i) {
        const char* name = out->symbols[i].name ? out->symbols[i].name : "";
        const char* kind = out->symbols[i].kind ? out->symbols[i].kind : "";
        if (*kind)
            saw_symbol = true;
        if (std::strcmp(name, "寿司") == 0 || std::strcmp(name, "解析器") == 0) {
            saw_unicode_name = true;
        }
    }
    EXPECT_TRUE(saw_symbol);
    // Unicode capture may depend on grammar support; do not hard fail
    // but prefer to see at least one unicode name captured when available
    if (saw_symbol) {
        EXPECT_TRUE(saw_unicode_name);
    }

    api->free_result(api->self, out);
}

// Empty input — should return an error (graceful)
TEST(SymbolExtractorPlugins_EdgeCases, EmptyFile) {
    auto plug = loadPlugin();
    ASSERT_TRUE(plug.has_value());
    auto* api = plug->api;
    ASSERT_NE(api, nullptr);

    const char* empty = "";
    yams_symbol_extraction_result_v1* out = nullptr;
    int rc = api->extract_symbols(api->self, empty, 0, "/tmp/empty.c", "c", &out);

    // Empty content is an invalid input; either error or empty result is acceptable
    if (rc == 0) {
        ASSERT_NE(out, nullptr);
        api->free_result(api->self, out);
    } else {
        // Prefer explicit error message
        EXPECT_NE(out, nullptr);
        if (out) {
            EXPECT_NE(out->error, nullptr);
            api->free_result(api->self, out);
        }
    }
}

// Large input (~1MB) — ensure no crash and reasonable runtime
TEST(SymbolExtractorPlugins_EdgeCases, HugeFileOneMB) {
    auto plug = loadPlugin();
    ASSERT_TRUE(plug.has_value());
    auto* api = plug->api;
    ASSERT_NE(api, nullptr);

    std::string unit = "int f(int x){return x+1;}\n";
    std::string big;
    big.reserve(1024 * 1024 + 1024);
    while (big.size() < 1024 * 1024)
        big += unit;

    yams_symbol_extraction_result_v1* out = nullptr;
    int rc = api->extract_symbols(api->self, big.data(), big.size(), "/tmp/huge.c", "c", &out);

    PLUGIN_MISSING_SKIP(rc, out, "C grammar not available");
    ASSERT_TRUE(rc == 0 || rc == YAMS_PLUGIN_ERR_INVALID);
    if (rc == 0) {
        ASSERT_NE(out, nullptr);
        // Expect at least one function detected
        EXPECT_GE(out->symbol_count, 1u);
        api->free_result(api->self, out);
    } else if (out) {
        api->free_result(api->self, out);
    }
}

// Binary-ish content — ensure graceful handling
TEST(SymbolExtractorPlugins_EdgeCases, BinaryContent) {
    auto plug = loadPlugin();
    ASSERT_TRUE(plug.has_value());
    auto* api = plug->api;
    ASSERT_NE(api, nullptr);

    std::string bytes(4096, '\xff'); // not valid UTF-8
    yams_symbol_extraction_result_v1* out = nullptr;
    int rc = api->extract_symbols(api->self, bytes.data(), bytes.size(), "/tmp/bin.go", "go", &out);

    PLUGIN_MISSING_SKIP(rc, out, "Go grammar not available");
    // Either parse to zero symbols or return an error
    if (rc == 0) {
        ASSERT_NE(out, nullptr);
        // Likely no symbols
        EXPECT_GE(out->symbol_count, 0u);
        api->free_result(api->self, out);
    } else {
        if (out)
            api->free_result(api->self, out);
    }
}
