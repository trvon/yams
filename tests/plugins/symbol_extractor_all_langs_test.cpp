#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <yams/compat/dlfcn.h>

#include "../common/plugins.h"
#include <yams/plugins/abi.h>
#include <yams/plugins/symbol_extractor_v1.h>

namespace {
struct PluginAPI {
    void* handle{};
    yams_symbol_extractor_v1* api{};

    PluginAPI() = default;
    PluginAPI(const PluginAPI&) = delete;
    PluginAPI& operator=(const PluginAPI&) = delete;

    PluginAPI(PluginAPI&& other) noexcept : handle(other.handle), api(other.api) {
        other.handle = nullptr;
        other.api = nullptr;
    }
    PluginAPI& operator=(PluginAPI&& other) noexcept {
        if (this != &other) {
            if (handle) { /* skip dlclose in tests to avoid teardown segfaults */
            }
            handle = other.handle;
            api = other.api;
            other.handle = nullptr;
            other.api = nullptr;
        }
        return *this;
    }

    ~PluginAPI() { /* avoid dlclose in tests to prevent teardown crashes */ }
};

std::optional<PluginAPI> load_extractor(const char* so_path) {
    PluginAPI p;
    p.handle = dlopen(so_path, RTLD_LAZY | RTLD_LOCAL);
    if (!p.handle) {
        fprintf(stderr, "dlopen failed for %s: %s\n", so_path, dlerror());
        return std::nullopt;
    }
    auto getabi = (int (*)())dlsym(p.handle, "yams_plugin_get_abi_version");
    auto getiface =
        (int (*)(const char*, uint32_t, void**))dlsym(p.handle, "yams_plugin_get_interface");
    auto init = (int (*)(const char*, const void*))dlsym(p.handle, "yams_plugin_init");
    if (!getabi || !getiface || !init) {
        fprintf(stderr, "Symbol lookup failed: getabi=%p getiface=%p init=%p\n", (void*)getabi,
                (void*)getiface, (void*)init);
        return std::nullopt;
    }
    EXPECT_GT(getabi(), 0);
    EXPECT_EQ(0, init("{\n  \"languages\": [\"cpp\", \"python\", \"rust\", \"go\", \"javascript\", "
                      "\"typescript\"]\n}",
                      nullptr));
    void* ptr = nullptr;
    int rc = getiface(YAMS_IFACE_SYMBOL_EXTRACTOR_V1, YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION, &ptr);
    if (rc != 0 || !ptr) {
        fprintf(stderr, "get_interface failed: rc=%d ptr=%p\n", rc, ptr);
        return std::nullopt;
    }
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

// Helper to load the plugin (refactored from test code)
std::optional<PluginAPI> loadPlugin() {
#ifndef PLUGIN_PATH
#ifdef __APPLE__
    const char* libname = "yams_symbol_extractor.dylib";
#else
    const char* libname = "yams_symbol_extractor.so";
#endif
    const char* buildroot_env = std::getenv("MESON_BUILD_ROOT");
    std::vector<std::string> paths;
    if (buildroot_env && *buildroot_env) {
        std::string br(buildroot_env);
        paths.push_back(br + "/plugins/symbol_extractor_treesitter/" + libname);
        paths.push_back(br + "/tools/yams.d/plugins/symbol_extractor_treesitter/" + libname);
    }
    paths.push_back(std::string("plugins/symbol_extractor_treesitter/") + libname);
    paths.push_back(std::string("tools/yams.d/plugins/symbol_extractor_treesitter/") + libname);
    paths.push_back(std::string("../plugins/symbol_extractor_treesitter/") + libname);
    paths.push_back(std::string("../tools/yams.d/plugins/symbol_extractor_treesitter/") + libname);
    std::optional<PluginAPI> plug;
    for (const auto& cand : paths) {
        plug = load_extractor(cand.c_str());
        if (plug.has_value())
            break;
    }
    return plug;
#else
    std::optional<PluginAPI> plug = load_extractor(PLUGIN_PATH);
    if (!plug.has_value()) {
#ifdef __APPLE__
        const char* libname = "yams_symbol_extractor.dylib";
#else
        const char* libname = "yams_symbol_extractor.so";
#endif
        const char* buildroot_env = std::getenv("MESON_BUILD_ROOT");
        std::vector<std::string> paths;
        if (buildroot_env && *buildroot_env) {
            std::string br(buildroot_env);
            paths.push_back(br + "/plugins/symbol_extractor_treesitter/" + libname);
            paths.push_back(br + "/tools/yams.d/plugins/symbol_extractor_treesitter/" + libname);
        }
        paths.push_back(std::string("plugins/symbol_extractor_treesitter/") + libname);
        paths.push_back(std::string("tools/yams.d/plugins/symbol_extractor_treesitter/") + libname);
        paths.push_back(std::string("../plugins/symbol_extractor_treesitter/") + libname);
        paths.push_back(std::string("../tools/yams.d/plugins/symbol_extractor_treesitter/") +
                        libname);
        for (const auto& cand : paths) {
            plug = load_extractor(cand.c_str());
            if (plug.has_value())
                break;
        }
    }
    return plug;
#endif
}

TEST(SymbolExtractorPlugins, RustAndGoAndJS) {
    auto plug = loadPlugin();
    ASSERT_TRUE(plug.has_value());
    fprintf(stderr, "Plugin loaded successfully\n");

    auto* api = plug->api;
    fprintf(stderr, "api pointer=%p\n", (void*)api);
    if (!api) {
        fprintf(stderr, "ERROR: api is null!\n");
        FAIL();
    }

    // Safely access struct fields
    uint32_t ver = 0;
    memcpy(&ver, &api->abi_version, sizeof(uint32_t));
    fprintf(stderr, "  api->abi_version=%u\n", ver);

    void* self_ptr = nullptr;
    memcpy(&self_ptr, &api->self, sizeof(void*));
    fprintf(stderr, "  api->self=%p\n", self_ptr);

    void* extract_ptr = nullptr;
    memcpy(&extract_ptr, &api->extract_symbols, sizeof(void*));
    fprintf(stderr, "  api->extract_symbols=%p\n", extract_ptr);

    struct Case {
        const char* lang;
        const char* path;
        const char* code;
        const char* want_kind;
    } cases[] = {
        {"c", "/tmp/t.c", "int add(int x, int y) { return x + y; }", "function"},
        {"cpp", "/tmp/t.cpp", "class MyClass { public: void method() {} };", "class"},
        {"python", "/tmp/t.py", "def greet(name):\n    return f'Hello {name}'", "function"},
        {"rust", "/tmp/t.rs", "fn hello(x:i32)->i32{ x+1 }", "function"},
        {"go", "/tmp/t.go", "package main\nfunc Hello(x int) int { return x+1 }", "function"},
        {"javascript", "/tmp/t.js", "function foo(a){ return a+1 }", "function"},
        {"typescript", "/tmp/t.ts", "function foo(a:number){ return a+1 }", "function"},
        {"java", "/tmp/T.java", "public class Test { public void run() {} }", "class"},
        {"csharp", "/tmp/T.cs", "public class Test { public void Run() {} }", "class"},
        {"php", "/tmp/t.php", "<?php function greet($name) { return \"Hello\"; }", "function"},
        {"kotlin", "/tmp/T.kt", "class Test { fun run() {} }", "class"},
        {"perl", "/tmp/t.pl", "sub greet { my $name = shift; }", "function"},
        {"r", "/tmp/t.R", "add <- function(x, y) { x + y }", "function"},
        {"sql", "/tmp/t.sql", "CREATE TABLE users (id INT, name VARCHAR(100))", "statement"},
    };

    for (auto& c : cases) {
        fprintf(stderr, "Testing language: %s\n", c.lang);
        yams_symbol_extraction_result_v1* out = nullptr;
        fprintf(stderr, "  Calling extract_symbols...\n");
        fprintf(stderr, "  api=%p, api->extract_symbols=%p, api->self=%p\n", (void*)api,
                (void*)api->extract_symbols, api->self);
        int rc = api->extract_symbols(api->self, c.code, std::strlen(c.code), c.path, c.lang, &out);
        fprintf(stderr, "  extract_symbols returned: rc=%d, out=%p\n", rc, (void*)out);
        // Allow environments without grammars to skip gracefully
        PLUGIN_MISSING_SKIP(rc, out, "symbol grammar not available");
        ASSERT_EQ(rc, 0);
        ASSERT_NE(out, nullptr);
        assert_has_kind(out, c.want_kind, 1);
        api->free_result(api->self, out);
    }
}

// Test: Multiple symbols extraction
TEST(SymbolExtractorPlugins, ExtractMultipleSymbols) {
    auto plug = loadPlugin();
    ASSERT_TRUE(plug.has_value());

    auto* api = plug->api;
    ASSERT_NE(api, nullptr);

    // C++ code with multiple symbols
    const char* cpp_code = R"(
        class Calculator {
        public:
            int add(int a, int b) { return a + b; }
            int subtract(int a, int b) { return a - b; }
        };
        
        void helper() {}
    )";

    yams_symbol_extraction_result_v1* out = nullptr;
    int rc = api->extract_symbols(api->self, cpp_code, std::strlen(cpp_code), "/tmp/calc.cpp",
                                  "cpp", &out);

    PLUGIN_MISSING_SKIP(rc, out, "C++ grammar not available");
    ASSERT_EQ(rc, 0);
    ASSERT_NE(out, nullptr);

    // Should have at least the class and some methods/functions
    ASSERT_GE(out->symbol_count, 1) << "Expected at least 1 symbol";

    // Verify we have at least one class or function
    bool found_symbol = false;
    for (size_t i = 0; i < out->symbol_count; i++) {
        std::string kind = out->symbols[i].kind ? out->symbols[i].kind : "";
        if (kind == "class" || kind == "function" || kind == "method") {
            found_symbol = true;
            break;
        }
    }
    EXPECT_TRUE(found_symbol) << "Expected at least one class/function/method";

    api->free_result(api->self, out);
}

// Test: Python class and methods
TEST(SymbolExtractorPlugins, PythonClassExtraction) {
    auto plug = loadPlugin();
    ASSERT_TRUE(plug.has_value());

    auto* api = plug->api;
    ASSERT_NE(api, nullptr);

    const char* py_code = R"(
class DataProcessor:
    def __init__(self, name):
        self.name = name
    
    def process(self, data):
        return data.upper()
)";

    yams_symbol_extraction_result_v1* out = nullptr;
    int rc = api->extract_symbols(api->self, py_code, std::strlen(py_code), "/tmp/processor.py",
                                  "python", &out);

    PLUGIN_MISSING_SKIP(rc, out, "Python grammar not available");
    ASSERT_EQ(rc, 0);
    ASSERT_NE(out, nullptr);
    ASSERT_GT(out->symbol_count, 0) << "No symbols extracted";

    // Should have class and methods
    bool has_class = false;
    bool has_init = false;
    bool has_process = false;

    for (size_t i = 0; i < out->symbol_count; i++) {
        std::string kind = out->symbols[i].kind ? out->symbols[i].kind : "";
        std::string name = out->symbols[i].name ? out->symbols[i].name : "";

        if (kind == "class" && name == "DataProcessor")
            has_class = true;
        if (name == "__init__")
            has_init = true;
        if (name == "process")
            has_process = true;
    }

    EXPECT_TRUE(has_class) << "Class 'DataProcessor' not found";
    EXPECT_TRUE(has_init || has_process) << "No methods found";

    api->free_result(api->self, out);
}

// Test: Error handling for invalid input
TEST(SymbolExtractorPlugins, ErrorHandling) {
    auto plug = loadPlugin();
    ASSERT_TRUE(plug.has_value());

    auto* api = plug->api;
    ASSERT_NE(api, nullptr);

    // Test with invalid/incomplete code
    const char* bad_code = "int foo( { // incomplete";
    yams_symbol_extraction_result_v1* out = nullptr;

    int rc =
        api->extract_symbols(api->self, bad_code, std::strlen(bad_code), "/tmp/bad.c", "c", &out);

    PLUGIN_MISSING_SKIP(rc, out, "C grammar not available");

    // Should either succeed with 0 symbols or return error
    if (rc == 0) {
        ASSERT_NE(out, nullptr);
        // Parser might recover and extract nothing - that's ok
        api->free_result(api->self, out);
    } else {
        // Error is acceptable for invalid syntax
        EXPECT_NE(rc, 0);
        if (out) {
            EXPECT_NE(out->error, nullptr) << "Error code but no error message";
            api->free_result(api->self, out);
        }
    }
}
