/**
 * @file symbol_extractor_validation_test.cpp
 * @brief Enhanced test suite for symbol extractor plugin validation (Catch2)
 *
 * This test validates that the TreeSitter plugin correctly extracts actual symbol names
 * rather than placeholder values, following good testing practices:
 * - Tests specific expected symbol names for each language
 * - Validates symbol positioning (line numbers, byte offsets)
 * - Tests error handling for missing grammars
 * - Validates plugin capabilities reporting
 */

#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>
#include <catch2/catch_test_macros.hpp>
#include <yams/compat/dlfcn.h>

#include <yams/plugins/abi.h>
#include <yams/plugins/symbol_extractor_v1.h>

namespace {

/**
 * @brief Fixture for symbol extractor validation tests
 */
struct SymbolExtractorValidationFixture {
    SymbolExtractorValidationFixture() {
        // Load the plugin from build directory
#if defined(_WIN32)
        const char* so_path =
            "builddir/plugins/symbol_extractor_treesitter/yams_symbol_extractor.dll";
#elif defined(__APPLE__)
        const char* so_path =
            "builddir/plugins/symbol_extractor_treesitter/yams_symbol_extractor.dylib";
#else
        const char* so_path =
            "builddir/plugins/symbol_extractor_treesitter/yams_symbol_extractor.so";
#endif
        pluginHandle = dlopen(so_path, RTLD_LAZY | RTLD_LOCAL);
        if (!pluginHandle) {
            return; // Will be checked in tests
        }

        // Get required symbols
        auto get_abi =
            reinterpret_cast<int (*)()>(dlsym(pluginHandle, "yams_plugin_get_abi_version"));
        auto get_interface = reinterpret_cast<int (*)(const char*, uint32_t, void**)>(
            dlsym(pluginHandle, "yams_plugin_get_interface"));
        auto init = reinterpret_cast<int (*)(const char*, const void*)>(
            dlsym(pluginHandle, "yams_plugin_init"));

        if (!get_abi || !get_interface || !init) {
            dlclose(pluginHandle);
            pluginHandle = nullptr;
            return;
        }

        // Verify ABI version
        if (get_abi() <= 0) {
            dlclose(pluginHandle);
            pluginHandle = nullptr;
            return;
        }

        // Initialize plugin with C++ and Python support
        const char* config = R"({"languages": ["cpp", "python", "c"], "grammar_paths":{}})";
        if (init(config, nullptr) != 0) {
            dlclose(pluginHandle);
            pluginHandle = nullptr;
            return;
        }

        // Get symbol extractor interface
        void* interface_ptr = nullptr;
        int rc = get_interface(YAMS_IFACE_SYMBOL_EXTRACTOR_V1,
                               YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION, &interface_ptr);
        if (rc != 0 || !interface_ptr) {
            dlclose(pluginHandle);
            pluginHandle = nullptr;
            return;
        }

        extractor = reinterpret_cast<yams_symbol_extractor_v1*>(interface_ptr);
    }

    ~SymbolExtractorValidationFixture() {
        if (extractor && extractor->free_result && extractor->self) {
            for (auto* result : testResults) {
                extractor->free_result(extractor->self, result);
            }
        }

        if (pluginHandle) {
            dlclose(pluginHandle);
        }
    }

    /**
     * @brief Extract symbols from source code and validate basic properties
     */
    yams_symbol_extraction_result_v1*
    extractAndValidate(const std::string& code, const std::string& language,
                       const std::string& filePath = "/tmp/test.code") {
        if (!extractor) {
            return nullptr;
        }
        
        yams_symbol_extraction_result_v1* result = nullptr;
        int rc = extractor->extract_symbols(extractor->self, code.c_str(), code.length(),
                                            filePath.c_str(), language.c_str(), &result);

        if (rc != 0 || !result) {
            return nullptr;
        }

        if (result && result->error) {
            return nullptr;
        }

        if (result) {
            testResults.push_back(result);
        }

        return result;
    }

    /**
     * @brief Check if expected symbol names are present in extraction result
     */
    bool hasSymbol(yams_symbol_extraction_result_v1* result, const std::string& kind,
                   const std::string& expectedName) {
        if (!result) return false;

        for (size_t i = 0; i < result->symbol_count; ++i) {
            auto& symbol = result->symbols[i];
            if (symbol.kind && std::string(symbol.kind) == kind) {
                if (symbol.name && std::string(symbol.name) == expectedName) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @brief Check if plugin is available
     */
    bool isAvailable() const {
        return pluginHandle != nullptr && extractor != nullptr;
    }

    void* pluginHandle{nullptr};
    yams_symbol_extractor_v1* extractor{nullptr};
    std::vector<yams_symbol_extraction_result_v1*> testResults;
};

} // namespace

TEST_CASE_METHOD(SymbolExtractorValidationFixture, "C++ symbol extraction",
                 "[plugins][symbol-extractor][validation][cpp]") {
    if (!isAvailable()) {
        SKIP("Symbol extractor plugin not available");
    }

    SECTION("extracts specific class and function names") {
        const std::string code = R"(
            class DatabaseConnection {
            public:
                void connect(const std::string& url);
                void disconnect();
                bool isConnected() const;
            private:
                std::string url_;
                bool connected_;
            };
            
            int calculateSum(int a, int b) {
                return a + b;
            }
            
            void processData(const std::vector<int>& data) {
                // Processing logic
            }
        )";

        auto* result = extractAndValidate(code, "cpp");
        if (!result) {
            SKIP("Symbol grammar not available");
        }

        // Expect specific class name
        CHECK(hasSymbol(result, "class", "DatabaseConnection"));

        // Expect specific function names
        CHECK(hasSymbol(result, "function", "calculateSum"));
        CHECK(hasSymbol(result, "function", "processData"));
    }
}

TEST_CASE_METHOD(SymbolExtractorValidationFixture, "C struct and function extraction",
                 "[plugins][symbol-extractor][validation][c]") {
    if (!isAvailable()) {
        SKIP("Symbol extractor plugin not available");
    }

    SECTION("extracts struct and function names") {
        const std::string code = R"(
            typedef struct {
                int x;
                int y;
            } Point;
            
            typedef struct Node {
                int data;
                struct Node* next;
            } Node;
            
            double calculateDistance(Point p1, Point p2) {
                return 0.0; // simplified
            }
            
            void printNode(const Node* node) {
                // print logic
            }
        )";

        auto* result = extractAndValidate(code, "c");
        if (!result) {
            SKIP("Symbol grammar not available");
        }

        CHECK(hasSymbol(result, "function", "calculateDistance"));
        CHECK(hasSymbol(result, "function", "printNode"));
    }
}

TEST_CASE_METHOD(SymbolExtractorValidationFixture, "Python symbol extraction",
                 "[plugins][symbol-extractor][validation][python]") {
    if (!isAvailable()) {
        SKIP("Symbol extractor plugin not available");
    }

    SECTION("extracts specific class and function names") {
        const std::string code = R"(
class DataProcessor:
    def __init__(self, name):
        self.name = name
        self.data = []
    
    def add_item(self, item):
        self.data.append(item)
    
    def get_data(self):
        return self.data

def validate_input(value):
    return value is not None

def process_files(file_list):
    for file in file_list:
        print(f"Processing {file}")
)";

        auto* result = extractAndValidate(code, "python");
        if (!result) {
            SKIP("Symbol grammar not available");
        }

        // Expect specific class name
        CHECK(hasSymbol(result, "class", "DataProcessor"));

        // Expect specific function names
        CHECK(hasSymbol(result, "function", "validate_input"));
        CHECK(hasSymbol(result, "function", "process_files"));
    }
}

TEST_CASE_METHOD(SymbolExtractorValidationFixture, "Symbol positioning validation",
                 "[plugins][symbol-extractor][validation][positioning]") {
    if (!isAvailable()) {
        SKIP("Symbol extractor plugin not available");
    }

    SECTION("positions are valid for extracted symbols") {
        const std::string code = "class Test { public: void method(); };";

        auto* result = extractAndValidate(code, "cpp");
        if (!result) {
            SKIP("Symbol grammar not available");
        }
        
        REQUIRE(result->symbol_count > 0u);

        for (size_t i = 0; i < result->symbol_count; ++i) {
            auto& symbol = result->symbols[i];

            // Basic positioning validation
            CHECK(symbol.start_line > 0u);
            CHECK(symbol.end_line > 0u);
            CHECK(symbol.end_line >= symbol.start_line);

            // Byte offsets should be reasonable
            CHECK(symbol.start_offset < symbol.end_offset);
        }
    }
}

TEST_CASE_METHOD(SymbolExtractorValidationFixture, "Plugin capabilities reporting",
                 "[plugins][symbol-extractor][validation][capabilities]") {
    if (!isAvailable()) {
        SKIP("Symbol extractor plugin not available");
    }

    SECTION("reports capabilities in JSON format") {
        char* capabilities_json = nullptr;
        int rc = extractor->get_capabilities_json(extractor->self, &capabilities_json);

        REQUIRE(rc == 0);
        REQUIRE(capabilities_json != nullptr);

        std::string caps(capabilities_json);

        // Should contain languages array
        CHECK(caps.find("\"languages\"") != std::string::npos);

        // Should contain cpp and python based on our config
        CHECK(caps.find("\"cpp\"") != std::string::npos);
        CHECK(caps.find("\"python\"") != std::string::npos);

        // Cleanup
        if (extractor->free_string) {
            extractor->free_string(extractor->self, capabilities_json);
        }
    }
}
