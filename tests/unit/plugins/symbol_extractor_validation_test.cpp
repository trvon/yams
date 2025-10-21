#pragma once

#include <cstring>
#include <dlfcn.h>
#include <string>
#include <unordered_set>
#include <vector>
#include <gtest/gtest.h>

#include "../../common/plugins.h"
#include <yams/plugins/abi.h>
#include <yams/plugins/symbol_extractor_v1.h>

/**
 * @brief Enhanced test suite for symbol extractor plugin validation
 *
 * This test validates that the TreeSitter plugin correctly extracts actual symbol names
 * rather than placeholder values, following good testing practices:
 * - Tests specific expected symbol names for each language
 * - Validates symbol positioning (line numbers, byte offsets)
 * - Tests error handling for missing grammars
 * - Validates plugin capabilities reporting
 */
class SymbolExtractorValidationTest : public ::testing::Test {
protected:
    void SetUp() override {
        pluginHandle_ = nullptr;
        extractor_ = nullptr;

        // Load the plugin from build directory
#ifdef __APPLE__
        const char* so_path =
            "builddir/plugins/symbol_extractor_treesitter/libsymbol_extractor_treesitter.dylib";
#else
        const char* so_path =
            "builddir/plugins/symbol_extractor_treesitter/libsymbol_extractor_treesitter.so";
#endif
        pluginHandle_ = dlopen(so_path, RTLD_LAZY | RTLD_LOCAL);
        ASSERT_NE(pluginHandle_, nullptr) << "Failed to load plugin: " << dlerror();

        // Get required symbols
        auto get_abi =
            reinterpret_cast<int (*)()>(dlsym(pluginHandle_, "yams_plugin_get_abi_version"));
        auto get_interface = reinterpret_cast<int (*)(const char*, uint32_t, void**)>(
            dlsym(pluginHandle_, "yams_plugin_get_interface"));
        auto init = reinterpret_cast<int (*)(const char*, const void*)>(
            dlsym(pluginHandle_, "yams_plugin_init"));

        ASSERT_NE(get_abi, nullptr);
        ASSERT_NE(get_interface, nullptr);
        ASSERT_NE(init, nullptr);

        // Verify ABI version
        EXPECT_GT(get_abi(), 0);

        // Initialize plugin with C++ and Python support
        const char* config = R"({"languages": ["cpp", "python", "c"], "grammar_paths":{}})";
        EXPECT_EQ(init(config, nullptr), 0);

        // Get symbol extractor interface
        void* interface_ptr = nullptr;
        int rc = get_interface(YAMS_IFACE_SYMBOL_EXTRACTOR_V1,
                               YAMS_IFACE_SYMBOL_EXTRACTOR_V1_VERSION, &interface_ptr);
        ASSERT_EQ(rc, 0);
        ASSERT_NE(interface_ptr, nullptr);

        extractor_ = reinterpret_cast<yams_symbol_extractor_v1*>(interface_ptr);
    }

    void TearDown() override {
        if (extractor_ && extractor_->free_result && extractor_->self) {
            // Clean up any remaining results
            for (auto* result : testResults_) {
                extractor_->free_result(extractor_->self, result);
            }
        }

        if (pluginHandle_) {
            dlclose(pluginHandle_);
        }
    }

    /**
     * @brief Extract symbols from source code and validate basic properties
     */
    yams_symbol_extraction_result_v1*
    extractAndValidate(const std::string& code, const std::string& language,
                       const std::string& filePath = "/tmp/test.code") {
        yams_symbol_extraction_result_v1* result = nullptr;
        int rc = extractor_->extract_symbols(extractor_->self, code.c_str(), code.length(),
                                             filePath.c_str(), language.c_str(), &result);

        PLUGIN_MISSING_SKIP(rc, result, "symbol grammar not available");
        EXPECT_EQ(rc, 0) << "Extraction failed with code: " << rc;
        EXPECT_NE(result, nullptr) << "Result is null";

        if (result && result->error) {
            EXPECT_EQ(result->error, nullptr) << "Unexpected error: " << result->error;
        }

        if (result) {
            testResults_.push_back(result);
        }

        return result;
    }

    /**
     * @brief Check if expected symbol names are present in extraction result
     */
    void expectSymbols(yams_symbol_extraction_result_v1* result, const std::string& kind,
                       const std::unordered_set<std::string>& expectedNames) {
        ASSERT_NE(result, nullptr);

        std::unordered_set<std::string> foundNames;
        for (size_t i = 0; i < result->symbol_count; ++i) {
            auto& symbol = result->symbols[i];
            if (symbol.kind && std::string(symbol.kind) == kind) {
                if (symbol.name) {
                    foundNames.insert(symbol.name);
                }
            }
        }

        for (const auto& expected : expectedNames) {
            EXPECT_TRUE(foundNames.count(expected) > 0)
                << "Expected " << kind << " '" << expected << "' not found. Found names: ";

            // Print found names for debugging
            if (foundNames.count(expected) == 0) {
                std::cout << "Available " << kind << " names: ";
                for (const auto& name : foundNames) {
                    std::cout << "'" << name << "' ";
                }
                std::cout << std::endl;
            }
        }
    }

protected:
    void* pluginHandle_;
    yams_symbol_extractor_v1* extractor_;
    std::vector<yams_symbol_extraction_result_v1*> testResults_;
};

/**
 * @brief Test C++ class and function extraction with specific names
 */
TEST_F(SymbolExtractorValidationTest, CppExtractsSpecificSymbolNames) {
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
    ASSERT_NE(result, nullptr);

    // Expect specific class name
    expectSymbols(result, "class", {"DatabaseConnection"});

    // Expect specific function names
    expectSymbols(result, "function",
                  {"calculateSum", "processData", "connect", "disconnect", "isConnected"});
}

/**
 * @brief Test C struct extraction
 */
TEST_F(SymbolExtractorValidationTest, CExtractsStructAndFunctionNames) {
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
    ASSERT_NE(result, nullptr);

    // Note: TreeSitter might detect these as classes or structs depending on grammar
    expectSymbols(result, "struct", {"Point", "Node"});
    expectSymbols(result, "function", {"calculateDistance", "printNode"});
}

/**
 * @brief Test Python class and function extraction
 */
TEST_F(SymbolExtractorValidationTest, PythonExtractsSpecificSymbolNames) {
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
    ASSERT_NE(result, nullptr);

    // Expect specific class name
    expectSymbols(result, "class", {"DataProcessor"});

    // Expect specific function names
    expectSymbols(result, "function", {"validate_input", "process_files", "add_item", "get_data"});
}

/**
 * @brief Test symbol positioning is valid
 */
TEST_F(SymbolExtractorValidationTest, SymbolPositioningIsValid) {
    const std::string code = "class Test { public: void method(); };";

    auto* result = extractAndValidate(code, "cpp");
    ASSERT_NE(result, nullptr);
    ASSERT_GT(result->symbol_count, 0u);

    for (size_t i = 0; i < result->symbol_count; ++i) {
        auto& symbol = result->symbols[i];

        // Basic positioning validation
        EXPECT_GT(symbol.start_line, 0u)
            << "Invalid start line for symbol: " << (symbol.name ? symbol.name : "unknown");
        EXPECT_GT(symbol.end_line, 0u)
            << "Invalid end line for symbol: " << (symbol.name ? symbol.name : "unknown");
        EXPECT_GE(symbol.end_line, symbol.start_line) << "End line before start line";

        // Byte offsets should be reasonable
        EXPECT_LT(symbol.start_offset, symbol.end_offset) << "Start offset >= end offset";
    }
}

/**
 * @brief Test plugin capabilities reporting
 */
TEST_F(SymbolExtractorValidationTest, PluginReportsCapabilities) {
    char* capabilities_json = nullptr;
    int rc = extractor_->get_capabilities_json(extractor_->self, &capabilities_json);

    EXPECT_EQ(rc, 0);
    EXPECT_NE(capabilities_json, nullptr);

    if (capabilities_json) {
        std::string caps(capabilities_json);

        // Should contain languages array
        EXPECT_NE(caps.find("\"languages\""), std::string::npos)
            << "Missing languages in capabilities";

        // Should contain cpp and python based on our config
        EXPECT_NE(caps.find("\"cpp\""), std::string::npos) << "Missing cpp in languages";
        EXPECT_NE(caps.find("\"python\""), std::string::npos) << "Missing python in languages";

        // Cleanup
        if (extractor_->free_string) {
            extractor_->free_string(extractor_->self, capabilities_json);
        }
    }
}