#include <filesystem>
#include <catch2/catch_test_macros.hpp>
#include <yams/extraction/jsonrpc_client.hpp>
#include <yams/extraction/plugin_process.hpp>

using namespace yams::extraction;
using json = nlohmann::json;

namespace {
// Helper: Get platform-appropriate Python executable
std::string getPythonExecutable() {
    const char* python_env = std::getenv("PYTHON");
    if (python_env) {
        return python_env;
    }
#ifdef _WIN32
    return "python";  // Windows uses 'python' not 'python3'
#else
    return "python3";  // Unix typically uses python3
#endif
}

// Helper: Get mock plugin path relative to test source file
std::filesystem::path getMockPluginPath() {
    // __FILE__ is tests/unit/extraction/jsonrpc_client_test.cpp
    // Navigate up 3 levels to tests/, then into fixtures/
    return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() / "fixtures" / "mock_plugin.py";
}
}  // namespace

TEST_CASE("JsonRpcClient basic communication", "[extraction][jsonrpc]") {
    std::string python = getPythonExecutable();
    std::filesystem::path mock_plugin = getMockPluginPath();
    
    INFO("Python executable: " << python);
    INFO("Mock plugin path: " << mock_plugin.string());
    REQUIRE(std::filesystem::exists(mock_plugin));

    PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};
    PluginProcess process{std::move(config)};

    REQUIRE(process.is_alive());

    JsonRpcClient client{process};

    SECTION("Handshake manifest") {
        auto result = client.call("handshake.manifest");
        REQUIRE(result.has_value());
        REQUIRE((*result)["name"] == "mock_plugin");
        REQUIRE((*result)["version"] == "1.0.0");
        REQUIRE((*result)["interfaces"].is_array());
        REQUIRE((*result)["interfaces"].size() > 0);
    }

    SECTION("Plugin init") {
        auto result = client.call("plugin.init", json::object());
        REQUIRE(result.has_value());
        REQUIRE((*result)["status"] == "ok");
    }

    SECTION("Plugin health") {
        auto result = client.call("plugin.health");
        REQUIRE(result.has_value());
        REQUIRE((*result)["status"] == "ok");
    }

    SECTION("Extractor supports") {
        json params = {{"mime_type", "text/plain"}, {"extension", ".txt"}};
        auto result = client.call("extractor.supports", params);
        REQUIRE(result.has_value());
        REQUIRE((*result)["supported"] == true);
    }

    SECTION("Extractor extract") {
        json params = {{"source", {{"type", "bytes"}, {"data", "SGVsbG8gV29ybGQ="}}}};
        auto result = client.call("extractor.extract", params);
        REQUIRE(result.has_value());
        REQUIRE((*result)["text"] == "Mock extracted text from file");
        REQUIRE((*result)["metadata"]["extractor"] == "mock_plugin");
    }

    SECTION("Multiple sequential calls") {
        for (int i = 0; i < 5; ++i) {
            auto result = client.call("plugin.health");
            REQUIRE(result.has_value());
            REQUIRE((*result)["status"] == "ok");
        }
    }

    SECTION("Invalid method") {
        auto result = client.call("nonexistent.method");
        // Should get an error response or nullopt
        REQUIRE_FALSE(result.has_value());
    }
}

TEST_CASE("JsonRpcClient typed calls", "[extraction][jsonrpc]") {
    std::string python = getPythonExecutable();
    std::filesystem::path mock_plugin = getMockPluginPath();
    
    INFO("Python executable: " << python);
    INFO("Mock plugin path: " << mock_plugin.string());
    REQUIRE(std::filesystem::exists(mock_plugin));

    PluginProcessConfig config{.executable = python, .args = {mock_plugin.string()}};
    PluginProcess process{std::move(config)};
    JsonRpcClient client{process};

    SECTION("Typed call with correct type") {
        auto result = client.call<json>("handshake.manifest");
        REQUIRE(result.has_value());
        REQUIRE((*result)["name"] == "mock_plugin");
    }

    SECTION("Notification (no response expected)") {
        // Notifications don't expect a response
        client.notify("plugin.log", {{"level", "info"}, {"message", "test"}});
        // Should not crash or hang
    }
}
