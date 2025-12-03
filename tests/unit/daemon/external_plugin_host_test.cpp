// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 Trevon Sides

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <nlohmann/json.hpp>
#include <yams/daemon/resource/external_plugin_host.h>

#include <chrono>
#include <filesystem>
#include <fstream>

namespace yams::daemon {
namespace fs = std::filesystem;

struct ExternalPluginHostFixture {
    ExternalPluginHostFixture() {
        tempDir_ = fs::temp_directory_path() / ("yams_eph_" + std::to_string(time(nullptr)));
        fs::create_directories(tempDir_);
        trustFile_ = tempDir_ / "external_plugins_trust.txt";
    }

    ~ExternalPluginHostFixture() {
        std::error_code ec;
        fs::remove_all(tempDir_, ec);
    }

    fs::path makeFile(std::string_view name, std::string_view content = "") {
        auto p = tempDir_ / name;
        std::ofstream{p, std::ios::binary} << content;
        return p;
    }

    fs::path tempDir_;
    fs::path trustFile_;
};

TEST_CASE("ExternalPluginHost - Static File Detection", "[external-plugin][host]") {
    SECTION("Python files are detected") {
        auto temp = fs::temp_directory_path() / "test_plugin.py";
        std::ofstream{temp} << "# test";
        CHECK(ExternalPluginHost::isExternalPluginFile(temp));
        fs::remove(temp);
    }

    SECTION("JavaScript files are detected") {
        auto temp = fs::temp_directory_path() / "test_plugin.js";
        std::ofstream{temp} << "// test";
        CHECK(ExternalPluginHost::isExternalPluginFile(temp));
        fs::remove(temp);
    }

    SECTION("C++ files are not detected") {
        auto temp = fs::temp_directory_path() / "test.cpp";
        std::ofstream{temp} << "// test";
        CHECK_FALSE(ExternalPluginHost::isExternalPluginFile(temp));
        fs::remove(temp);
    }

    SECTION("Non-existent files return false") {
        CHECK_FALSE(ExternalPluginHost::isExternalPluginFile("/nonexistent/path.py"));
    }
}

TEST_CASE("ExternalPluginHost - Supported Extensions", "[external-plugin][host]") {
    auto exts = ExternalPluginHost::supportedExtensions();
    REQUIRE(exts.size() >= 2);
    
    bool hasPython = false;
    bool hasJs = false;
    for (const auto& ext : exts) {
        if (ext == ".py") hasPython = true;
        if (ext == ".js") hasJs = true;
    }
    
    CHECK(hasPython);
    CHECK(hasJs);
}

TEST_CASE("ExternalPluginHost - Construction and Destruction", "[external-plugin][host]") {
    ExternalPluginHostFixture fixture;

    SECTION("Default construction") {
        ExternalPluginHostConfig config;
        ExternalPluginHost host(nullptr, fixture.trustFile_, config);
        
        // Host should be constructible with nullptr ServiceManager
        auto loaded = host.listLoaded();
        CHECK(loaded.empty());
    }

    SECTION("Construction with custom config") {
        ExternalPluginHostConfig config;
        config.maxPlugins = 5;
        config.defaultRpcTimeout = std::chrono::milliseconds{5000};
        config.pythonExecutable = "python";
        
        ExternalPluginHost host(nullptr, fixture.trustFile_, config);
        CHECK(host.listLoaded().empty());
    }
}

TEST_CASE("ExternalPluginHost - Trust Management", "[external-plugin][host][trust]") {
    ExternalPluginHostFixture fixture;
    ExternalPluginHostConfig config;
    ExternalPluginHost host(nullptr, fixture.trustFile_, config);

    SECTION("Initially empty trust list") {
        auto trustList = host.trustList();
        CHECK(trustList.empty());
    }

    SECTION("Add and remove from trust list") {
        auto pluginPath = fixture.makeFile("trusted_plugin.py", "# trusted");
        
        // Add to trust
        auto addResult = host.trustAdd(pluginPath);
        REQUIRE(addResult.has_value());
        
        auto trustList = host.trustList();
        REQUIRE(trustList.size() == 1);
        
        // Verify canonical path is stored
        auto canonical = fs::weakly_canonical(pluginPath);
        bool found = std::find(trustList.begin(), trustList.end(), canonical) != trustList.end();
        CHECK(found);

        // Remove from trust
        auto removeResult = host.trustRemove(pluginPath);
        REQUIRE(removeResult.has_value());
        CHECK(host.trustList().empty());
    }

    SECTION("Trust list persists") {
        auto pluginPath = fixture.makeFile("persistent_plugin.py", "# persist");
        
        {
            ExternalPluginHost host1(nullptr, fixture.trustFile_, config);
            host1.trustAdd(pluginPath);
        }
        
        // Check file was written
        REQUIRE(fs::exists(fixture.trustFile_));
        
        {
            ExternalPluginHost host2(nullptr, fixture.trustFile_, config);
            auto trustList = host2.trustList();
            CHECK(trustList.size() == 1);
        }
    }
}

TEST_CASE("ExternalPluginHost - Scan Invalid Files", "[external-plugin][host][scan]") {
    ExternalPluginHostFixture fixture;
    ExternalPluginHostConfig config;
    ExternalPluginHost host(nullptr, fixture.trustFile_, config);

    SECTION("Scan non-existent file returns error") {
        auto result = host.scanTarget("/nonexistent/plugin.py");
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == ErrorCode::InvalidArgument);
    }

    SECTION("Scan non-plugin file returns error") {
        auto textFile = fixture.makeFile("readme.txt", "Not a plugin");
        auto result = host.scanTarget(textFile);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().code == ErrorCode::InvalidArgument);
    }
}

TEST_CASE("ExternalPluginHost - Load Non-Existent Plugin", "[external-plugin][host][load]") {
    ExternalPluginHostFixture fixture;
    ExternalPluginHostConfig config;
    ExternalPluginHost host(nullptr, fixture.trustFile_, config);

    auto result = host.load("/nonexistent/plugin.py", "{}");
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::FileNotFound);
}

TEST_CASE("ExternalPluginHost - Unload Non-Existent Plugin", "[external-plugin][host][unload]") {
    ExternalPluginHostFixture fixture;
    ExternalPluginHostConfig config;
    ExternalPluginHost host(nullptr, fixture.trustFile_, config);

    auto result = host.unload("nonexistent-plugin");
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::NotFound);
}

TEST_CASE("ExternalPluginHost - Health Check Non-Existent Plugin", "[external-plugin][host][health]") {
    ExternalPluginHostFixture fixture;
    ExternalPluginHostConfig config;
    ExternalPluginHost host(nullptr, fixture.trustFile_, config);

    auto result = host.health("nonexistent-plugin");
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::NotFound);
}

TEST_CASE("ExternalPluginHost - RPC Call Non-Existent Plugin", "[external-plugin][host][rpc]") {
    ExternalPluginHostFixture fixture;
    ExternalPluginHostConfig config;
    ExternalPluginHost host(nullptr, fixture.trustFile_, config);

    nlohmann::json emptyParams = nlohmann::json::object();
    auto result = host.callRpc("nonexistent-plugin", "test.method", emptyParams);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::NotFound);
}

TEST_CASE("ExternalPluginHost - Scan Empty Directory", "[external-plugin][host][scan]") {
    ExternalPluginHostFixture fixture;
    ExternalPluginHostConfig config;
    ExternalPluginHost host(nullptr, fixture.trustFile_, config);

    auto emptyDir = fixture.tempDir_ / "empty";
    fs::create_directories(emptyDir);

    auto result = host.scanDirectory(emptyDir);
    REQUIRE(result.has_value());
    CHECK(result.value().empty());
}

TEST_CASE("ExternalPluginHost - Scan Non-Existent Directory", "[external-plugin][host][scan]") {
    ExternalPluginHostFixture fixture;
    ExternalPluginHostConfig config;
    ExternalPluginHost host(nullptr, fixture.trustFile_, config);

    auto result = host.scanDirectory("/nonexistent/directory");
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::InvalidPath);
}

TEST_CASE("ExternalPluginHost - Get Stats Empty", "[external-plugin][host][stats]") {
    ExternalPluginHostFixture fixture;
    ExternalPluginHostConfig config;
    ExternalPluginHost host(nullptr, fixture.trustFile_, config);

    auto stats = host.getStats();
    REQUIRE(stats.contains("total_loaded"));
    CHECK(stats["total_loaded"].get<size_t>() == 0);
    REQUIRE(stats.contains("plugins"));
    CHECK(stats["plugins"].is_array());
    CHECK(stats["plugins"].empty());
}

TEST_CASE("ExternalPluginHost - State Callback", "[external-plugin][host][callback]") {
    ExternalPluginHostFixture fixture;
    ExternalPluginHostConfig config;
    ExternalPluginHost host(nullptr, fixture.trustFile_, config);

    std::vector<std::pair<std::string, std::string>> events;
    host.setStateCallback([&events](const std::string& name, const std::string& event) {
        events.emplace_back(name, event);
    });

    // Callback shouldn't be called for operations that fail
    host.unload("nonexistent");
    CHECK(events.empty());
}

} // namespace yams::daemon
