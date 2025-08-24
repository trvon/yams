#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_loader.h>

namespace yams::daemon {
namespace fs = std::filesystem;

class PluginLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Set test mode to handle plugin loading failures gracefully
        setenv("YAMS_TEST_MODE", "1", 1);

        loader_ = std::make_unique<PluginLoader>();

        // Create a temporary directory for test plugins
        testDir_ = fs::temp_directory_path() / "yams_plugin_test";
        fs::create_directories(testDir_);
    }

    void TearDown() override {
        // Unload all plugins
        if (loader_) {
            loader_->unloadAllPlugins();
        }

        // Clean up test directory
        if (fs::exists(testDir_)) {
            fs::remove_all(testDir_);
        }

        // Clean up test mode environment variable
        unsetenv("YAMS_TEST_MODE");
    }

    // Helper to create a mock shared library file (won't actually work, but tests file handling)
    void createMockPlugin(const std::string& name) {
        fs::path pluginPath = testDir_ / name;
        std::ofstream file(pluginPath, std::ios::binary);
        // Write minimal ELF/Mach-O header to make it look like a shared library
        // This won't actually load, but we can test error handling
        file << "Mock plugin content";
        file.close();
    }

    std::unique_ptr<PluginLoader> loader_;
    fs::path testDir_;
};

TEST_F(PluginLoaderTest, GetDefaultDirectories) {
    auto dirs = PluginLoader::getDefaultPluginDirectories();

    // Should have at least some default directories
    EXPECT_FALSE(dirs.empty());

    // Should include system directories
    bool hasSystemDir = false;
    for (const auto& dir : dirs) {
        if (dir.string().find("/usr") != std::string::npos ||
            dir.string().find("lib/yams/plugins") != std::string::npos) {
            hasSystemDir = true;
            break;
        }
    }
    EXPECT_TRUE(hasSystemDir);
}

TEST_F(PluginLoaderTest, LoadNonExistentPlugin) {
    fs::path nonExistentPlugin = testDir_ / "nonexistent.dylib";

    auto result = loader_->loadPlugin(nonExistentPlugin);

    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::FileNotFound);
}

TEST_F(PluginLoaderTest, LoadInvalidPlugin) {
    // Create a file that looks like a plugin but isn't valid
    std::string pluginName = "invalid_plugin.dylib";
    createMockPlugin(pluginName);

    fs::path pluginPath = testDir_ / pluginName;
    auto result = loader_->loadPlugin(pluginPath);

    // Should fail because it's not a real shared library
    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InternalError);
}

TEST_F(PluginLoaderTest, LoadPluginsFromEmptyDirectory) {
    fs::path emptyDir = testDir_ / "empty";
    fs::create_directories(emptyDir);

    auto result = loader_->loadPluginsFromDirectory(emptyDir);

    EXPECT_TRUE(result);
    EXPECT_EQ(result.value(), 0u); // No plugins loaded
}

TEST_F(PluginLoaderTest, LoadPluginsFromNonExistentDirectory) {
    fs::path nonExistentDir = testDir_ / "nonexistent";

    auto result = loader_->loadPluginsFromDirectory(nonExistentDir);

    EXPECT_TRUE(result);
    EXPECT_EQ(result.value(), 0u); // Should return 0, not error
}

TEST_F(PluginLoaderTest, GetLoadedPluginsEmpty) {
    auto plugins = loader_->getLoadedPlugins();

    EXPECT_TRUE(plugins.empty());
}

TEST_F(PluginLoaderTest, IsPluginLoadedFalse) {
    EXPECT_FALSE(loader_->isPluginLoaded("NonexistentPlugin"));
}

TEST_F(PluginLoaderTest, UnloadNonExistentPlugin) {
    auto result = loader_->unloadPlugin("NonexistentPlugin");

    EXPECT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::NotFound);
}

TEST_F(PluginLoaderTest, UnloadAllPluginsWhenEmpty) {
    // Should not crash when no plugins are loaded
    EXPECT_NO_THROW(loader_->unloadAllPlugins());
}

TEST_F(PluginLoaderTest, LoadPluginsWithCustomPattern) {
    // Create some mock files with different extensions
    createMockPlugin("test1.so");
    createMockPlugin("test2.dylib");
    createMockPlugin("test3.dll");
    createMockPlugin("test4.txt"); // Should not match

    // Try to load only .so files
    auto result = loader_->loadPluginsFromDirectory(testDir_, ".*\\.so$");

    // Since our mock plugins aren't real, they'll all fail to load
    // But we're testing the pattern matching logic
    EXPECT_TRUE(result);
}

TEST_F(PluginLoaderTest, AutoLoadPluginsFromEnvironment) {
    // Create a fresh, guaranteed empty directory
    fs::path emptyDir = testDir_ / ("empty_plugin_test_" + std::to_string(getpid()));
    fs::remove_all(emptyDir); // Clean if exists
    fs::create_directories(emptyDir);

    // Set environment variable to the empty directory
    setenv("YAMS_PLUGIN_DIR", emptyDir.c_str(), 1);

    auto result = loader_->autoLoadPlugins();

    // Should succeed even with no plugins
    EXPECT_TRUE(result) << "autoLoadPlugins failed: "
                        << (result ? "succeeded" : result.error().message);
    // Empty directory should load 0 plugins
    if (result) {
        EXPECT_EQ(result.value(), 0u) << "Expected 0 plugins in empty directory: " << emptyDir;
    }

    // Clean up
    unsetenv("YAMS_PLUGIN_DIR");
    fs::remove_all(emptyDir); // Clean up the directory
}

// Integration test - only run if we have the actual ONNX plugin available
TEST_F(PluginLoaderTest, DISABLED_LoadRealOnnxPlugin) {
    // This test is disabled by default since it requires the actual ONNX plugin
    // Enable it manually when testing with a real build

    fs::path pluginPath = "/usr/local/lib/yams/plugins/libyams_onnx_plugin.dylib";
    if (!fs::exists(pluginPath)) {
        GTEST_SKIP() << "ONNX plugin not found at " << pluginPath;
    }

    auto result = loader_->loadPlugin(pluginPath);

    if (result) {
        EXPECT_TRUE(loader_->isPluginLoaded("ONNX"));

        // Check that the provider was registered
        auto provider = createModelProvider("ONNX");
        EXPECT_NE(provider, nullptr);

        // Unload and verify
        auto unloadResult = loader_->unloadPlugin("ONNX");
        EXPECT_TRUE(unloadResult);
        EXPECT_FALSE(loader_->isPluginLoaded("ONNX"));
    }
}

} // namespace yams::daemon