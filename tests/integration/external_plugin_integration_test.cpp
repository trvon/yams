#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <cstdio>

namespace {

std::string run_cmd(const std::string& cmd) {
    std::string out;
    FILE* fp = popen(cmd.c_str(), "r");
    if (!fp)
        return out;
    char buf[4096];
    while (fgets(buf, sizeof(buf), fp)) {
        out.append(buf);
    }
    pclose(fp);
    return out;
}

namespace fs = std::filesystem;

// Resolve a repository root by walking up until we find
// external/yams-sdk/python/yams_sdk/__init__.py
std::optional<fs::path> resolveRepoRoot() {
    fs::path cur = fs::current_path();
    for (int i = 0; i < 8; ++i) {
        fs::path candidate = cur / "external" / "yams-sdk" / "python" / "yams_sdk" / "__init__.py";
        std::error_code ec;
        if (fs::exists(candidate, ec)) {
            return cur;
        }
        if (!cur.has_parent_path())
            break;
        cur = cur.parent_path();
    }
    if (const char* repoEnv = std::getenv("REPO_ROOT")) {
        fs::path fromEnv(repoEnv);
        fs::path candidate =
            fromEnv / "external" / "yams-sdk" / "python" / "yams_sdk" / "__init__.py";
        std::error_code ec;
        if (fs::exists(candidate, ec))
            return fromEnv;
    }
    return std::nullopt;
}

fs::path defaultDataDir() {
    if (const char* storageEnv = std::getenv("YAMS_STORAGE")) {
        return fs::path(storageEnv);
    }
    if (const char* dataEnv = std::getenv("YAMS_DATA_DIR")) {
        return fs::path(dataEnv);
    }
    if (const char* homeEnv = std::getenv("HOME")) {
        return fs::path(homeEnv) / ".local" / "share" / "yams";
    }
    return fs::path(".") / "yams_data";
}

std::string readFileToString(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in)
        return {};
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

bool writeStringToFile(const fs::path& p, const std::string& s) {
    std::error_code ec;
    fs::create_directories(p.parent_path(), ec);
    std::ofstream out(p, std::ios::binary);
    if (!out)
        return false;
    out << s;
    return true;
}

fs::path makeTestPluginDir() {
    const char* home = std::getenv("HOME");
    fs::path base = home ? fs::path(home) / ".local" / "share" / "yams" / "test_plugins"
                         : fs::path(".") / "yams_test_plugins";
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    fs::path dir = base / ("external_python_" + std::to_string(ts));
    std::error_code ec;
    fs::create_directories(dir, ec);
    // Try to set safe permissions (remove world-writable bit)
    std::error_code ec2;
    auto perms = fs::status(dir, ec2).permissions();
    if (!ec2) {
        perms &= ~fs::perms::others_write;
        perms &= ~fs::perms::group_write;
        fs::permissions(dir, perms, ec2);
    }
    return dir;
}

} // namespace

TEST(ExternalPluginIntegration, LoadListUnloadPythonSDKTemplate) {
    // Resolve repo root and SDK/template paths
    auto repoRootOpt = resolveRepoRoot();
    if (!repoRootOpt) {
        GTEST_SKIP()
            << "Repo root could not be resolved; set REPO_ROOT or run from repository tree";
    }
    fs::path repoRoot = *repoRootOpt;

    fs::path sdkPython = repoRoot / "external" / "yams-sdk" / "python";
    fs::path templatePluginPy =
        repoRoot / "external" / "yams-sdk" / "python" / "templates" / "external-dr" / "plugin.py";
    {
        std::error_code ec;
        if (!fs::exists(templatePluginPy, ec)) {
            GTEST_SKIP() << "Template plugin not found: " << templatePluginPy.string();
        }
    }

    // Create a test plugin directory under ~/.local/share/yams/test_plugins (non-world-writable
    // parent)
    fs::path pluginDir = makeTestPluginDir();
    fs::path pluginPy = pluginDir / "plugin.py";
    // Copy plugin.py into test directory
    auto pluginSrc = readFileToString(templatePluginPy);
    ASSERT_FALSE(pluginSrc.empty()) << "Failed to read template plugin.py";
    ASSERT_TRUE(writeStringToFile(pluginPy, pluginSrc)) << "Failed to write plugin.py";

    // Write yams-plugin.json manifest with a command that sets PYTHONPATH to locate yams_sdk
    // Command: prepend PYTHONPATH to include SDK and run plugin.py
    std::string cmd = "PYTHONPATH=" + (sdkPython.string()) + ":$PYTHONPATH python3 plugin.py";
    std::string manifest = std::string("{\"name\":\"dr_python\",\"version\":\"0.0.1\","
                                       "\"interfaces\":[\"dr_provider_v1\"],\"cmd\":\"") +
                           cmd + "\"}";

    fs::path manifestPath = pluginDir / "yams-plugin.json";
    ASSERT_TRUE(writeStringToFile(manifestPath, manifest)) << "Failed to write yams-plugin.json";

    // 1) Trust the plugin directory via CLI
    {
        std::string cmdTrust = std::string("yams plugin trust add \"") + pluginDir.string() + "\"";
        int rc = std::system(cmdTrust.c_str());
        if (rc != 0) {
            GTEST_SKIP() << "Trust add failed via CLI";
        }
    }

    // 2) Load the plugin (by path) via CLI
    std::string pluginName;
    {
        std::string out = run_cmd(std::string("yams plugin load \"") + pluginDir.string() + "\"");
        ASSERT_TRUE(out.find("Loaded") != std::string::npos) << "Load failed: " << out;
        pluginName = "dr_python";
        EXPECT_FALSE(pluginName.empty());
    }

    // 3) Verify via CLI that the plugin is listed
    {
        std::string out = run_cmd("yams plugin list");
        bool found = out.find(pluginName) != std::string::npos;
        EXPECT_TRUE(found) << "Loaded plugin not listed";
    }

    // 4) Unload the plugin via CLI
    {
        std::string out = run_cmd(std::string("yams plugin unload ") + pluginName);
        ASSERT_TRUE(out.find("Unloaded") != std::string::npos) << "Unload failed: " << out;
    }

    // 5) Ensure it's no longer listed (via CLI)
    {
        std::string out = run_cmd("yams plugin list");
        bool found = out.find(pluginName) != std::string::npos;
        EXPECT_FALSE(found) << "Plugin still listed after unload";
    }
}
