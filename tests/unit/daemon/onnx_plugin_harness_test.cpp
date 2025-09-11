#include <filesystem>
#include <gtest/gtest.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/plugins/model_provider_v1.h>

namespace yams::daemon {
namespace fs = std::filesystem;

class OnnxPluginHarnessTest : public ::testing::Test {
protected:
    void SetUp() override {
        tempDir_ = fs::temp_directory_path() / ("yams_onnx_ht_" + std::to_string(::getpid()));
        fs::create_directories(tempDir_);
        trustFile_ = tempDir_ / "plugins_trust.txt";
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(tempDir_, ec);
    }
    fs::path tempDir_;
    fs::path trustFile_;
};

TEST_F(OnnxPluginHarnessTest, ConditionalLoadOnnxIfPresent) {
    const char* envPath = std::getenv("TEST_ONNX_PLUGIN_FILE");
    fs::path pluginPath;
#ifdef yams_onnx_plugin_BUILT
    pluginPath = fs::path(yams_onnx_plugin_BUILT);
#endif
    if (envPath && *envPath)
        pluginPath = fs::path(envPath);

    if (pluginPath.empty() || !fs::exists(pluginPath)) {
        GTEST_SKIP()
            << "ONNX plugin file not provided; set TEST_ONNX_PLUGIN_FILE or build plugins/onnx";
    }

    AbiPluginHost host(nullptr);
    host.setTrustFile(trustFile_);
    ASSERT_TRUE(host.trustAdd(pluginPath.parent_path()));

    auto scan = host.scanTarget(pluginPath);
    if (!scan) {
        GTEST_SKIP() << "ONNX plugin scan failed (likely missing runtime dependencies): "
                     << pluginPath;
    }
    // Name can be "onnx" per manifest, interfaces should include model_provider_v1
    bool hasMp = false;
    for (const auto& id : scan.value().interfaces)
        if (id.find("model_provider_v1") != std::string::npos) {
            hasMp = true;
            break;
        }
    EXPECT_TRUE(hasMp);

    auto lr = host.load(pluginPath, "{}");
    ASSERT_TRUE(lr);
    auto ifaceRes = host.getInterface(lr.value().name, "model_provider_v1", 1);
    ASSERT_TRUE(ifaceRes);
    auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceRes.value());
    ASSERT_NE(table, nullptr);
    ASSERT_EQ(table->abi_version, 1u);
    // Do not load any model here; availability depends on local model files.

    EXPECT_TRUE(host.unload(lr.value().name));
}

} // namespace yams::daemon
