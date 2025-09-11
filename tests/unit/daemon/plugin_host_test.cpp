#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/plugins/model_provider_v1.h>

namespace yams::daemon {
namespace fs = std::filesystem;

class PluginHostTest : public ::testing::Test {
protected:
    void SetUp() override {
        tempDir_ = fs::temp_directory_path() / ("yams_ph_" + std::to_string(::getpid()));
        fs::create_directories(tempDir_);
        trustFile_ = tempDir_ / "plugins_trust.txt";
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(tempDir_, ec);
    }

    fs::path makeFile(const fs::path& name, const std::string& content = "") {
        auto p = tempDir_ / name;
        std::ofstream of(p, std::ios::binary);
        of << content;
        return p;
    }

    fs::path tempDir_;
    fs::path trustFile_;
};

TEST_F(PluginHostTest, AbiHostTrustPolicyAddRemove) {
    AbiPluginHost host(nullptr);
    host.setTrustFile(trustFile_);
    // Initially empty
    EXPECT_TRUE(host.trustList().empty());
    // Add path
    auto dir = tempDir_ / "trusted_abi";
    fs::create_directories(dir);
    ASSERT_TRUE(host.trustAdd(dir));
    auto tl = host.trustList();
    ASSERT_EQ(tl.size(), 1u);
    EXPECT_EQ(fs::weakly_canonical(tl[0]), fs::weakly_canonical(dir));
    // Remove
    ASSERT_TRUE(host.trustRemove(dir));
    EXPECT_TRUE(host.trustList().empty());
}

TEST_F(PluginHostTest, AbiHostLoadUntrustedReturnsUnauthorized) {
    AbiPluginHost host(nullptr);
    host.setTrustFile(trustFile_);
    auto fake = makeFile("fake_plugin.so", "not a real so");
    auto res = host.load(fake, "{}");
    ASSERT_FALSE(res);
    EXPECT_EQ(res.error().code, ErrorCode::Unauthorized);
}

TEST_F(PluginHostTest, WasmHostScanParsesManifest) {
    WasmPluginHost host(trustFile_);
    auto wasm = makeFile("dummy.wasm", "00asm");
    // Sidecar manifest
    auto manifest = wasm;
    manifest += ".manifest.json";
    std::ofstream mf(manifest);
    mf << R"({"name":"os_wasm","version":"0.1.0","interfaces":["object_storage_v1"]})";
    mf.close();
    auto sr = host.scanTarget(wasm);
    ASSERT_TRUE(sr);
    auto desc = sr.value();
    EXPECT_EQ(desc.name, "os_wasm");
    EXPECT_EQ(desc.version, "0.1.0");
    ASSERT_FALSE(desc.interfaces.empty());
    EXPECT_EQ(desc.interfaces[0], std::string("object_storage_v1"));
}

TEST_F(PluginHostTest, WasmHostTrustPolicyAddRemove) {
    WasmPluginHost host(trustFile_);
    EXPECT_TRUE(host.trustList().empty());
    auto dir = tempDir_ / "trusted_wasm";
    fs::create_directories(dir);
    ASSERT_TRUE(host.trustAdd(dir));
    auto tl = host.trustList();
    ASSERT_EQ(tl.size(), 1u);
    EXPECT_EQ(fs::weakly_canonical(tl[0]), fs::weakly_canonical(dir));
    ASSERT_TRUE(host.trustRemove(dir));
    EXPECT_TRUE(host.trustList().empty());
}

TEST_F(PluginHostTest, WasmHostLoadUntrustedReturnsUnauthorized) {
    WasmPluginHost host(trustFile_);
    auto wasm = makeFile("dummy2.wasm", "00asm");
    auto res = host.load(wasm, "{}");
    ASSERT_FALSE(res);
    EXPECT_EQ(res.error().code, ErrorCode::Unauthorized);
}

#ifdef TEST_ABI_PLUGIN_FILE
TEST_F(PluginHostTest, AbiHostLoadMockModelPluginAndGetInterface) {
    AbiPluginHost host(nullptr);
    host.setTrustFile(trustFile_);
    // Trust the directory containing the plugin
    fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
    auto dir = pluginPath.parent_path();
    ASSERT_TRUE(host.trustAdd(dir));

    // Dry scan
    auto scan = host.scanTarget(pluginPath);
    ASSERT_TRUE(scan);
    auto sdesc = scan.value();
    EXPECT_EQ(sdesc.name, "mock_model");
    ASSERT_FALSE(sdesc.interfaces.empty());
    bool hasMp = false;
    for (const auto& id : sdesc.interfaces)
        if (id.find("model_provider_v1") != std::string::npos) {
            hasMp = true;
            break;
        }
    EXPECT_TRUE(hasMp);

    // Load
    auto lr = host.load(pluginPath, "{}");
    ASSERT_TRUE(lr);
    auto ldesc = lr.value();
    EXPECT_EQ(ldesc.name, "mock_model");

    // Retrieve interface table and create provider
    auto ifaceRes = host.getInterface("mock_model", "model_provider_v1", 1);
    ASSERT_TRUE(ifaceRes);
    auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceRes.value());
    ASSERT_NE(table, nullptr);
    // Basic smoke: v1 table should allow load and list
    ASSERT_EQ(table->abi_version, 1u);
    bool loaded = false;
    ASSERT_EQ(table->load_model(table->self, "test_model", nullptr, nullptr), 0);
    ASSERT_EQ(table->is_model_loaded(table->self, "test_model", &loaded), 0);
    EXPECT_TRUE(loaded);
    const char** ids = nullptr;
    size_t count = 0;
    ASSERT_EQ(table->get_loaded_models(table->self, &ids, &count), 0);
    EXPECT_GE(count, 1u);
    table->free_model_list(table->self, ids, count);
    ASSERT_EQ(table->unload_model(table->self, "test_model"), 0);

    // Unload
    auto ur = host.unload("mock_model");
    EXPECT_TRUE(ur);
}
#endif

} // namespace yams::daemon
