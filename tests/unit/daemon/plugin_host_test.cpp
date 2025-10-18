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
        // Isolate config for this test process to avoid reading a real trust/config file
        ::setenv("XDG_CONFIG_HOME", tempDir_.c_str(), 1);
        // Ensure daemon-side code paths recognize test mode and deterministic naming
        ::setenv("YAMS_TESTING", "1", 1);
        ::setenv("YAMS_PLUGIN_NAME_POLICY", "spec", 1);
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
    // Capture initial size (may be non-zero on some environments)
    auto initial = host.trustList().size();
    // Add path
    auto dir = tempDir_ / "trusted_abi";
    fs::create_directories(dir);
    ASSERT_TRUE(host.trustAdd(dir));
    auto tl = host.trustList();
    ASSERT_EQ(tl.size(), initial + 1);
    auto canon_dir = fs::weakly_canonical(dir);
    bool found = false;
    for (const auto& p : tl) {
        if (fs::weakly_canonical(p) == canon_dir) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
    // Remove
    ASSERT_TRUE(host.trustRemove(dir));
    auto after = host.trustList();
    EXPECT_EQ(after.size(), initial);
    // Ensure removed path is not present
    bool still_present = false;
    for (const auto& p : after) {
        if (fs::weakly_canonical(p) == canon_dir) {
            still_present = true;
            break;
        }
    }
    EXPECT_FALSE(still_present);
}

TEST_F(PluginHostTest, AbiHostLoadUntrustedReturnsUnauthorized) {
    AbiPluginHost host(nullptr);
    host.setTrustFile(trustFile_);
    auto fake = makeFile("fake_plugin.so", "not a real so");
    auto res = host.load(fake, "{}");
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

    // Load (full load populates name and interfaces under hardened scanning policy)
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
    ASSERT_EQ(table->abi_version, YAMS_IFACE_MODEL_PROVIDER_V1_VERSION);
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
