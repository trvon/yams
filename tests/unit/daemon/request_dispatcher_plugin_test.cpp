#include <filesystem>
#include <gtest/gtest.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {
namespace fs = std::filesystem;

static DaemonConfig makeConfig(const fs::path& dataDir) {
    DaemonConfig cfg;
    cfg.dataDir = dataDir;
    cfg.autoLoadPlugins = false;
    cfg.enableModelProvider = true;
    return cfg;
}

class RequestDispatcherPluginTest : public ::testing::Test {
protected:
    void SetUp() override {
        tempDir_ = fs::temp_directory_path() / ("yams_rdph_" + std::to_string(::getpid()));
        fs::create_directories(tempDir_);
        state_ = std::make_unique<StateComponent>();
        lifecycleFsm_ = std::make_unique<DaemonLifecycleFsm>();
        svc_ = std::make_unique<ServiceManager>(makeConfig(tempDir_), *state_, *lifecycleFsm_);
        dispatcher_ = std::make_unique<RequestDispatcher>(svc_.get(), state_.get());
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(tempDir_, ec);
        dispatcher_.reset();
        svc_.reset();
        lifecycleFsm_.reset();
        state_.reset();
    }
    fs::path tempDir_;
    std::unique_ptr<StateComponent> state_;
    std::unique_ptr<DaemonLifecycleFsm> lifecycleFsm_;
    std::unique_ptr<ServiceManager> svc_;
    std::unique_ptr<RequestDispatcher> dispatcher_;
};

TEST_F(RequestDispatcherPluginTest, LoadAbiMockPluginAdoptsProvider) {
#ifndef TEST_ABI_PLUGIN_FILE
    GTEST_SKIP() << "Mock ABI plugin not available";
#else
    fs::path plugin = fs::path(TEST_ABI_PLUGIN_FILE);
    // Trust the directory via dispatcher calls
    PluginTrustAddRequest tadd;
    tadd.path = plugin.parent_path().string();
    auto r1 = dispatcher_->handlePluginTrustAddRequest(tadd);
    ASSERT_TRUE(std::holds_alternative<SuccessResponse>(r1));

    // Dry-run load
    PluginLoadRequest lreq;
    lreq.pathOrName = plugin.string();
    lreq.dryRun = true;
    auto dr = dispatcher_->handlePluginLoadRequest(lreq);
    ASSERT_TRUE(std::holds_alternative<PluginLoadResponse>(dr));
    auto drr = std::get<PluginLoadResponse>(dr);
    EXPECT_FALSE(drr.loaded);
    EXPECT_EQ(drr.message, "dry-run");

    // Real load
    lreq.dryRun = false;
    auto lr = dispatcher_->handlePluginLoadRequest(lreq);
    ASSERT_TRUE(std::holds_alternative<PluginLoadResponse>(lr));
    auto lrv = std::get<PluginLoadResponse>(lr);
    EXPECT_TRUE(lrv.loaded);

    // After load, ServiceManager should have adopted a model provider
    auto mp = svc_->getModelProvider();
    ASSERT_TRUE(mp != nullptr);
    EXPECT_EQ(mp->getProviderName(), "MOCK_ABI");

    // Trust list should include the dir
    PluginTrustListRequest tlr;
    auto tl = dispatcher_->handlePluginTrustListRequest(tlr);
    ASSERT_TRUE(std::holds_alternative<PluginTrustListResponse>(tl));
    auto tlv = std::get<PluginTrustListResponse>(tl);
    bool found = false;
    for (const auto& p : tlv.paths)
        if (fs::weakly_canonical(p) == fs::weakly_canonical(plugin.parent_path())) {
            found = true;
            break;
        }
    EXPECT_TRUE(found);

    // Unload
    PluginUnloadRequest ureq;
    ureq.name = lrv.record.name;
    auto ur = dispatcher_->handlePluginUnloadRequest(ureq);
    ASSERT_TRUE(std::holds_alternative<SuccessResponse>(ur));
#endif
}

} // namespace yams::daemon
