#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <thread>
#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/plugins/model_provider_v1.h>

namespace yams::daemon {
namespace fs = std::filesystem;

struct PluginHostFixture {
    PluginHostFixture() {
        tempDir_ = fs::temp_directory_path() / ("yams_ph_" + std::to_string(::getpid()));
        fs::create_directories(tempDir_);
        trustFile_ = tempDir_ / "plugins_trust.txt";
        ::setenv("XDG_CONFIG_HOME", tempDir_.c_str(), 1);
        ::setenv("YAMS_TESTING", "1", 1);
        ::setenv("YAMS_PLUGIN_NAME_POLICY", "spec", 1);
    }

    ~PluginHostFixture() {
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

struct RequestDispatcherFixture {
    RequestDispatcherFixture() {
        tempDir_ = fs::temp_directory_path() / ("yams_rdph_" + std::to_string(::getpid()));
        fs::create_directories(tempDir_);
        state_ = std::make_unique<StateComponent>();
        lifecycleFsm_ = std::make_unique<DaemonLifecycleFsm>();

        DaemonConfig cfg;
        cfg.dataDir = tempDir_;
        cfg.autoLoadPlugins = false;
        cfg.enableModelProvider = true;

        svc_ = std::make_unique<ServiceManager>(cfg, *state_, *lifecycleFsm_);
        dispatcher_ = std::make_unique<RequestDispatcher>(nullptr, svc_.get(), state_.get());
    }

    ~RequestDispatcherFixture() {
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

TEST_CASE("PluginHost - Trust Management", "[plugin][host][trust]") {
    PluginHostFixture fixture;

    SECTION("Trust policy add and remove") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        auto initial = host.trustList().size();
        auto dir = fixture.tempDir_ / "trusted_abi";
        fs::create_directories(dir);

        REQUIRE(host.trustAdd(dir));
        auto tl = host.trustList();
        CHECK(tl.size() == initial + 1);

        auto canon_dir = fs::weakly_canonical(dir);
        bool found = false;
        for (const auto& p : tl) {
            if (fs::weakly_canonical(p) == canon_dir) {
                found = true;
                break;
            }
        }
        CHECK(found);

        REQUIRE(host.trustRemove(dir));
        auto after = host.trustList();
        CHECK(after.size() == initial);

        bool still_present = false;
        for (const auto& p : after) {
            if (fs::weakly_canonical(p) == canon_dir) {
                still_present = true;
                break;
            }
        }
        CHECK_FALSE(still_present);
    }

    SECTION("Load untrusted plugin returns unauthorized") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        auto fake = fixture.makeFile("fake_plugin.so", "not a real so");
        auto res = host.load(fake, "{}");

        REQUIRE_FALSE(res);
        CHECK(res.error().code == ErrorCode::Unauthorized);
    }

    SECTION("Plugin discovery - scan non-existent target") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        auto fake = fixture.tempDir_ / "nonexistent.so";
        auto res = host.scanTarget(fake);

        REQUIRE_FALSE(res);
    }

    SECTION("Plugin discovery - scan empty directory") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        auto emptyDir = fixture.tempDir_ / "empty_plugins";
        fs::create_directories(emptyDir);

        auto res = host.scanDirectory(emptyDir);
        REQUIRE(res);
        CHECK(res.value().empty());
    }

    SECTION("Trust file corruption - invalid JSON") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        {
            std::ofstream out(fixture.trustFile_);
            out << "{ invalid json syntax ][";
        }

        auto tl = host.trustList();
        CHECK(tl.empty());
    }

    SECTION("Trust operations with same trust file path") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        auto dir1 = fixture.tempDir_ / "trusted_dir1";
        auto dir2 = fixture.tempDir_ / "trusted_dir2";
        fs::create_directories(dir1);
        fs::create_directories(dir2);

        REQUIRE(host.trustAdd(dir1));
        REQUIRE(host.trustAdd(dir2));

        auto tl = host.trustList();
        CHECK(tl.size() >= 2);

        REQUIRE(host.trustRemove(dir1));
        auto after_remove = host.trustList();
        CHECK(after_remove.size() == tl.size() - 1);
    }

    SECTION("List loaded plugins when empty") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        auto loaded = host.listLoaded();
        CHECK(loaded.empty());
    }

#ifdef TEST_ABI_PLUGIN_FILE
    SECTION("Load mock model plugin and get interface") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();
        REQUIRE(host.trustAdd(dir));

        auto lr = host.load(pluginPath, "{}");
        REQUIRE(lr);
        auto ldesc = lr.value();
        CHECK(ldesc.name == "mock_model");

        auto ifaceRes = host.getInterface("mock_model", "model_provider_v1", 1);
        REQUIRE(ifaceRes);
        auto* table = reinterpret_cast<yams_model_provider_v1*>(ifaceRes.value());
        REQUIRE(table != nullptr);

        CHECK(table->abi_version == YAMS_IFACE_MODEL_PROVIDER_V1_VERSION);

        bool loaded = false;
        CHECK(table->load_model(table->self, "test_model", nullptr, nullptr) == 0);
        CHECK(table->is_model_loaded(table->self, "test_model", &loaded) == 0);
        CHECK(loaded);

        const char** ids = nullptr;
        size_t count = 0;
        CHECK(table->get_loaded_models(table->self, &ids, &count) == 0);
        CHECK(count >= 1u);
        table->free_model_list(table->self, ids, count);
        CHECK(table->unload_model(table->self, "test_model") == 0);

        auto ur = host.unload("mock_model");
        CHECK(ur);
    }

    SECTION("Plugin unloading - unload non-existent plugin") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        auto res = host.unload("nonexistent_plugin");
        REQUIRE_FALSE(res);
    }

    SECTION("Plugin unloading - list loaded after unload") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();
        REQUIRE(host.trustAdd(dir));

        auto lr = host.load(pluginPath, "{}");
        REQUIRE(lr);

        auto loaded_before = host.listLoaded();
        CHECK(loaded_before.size() == 1);

        auto ur = host.unload(lr.value().name);
        REQUIRE(ur);

        auto loaded_after = host.listLoaded();
        CHECK(loaded_after.empty());
    }

    SECTION("Plugin health check - healthy plugin") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();
        REQUIRE(host.trustAdd(dir));

        auto lr = host.load(pluginPath, "{}");
        REQUIRE(lr);

        auto health = host.health(lr.value().name);
        REQUIRE(health);
        CHECK_FALSE(health.value().empty());
    }

    SECTION("Plugin health check - non-existent plugin") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        auto health = host.health("nonexistent_plugin");
        REQUIRE_FALSE(health);
    }

    SECTION("Plugin discovery - scan target finds plugin") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto res = host.scanTarget(pluginPath);

        REQUIRE(res);
        auto desc = res.value();
        CHECK(desc.name == "mock_model");
        CHECK(desc.abiVersion > 0);
        CHECK_FALSE(desc.path.empty());
    }

    SECTION("Plugin discovery - scan directory finds plugins") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();

        auto res = host.scanDirectory(dir);
        REQUIRE(res);

        auto plugins = res.value();
        CHECK_FALSE(plugins.empty());

        bool found_mock = false;
        for (const auto& p : plugins) {
            if (p.name == "mock_model") {
                found_mock = true;
                CHECK(p.abiVersion > 0);
                CHECK_FALSE(p.interfaces.empty());
            }
        }
        CHECK(found_mock);
    }

    SECTION("Plugin discovery - getLastScanSkips after scan") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();

        host.scanDirectory(dir);
        auto skips = host.getLastScanSkips();
    }

    SECTION("Plugin reload - unload and reload same plugin") {
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();
        REQUIRE(host.trustAdd(dir));

        auto lr1 = host.load(pluginPath, "{}");
        REQUIRE(lr1);
        auto name = lr1.value().name;

        auto ur = host.unload(name);
        REQUIRE(ur);

        auto lr2 = host.load(pluginPath, "{}");
        REQUIRE(lr2);
        CHECK(lr2.value().name == name);

        host.unload(name);
    }
#endif
}

TEST_CASE("RequestDispatcher - Plugin Integration", "[plugin][dispatcher][integration]") {
    RequestDispatcherFixture fixture;

#ifdef TEST_ABI_PLUGIN_FILE
    SECTION("Load ABI mock plugin and adopt provider") {
        fs::path plugin(TEST_ABI_PLUGIN_FILE);

        PluginTrustAddRequest tadd;
        tadd.path = plugin.parent_path().string();
        auto r1 = fixture.dispatcher_->handlePluginTrustAddRequest(tadd);
        REQUIRE(std::holds_alternative<SuccessResponse>(r1));

        PluginLoadRequest lreq;
        lreq.pathOrName = plugin.string();
        lreq.dryRun = true;
        auto dr = fixture.dispatcher_->handlePluginLoadRequest(lreq);
        REQUIRE(std::holds_alternative<PluginLoadResponse>(dr));
        auto drr = std::get<PluginLoadResponse>(dr);
        CHECK_FALSE(drr.loaded);
        CHECK(drr.message == "dry-run");

        lreq.dryRun = false;
        auto lr = fixture.dispatcher_->handlePluginLoadRequest(lreq);
        REQUIRE(std::holds_alternative<PluginLoadResponse>(lr));
        auto lrv = std::get<PluginLoadResponse>(lr);
        CHECK(lrv.loaded);

        auto mp = fixture.svc_->getModelProvider();
        REQUIRE(mp != nullptr);
        CHECK(mp->getProviderName() == "MOCK_ABI");

        PluginTrustListRequest tlr;
        auto tl = fixture.dispatcher_->handlePluginTrustListRequest(tlr);
        REQUIRE(std::holds_alternative<PluginTrustListResponse>(tl));
        auto tlv = std::get<PluginTrustListResponse>(tl);

        bool found = false;
        for (const auto& p : tlv.paths) {
            if (fs::weakly_canonical(p) == fs::weakly_canonical(plugin.parent_path())) {
                found = true;
                break;
            }
        }
        CHECK(found);

        PluginUnloadRequest ureq;
        ureq.name = lrv.record.name;
        auto ur = fixture.dispatcher_->handlePluginUnloadRequest(ureq);
        REQUIRE(std::holds_alternative<SuccessResponse>(ur));
    }

    SECTION("Plugin state transitions - degraded to healthy") {
        fs::path plugin(TEST_ABI_PLUGIN_FILE);

        PluginTrustAddRequest tadd;
        tadd.path = plugin.parent_path().string();
        fixture.dispatcher_->handlePluginTrustAddRequest(tadd);

        PluginLoadRequest lreq;
        lreq.pathOrName = plugin.string();
        auto lr = fixture.dispatcher_->handlePluginLoadRequest(lreq);
        REQUIRE(std::holds_alternative<PluginLoadResponse>(lr));
        auto lrv = std::get<PluginLoadResponse>(lr);
        REQUIRE(lrv.loaded);

        auto mp = fixture.svc_->getModelProvider();
        REQUIRE(mp != nullptr);

        fixture.svc_->__test_setModelProviderDegraded(true, "simulated error");
        auto degraded_subsystems = fixture.lifecycleFsm_->degradedSubsystems();
        CHECK(degraded_subsystems.find("ModelProvider") != degraded_subsystems.end());

        fixture.svc_->__test_setModelProviderDegraded(false);
        degraded_subsystems = fixture.lifecycleFsm_->degradedSubsystems();
        CHECK(degraded_subsystems.find("ModelProvider") == degraded_subsystems.end());
    }

    SECTION("Plugin state transitions - healthy to degraded and back") {
        fs::path plugin(TEST_ABI_PLUGIN_FILE);

        PluginTrustAddRequest tadd;
        tadd.path = plugin.parent_path().string();
        fixture.dispatcher_->handlePluginTrustAddRequest(tadd);

        PluginLoadRequest lreq;
        lreq.pathOrName = plugin.string();
        auto lr = fixture.dispatcher_->handlePluginLoadRequest(lreq);
        REQUIRE(std::holds_alternative<PluginLoadResponse>(lr));
        REQUIRE(std::get<PluginLoadResponse>(lr).loaded);

        auto mp = fixture.svc_->getModelProvider();
        REQUIRE(mp != nullptr);

        auto initial_degraded = fixture.lifecycleFsm_->degradedSubsystems();
        CHECK(initial_degraded.find("ModelProvider") == initial_degraded.end());

        fixture.svc_->__test_setModelProviderDegraded(true, "test error 1");
        auto after_degrade = fixture.lifecycleFsm_->degradedSubsystems();
        CHECK(after_degrade.find("ModelProvider") != after_degrade.end());

        fixture.svc_->__test_setModelProviderDegraded(false);
        auto after_recover = fixture.lifecycleFsm_->degradedSubsystems();
        CHECK(after_recover.find("ModelProvider") == after_recover.end());

        fixture.svc_->__test_setModelProviderDegraded(true, "test error 2");
        auto after_degrade_again = fixture.lifecycleFsm_->degradedSubsystems();
        CHECK(after_degrade_again.find("ModelProvider") != after_degrade_again.end());

        fixture.svc_->__test_setModelProviderDegraded(false);
        auto final_state = fixture.lifecycleFsm_->degradedSubsystems();
        CHECK(final_state.find("ModelProvider") == final_state.end());
    }

    SECTION("Plugin status snapshot after load") {
        fs::path plugin(TEST_ABI_PLUGIN_FILE);

        PluginTrustAddRequest tadd;
        tadd.path = plugin.parent_path().string();
        fixture.dispatcher_->handlePluginTrustAddRequest(tadd);

        PluginLoadRequest lreq;
        lreq.pathOrName = plugin.string();
        auto lr = fixture.dispatcher_->handlePluginLoadRequest(lreq);
        REQUIRE(std::holds_alternative<PluginLoadResponse>(lr));

        auto snapshot = fixture.svc_->getPluginStatusSnapshot();
        CHECK_FALSE(snapshot.records.empty());

        bool found_provider = false;
        for (const auto& rec : snapshot.records) {
            if (rec.isProvider) {
                found_provider = true;
                CHECK(rec.ready);
                CHECK_FALSE(rec.degraded);
            }
        }
        CHECK(found_provider);
    }

    SECTION("Plugin unload through dispatcher") {
        fs::path plugin(TEST_ABI_PLUGIN_FILE);

        PluginTrustAddRequest tadd;
        tadd.path = plugin.parent_path().string();
        fixture.dispatcher_->handlePluginTrustAddRequest(tadd);

        PluginLoadRequest lreq;
        lreq.pathOrName = plugin.string();
        auto lr = fixture.dispatcher_->handlePluginLoadRequest(lreq);
        REQUIRE(std::holds_alternative<PluginLoadResponse>(lr));
        auto lrv = std::get<PluginLoadResponse>(lr);

        PluginUnloadRequest ureq;
        ureq.name = lrv.record.name;
        auto ur = fixture.dispatcher_->handlePluginUnloadRequest(ureq);
        REQUIRE(std::holds_alternative<SuccessResponse>(ur));

        auto mp = fixture.svc_->getModelProvider();
        CHECK(mp == nullptr);
    }

    SECTION("Plugin list request after load") {
        fs::path plugin(TEST_ABI_PLUGIN_FILE);

        PluginTrustAddRequest tadd;
        tadd.path = plugin.parent_path().string();
        fixture.dispatcher_->handlePluginTrustAddRequest(tadd);

        PluginLoadRequest lreq;
        lreq.pathOrName = plugin.string();
        auto lr = fixture.dispatcher_->handlePluginLoadRequest(lreq);
        REQUIRE(std::holds_alternative<PluginLoadResponse>(lr));

        PluginListRequest listreq;
        auto list_resp = fixture.dispatcher_->handlePluginListRequest(listreq);
        REQUIRE(std::holds_alternative<PluginListResponse>(list_resp));
        auto records = std::get<PluginListResponse>(list_resp).records;

        CHECK_FALSE(records.empty());
        CHECK(records[0].name == "mock_model");
    }
#endif
}

TEST_CASE("Plugin Advanced Scenarios", "[plugin][advanced][stress]") {
#ifdef TEST_ABI_PLUGIN_FILE
    SECTION("Concurrent trust operations") {
        PluginHostFixture fixture;
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        std::vector<std::future<Result<void>>> futures;
        std::vector<fs::path> dirs;

        for (int i = 0; i < 5; ++i) {
            auto dir = fixture.tempDir_ / ("concurrent_trust_" + std::to_string(i));
            fs::create_directories(dir);
            dirs.push_back(dir);
        }

        for (const auto& dir : dirs) {
            futures.push_back(
                std::async(std::launch::async, [&host, dir]() { return host.trustAdd(dir); }));
        }

        int success_count = 0;
        for (auto& f : futures) {
            auto result = f.get();
            if (result) {
                ++success_count;
            }
        }

        CHECK(success_count >= 3);
        auto final_list = host.trustList();
        CHECK(final_list.size() >= 3);
    }

    SECTION("Rapid load and unload cycles") {
        PluginHostFixture fixture;
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();
        REQUIRE(host.trustAdd(dir));

        constexpr int cycles = 5;
        for (int i = 0; i < cycles; ++i) {
            auto lr = host.load(pluginPath, "{}");
            REQUIRE(lr);

            auto loaded = host.listLoaded();
            CHECK(loaded.size() == 1);

            auto ur = host.unload(lr.value().name);
            REQUIRE(ur);

            auto after_unload = host.listLoaded();
            CHECK(after_unload.empty());
        }
    }

    SECTION("Plugin scan with invalid files in directory") {
        PluginHostFixture fixture;
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        auto testDir = fixture.tempDir_ / "mixed_files";
        fs::create_directories(testDir);

        fixture.makeFile("mixed_files/not_a_plugin.txt", "just text");
        fixture.makeFile("mixed_files/fake.so", "not a real shared object");
        fixture.makeFile("mixed_files/empty.so", "");

        auto res = host.scanDirectory(testDir);
        REQUIRE(res);
        CHECK(res.value().empty());

        auto skips = host.getLastScanSkips();
        CHECK(skips.size() >= 1);
    }

    SECTION("Load plugin with custom configuration JSON") {
        PluginHostFixture fixture;
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();
        REQUIRE(host.trustAdd(dir));

        std::string customConfig = R"({"model_path": "/custom/path", "threads": 4})";
        auto lr = host.load(pluginPath, customConfig);
        REQUIRE(lr);

        auto loaded = host.listLoaded();
        CHECK(loaded.size() == 1);

        host.unload(lr.value().name);
    }

    SECTION("Plugin interface retrieval with wrong version") {
        PluginHostFixture fixture;
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();
        REQUIRE(host.trustAdd(dir));

        auto lr = host.load(pluginPath, "{}");
        REQUIRE(lr);

        auto ifaceRes = host.getInterface("mock_model", "model_provider_v1", 999);
        REQUIRE_FALSE(ifaceRes);

        host.unload(lr.value().name);
    }

    SECTION("Plugin interface retrieval with wrong interface ID") {
        PluginHostFixture fixture;
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();
        REQUIRE(host.trustAdd(dir));

        auto lr = host.load(pluginPath, "{}");
        REQUIRE(lr);

        auto ifaceRes = host.getInterface("mock_model", "nonexistent_interface", 1);
        REQUIRE_FALSE(ifaceRes);

        host.unload(lr.value().name);
    }

    SECTION("RequestDispatcher - multiple state transitions with status checks") {
        RequestDispatcherFixture fixture;
        fs::path plugin(TEST_ABI_PLUGIN_FILE);

        PluginTrustAddRequest tadd;
        tadd.path = plugin.parent_path().string();
        fixture.dispatcher_->handlePluginTrustAddRequest(tadd);

        PluginLoadRequest lreq;
        lreq.pathOrName = plugin.string();
        auto lr = fixture.dispatcher_->handlePluginLoadRequest(lreq);
        REQUIRE(std::holds_alternative<PluginLoadResponse>(lr));

        for (int cycle = 0; cycle < 3; ++cycle) {
            fixture.svc_->__test_setModelProviderDegraded(true, "cycle " + std::to_string(cycle));

            auto snapshot = fixture.svc_->getPluginStatusSnapshot();
            bool found_degraded = false;
            for (const auto& rec : snapshot.records) {
                if (rec.isProvider && rec.degraded) {
                    found_degraded = true;
                    CHECK_FALSE(rec.error.empty());
                }
            }
            CHECK(found_degraded);

            fixture.svc_->__test_setModelProviderDegraded(false);

            snapshot = fixture.svc_->getPluginStatusSnapshot();
            for (const auto& rec : snapshot.records) {
                if (rec.isProvider) {
                    CHECK_FALSE(rec.degraded);
                }
            }
        }
    }

    SECTION("Plugin load with empty configuration") {
        PluginHostFixture fixture;
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        fs::path pluginPath(TEST_ABI_PLUGIN_FILE);
        auto dir = pluginPath.parent_path();
        REQUIRE(host.trustAdd(dir));

        auto lr1 = host.load(pluginPath, "");
        REQUIRE(lr1);

        auto ur = host.unload(lr1.value().name);
        REQUIRE(ur);

        auto lr2 = host.load(pluginPath, "{}");
        REQUIRE(lr2);

        host.unload(lr2.value().name);
    }

    SECTION("Trust list after multiple add/remove operations") {
        PluginHostFixture fixture;
        AbiPluginHost host(nullptr);
        host.setTrustFile(fixture.trustFile_);

        std::vector<fs::path> dirs;
        for (int i = 0; i < 10; ++i) {
            auto dir = fixture.tempDir_ / ("trust_test_" + std::to_string(i));
            fs::create_directories(dir);
            dirs.push_back(dir);
            REQUIRE(host.trustAdd(dir));
        }

        auto tl = host.trustList();
        CHECK(tl.size() >= 10);

        for (int i = 0; i < 5; ++i) {
            REQUIRE(host.trustRemove(dirs[i]));
        }

        auto after_remove = host.trustList();
        CHECK(after_remove.size() == tl.size() - 5);

        for (int i = 5; i < 10; ++i) {
            REQUIRE(host.trustRemove(dirs[i]));
        }

        auto final_list = host.trustList();
        CHECK(final_list.size() == tl.size() - 10);
    }

    SECTION("Plugin health status during state transitions") {
        RequestDispatcherFixture fixture;
        fs::path plugin(TEST_ABI_PLUGIN_FILE);

        PluginTrustAddRequest tadd;
        tadd.path = plugin.parent_path().string();
        fixture.dispatcher_->handlePluginTrustAddRequest(tadd);

        PluginLoadRequest lreq;
        lreq.pathOrName = plugin.string();
        auto lr = fixture.dispatcher_->handlePluginLoadRequest(lreq);
        REQUIRE(std::holds_alternative<PluginLoadResponse>(lr));

        fixture.svc_->__test_setModelProviderDegraded(true, "testing health during degraded state");

        auto snapshot_degraded = fixture.svc_->getPluginStatusSnapshot();
        bool found_degraded_with_error = false;
        for (const auto& rec : snapshot_degraded.records) {
            if (rec.isProvider && rec.degraded) {
                found_degraded_with_error = true;
                CHECK(rec.error.find("testing health") != std::string::npos);
            }
        }
        CHECK(found_degraded_with_error);

        fixture.svc_->__test_setModelProviderDegraded(false);

        auto snapshot_healthy = fixture.svc_->getPluginStatusSnapshot();
        for (const auto& rec : snapshot_healthy.records) {
            if (rec.isProvider) {
                CHECK_FALSE(rec.degraded);
                CHECK(rec.ready);
            }
        }
    }
#endif
}

} // namespace yams::daemon
