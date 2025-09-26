#include <filesystem>
#include <random>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>

using namespace yams::daemon;

namespace {
std::filesystem::path make_temp_dir(const std::string& prefix) {
    auto base = std::filesystem::temp_directory_path();
    auto dir = base / (prefix + std::to_string(std::random_device{}()));
    std::filesystem::create_directories(dir);
    return dir;
}
} // namespace

TEST(RequestDispatcherStatus, ReportsVectorDbReadyWithoutMetrics) {
    DaemonConfig cfg;
    cfg.dataDir = make_temp_dir("yams_req_dispatch_status_");

    YamsDaemon daemon(cfg);

    StateComponent state;
    ServiceManager svc(cfg, state);

    auto initRes = svc.__test_forceVectorDbInitOnce(cfg.dataDir);
    ASSERT_TRUE(initRes) << "Vector DB init should succeed for test data dir";

    // Simulate stale readiness flags even though the DB exists on disk.
    state.readiness.vectorDbReady.store(false, std::memory_order_relaxed);
    state.readiness.vectorDbDim.store(0, std::memory_order_relaxed);

    RequestDispatcher dispatcher(&daemon, &svc, &state);

    StatusRequest req;
    Request anyReq = req;

    boost::asio::io_context ioc;
    auto fut = boost::asio::co_spawn(ioc, dispatcher.dispatch(anyReq), boost::asio::use_future);
    ioc.run();
    auto resp = fut.get();

    ASSERT_TRUE(std::holds_alternative<StatusResponse>(resp))
        << "Status request should yield StatusResponse";
    const auto& status = std::get<StatusResponse>(resp);

    EXPECT_TRUE(status.vectorDbReady);
    EXPECT_GT(status.vectorDbDim, 0u);
    EXPECT_TRUE(state.readiness.vectorDbReady.load())
        << "Fallback path should heal state readiness flag";
}
