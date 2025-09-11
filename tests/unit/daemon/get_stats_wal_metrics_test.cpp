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

TEST(DaemonStatsWalMetricsTest, GetStatsIncludesWalKeys) {
    // Minimal daemon objects (no start); use separate ServiceManager and StateComponent for
    // dispatcher
    DaemonConfig cfg;
    cfg.dataDir = make_temp_dir("yams_daemon_stats_");

    YamsDaemon daemon(cfg);

    StateComponent state; // default readiness/metrics
    ServiceManager svc(cfg, state);
    RequestDispatcher dispatcher(&daemon, &svc, &state);

    GetStatsRequest req; // defaults ok
    Request r = req;
    boost::asio::io_context ioc;
    auto fut = boost::asio::co_spawn(ioc, dispatcher.dispatch(r), boost::asio::use_future);
    ioc.run();
    auto resp = fut.get();

    ASSERT_TRUE(std::holds_alternative<GetStatsResponse>(resp));
    auto s = std::get<GetStatsResponse>(resp);

    // The provider returns zeros until attached; we only assert presence of keys
    auto it1 = s.additionalStats.find("wal_active_transactions");
    auto it2 = s.additionalStats.find("wal_pending_entries");
    EXPECT_NE(it1, s.additionalStats.end());
    EXPECT_NE(it2, s.additionalStats.end());
}
