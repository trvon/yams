#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <thread>
#include <vector>

#include "test_daemon_harness.h"
#include "../../common/env_compat.h"

#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>

#include <yams/compat/unistd.h>
#include <yams/daemon/client/daemon_client.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {

class EnvGuard {
    std::string name_;
    std::string prev_;
    bool hadPrev_{false};

public:
    EnvGuard(const char* name, const char* value) : name_(name) {
        if (const char* existing = std::getenv(name)) {
            prev_ = existing;
            hadPrev_ = true;
        }
        setenv(name, value, 1);
    }
    ~EnvGuard() {
        if (hadPrev_) {
            setenv(name_.c_str(), prev_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }
    EnvGuard(const EnvGuard&) = delete;
    EnvGuard& operator=(const EnvGuard&) = delete;
};

DaemonClient createClient(const std::filesystem::path& socketPath) {
    ClientConfig cfg;
    cfg.socketPath = socketPath;
    cfg.autoStart = false;
    cfg.connectTimeout = 2s;
    cfg.requestTimeout = 3s;
    return DaemonClient(cfg);
}

bool connectWithRetry(DaemonClient& client, int maxRetries = 5) {
    for (int i = 0; i < maxRetries; ++i) {
        auto r = yams::cli::run_sync(client.connect(), 3s);
        if (r.has_value()) {
            return true;
        }
        std::this_thread::sleep_for(100ms);
    }
    return false;
}

} // namespace

TEST_CASE("Proxy connections cannot starve main status/shutdown", "[daemon][proxy][slots]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Keep the main slot budget small so starvation is easy to reproduce.
    EnvGuard envMaxConn("YAMS_MAX_ACTIVE_CONN", "8");

    // Proxy socket is derived from daemon socket (no env override required).
    // This avoids global /tmp/proxy.sock collisions and keeps tests isolated.

    constexpr size_t kMainSlots = 8;
    constexpr size_t kProxyTarget = kMainSlots - 1; // keep 1 slot for the initial main client

    DaemonHarness harness;
    REQUIRE(harness.start());

    // Use daemon socket explicitly (bypass proxy auto-discovery).
    auto mainClient = createClient(harness.socketPath());
    REQUIRE(connectWithRetry(mainClient));

    // DaemonMetrics may return a cached snapshot created before SocketServer is wired in,
    // so poll briefly until proxy socket path is populated.
    std::filesystem::path proxyPath;
    for (int i = 0; i < 50; ++i) {
        auto status = yams::cli::run_sync(mainClient.status(), 2s);
        if (status && !status.value().proxySocketPath.empty()) {
            proxyPath = std::filesystem::path(status.value().proxySocketPath);
            break;
        }
        std::this_thread::sleep_for(50ms);
    }
    REQUIRE_FALSE(proxyPath.empty());
    // Sanity: derived proxy path should match daemon socket stem + ".proxy.sock".
    {
        const auto daemonSock = harness.socketPath();
        auto base = daemonSock.stem().string();
        if (base.empty())
            base = daemonSock.filename().string();
        if (base.empty())
            base = "yams-daemon";
        const auto expected = daemonSock.parent_path() / (base + ".proxy.sock");
        REQUIRE(proxyPath == expected);
    }

    auto waitForProxyAccepted = [&](size_t want) {
        for (int i = 0; i < 50; ++i) {
            auto s = yams::cli::run_sync(mainClient.status(), 2s);
            if (s && s.value().proxyActiveConnections >= want) {
                return true;
            }
            std::this_thread::sleep_for(50ms);
        }
        return false;
    };

    // Hold proxy connections open to fill its hard cap.
    // The server default cap is 512, but tests should remain stable with lower caps.
    // We only need a small number to show main stays responsive.
    constexpr int kProxyHolders = static_cast<int>(kProxyTarget);
    std::vector<std::unique_ptr<boost::asio::io_context>> ios;
    std::vector<std::unique_ptr<boost::asio::local::stream_protocol::socket>> socks;
    ios.reserve(kProxyHolders);
    socks.reserve(kProxyHolders);

    for (int i = 0; i < kProxyHolders; ++i) {
        auto io = std::make_unique<boost::asio::io_context>();
        auto sock = std::make_unique<boost::asio::local::stream_protocol::socket>(*io);
        boost::system::error_code ec;
        sock->connect(boost::asio::local::stream_protocol::endpoint(proxyPath.string()), ec);
        if (ec) {
            // Proxy cap reached or acceptor transiently unavailable.
            break;
        }
        socks.push_back(std::move(sock));
        ios.push_back(std::move(io));
    }

    // Ensure the daemon has actually accepted the proxy connections (not just queued in backlog).
    REQUIRE(waitForProxyAccepted(socks.size()));

    // Under the old shared-slot implementation, exhausting proxy connections could prevent new
    // *main* connections from being accepted, breaking status/shutdown commands.
    auto mainClient2 = createClient(harness.socketPath());
    REQUIRE(connectWithRetry(mainClient2));

    auto r = yams::cli::run_sync(mainClient2.status(), 2s);
    REQUIRE(r.has_value());

    auto shutdown = yams::cli::run_sync(mainClient2.shutdown(true), 5s);
    REQUIRE(shutdown.has_value());
}
