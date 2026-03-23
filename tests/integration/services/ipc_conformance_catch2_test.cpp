#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "../daemon/test_async_helpers.h"
#include "../daemon/test_daemon_harness.h"

#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>

#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;
using yams::daemon::ClientConfig;
using yams::daemon::DaemonClient;
using yams::daemon::Request;

namespace fs = std::filesystem;

namespace {

#ifdef _WIN32
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN()                                                          \
    SKIP("Daemon IPC tests unstable on Windows - see windows-daemon-ipc-plan.md")
#else
#define SKIP_ON_WINDOWS_DAEMON_SHUTDOWN() ((void)0)
#endif

class IpcConformanceFixture {
public:
    IpcConformanceFixture() = default;

    ~IpcConformanceFixture() {
        harness_.reset();
        yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(500));
        yams::daemon::GlobalIOContext::reset();
    }

    void start() {
        SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();
        if (!canBindUnixSocketHere()) {
            SKIP("Skipping IPC conformance: AF_UNIX bind unavailable in this environment.");
        }

        yams::test::DaemonHarnessOptions options;
        options.isolateState = true;
        harness_ = std::make_unique<yams::test::DaemonHarness>(options);
        REQUIRE(harness_->start(15s));
    }

    ClientConfig clientConfig(std::chrono::milliseconds requestTimeout = 2s) const {
        ClientConfig cc;
        cc.socketPath = harness_->socketPath();
        cc.autoStart = false;
        cc.requestTimeout = requestTimeout;
        return cc;
    }

    const fs::path& socketPath() const { return harness_->socketPath(); }
    const fs::path& storageDir() const { return harness_->dataDir(); }
    fs::path root() const { return harness_->dataDir().parent_path(); }

private:
    static bool canBindUnixSocketHere() {
        try {
            boost::asio::io_context io;
            boost::asio::local::stream_protocol::acceptor acc(io);
            auto path = fs::path("/tmp") /
                        (std::string("yams-bind-probe-") + std::to_string(::getpid()) + ".sock");
            std::error_code ec;
            fs::remove(path, ec);
            boost::system::error_code bec;
            acc.open(boost::asio::local::stream_protocol::endpoint(path.string()).protocol(), bec);
            if (bec) {
                return false;
            }
            acc.bind(boost::asio::local::stream_protocol::endpoint(path.string()), bec);
            acc.close();
            fs::remove(path, ec);
            return !bec;
        } catch (...) {
            return false;
        }
    }

    std::unique_ptr<yams::test::DaemonHarness> harness_;
};

} // namespace

TEST_CASE_METHOD(IpcConformanceFixture, "IpcConformance: cat and sessions roundtrip",
                 "[catch2][integration][services][ipc]") {
    start();

    DaemonClient client(clientConfig(10s));

    yams::daemon::AddDocumentRequest add{};
    add.name = "inline.txt";
    add.content = std::string("Hello, IPC!");
    add.tags = {"test"};
    auto addRes = yams::test_async::res(client.streamingAddDocument(add), 5s);
    REQUIRE(addRes);

    std::string addedHash = addRes.value().hash;
    REQUIRE_FALSE(addedHash.empty());

    yams::daemon::GetResponse got{};
    bool ready = false;
    for (int i = 0; i < 40 && !ready; ++i) {
        yams::daemon::GetRequest gh{};
        gh.hash = addedHash;
        gh.metadataOnly = false;
        auto r = yams::test_async::res(client.get(gh), 250ms);
        if (r) {
            got = r.value();
            ready = (!got.name.empty() && got.hasContent && !got.content.empty());
        }
        if (!ready) {
            std::this_thread::sleep_for(50ms);
        }
    }

    REQUIRE_FALSE(got.name.empty());
    REQUIRE(got.hasContent);
    CHECK(got.name == add.name);
    CHECK(got.content.find(add.content) != std::string::npos);

    auto pingRes = yams::test_async::res(client.ping(), 2s);
    REQUIRE(pingRes);

    yams::daemon::ListSessionsRequest lsr{};
    auto lsrRes = yams::test_async::res(client.executeRequest(Request{lsr}), 5s);
    REQUIRE(lsrRes);
    auto* ls = std::get_if<yams::daemon::ListSessionsResponse>(&lsrRes.value());
    REQUIRE(ls != nullptr);
}

TEST_CASE_METHOD(IpcConformanceFixture, "IpcConformance: second daemon startup is controlled",
                 "[catch2][integration][services][ipc]") {
    start();

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = storageDir();
    cfg.socketPath = socketPath();
    cfg.pidFile = root() / "daemon.pid";
    cfg.logFile = root() / "daemon-second.log";

    auto other = std::make_unique<yams::daemon::YamsDaemon>(cfg);
    auto res = other->start();
    if (res) {
        other->stop();
        SUCCEED("Second daemon performed controlled takeover/startup and stopped cleanly");
    } else {
        CHECK(res.error().code == yams::ErrorCode::InvalidState);
    }
}

TEST_CASE("IpcConformance: unreachable socket error shape",
          "[catch2][integration][services][ipc]") {
    SKIP_ON_WINDOWS_DAEMON_SHUTDOWN();

    ClientConfig cc;
    cc.socketPath = fs::path("/tmp") /
                    (std::string("yams-no-listener-") + std::to_string(::getpid()) + ".sock");
    cc.autoStart = false;
    cc.requestTimeout = 500ms;
    DaemonClient client(cc);

    auto res = yams::test_async::res(client.ping(), 1s);
    REQUIRE_FALSE(res);
}

TEST_CASE_METHOD(IpcConformanceFixture, "IpcConformance: stress tail ping loop",
                 "[catch2][integration][services][ipc]") {
    start();

    DaemonClient client(clientConfig(2s));
    auto stressIters = [] {
        if (const char* s = std::getenv("YAMS_STRESS_ITERS")) {
            int v = std::atoi(s);
            if (v > 0 && v < 100000) {
                return v;
            }
        }
        return 100;
    }();

    for (int i = 0; i < stressIters; ++i) {
        auto pr = yams::test_async::res(client.ping(), 1s);
        REQUIRE(pr);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}
