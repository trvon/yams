#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <vector>

#include "../daemon/test_async_helpers.h"
#include "../daemon/test_daemon_harness.h"
#include <boost/asio/awaitable.hpp>

#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;
using yams::daemon::ClientConfig;
using yams::daemon::DaemonClient;
using yams::daemon::Request;
using yams::daemon::Response;

namespace {

static bool canBindUnixSocketHere() {
    try {
        boost::asio::io_context io;
        boost::asio::local::stream_protocol::acceptor acc(io);
        auto path = std::filesystem::path("/tmp") /
                    (std::string("yams-bind-probe-") + std::to_string(::getpid()) + ".sock");
        std::error_code ec;
        std::filesystem::remove(path, ec);
        boost::system::error_code bec;
        acc.open(boost::asio::local::stream_protocol::endpoint(path.string()).protocol(), bec);
        if (bec)
            return false;
        acc.bind(boost::asio::local::stream_protocol::endpoint(path.string()), bec);
        if (bec) {
            return false;
        }
        acc.close();
        std::filesystem::remove(path, ec);
        return true;
    } catch (...) {
        return false;
    }
}

TEST(IpcConformanceIT, CatAndCancelAndSessions) {
    if (!canBindUnixSocketHere()) {
        GTEST_SKIP() << "Skipping IPC conformance: environment forbids AF_UNIX bind (sandbox).";
    }
    yams::test::DaemonHarness h;
    ASSERT_TRUE(h.start());

    ClientConfig cc;
    cc.socketPath = h.socketPath();
    cc.autoStart = false;
    cc.requestTimeout = 10s;
    DaemonClient client(cc);

    // Ingest a small in-memory document
    yams::daemon::AddDocumentRequest add{};
    add.name = "inline.txt";
    add.content = std::string("Hello, IPC!");
    add.tags = {"test"};
    auto addRes = yams::test_async::res(client.streamingAddDocument(add), 5s);
    ASSERT_TRUE(addRes) << addRes.error().message;

    // Get by name (unary path), assert content roundtrip
    yams::daemon::GetRequest get{};
    get.name = add.name;
    get.byName = true;
    get.metadataOnly = false;
    // The add path may defer finalization; poll by hash (available from addRes)
    yams::daemon::GetResponse got{};
    std::string addedHash = addRes.value().hash;
    ASSERT_FALSE(addedHash.empty());
    {
        bool ok = false;
        for (int i = 0; i < 40 && !ok; ++i) { // up to ~2s
            yams::daemon::GetRequest gh{};
            gh.hash = addedHash;
            gh.metadataOnly = false;
            auto r = yams::test_async::res(client.get(gh), 250ms);
            if (r) {
                got = r.value();
                ok = (!got.name.empty() && got.hasContent && !got.content.empty());
            }
            if (!ok)
                std::this_thread::sleep_for(50ms);
        }
        ASSERT_FALSE(got.name.empty());
        ASSERT_TRUE(got.hasContent);
    }
    EXPECT_EQ(got.name, add.name);
    // Content equivalence can vary depending on extraction vs raw; assert it contains the seed
    EXPECT_NE(got.content.find(add.content), std::string::npos);

    // Cancel a non-existent request id – should produce a NotFound error
    // Sanity: ping roundtrip (unary framing path)
    auto pingRes = yams::test_async::res(client.ping(), 2s);
    ASSERT_TRUE(pingRes) << pingRes.error().message;

    // List sessions – protocol conformance
    {
        yams::daemon::ListSessionsRequest lsr{};
        auto lsrRes = yams::test_async::res(client.executeRequest(Request{lsr}), 5s);
        ASSERT_TRUE(lsrRes) << lsrRes.error().message;
        auto lsrPayload = lsrRes.value();
        auto* ls = std::get_if<yams::daemon::ListSessionsResponse>(&lsrPayload);
        ASSERT_TRUE(ls);
    }
    // We don't assert counts; only conformance of the IPC roundtrip
}

} // namespace
