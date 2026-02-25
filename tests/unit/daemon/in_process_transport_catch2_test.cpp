#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <filesystem>
#include <string>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <yams/daemon/client/in_process_transport.h>
#include <yams/daemon/embedded_service_host.h>

#include "../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;

namespace {

yams::Result<void> run_streaming(yams::daemon::InProcessTransport& transport,
                                 const yams::daemon::Request& request,
                                 yams::daemon::IClientTransport::HeaderCallback onHeader,
                                 yams::daemon::IClientTransport::ChunkCallback onChunk,
                                 yams::daemon::IClientTransport::ErrorCallback onError,
                                 yams::daemon::IClientTransport::CompleteCallback onComplete) {
    boost::asio::io_context io;
    auto fut = boost::asio::co_spawn(
        io,
        transport.send_request_streaming(request, std::move(onHeader), std::move(onChunk),
                                         std::move(onError), std::move(onComplete)),
        boost::asio::use_future);
    io.run();
    return fut.get();
}

std::shared_ptr<yams::daemon::EmbeddedServiceHost> make_host_for_test(const fs::path& dataDir) {
    yams::daemon::EmbeddedServiceHost::Options options;
    options.dataDir = dataDir;
    options.ioThreads = 1;
    options.enableAutoRepair = false;
    options.autoLoadPlugins = false;
    options.enableModelProvider = false;
    options.initTimeoutSeconds = 30;

    auto host = yams::daemon::EmbeddedServiceHost::getOrCreate(options);
    REQUIRE(host.has_value());
    return host.value();
}

} // namespace

TEST_CASE("InProcessTransport orders callbacks for stream-capable requests",
          "[daemon][streaming][inprocess]") {
    const auto dataDir = yams::test::make_temp_dir("yams_in_process_order_stream_");

    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    auto host = make_host_for_test(dataDir);
    yams::daemon::InProcessTransport transport(host);

    std::vector<std::string> events;
    yams::daemon::Request req = yams::daemon::ListRequest{};

    auto result = run_streaming(
        transport, req, [&](const yams::daemon::Response&) { events.push_back("header"); },
        [&](const yams::daemon::Response&, bool) {
            events.push_back("chunk");
            return true;
        },
        [&](const yams::Error&) { events.push_back("error"); },
        [&]() { events.push_back("complete"); });

    REQUIRE(result.has_value());
    REQUIRE_FALSE(events.empty());
    REQUIRE(events.front() == "header");
    REQUIRE(events.back() == "complete");

    const auto completePos = std::find(events.begin(), events.end(), "complete");
    REQUIRE(completePos != events.end());
    REQUIRE(std::find(events.begin(), completePos, "chunk") != completePos);
    REQUIRE(std::find(events.begin(), events.end(), "error") == events.end());

    auto shutdownResult = host->shutdown();
    REQUIRE(shutdownResult.has_value());

    std::error_code ec;
    fs::remove_all(dataDir, ec);
}

TEST_CASE("InProcessTransport orders callbacks for unary requests",
          "[daemon][streaming][inprocess]") {
    const auto dataDir = yams::test::make_temp_dir("yams_in_process_order_unary_");

    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    auto host = make_host_for_test(dataDir);
    yams::daemon::InProcessTransport transport(host);

    std::vector<std::string> events;
    yams::daemon::Request req = yams::daemon::StatusRequest{};

    auto result = run_streaming(
        transport, req, [&](const yams::daemon::Response&) { events.push_back("header"); },
        [&](const yams::daemon::Response&, bool) {
            events.push_back("chunk");
            return true;
        },
        [&](const yams::Error&) { events.push_back("error"); },
        [&]() { events.push_back("complete"); });

    REQUIRE(result.has_value());
    REQUIRE(events.size() == 2);
    REQUIRE(events[0] == "header");
    REQUIRE(events[1] == "complete");

    auto shutdownResult = host->shutdown();
    REQUIRE(shutdownResult.has_value());

    std::error_code ec;
    fs::remove_all(dataDir, ec);
}
