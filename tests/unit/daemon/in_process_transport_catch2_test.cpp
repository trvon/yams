#if __has_include(<catch2/catch_test_macros.hpp>)
#include <catch2/catch_test_macros.hpp>
#elif __has_include("catch2/catch_test_macros.hpp")
#include "catch2/catch_test_macros.hpp"
#endif

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <vector>

#if __has_include(<boost/asio/co_spawn.hpp>)
#include <boost/asio/co_spawn.hpp>
#elif __has_include("boost/asio/co_spawn.hpp")
#include "boost/asio/co_spawn.hpp"
#endif
#if __has_include(<boost/asio/io_context.hpp>)
#include <boost/asio/io_context.hpp>
#elif __has_include("boost/asio/io_context.hpp")
#include "boost/asio/io_context.hpp"
#endif
#if __has_include(<boost/asio/use_future.hpp>)
#include <boost/asio/use_future.hpp>
#elif __has_include("boost/asio/use_future.hpp")
#include "boost/asio/use_future.hpp"
#endif

#if __has_include(<yams/daemon/client/in_process_transport.h>)
#include <yams/daemon/client/in_process_transport.h>
#elif __has_include("yams/daemon/client/in_process_transport.h")
#include "yams/daemon/client/in_process_transport.h"
#else
#include "../../../include/yams/daemon/client/in_process_transport.h"
#endif
#if __has_include(<yams/daemon/embedded_service_host.h>)
#include <yams/daemon/embedded_service_host.h>
#elif __has_include("yams/daemon/embedded_service_host.h")
#include "yams/daemon/embedded_service_host.h"
#else
#include "../../../include/yams/daemon/embedded_service_host.h"
#endif

#include "../../common/test_helpers_catch2.h"

#if defined(__has_feature)
#if __has_feature(undefined_behavior_sanitizer)
#define YAMS_UBSAN_ACTIVE 1
#endif
#endif
#if defined(__SANITIZE_UNDEFINED__)
#define YAMS_UBSAN_ACTIVE 1
#endif
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define YAMS_TSAN_ACTIVE 1
#endif
#endif
#if defined(__SANITIZE_THREAD__)
#define YAMS_TSAN_ACTIVE 1
#endif

namespace fs = std::filesystem;

namespace {

yams::Result<void> run_streaming(yams::daemon::InProcessTransport& transport,
                                 yams::daemon::Request request,
                                 yams::daemon::IClientTransport::HeaderCallback onHeader,
                                 yams::daemon::IClientTransport::ChunkCallback onChunk,
                                 yams::daemon::IClientTransport::ErrorCallback onError,
                                 yams::daemon::IClientTransport::CompleteCallback onComplete) {
    boost::asio::io_context io;
    auto headerCallback = std::move(onHeader);
    auto chunkCallback = std::move(onChunk);
    auto errorCallback = std::move(onError);
    auto completeCallback = std::move(onComplete);
    auto fut = boost::asio::co_spawn(
        io,
        transport.send_request_streaming(std::move(request), headerCallback, chunkCallback,
                                         errorCallback, completeCallback),
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
    REQUIRE((host.has_value()));
    return host.value();
}

} // namespace

TEST_CASE("InProcessTransport orders callbacks for stream-capable requests",
          "[daemon][streaming][inprocess]") {
#ifdef _WIN32
    SKIP("InProcessTransport embedded host tests are unstable on Windows");
#endif
#ifdef YAMS_UBSAN_ACTIVE
    SKIP("InProcessTransport embedded host teardown trips a known UBSan timer-service race");
#endif
#ifdef YAMS_TSAN_ACTIVE
    SKIP("TSAN-only crash in stream-capable in-process transport path");
#endif
    const auto dataDir = yams::test::make_temp_dir("yams_in_process_order_stream_");

    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));
    yams::test::ScopedEnvVar syncSingleFileAdd("YAMS_SYNC_SINGLE_FILE_ADD", std::string("1"));

    const auto inputPath = dataDir / "stream-order.txt";
    {
        std::ofstream out(inputPath);
        REQUIRE((out.is_open()));
        out << "streaming request body\n";
    }

    auto host = make_host_for_test(dataDir);
    yams::daemon::InProcessTransport transport(host);

    std::mutex eventsMutex;
    std::vector<std::string> events;
    const auto recordEvent = [&](std::string event) {
        std::lock_guard<std::mutex> lock(eventsMutex);
        events.push_back(std::move(event));
    };
    yams::daemon::EmbeddedServiceHost::TestingShutdownSnapshot beforeJoin{};
    bool sawBeforeJoinSnapshot = false;
    host->testing_setShutdownHook(
        [&](const yams::daemon::EmbeddedServiceHost::TestingShutdownSnapshot& snapshot) {
            if (snapshot.phase ==
                yams::daemon::EmbeddedServiceHost::TestingShutdownPhase::BeforeThreadJoin) {
                beforeJoin = snapshot;
                sawBeforeJoinSnapshot = true;
            }
        });
    yams::daemon::AddDocumentRequest addReq;
    addReq.path = inputPath.string();
    addReq.noEmbeddings = true;
    addReq.waitForProcessing = true;
    yams::daemon::Request req = addReq;

    auto result = run_streaming(
        transport, req, [&](const yams::daemon::Response&) { recordEvent("header"); },
        [&](const yams::daemon::Response&, bool) {
            recordEvent("chunk");
            return true;
        },
        [&](const yams::Error&) { recordEvent("error"); }, [&]() { recordEvent("complete"); });

    std::vector<std::string> recordedEvents;
    {
        std::lock_guard<std::mutex> lock(eventsMutex);
        recordedEvents = events;
    }

    REQUIRE((result.has_value()));
    REQUIRE_FALSE(recordedEvents.empty());
    REQUIRE((recordedEvents.front() == "header"));
    REQUIRE((recordedEvents.back() == "complete"));

    const auto completePos = std::find(recordedEvents.begin(), recordedEvents.end(), "complete");
    REQUIRE((completePos != recordedEvents.end()));
    REQUIRE((std::find(recordedEvents.begin(), completePos, "chunk") != completePos));
    REQUIRE(
        (std::find(recordedEvents.begin(), recordedEvents.end(), "error") == recordedEvents.end()));

    auto shutdownResult = host->shutdown();
    REQUIRE((shutdownResult.has_value()));

    REQUIRE((sawBeforeJoinSnapshot));
    CHECK((beforeJoin.ioContextAlive));
    CHECK((beforeJoin.lifecycleAlive));
    CHECK((beforeJoin.dispatcherAlive));
    CHECK((beforeJoin.serviceManagerAlive));
    CHECK((beforeJoin.ioThreadCount >= 1));

    std::error_code ec;
    fs::remove_all(dataDir, ec);
}

TEST_CASE("InProcessTransport orders callbacks for unary requests",
          "[daemon][streaming][inprocess]") {
#ifdef _WIN32
    SKIP("InProcessTransport embedded host tests are unstable on Windows");
#endif
#ifdef YAMS_UBSAN_ACTIVE
    SKIP("InProcessTransport embedded host teardown trips a known UBSan timer-service race");
#endif
    const auto dataDir = yams::test::make_temp_dir("yams_in_process_order_unary_");

    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));

    auto host = make_host_for_test(dataDir);
    yams::daemon::InProcessTransport transport(host);

    std::mutex eventsMutex;
    std::vector<std::string> events;
    const auto recordEvent = [&](std::string event) {
        std::lock_guard<std::mutex> lock(eventsMutex);
        events.push_back(std::move(event));
    };
    yams::daemon::Request req = yams::daemon::StatusRequest{};

    auto result = run_streaming(
        transport, req, [&](const yams::daemon::Response&) { recordEvent("header"); },
        [&](const yams::daemon::Response&, bool) {
            recordEvent("chunk");
            return true;
        },
        [&](const yams::Error&) { recordEvent("error"); }, [&]() { recordEvent("complete"); });

    std::vector<std::string> recordedEvents;
    {
        std::lock_guard<std::mutex> lock(eventsMutex);
        recordedEvents = events;
    }

    REQUIRE((result.has_value()));
    REQUIRE((recordedEvents.size() == 2));
    REQUIRE((recordedEvents[0] == "header"));
    REQUIRE((recordedEvents[1] == "complete"));

    auto shutdownResult = host->shutdown();
    REQUIRE((shutdownResult.has_value()));

    std::error_code ec;
    fs::remove_all(dataDir, ec);
}
