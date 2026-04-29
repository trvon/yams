#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>

#include <yams/daemon/client/sandbox_detection.h>
#include <yams/daemon/client/sandbox_probe.h>

#include "../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;
using yams::daemon::ClientConfig;
using yams::daemon::ClientTransportMode;

namespace {

class ProbeStub {
public:
    explicit ProbeStub(bool permitted) {
        yams::daemon::set_unix_socket_probe_for_tests([permitted]() { return permitted; });
    }
    ~ProbeStub() { yams::daemon::set_unix_socket_probe_for_tests(nullptr); }
    ProbeStub(const ProbeStub&) = delete;
    ProbeStub& operator=(const ProbeStub&) = delete;
};

} // namespace

TEST_CASE("resolve_transport_mode honors explicit client mode", "[daemon][sandbox]") {
    ProbeStub probe(false); // restricted; explicit override must still win
    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("1"));

    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Socket;
    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::Socket);

    cfg.transportMode = ClientTransportMode::InProcess;
    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::InProcess);
}

TEST_CASE("resolve_transport_mode honors YAMS_EMBEDDED true/false", "[daemon][sandbox]") {
    ProbeStub probe(false);

    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Auto;

    {
        yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("true"));
        CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::InProcess);
    }
    {
        yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("false"));
        CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::Socket);
    }
}

TEST_CASE("resolve_transport_mode auto honors daemon-process guard", "[daemon][sandbox]") {
    ProbeStub probe(false);

    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Auto;

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("auto"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::string("1"));

    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::Socket);
}

TEST_CASE("resolve_transport_mode probe-restricted routes to in-process",
          "[daemon][sandbox][probe]") {
    ProbeStub probe(false);

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar configEnv("YAMS_CONFIG",
                                       std::string("/nonexistent/yams-test-config.toml"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);

    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Auto;
    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::InProcess);
}

TEST_CASE("resolve_transport_mode probe-permitted defaults to socket", "[daemon][sandbox][probe]") {
    ProbeStub probe(true);

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar configEnv("YAMS_CONFIG",
                                       std::string("/nonexistent/yams-test-config.toml"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);

    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Auto;
    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::Socket);
}

TEST_CASE("resolve_transport_mode probe restriction does not override explicit socket",
          "[daemon][sandbox][probe]") {
    ProbeStub probe(false);

    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Auto;

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("false"));

    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::Socket);
}

TEST_CASE("resolve_transport_mode probe restriction honors daemon-process guard",
          "[daemon][sandbox][probe]") {
    ProbeStub probe(false);

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar configEnv("YAMS_CONFIG",
                                       std::string("/nonexistent/yams-test-config.toml"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::string("1"));

    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Auto;
    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::Socket);
}

TEST_CASE("resolve_transport_mode auto-probe falls back to in-process when socket missing",
          "[daemon][sandbox]") {
    ProbeStub probe(true);

    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Auto;
    cfg.socketPath = fs::temp_directory_path() / "yams-sandbox-test" / "missing" / "daemon.sock";

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("auto"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);

    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::InProcess);
}

TEST_CASE("resolve_transport_mode uses config daemon.mode when env unset", "[daemon][sandbox]") {
    ProbeStub probe(true);

    const auto tempDir = yams::test::make_temp_dir("yams_mode_cfg_");
    const auto cfgPath = tempDir / "config.toml";

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar configEnv("YAMS_CONFIG", cfgPath.string());

    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Auto;

    yams::test::write_file(cfgPath, "[daemon]\nmode = \"embedded\"\n");
    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::InProcess);

    yams::test::write_file(cfgPath, "[daemon]\nmode = \"socket\"\n");
    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::Socket);

    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

TEST_CASE("unix_socket_io_permitted: real probe completes within budget",
          "[daemon][sandbox][probe][profile]") {
    yams::daemon::set_unix_socket_probe_for_tests(nullptr);

    auto start = std::chrono::steady_clock::now();
    bool first = yams::daemon::unix_socket_io_permitted();
    auto firstElapsed = std::chrono::steady_clock::now() - start;

    auto firstNs = yams::daemon::last_unix_socket_probe_duration();

    auto cachedStart = std::chrono::steady_clock::now();
    bool second = yams::daemon::unix_socket_io_permitted();
    auto cachedElapsed = std::chrono::steady_clock::now() - cachedStart;

    CHECK(first == second);
    CHECK(std::chrono::duration_cast<std::chrono::milliseconds>(firstElapsed).count() < 100);
    CHECK(firstNs.count() < 100'000'000); // 100ms budget for the syscall path
    CHECK(std::chrono::duration_cast<std::chrono::microseconds>(cachedElapsed).count() < 50);
}

TEST_CASE("unix_socket_io_permitted: stub probe is honored and cached",
          "[daemon][sandbox][probe]") {
    {
        ProbeStub probe(false);
        CHECK(yams::daemon::unix_socket_io_permitted() == false);
        // Cached result on second call
        CHECK(yams::daemon::unix_socket_io_permitted() == false);
    }
    {
        ProbeStub probe(true);
        CHECK(yams::daemon::unix_socket_io_permitted() == true);
    }
}
