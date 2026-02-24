#include <catch2/catch_test_macros.hpp>

#include <filesystem>

#include <yams/daemon/client/sandbox_detection.h>

#include "../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;
using yams::daemon::ClientConfig;
using yams::daemon::ClientTransportMode;

TEST_CASE("resolve_transport_mode honors explicit client mode", "[daemon][sandbox]") {
    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("1"));

    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Socket;
    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::Socket);

    cfg.transportMode = ClientTransportMode::InProcess;
    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::InProcess);
}

TEST_CASE("resolve_transport_mode honors YAMS_EMBEDDED true/false", "[daemon][sandbox]") {
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
    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Auto;

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("auto"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::string("1"));

    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::Socket);
}

TEST_CASE("resolve_transport_mode auto falls back to in-process when socket missing",
          "[daemon][sandbox]") {
    ClientConfig cfg;
    cfg.transportMode = ClientTransportMode::Auto;
    cfg.socketPath = fs::temp_directory_path() / "yams-sandbox-test" / "missing" / "daemon.sock";

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("auto"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);

    CHECK(yams::daemon::resolve_transport_mode(cfg) == ClientTransportMode::InProcess);
}

TEST_CASE("resolve_transport_mode uses config daemon.mode when env unset", "[daemon][sandbox]") {
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
