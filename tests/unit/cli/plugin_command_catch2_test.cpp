#include <catch2/catch_test_macros.hpp>

#include <yams/cli/yams_cli.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;

namespace {

struct CliTestHelper {
    fs::path tempDir;
    fs::path dataDir;
    fs::path configPath;
    std::optional<yams::test::ScopedEnvVar> configEnv;
    std::optional<yams::test::ScopedEnvVar> dataEnv;
    std::optional<yams::test::ScopedEnvVar> nonInteractiveEnv;
    std::optional<yams::test::ScopedEnvVar> disableDaemonEnv;

    CliTestHelper() {
        tempDir = yams::test::make_temp_dir("yams_plugin_catch2_test_");
        dataDir = tempDir / "data";
        configPath = tempDir / "config.toml";
        fs::create_directories(dataDir);

        configEnv.emplace("YAMS_CONFIG", configPath.string());
        dataEnv.emplace("YAMS_DATA_DIR", dataDir.string());
        nonInteractiveEnv.emplace(std::string("YAMS_NON_INTERACTIVE"),
                                  std::optional<std::string>("1"));
        disableDaemonEnv.emplace(std::string("YAMS_CLI_DISABLE_DAEMON_AUTOSTART"),
                                 std::optional<std::string>("1"));
    }

    ~CliTestHelper() {
        configEnv.reset();
        dataEnv.reset();
        nonInteractiveEnv.reset();
        disableDaemonEnv.reset();

        std::error_code ec;
        fs::remove_all(tempDir, ec);
    }

    int runCommand(const std::vector<std::string>& args) {
        auto cli = std::make_unique<yams::cli::YamsCLI>();
        std::vector<char*> argv;
        argv.reserve(args.size());
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        return cli->run(static_cast<int>(argv.size()), argv.data());
    }
};

class CaptureStdout {
public:
    CaptureStdout() : oldCout_(std::cout.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStdout() { std::cout.rdbuf(oldCout_); }

    std::string str() const { return buffer_.str(); }

    CaptureStdout(const CaptureStdout&) = delete;
    CaptureStdout& operator=(const CaptureStdout&) = delete;

private:
    std::ostringstream buffer_;
    std::streambuf* oldCout_;
};

} // namespace

TEST_CASE("PluginCommand - trust status reports defaults cleanly", "[cli][plugin][catch2]") {
    CliTestHelper helper;
    CaptureStdout capture;

    const int rc = helper.runCommand({"yams", "plugin", "trust", "status"});
    const std::string output = capture.str();

    CHECK(rc == 0);
    CHECK(output.find("Plugin trust/discovery status") != std::string::npos);
    CHECK(output.find("Trust file:") != std::string::npos);
    CHECK(output.find("Effective scan roots") != std::string::npos);
}

TEST_CASE("PluginCommand - list handles empty temp store without crashing",
          "[cli][plugin][catch2]") {
    CliTestHelper helper;
    CaptureStdout capture;

    const int rc = helper.runCommand({"yams", "plugin", "list"});
    const std::string output = capture.str();

    CHECK(rc == 0);
    CHECK(output.find("Loaded plugins (0):") != std::string::npos);
    CHECK(output.find("terminate") == std::string::npos);
}

TEST_CASE("PluginCommand - search reads local catalog file", "[cli][plugin][catch2]") {
    CliTestHelper helper;
    CaptureStdout capture;

    const fs::path catalogPath = helper.tempDir / "catalog.json";
    std::ofstream out(catalogPath);
    out << R"({"plugins":[)"
        << R"({"name":"onnx","transport":"native","repo":"https://example.invalid/onnx","interfaces":["model_provider_v1","onnx_request_v1"]},)"
        << R"({"name":"glint","transport":"native","interfaces":["entity_extractor_v2"]})"
        << R"(]})";
    out.close();

    const int rc = helper.runCommand({"yams", "plugin", "search", "--index", catalogPath.string()});
    const std::string output = capture.str();

    CHECK(rc == 0);
    CHECK(output.find("Plugins (2):") != std::string::npos);
    CHECK(output.find("onnx [native]") != std::string::npos);
    CHECK(output.find("entity_extractor_v2") != std::string::npos);
}
