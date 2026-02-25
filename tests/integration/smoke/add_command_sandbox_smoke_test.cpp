#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "common/test_helpers.h"

#include <yams/cli/yams_cli.h>

namespace fs = std::filesystem;

namespace {

class CaptureStdout {
public:
    CaptureStdout() : old_(std::cout.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStdout() { std::cout.rdbuf(old_); }

    std::string str() const { return buffer_.str(); }

private:
    std::ostringstream buffer_;
    std::streambuf* old_{nullptr};
};

int run_cli(const std::vector<std::string>& args, std::string* output = nullptr,
            std::optional<std::string> stdinData = std::nullopt) {
    yams::cli::YamsCLI cli;
    std::vector<char*> argv;
    argv.reserve(args.size());
    for (const auto& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }

    CaptureStdout capture;

    std::istringstream in;
    std::streambuf* oldIn = nullptr;
    if (stdinData.has_value()) {
        in.str(*stdinData);
        oldIn = std::cin.rdbuf(in.rdbuf());
    }

    const int rc = cli.run(static_cast<int>(argv.size()), argv.data());

    if (oldIn) {
        std::cin.rdbuf(oldIn);
    }
    if (output) {
        *output = capture.str();
    }
    return rc;
}

nlohmann::json parse_json_output(const std::string& raw) {
    const auto pos = raw.find('{');
    if (pos == std::string::npos) {
        return nlohmann::json{};
    }
    return nlohmann::json::parse(raw.substr(pos));
}

} // namespace

TEST(IntegrationSmoke, AddCommandSandboxOperations) {
    const fs::path root = yams::test::make_temp_dir("yams_add_sandbox_");
    const fs::path dataDir = root / "data";
    const fs::path inputDir = root / "input";
    fs::create_directories(dataDir);
    fs::create_directories(inputDir / "sub");

    const fs::path fileA = inputDir / "alpha.txt";
    const fs::path fileB = inputDir / "sub" / "beta.md";
    const fs::path fileSkip = inputDir / "sub" / "skip.log";
    const fs::path fileM1 = inputDir / "multi1.txt";
    const fs::path fileM2 = inputDir / "multi2.txt";

    yams::test::write_file(fileA, "alpha sandbox add content\n");
    yams::test::write_file(fileB, "beta sandbox add content\n");
    yams::test::write_file(fileSkip, "skip me\n");
    yams::test::write_file(fileM1, "multi file one\n");
    yams::test::write_file(fileM2, "multi file two\n");

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("1"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));
    yams::test::ScopedEnvVar disableAutoStart("YAMS_CLI_DISABLE_DAEMON_AUTOSTART",
                                              std::string("1"));

    std::string out;

    int rc = run_cli({"yams", "--json", "add", fileA.string(), "--name", "single-alpha.txt",
                      "--tags", "sandbox,single", "--metadata", "source=test"},
                     &out);
    ASSERT_EQ(rc, 0) << out;
    auto j = parse_json_output(out);
    ASSERT_TRUE(j.is_object()) << out;
    ASSERT_TRUE(j.contains("summary"));
    EXPECT_EQ(j["summary"]["failed"].get<int>(), 0);

    rc = run_cli({"yams", "--json", "add", inputDir.string(), "--recursive", "--include",
                  "*.txt,*.md", "--exclude", "*.log", "--tags", "sandbox,dir"},
                 &out);
    ASSERT_EQ(rc, 0) << out;
    j = parse_json_output(out);
    ASSERT_TRUE(j.is_object()) << out;
    ASSERT_TRUE(j.contains("summary"));
    EXPECT_EQ(j["summary"]["failed"].get<int>(), 0);
    EXPECT_TRUE(j.contains("results"));
    EXPECT_FALSE(j["results"].empty());

    rc = run_cli(
        {"yams", "--json", "add", "-", "--name", "stdin-sandbox.txt", "--tags", "sandbox,stdin"},
        &out, std::string("stdin payload for sandbox add\n"));
    ASSERT_EQ(rc, 0) << out;
    j = parse_json_output(out);
    ASSERT_TRUE(j.is_object()) << out;
    ASSERT_TRUE(j.contains("summary"));
    EXPECT_EQ(j["summary"]["failed"].get<int>(), 0);

    rc = run_cli(
        {"yams", "--json", "add", fileM1.string(), fileM2.string(), "--tags", "sandbox,multi"},
        &out);
    ASSERT_EQ(rc, 0) << out;
    j = parse_json_output(out);
    ASSERT_TRUE(j.is_object()) << out;
    ASSERT_TRUE(j.contains("results"));
    EXPECT_GE(static_cast<int>(j["results"].size()), 2);
    EXPECT_EQ(j["summary"]["failed"].get<int>(), 0);

    rc = run_cli({"yams", "--json", "list", "--limit", "200"}, &out);
    ASSERT_EQ(rc, 0) << out;
    j = parse_json_output(out);
    ASSERT_TRUE(j.is_array() || (j.is_object() && j.contains("items")) ||
                (j.is_object() && j.contains("documents")))
        << out;

    std::error_code ec;
    fs::remove_all(root, ec);
}
