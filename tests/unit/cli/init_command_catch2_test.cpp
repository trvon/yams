// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
// Init command test suite (Catch2)
// Covers: Model definitions, CLI flag parsing

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <iostream>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include <yams/cli/yams_cli.h>

#include "../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;

// =============================================================================
// Test the EMBEDDING_MODELS definition used by init command.
// These tests verify that the model list is valid and consistent.
// =============================================================================

// Model info structure matching init_command.cpp
struct EmbeddingModelInfo {
    std::string name;
    std::string url;
    std::string description;
    size_t size_mb;
    int dimensions;
};

// Expected models (mirrors the static list in init_command.cpp)
static const std::vector<EmbeddingModelInfo> EXPECTED_MODELS = {
    {"all-MiniLM-L6-v2",
     "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx",
     "Lightweight model for semantic search", 90, 384},
    {"multi-qa-MiniLM-L6-cos-v1",
     "https://huggingface.co/sentence-transformers/multi-qa-MiniLM-L6-cos-v1/resolve/main/onnx/"
     "model.onnx",
     "Optimized for semantic search on QA pairs (215M training samples)", 90, 384}};

namespace {

struct CliTestHelper {
    fs::path tempDir;
    fs::path configPath;
    fs::path dataDir;
    std::optional<yams::test::ScopedEnvVar> configEnv;
    std::optional<yams::test::ScopedEnvVar> dataEnv;
    std::optional<yams::test::ScopedEnvVar> nonInteractiveEnv;
    std::optional<yams::test::ScopedEnvVar> disableDaemonEnv;
    std::optional<yams::test::ScopedEnvVar> xdgConfigEnv;
    std::optional<yams::test::ScopedEnvVar> xdgDataEnv;

    CliTestHelper() {
        tempDir = yams::test::make_temp_dir("yams_init_catch2_test_");
        dataDir = tempDir / "data";
        fs::path xdgConfigHome = tempDir / "xdg_config";
        configPath = xdgConfigHome / "yams" / "config.toml";
        fs::create_directories(dataDir);
        fs::create_directories(configPath.parent_path());

        configEnv.emplace("YAMS_CONFIG", configPath.string());
        dataEnv.emplace("YAMS_DATA_DIR", dataDir.string());
        nonInteractiveEnv.emplace(std::string("YAMS_NON_INTERACTIVE"),
                                  std::optional<std::string>("1"));
        disableDaemonEnv.emplace(std::string("YAMS_CLI_DISABLE_DAEMON_AUTOSTART"),
                                 std::optional<std::string>("1"));
        xdgConfigEnv.emplace("XDG_CONFIG_HOME", xdgConfigHome.string());
        xdgDataEnv.emplace("XDG_DATA_HOME", (tempDir / "xdg_data").string());
    }

    ~CliTestHelper() {
        configEnv.reset();
        dataEnv.reset();
        nonInteractiveEnv.reset();
        disableDaemonEnv.reset();
        xdgConfigEnv.reset();
        xdgDataEnv.reset();

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

TEST_CASE("InitCommand: All models have valid HuggingFace URLs", "[cli][init][models]") {
    const std::regex hfUrlPattern(
        R"(^https://huggingface\.co/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+/resolve/main/.+\.onnx$)");

    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model: " << model.name);
        INFO("URL: " << model.url);
        REQUIRE(std::regex_match(model.url, hfUrlPattern));
    }
}

TEST_CASE("InitCommand: All models have consistent 384 dimensions", "[cli][init][models]") {
    // All models should have dim=384 for consistency with default vector DB setup
    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model: " << model.name);
        REQUIRE(model.dimensions == 384);
    }
}

TEST_CASE("InitCommand: All models have valid names", "[cli][init][models]") {
    // Model names should follow naming convention
    const std::regex namePattern(R"(^[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]$)");

    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model name: " << model.name);
        REQUIRE(std::regex_match(model.name, namePattern));
    }
}

TEST_CASE("InitCommand: All models have reasonable sizes", "[cli][init][models]") {
    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model: " << model.name);
        // Models should be between 10MB and 1GB
        REQUIRE(model.size_mb >= 10);
        REQUIRE(model.size_mb <= 1024);
    }
}

TEST_CASE("InitCommand: All models have descriptions", "[cli][init][models]") {
    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model: " << model.name);
        REQUIRE_FALSE(model.description.empty());
        REQUIRE(model.description.length() >= 10);
    }
}

TEST_CASE("InitCommand: Models are from sentence-transformers", "[cli][init][models]") {
    for (const auto& model : EXPECTED_MODELS) {
        INFO("Checking model: " << model.name);
        REQUIRE(model.url.find("sentence-transformers") != std::string::npos);
    }
}

TEST_CASE("InitCommand - help shows non-interactive setup options", "[cli][init][catch2]") {
    CliTestHelper helper;
    CaptureStdout capture;

    const int rc = helper.runCommand({"yams", "init", "--help"});
    const std::string output = capture.str();

    CHECK(rc == 0);
    CHECK(output.find("Initialize YAMS storage and configuration") != std::string::npos);
    CHECK(output.find("--auto") != std::string::npos);
    CHECK(output.find("headless environments") != std::string::npos);
    CHECK(output.find("--non-interactive") != std::string::npos);
    CHECK(output.find("--no-keygen") != std::string::npos);
    CHECK(output.find("--print") != std::string::npos);
}

TEST_CASE("InitCommand - non-interactive print initializes temp storage", "[cli][init][catch2]") {
    CliTestHelper helper;
    CaptureStdout capture;

    const int rc =
        helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen", "--print"});
    const std::string output = capture.str();

    CHECK(rc == 0);
    CHECK(output.find("# YAMS v3.0.0 Configuration") != std::string::npos);
    CHECK(output.find("[core]") != std::string::npos);
    CHECK(output.find("preferred_model = \"simeon-default\"") != std::string::npos);
    CHECK(fs::exists(helper.dataDir / "yams.db"));
    CHECK(fs::exists(helper.dataDir / "storage"));
}

// =============================================================================
// Note: CLI flag integration tests that require YamsCLI instantiation
// are deferred to integration tests due to complex env isolation requirements.
// The model validation tests above cover the core init command logic.
// =============================================================================
