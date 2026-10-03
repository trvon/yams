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

#include <yams/cli/doctor/checks/dim_consistency.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/daemon.h>

#include "../../common/test_helpers_catch2.h"

namespace yams::cli {
std::string formatLaterCommand(const std::string& command);
std::string formatInitSummary(const std::filesystem::path& configPath,
                              const std::filesystem::path& dataPath);
size_t resolveModelDimension(std::string_view modelName);
} // namespace yams::cli

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

// Pins the vector-store environment for one case. CI runs the unit lanes with vectors
// disabled and the vector DB forced in memory; init cases that assert on the on-disk store
// and its dimension sentinel must not inherit that, and the in-memory case must not depend
// on it either.
struct VectorStoreEnv {
    yams::test::ScopedEnvVar disable{"YAMS_DISABLE_VECTORS", std::nullopt};
    yams::test::ScopedEnvVar disableSingular{"YAMS_DISABLE_VECTOR", std::nullopt};
    yams::test::ScopedEnvVar disableDb{"YAMS_DISABLE_VECTOR_DB", std::nullopt};
    yams::test::ScopedEnvVar skipVecInit{"YAMS_SQLITE_VEC_SKIP_INIT", std::nullopt};
    yams::test::ScopedEnvVar inMemory;

    static VectorStoreEnv onDisk() { return VectorStoreEnv{std::nullopt}; }
    static VectorStoreEnv forcedInMemory() { return VectorStoreEnv{std::string("1")}; }

private:
    explicit VectorStoreEnv(std::optional<std::string> inMemoryValue)
        : inMemory("YAMS_VDB_IN_MEMORY", std::move(inMemoryValue)) {}
};

// Redirects std::cin from a scripted string for the duration of the scope.
class ScopedStdin {
public:
    explicit ScopedStdin(const std::string& input)
        : stream_(input), old_(std::cin.rdbuf(stream_.rdbuf())) {}
    ~ScopedStdin() { std::cin.rdbuf(old_); }

    ScopedStdin(const ScopedStdin&) = delete;
    ScopedStdin& operator=(const ScopedStdin&) = delete;

private:
    std::istringstream stream_;
    std::streambuf* old_;
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

TEST_CASE("InitCommand - post-setup guidance is formatted per branch", "[cli][init][catch2]") {
    SECTION("later-command guidance") {
        const auto text = yams::cli::formatLaterCommand("yams model download all-MiniLM-L6-v2");
        CHECK(text.find("Later: yams model download all-MiniLM-L6-v2") != std::string::npos);
    }

    SECTION("ready summary guidance") {
        const auto text = yams::cli::formatInitSummary("/tmp/cfg.toml", "/tmp/data");
        CHECK(text.find("YAMS is ready") != std::string::npos);
        CHECK(text.find("Config") != std::string::npos);
        CHECK(text.find("/tmp/cfg.toml") != std::string::npos);
        CHECK(text.find("Data") != std::string::npos);
        CHECK(text.find("/tmp/data") != std::string::npos);
        CHECK(text.find("Next") != std::string::npos);
        CHECK(text.find("yams list | yams doctor") != std::string::npos);
    }
}

TEST_CASE("InitCommand: resolveModelDimension maps model names to expected dimensions",
          "[cli][init][models]") {
    CHECK(yams::cli::resolveModelDimension("simeon-default") == 1024);
    CHECK(yams::cli::resolveModelDimension("simeon") == 1024);
    CHECK(yams::cli::resolveModelDimension("") == 1024);
    CHECK(yams::cli::resolveModelDimension("all-MiniLM-L6-v2") == 384);
    CHECK(yams::cli::resolveModelDimension("multi-qa-MiniLM-L6-cos-v1") == 384);
    CHECK(yams::cli::resolveModelDimension("mxbai-edge-colbert-v0-17m") == 48);
    CHECK(yams::cli::resolveModelDimension("embeddinggemma-300m") == 768);
}

TEST_CASE("ConfigMigrator: Default config has consistent 1024 dimensions across all sections",
          "[config][migration][catch2]") {
    const auto defaults = yams::config::ConfigMigrator::getLatestConfigDefaults();

    REQUIRE(defaults.find("embeddings") != defaults.end());
    REQUIRE(defaults.find("vector_database") != defaults.end());
    REQUIRE(defaults.find("vector_index") != defaults.end());

    CHECK(defaults.at("embeddings").at("embedding_dim") == "1024");
    CHECK(defaults.at("embeddings").at("backend") == "simeon");
    CHECK(defaults.at("embeddings").at("preferred_model") == "simeon-default");

    CHECK(defaults.at("vector_database").at("embedding_dim") == "1024");
    CHECK(defaults.at("vector_index").at("dimension") == "1024");
}

TEST_CASE("ConfigMigrator: createDefaultLatestConfig generates valid zero-warning config",
          "[config][migration][catch2]") {
    CliTestHelper helper;
    auto migrator = std::make_unique<yams::config::ConfigMigrator>();
    auto result = migrator->createDefaultLatestConfig(helper.configPath);
    REQUIRE(result.has_value());

    const auto dims = yams::config::read_dimension_config(helper.configPath);
    REQUIRE(dims.embeddings.has_value());
    REQUIRE(dims.vectorDb.has_value());
    REQUIRE(dims.index.has_value());
    CHECK(*dims.embeddings == 1024);
    CHECK(*dims.vectorDb == 1024);
    CHECK(*dims.index == 1024);

    yams::daemon::DaemonConfig dcfg;
    dcfg.configFilePath = helper.configPath;
    const auto resolved = yams::daemon::ConfigResolver::resolveEmbeddingConfig(dcfg, {});
    CHECK(resolved.dimension == std::optional<std::size_t>{1024U});
    CHECK(resolved.backend == "simeon");
    CHECK(resolved.preferredModel == "simeon-default");

    for (const auto& w : resolved.warnings) {
        INFO("Unexpected warning: " << w);
        CHECK(w.find("dimension conflict") == std::string::npos);
        CHECK(w.find("overrides compatibility key") == std::string::npos);
    }
}

TEST_CASE("InitCommand: Fresh instance setup generates conflict-free embedding config and sentinel",
          "[cli][init][catch2]") {
    const auto vectorEnv = VectorStoreEnv::onDisk();
    CliTestHelper helper;

    const int rc = helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen"});
    REQUIRE(rc == 0);

    // Verify config file was written
    REQUIRE(fs::exists(helper.configPath));

    // 1) Read dimension config using typed helper: all three must be 1024
    const auto dims = yams::config::read_dimension_config(helper.configPath);
    REQUIRE(dims.embeddings.has_value());
    REQUIRE(dims.vectorDb.has_value());
    REQUIRE(dims.index.has_value());
    CHECK(*dims.embeddings == 1024);
    CHECK(*dims.vectorDb == 1024);
    CHECK(*dims.index == 1024);

    // 2) Resolve embedding policy as a daemon/CLI component would on startup
    yams::daemon::DaemonConfig dcfg;
    dcfg.configFilePath = helper.configPath;
    const auto resolved =
        yams::daemon::ConfigResolver::resolveEmbeddingConfig(dcfg, helper.dataDir);

    CHECK(resolved.backend == "simeon");
    CHECK(resolved.preferredModel == "simeon-default");
    CHECK(resolved.isTrainingFree);
    REQUIRE(resolved.dimension.has_value());
    CHECK(*resolved.dimension == 1024);

    // CRITICAL: Ensure NO dimension conflict warnings were generated
    for (const auto& warning : resolved.warnings) {
        INFO("Unexpected warning: " << warning);
        CHECK(warning.find("dimension conflict") == std::string::npos);
        CHECK(warning.find("overrides compatibility key") == std::string::npos);
    }

    // 3) Verify vectors.db was created and vectors_sentinel.json was written with matching dim
    REQUIRE(fs::exists(helper.dataDir / "vectors.db"));
    const auto sentinelDim = yams::daemon::ConfigResolver::readVectorSentinelDim(helper.dataDir);
    REQUIRE(sentinelDim.has_value());
    CHECK(*sentinelDim == 1024);

    // 4) Verify Doctor consistency check reports clean state
    auto cli = std::make_unique<yams::cli::YamsCLI>();
    yams::cli::doctor::DoctorContext ctx(cli.get());
    yams::cli::doctor::DimConsistencyCheck check;
    const auto dimResult = check.execute(ctx, nullptr);
    CHECK_FALSE(dimResult.configInconsistent);
    CHECK_FALSE(dimResult.mismatch);
    CHECK(dimResult.targetDim == 1024);
}

TEST_CASE("ConfigMigrator: v2 migration keeps an existing 384 dimension across all keys",
          "[config][migration][catch2]") {
    CliTestHelper helper;
    // A v2 config that predates the current 1024 default: two keys are present and
    // agree on 384, while a sibling key is missing and would otherwise receive the
    // new 1024 default during migration.
    REQUIRE_FALSE(yams::test::write_file(helper.configPath, R"toml(
[version]
config_version = 2

[embeddings]
backend = "onnxruntime"
preferred_model = "all-MiniLM-L6-v2"
embedding_dim = 384

[vector_database]
embedding_dim = 384
)toml")
                      .empty());

    auto migrator = std::make_unique<yams::config::ConfigMigrator>();
    auto result = migrator->migrateToLatest(helper.configPath, false);
    REQUIRE(result.has_value());

    const auto dims = yams::config::read_dimension_config(helper.configPath);
    REQUIRE(dims.embeddings.has_value());
    REQUIRE(dims.vectorDb.has_value());
    REQUIRE(dims.index.has_value());
    CHECK(*dims.embeddings == 384);
    CHECK(*dims.vectorDb == 384);
    CHECK(*dims.index == 384);

    yams::daemon::DaemonConfig dcfg;
    dcfg.configFilePath = helper.configPath;
    const auto resolved = yams::daemon::ConfigResolver::resolveEmbeddingConfig(dcfg, {});
    REQUIRE(resolved.dimension.has_value());
    CHECK(*resolved.dimension == 384);
    for (const auto& warning : resolved.warnings) {
        INFO("Unexpected warning: " << warning);
        CHECK(warning.find("dimension conflict") == std::string::npos);
        CHECK(warning.find("overrides compatibility key") == std::string::npos);
    }
}

TEST_CASE("ConfigMigrator: v1 migration preserves the install's existing dimension",
          "[config][migration][catch2]") {
    CliTestHelper helper;
    // v1 configs have no [version] section and are treated as version 1.
    REQUIRE_FALSE(yams::test::write_file(helper.configPath, R"toml(
[core]
data_dir = "/tmp/yams-legacy-384"

[embeddings]
backend = "onnxruntime"
preferred_model = "all-MiniLM-L6-v2"
embedding_dim = 384

[vector_database]
embedding_dim = 384
)toml")
                      .empty());

    auto migrator = std::make_unique<yams::config::ConfigMigrator>();
    auto result = migrator->migrateToLatest(helper.configPath, false);
    REQUIRE(result.has_value());

    const auto dims = yams::config::read_dimension_config(helper.configPath);
    REQUIRE(dims.embeddings.has_value());
    REQUIRE(dims.vectorDb.has_value());
    REQUIRE(dims.index.has_value());
    CHECK(*dims.embeddings == 384);
    CHECK(*dims.vectorDb == 384);
    CHECK(*dims.index == 384);
}

TEST_CASE("InitCommand: re-init does not overwrite an existing vector dimension sentinel",
          "[cli][init][catch2]") {
    const auto vectorEnv = VectorStoreEnv::onDisk();
    CliTestHelper helper;
    REQUIRE(helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen"}) == 0);

    // Simulate an install whose stored dimension differs from the freshly resolved
    // default (for example an older 384-d corpus). A forced re-init must not clobber
    // that record with 1024, or `yams doctor` would stop seeing the real mismatch.
    yams::cli::vecutil::writeVectorSentinel(helper.dataDir, 384);

    REQUIRE(helper.runCommand({"yams", "init", "--force", "--non-interactive", "--no-keygen"}) ==
            0);

    const auto sentinelDim = yams::daemon::ConfigResolver::readVectorSentinelDim(helper.dataDir);
    REQUIRE(sentinelDim.has_value());
    CHECK(*sentinelDim == 384);
}

TEST_CASE("InitCommand: an in-memory vector store leaves no vectors.db and no sentinel",
          "[cli][init][catch2]") {
    // The sentinel records the dimension of the on-disk vector store. When the vector DB is
    // forced in memory nothing is written to disk, so init must not stamp a sentinel that a
    // later on-disk init would then treat as authoritative.
    const auto vectorEnv = VectorStoreEnv::forcedInMemory();
    CliTestHelper helper;

    REQUIRE(helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen"}) == 0);
    CHECK(fs::exists(helper.dataDir / "yams.db"));
    CHECK_FALSE(fs::exists(helper.dataDir / "vectors.db"));
    CHECK_FALSE(yams::daemon::ConfigResolver::readVectorSentinelDim(helper.dataDir).has_value());
}

TEST_CASE("InitCommand: already-initialized interactive init prompts for the tuning profile once",
          "[cli][init][catch2]") {
    CliTestHelper helper;
    // Create an initialized instance without prompts first.
    REQUIRE(helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen"}) == 0);

    // Re-run interactively with scripted answers: accept the default storage dir,
    // pick tuning profile 2, then decline GLiNER/reranker/skill so nothing is
    // downloaded. Before the fix the tuning prompt was shown twice (once in
    // execute() and again in handleAlreadyInitialized()).
    // Scripted stdin in prompt order: storage dir (accept default), tuning "2",
    // then "n" to decline GLiNER, reranker and agent-skill downloads. Each prompt
    // is answered explicitly so EOF never falls back to defaultYes=true.
    ScopedStdin stdinScript("\n2\nn\nn\nn\n");
    CaptureStdout capture;
    const int rc = helper.runCommand({"yams", "init", "--no-keygen"});
    const std::string output = capture.str();

    CHECK(rc == 0);
    size_t occurrences = 0;
    for (size_t pos = output.find("Select a tuning profile"); pos != std::string::npos;
         pos = output.find("Select a tuning profile", pos + 1)) {
        ++occurrences;
    }
    CHECK(occurrences == 1);
    CHECK(output.find("2. Efficient") != std::string::npos);
    CHECK(yams::config::parse_config_value(helper.configPath, "tuning", "profile") == "efficient");
}

TEST_CASE("InitCommand: declining semantic search disables both sections and writes no sentinel",
          "[cli][init][catch2]") {
    CliTestHelper helper;

    // Fresh interactive init, scripted in prompt order: storage dir (default),
    // tuning "2", semantic search "n", S3 "n", plugins "n", reranker "n",
    // agent skill "n". Answers are explicit so EOF cannot default to yes and
    // trigger a model download.
    ScopedStdin stdinScript("\n2\nn\nn\nn\nn\nn\n");
    CaptureStdout capture;
    const int rc = helper.runCommand({"yams", "init", "--no-keygen"});
    const std::string output = capture.str();
    CHECK(rc == 0);

    // The fresh path must still prompt for tuning exactly once.
    size_t occurrences = 0;
    for (size_t pos = output.find("Select a tuning profile"); pos != std::string::npos;
         pos = output.find("Select a tuning profile", pos + 1)) {
        ++occurrences;
    }
    CHECK(occurrences == 1);

    // Declining semantic search disables both sections and persists it.
    CHECK(yams::config::parse_config_value(helper.configPath, "vector_database", "enable") ==
          "false");
    CHECK(yams::config::parse_config_value(helper.configPath, "embeddings", "enable") == "false");

    // No vector store dimension sentinel should be written when disabled.
    CHECK_FALSE(fs::exists(helper.dataDir / "vectors_sentinel.json"));

    // Dimension keys stay mutually consistent even with semantic search off.
    const auto dims = yams::config::read_dimension_config(helper.configPath);
    if (dims.embeddings && dims.vectorDb && dims.index) {
        CHECK(*dims.embeddings == *dims.vectorDb);
        CHECK(*dims.embeddings == *dims.index);
    }
}
