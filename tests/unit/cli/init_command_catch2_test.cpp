// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
// Init command test suite (Catch2)
// Covers: Model definitions, CLI flag parsing

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <yams/cli/doctor/checks/dim_consistency.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/init_assets.hpp>
#include <yams/cli/static_tables.h>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/config/static_embedding_table.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/daemon.h>
#include <yams/vector/simeon_embedding_backend.h>

#include "../../common/test_helpers_catch2.h"

namespace yams::cli {
std::string formatLaterCommand(const std::string& command);
std::string formatInitSummary(const std::filesystem::path& configPath,
                              const std::filesystem::path& dataPath);
size_t resolveModelDimension(std::string_view modelName);
} // namespace yams::cli

namespace fs = std::filesystem;

namespace {

struct CliTestHelper {
    fs::path tempDir;
    fs::path configPath;
    fs::path dataDir;
    std::optional<yams::test::ScopedEnvVar> configEnv;
    std::optional<yams::test::ScopedEnvVar> compatibilityConfigEnv;
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
        compatibilityConfigEnv.emplace("YAMS_CONFIG_PATH", std::nullopt);
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
        compatibilityConfigEnv.reset();
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

TEST_CASE("InitCommand: offered model tables are well-formed", "[cli][init][models]") {
    namespace assets = yams::cli::init_assets;

    std::set<std::string> names; // unique within each table
    const auto checkCommon = [&names](const auto& model) {
        INFO("model: " << model.name);
        CHECK_FALSE(model.name.empty());
        CHECK_FALSE(model.description.empty());
        CHECK(model.size_mb > 0);
        CHECK(names.insert(model.name).second);
    };

    REQUIRE_FALSE(assets::embeddingModels().empty());
    for (const auto& model : assets::embeddingModels()) {
        checkCommon(model);
        CHECK(model.url.starts_with("https://"));
        CHECK(model.dimensions > 0);
    }

    names.clear();
    REQUIRE_FALSE(assets::glinerModels().empty());
    for (const auto& model : assets::glinerModels()) {
        checkCommon(model);
        CHECK_FALSE(model.repo.empty());
        CHECK_FALSE(model.model_file.empty());
    }

    names.clear();
    REQUIRE_FALSE(assets::rerankerModels().empty());
    for (const auto& model : assets::rerankerModels()) {
        checkCommon(model);
        CHECK_FALSE(model.repo.empty());
        CHECK_FALSE(model.model_file.empty());
        CHECK(model.max_tokens > 0);
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
    CHECK(output.find("--no-static-table") != std::string::npos);
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

namespace {

// Scratch data dir + config so Simeon dimension resolution does not see the developer's tables.
struct ScratchDataDir {
    std::filesystem::path dir;
    std::optional<yams::test::ScopedEnvVar> cfg, compat, data;
    ScratchDataDir() {
        dir = std::filesystem::temp_directory_path() /
              ("yams_init_dim_" + std::to_string(std::random_device{}()));
        std::filesystem::create_directories(dir);
        std::ofstream(dir / "config.toml") << "[core]\ndata_dir = \"" << dir.string() << "\"\n";
        cfg.emplace("YAMS_CONFIG", (dir / "config.toml").string());
        compat.emplace("YAMS_CONFIG_PATH", std::nullopt);
        data.emplace("YAMS_DATA_DIR", dir.string());
        yams::vector::setStaticEmbeddingDataDirHint(dir);
    }
    ~ScratchDataDir() {
        yams::vector::setStaticEmbeddingDataDirHint({});
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
};

// Minimal SMSTAT01 table, dim 4, two tokens.
std::string tinyStaticTableBytes() {
    std::string bytes = "SMSTAT01";
    auto u32 = [&](std::uint32_t v) {
        for (int k = 0; k < 4; ++k)
            bytes.push_back(static_cast<char>((v >> (8 * k)) & 0xFF));
    };
    u32(1);
    u32(4);
    u32(2);
    u32(3);
    u32(0);
    u32(100);
    for (const std::string t : {"[UNK]", "alpha"}) {
        bytes.push_back(static_cast<char>(t.size()));
        bytes.push_back(0);
        bytes += t;
    }
    u32(0x3F800000u);
    u32(0x3F800000u);
    bytes.append(8, '\x01');
    return bytes;
}

void writeFile(const fs::path& path, const std::string& bytes) {
    fs::create_directories(path.parent_path());
    std::ofstream(path, std::ios::binary | std::ios::trunc) << bytes;
}

std::string readFile(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

std::string fileUrl(const fs::path& path) {
    return "file://" + path.string();
}

// Every HTTPS download fails at once: curl honours these and nothing listens on port 1.
struct OfflineNetwork {
    yams::test::ScopedEnvVar https{"https_proxy", std::string{"http://127.0.0.1:1"}};
    yams::test::ScopedEnvVar httpsUpper{"HTTPS_PROXY", std::string{"http://127.0.0.1:1"}};
    yams::test::ScopedEnvVar http{"http_proxy", std::string{"http://127.0.0.1:1"}};
    yams::test::ScopedEnvVar all{"ALL_PROXY", std::string{"http://127.0.0.1:1"}};
    yams::test::ScopedEnvVar noProxy{"NO_PROXY", std::nullopt};
    yams::test::ScopedEnvVar noProxyLower{"no_proxy", std::nullopt};
};

// --auto installs plugins dirs and agent skills under $HOME and bootstraps a session for the
// cwd; keep all of it inside the test's temp dir.
struct IsolatedHome {
    yams::test::ScopedEnvVar home;
    yams::test::ScopedEnvVar userProfile;
    yams::test::ScopedEnvVar session{"YAMS_SESSION_CURRENT", std::string{"init-test"}};
    explicit IsolatedHome(const fs::path& dir)
        : home("HOME", dir.string()), userProfile("USERPROFILE", dir.string()) {
        fs::create_directories(dir);
    }
};

} // namespace

TEST_CASE("InitCommand: dimensions follow explicit selection rather than table presence",
          "[cli][init][models]") {
    ScratchDataDir scratch;
    std::filesystem::create_directories(scratch.dir / "models" / "static");
    const std::string bytes = tinyStaticTableBytes();
    std::ofstream(scratch.dir / "models" / "static" / "t.smstat", std::ios::binary) << bytes;

    CHECK(yams::cli::resolveModelDimension("simeon-default") == 1024);
    REQUIRE(yams::cli::selectStaticTable(scratch.dir / "models" / "static" / "t.smstat",
                                         scratch.dir / "config.toml"));
    CHECK(yams::cli::resolveModelDimension("simeon-default") == 4);
    CHECK(yams::cli::resolveModelDimension("") == 4);
    CHECK(yams::cli::resolveModelDimension("all-MiniLM-L6-v2") == 384);
}

TEST_CASE("InitCommand: resolveModelDimension maps model names to expected dimensions",
          "[cli][init][models]") {
    ScratchDataDir scratch;
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

TEST_CASE("InitCommand: initializing another data directory keeps the existing config",
          "[cli][init][catch2]") {
    // `yams --data-dir /tmp/scratch init --auto` replaced the user's config with a fresh
    // template pointing at the scratch store, and the daemon then served that store.
    const auto vectorEnv = VectorStoreEnv::onDisk();
    CliTestHelper helper;
    REQUIRE(helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen"}) == 0);
    {
        std::ofstream config(helper.configPath, std::ios::app);
        config << "\n# a setting the user added by hand\n";
    }
    const auto readConfig = [&] {
        std::ifstream in(helper.configPath);
        return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    };
    const auto before = readConfig();
    REQUIRE(before.find(helper.dataDir.string()) != std::string::npos);
    const auto scratch = helper.tempDir / "scratch-store";

    SECTION("without --force the config is left exactly as it was") {
        CaptureStdout capture;
        REQUIRE(helper.runCommand({"yams", "--data-dir", scratch.string(), "init",
                                   "--non-interactive", "--no-keygen"}) == 0);
        CHECK(readConfig() == before);
        // The new store is still set up, ready for --data-dir or YAMS_DATA_DIR.
        CHECK(fs::exists(scratch / "yams.db"));
        CHECK(capture.str().find("kept") != std::string::npos);
    }
    SECTION("--force replaces it") {
        REQUIRE(helper.runCommand({"yams", "--data-dir", scratch.string(), "init", "--force",
                                   "--non-interactive", "--no-keygen"}) == 0);
        CHECK(readConfig().find(scratch.string()) != std::string::npos);
    }
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

// --non-interactive is the scripted path (CI, test harnesses): it must not reach the network.
TEST_CASE("InitCommand: non-interactive init does not download the static table",
          "[cli][init][catch2]") {
    const auto vectorEnv = VectorStoreEnv::onDisk();
    CliTestHelper helper;
    CaptureStdout capture;
    REQUIRE(helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen"}) == 0);
    const auto staticDir = helper.dataDir / "models" / "static";
    std::error_code ec;
    CHECK_FALSE(fs::exists(staticDir / "yams-static-bge-c4-256.smstat", ec));
    CHECK(capture.str().find("yams model download yams-static-bge-c4-256") != std::string::npos);
}

TEST_CASE("Static embedding selection requires an explicit model opt-in",
          "[cli][init][static][static-opt-in]") {
    ScratchDataDir scratch;
    const auto installed = yams::cli::staticTablesDir(scratch.dir) / "unselected.smstat";
    fs::create_directories(installed.parent_path());
    {
        std::ofstream output(installed, std::ios::binary);
        output << tinyStaticTableBytes();
        REQUIRE(output.good());
    }

    SECTION("an installed file alone does not establish consent") {
        CHECK_FALSE(yams::config::findStaticEmbeddingTable(std::nullopt, scratch.dir).has_value());
    }
    SECTION("an explicit existing selection is honored") {
        const auto selected = yams::config::findStaticEmbeddingTable(installed, scratch.dir);
        REQUIRE(selected.has_value());
        CHECK(*selected == installed);
    }
    SECTION("a missing explicit selection does not activate a different installed table") {
        const auto missing = scratch.dir / "missing.smstat";
        CHECK_FALSE(yams::config::findStaticEmbeddingTable(missing, scratch.dir).has_value());
    }
}

TEST_CASE("Static table initialization requires affirmative interactive consent",
          "[cli][init][static][static-init-consent]") {
    for (const bool nonInteractive : {false, true}) {
        const auto automatic = yams::cli::staticTableInitDecision(true, nonInteractive);
        CHECK_FALSE(automatic.download);
        CHECK_FALSE(automatic.prompt);
    }
    const auto nonInteractive = yams::cli::staticTableInitDecision(false, true);
    CHECK_FALSE(nonInteractive.download);
    CHECK_FALSE(nonInteractive.prompt);
    const auto interactive = yams::cli::staticTableInitDecision(false, false);
    CHECK_FALSE(interactive.download);
    CHECK(interactive.prompt);
    CHECK_FALSE(interactive.defaultYes);
}

TEST_CASE("Explicit model download records the validated local table selection",
          "[cli][init][static][static-download-consent]") {
    CliTestHelper helper;
    REQUIRE(helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen"}) == 0);
    const auto installed = yams::cli::staticTablesDir(helper.dataDir) / "chosen.smstat";
    writeFile(installed, tinyStaticTableBytes());
    REQUIRE(yams::vector::loadSharedStaticEmbeddingTable(installed) != nullptr);
    // Both the existing-file fast path and the URL are local. No remote request
    // is needed to expose missing persistence after an explicit download.
    REQUIRE(helper.runCommand(
                {"yams", "model", "download", "chosen", "--url", fileUrl(installed)}) == 0);
    const auto selected =
        yams::config::parse_config_value(helper.configPath, "embeddings.simeon", "static_table");
    REQUIRE_FALSE(selected.empty());
    CHECK(fs::path(selected) == installed);
    CHECK(yams::config::findStaticEmbeddingTable(fs::path(selected), helper.dataDir) == installed);
}

TEST_CASE("Explicit model selection honors the existing compatibility config override",
          "[cli][init][static][static-download-consent]") {
    CliTestHelper helper;
    REQUIRE(helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen"}) == 0);
    const auto original = readFile(helper.configPath);
    const auto overridePath = helper.tempDir / "override.toml";
    writeFile(overridePath, original);
    const yams::test::ScopedEnvVar overrideConfig{"YAMS_CONFIG_PATH", overridePath.string()};
    const auto installed = yams::cli::staticTablesDir(helper.dataDir) / "chosen.smstat";
    writeFile(installed, tinyStaticTableBytes());
    REQUIRE(yams::vector::loadSharedStaticEmbeddingTable(installed) != nullptr);
    REQUIRE(helper.runCommand(
                {"yams", "model", "download", "chosen", "--url", fileUrl(installed)}) == 0);
    CHECK(yams::config::parse_config_value(overridePath, "embeddings.simeon", "static_table") ==
          installed.generic_string());
    CHECK(readFile(helper.configPath).compare(original) == 0);
}

TEST_CASE("Rejected selection never reports a validated local table as selected",
          "[cli][init][static][static-download-consent]") {
    CliTestHelper helper;
    REQUIRE(helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen"}) == 0);
    const auto installed = yams::cli::staticTablesDir(helper.dataDir) / "chosen.smstat";
    const auto tableBytes = tinyStaticTableBytes();
    writeFile(installed, tableBytes);
    REQUIRE(yams::vector::loadSharedStaticEmbeddingTable(installed) != nullptr);
    const std::string original =
        readFile(helper.configPath) + "\n[unrelated]\nvalue = \"\"\"first\nsecond\"\"\"\n";
    writeFile(helper.configPath, original);
    CaptureStdout capture;
    CHECK(helper.runCommand({"yams", "model", "download", "chosen", "--url", fileUrl(installed)}) !=
          0);
    CHECK(capture.str().find("Static table selected:") == std::string::npos);
    CHECK(readFile(helper.configPath) == original);
    CHECK(readFile(installed) == tableBytes);
}

TEST_CASE("Static selection preserves unrelated config representation and semantic values",
          "[cli][init][static][selection-persistence]") {
    ScratchDataDir scratch;
    const auto installed = scratch.dir / "selected.smstat";
    writeFile(installed, tinyStaticTableBytes());
    const auto configPath = scratch.dir / "config.toml";
    std::string original;
    SECTION("parent table with typed values") {
        original = "# keep this comment\n[embeddings]\nenable = false\n"
                   "backend = \"simeon\"\n[other]\ncount = 42\nlist = [1, 2]\n";
    }
    SECTION("nested table with a previous choice") {
        original = "[embeddings.simeon]\nstatic_table = \"missing\"\n"
                   "encoder_profile = \"configurable\"\n[other]\ncount = 42\n";
    }
    SECTION("root dotted keys") {
        original = "embeddings.simeon.encoder_profile = \"configurable\"\n"
                   "[other]\ncount = 42\n";
    }
    SECTION("no embeddings table") {
        original = "# keep this comment\n[other]\ncount = 42\n";
    }
    writeFile(configPath, original);
    auto expected = yams::config::parse_simple_toml(configPath);
    expected["embeddings.simeon.static_table"] = installed.generic_string();
    const auto permissions = fs::status(configPath).permissions();
    REQUIRE(yams::cli::selectStaticTable(installed, configPath));
    CHECK(yams::config::parse_simple_toml(configPath) == expected);
    CHECK(readFile(configPath).find("count = 42\n") != std::string::npos);
    CHECK(fs::status(configPath).permissions() == permissions);
    if (original.find("enable = false") != std::string::npos) {
        CHECK(readFile(configPath).find("enable = false\n") != std::string::npos);
        CHECK(readFile(configPath).find("list = [1, 2]\n") != std::string::npos);
    }
    for (const auto& entry : fs::directory_iterator(scratch.dir)) {
        CHECK_FALSE(entry.path().filename().string().starts_with(".yams-model-selection-"));
    }
}

TEST_CASE("Failed static selection leaves the config and validated table intact",
          "[cli][init][static][selection-persistence]") {
    ScratchDataDir scratch;
    const auto installed = scratch.dir / "selected.smstat";
    const auto tableBytes = tinyStaticTableBytes();
    writeFile(installed, tableBytes);
    const auto configPath = scratch.dir / "config.toml";
    SECTION("multiline config is rejected without a partial rewrite") {
        const std::string original = "[other]\nmessage = \"\"\"\n"
                                     "[embeddings.simeon]\nhello\n\"\"\"\n";
        writeFile(configPath, original);
        CHECK_FALSE(yams::cli::selectStaticTable(installed, configPath));
        CHECK(readFile(configPath) == original);
    }
    SECTION("inline embedding tables are not partially redefined") {
        const std::string original =
            "[embeddings]\nsimeon = { encoder_profile = \"configurable\" }\n";
        writeFile(configPath, original);
        CHECK_FALSE(yams::cli::selectStaticTable(installed, configPath));
        CHECK(readFile(configPath) == original);
    }
    SECTION("quoted embedding tables require a manual edit") {
        const std::string original =
            "[\"embeddings\".\"simeon\"]\nencoder_profile = \"configurable\"\n";
        writeFile(configPath, original);
        CHECK_FALSE(yams::cli::selectStaticTable(installed, configPath));
        CHECK(readFile(configPath) == original);
    }
    SECTION("quoted selection keys are not duplicated") {
        const std::string original = "[embeddings.simeon]\n\"static_table\" = \"missing\"\n";
        writeFile(configPath, original);
        CHECK_FALSE(yams::cli::selectStaticTable(installed, configPath));
        CHECK(readFile(configPath) == original);
    }
    SECTION("a directory is not overwritten as config") {
        const auto directory = scratch.dir / "config-directory";
        fs::create_directories(directory);
        writeFile(directory / "keep", "unchanged");
        CHECK_FALSE(yams::cli::selectStaticTable(installed, directory));
        CHECK(readFile(directory / "keep") == "unchanged");
    }
    SECTION("a non-directory config parent fails closed") {
        const auto parent = scratch.dir / "parent-file";
        writeFile(parent, "unchanged");
        CHECK_FALSE(yams::cli::selectStaticTable(installed, parent / "config.toml"));
        CHECK(readFile(parent) == "unchanged");
    }
    CHECK(readFile(installed) == tableBytes);
    for (const auto& entry : fs::directory_iterator(scratch.dir)) {
        CHECK_FALSE(entry.path().filename().string().starts_with(".yams-model-selection-"));
    }
}

TEST_CASE("Static selection creates a minimal explicit config and preserves operator symlinks",
          "[cli][init][static][selection-persistence]") {
    ScratchDataDir scratch;
    const auto installed = scratch.dir / "selected.smstat";
    writeFile(installed, tinyStaticTableBytes());
    SECTION("an absent config is created without enabling or switching a backend") {
        const auto destination = scratch.dir / "new" / "config.toml";
        REQUIRE(yams::cli::selectStaticTable(installed, destination));
        const auto values = yams::config::parse_simple_toml(destination);
        REQUIRE(values.size() == 1);
        CHECK(values.at("embeddings.simeon.static_table") == installed.generic_string());
    }
    SECTION("an existing config symlink is preserved") {
        const auto destination = scratch.dir / "config.toml";
        const auto alias = scratch.dir / "config-link";
        std::error_code error;
        fs::create_symlink(destination, alias, error);
        if (error) {
            SKIP("Platform cannot create config symlinks: " << error.message());
        }
        REQUIRE(yams::cli::selectStaticTable(installed, alias));
        CHECK(fs::is_symlink(alias));
        CHECK(yams::config::parse_config_value(destination, "embeddings.simeon", "static_table") ==
              installed.generic_string());
    }
    SECTION("a dangling operator symlink is not replaced") {
        const auto alias = scratch.dir / "dangling-config-link";
        std::error_code error;
        fs::create_symlink(scratch.dir / "missing-config.toml", alias, error);
        if (error) {
            SKIP("Platform cannot create config symlinks: " << error.message());
        }
        CHECK_FALSE(yams::cli::selectStaticTable(installed, alias));
        CHECK(fs::is_symlink(alias));
        CHECK_FALSE(fs::exists(scratch.dir / "missing-config.toml"));
    }
}

// ---- Download failures -------------------------------------------------------------------------

TEST_CASE("downloadStaticTable: a missing source leaves no table behind",
          "[cli][init][static][download]") {
    ScratchDataDir scratch;
    auto out = yams::cli::downloadStaticTable("t", fileUrl(scratch.dir / "nope.smstat"), "",
                                              scratch.dir, {}, false);
    REQUIRE_FALSE(out);
    CHECK(out.error().code == yams::ErrorCode::NetworkError);
    CHECK_FALSE(fs::exists(yams::cli::staticTablesDir(scratch.dir) / "t.smstat"));
    CHECK_FALSE(fs::exists(yams::cli::staticTablesDir(scratch.dir) / "t.smstat.part"));
}

TEST_CASE("downloadStaticTable: a checksum mismatch leaves no table behind",
          "[cli][init][static][download]") {
    ScratchDataDir scratch;
    const auto src = scratch.dir / "src" / "t.smstat";
    writeFile(src, tinyStaticTableBytes());
    auto out = yams::cli::downloadStaticTable("t", fileUrl(src), std::string(64, '0'), scratch.dir,
                                              {}, false);
    CHECK_FALSE(out);
    CHECK_FALSE(fs::exists(yams::cli::staticTablesDir(scratch.dir) / "t.smstat"));
}

TEST_CASE("downloadStaticTable: content that is not a table is rejected and removed",
          "[cli][init][static][download]") {
    ScratchDataDir scratch;
    const auto src = scratch.dir / "src" / "t.smstat";
    writeFile(src, "<html>rate limited</html>");
    auto out = yams::cli::downloadStaticTable("t", fileUrl(src), "", scratch.dir, {}, false);
    CHECK_FALSE(out);
    CHECK_FALSE(fs::exists(yams::cli::staticTablesDir(scratch.dir) / "t.smstat"));
}

// A truncated or corrupt file from an earlier failed download must not count as installed.
TEST_CASE("downloadStaticTable: replaces a corrupt existing table without --force",
          "[cli][init][static][download]") {
    ScratchDataDir scratch;
    const auto dest = yams::cli::staticTablesDir(scratch.dir) / "t.smstat";
    writeFile(dest, "SMSTAT01-truncated");
    const auto src = scratch.dir / "src" / "t.smstat";
    writeFile(src, tinyStaticTableBytes());
    auto out = yams::cli::downloadStaticTable("t", fileUrl(src), "", scratch.dir, {}, false);
    REQUIRE(out);
    CHECK(readFile(dest) == tinyStaticTableBytes());
    CHECK(yams::vector::loadSharedStaticEmbeddingTable(dest) != nullptr);
}

// --force with a bad source (wrong URL, an HTML error page) must not destroy a working table.
TEST_CASE("downloadStaticTable: a failed forced re-download keeps the working table",
          "[cli][init][static][download]") {
    ScratchDataDir scratch;
    const auto dest = yams::cli::staticTablesDir(scratch.dir) / "t.smstat";
    writeFile(dest, tinyStaticTableBytes());
    const auto bad = scratch.dir / "src" / "bad.smstat";
    writeFile(bad, "<html>not found</html>");

    SECTION("source is not a table") {
        auto out = yams::cli::downloadStaticTable("t", fileUrl(bad), "", scratch.dir, {}, true);
        CHECK_FALSE(out);
    }
    SECTION("source is missing") {
        auto out = yams::cli::downloadStaticTable("t", fileUrl(scratch.dir / "nope"), "",
                                                  scratch.dir, {}, true);
        CHECK_FALSE(out);
    }
    CHECK(readFile(dest) == tinyStaticTableBytes());
}

TEST_CASE("InitCommand: automatic setup stays offline on the built-in encoder",
          "[cli][init][static][download]") {
    const auto vectorEnv = VectorStoreEnv::onDisk();
    OfflineNetwork offline;
    CliTestHelper helper;
    IsolatedHome home(helper.tempDir / "home");
    CaptureStdout capture;
    // The pure policy regression was red before changing --auto. It is now safe
    // to exercise the command; retain the offline proxy guard as defense in depth.
    REQUIRE(helper.runCommand({"yams", "init", "--auto", "--no-keygen"}) == 0);
    CHECK(capture.str().find("Downloading static table") == std::string::npos);
    CHECK(yams::config::parse_config_value(helper.configPath, "embeddings.simeon", "static_table")
              .empty());
    CHECK_FALSE(fs::exists(helper.dataDir / "models" / "static" / "yams-static-bge-c4-256.smstat"));
    CHECK(yams::daemon::ConfigResolver::readVectorSentinelDim(helper.dataDir) ==
          std::optional<size_t>{1024U});
    CHECK(capture.str().find("yams model download yams-static-bge-c4-256") != std::string::npos);
}

TEST_CASE("InitCommand: a corrupt static table in the data dir does not count as installed",
          "[cli][init][static][download]") {
    const auto vectorEnv = VectorStoreEnv::onDisk();
    CliTestHelper helper;
    writeFile(helper.dataDir / "models" / "static" / "yams-static-bge-c4-256.smstat",
              "SMSTAT01-truncated");
    CaptureStdout capture;
    REQUIRE(helper.runCommand({"yams", "init", "--non-interactive", "--no-keygen"}) == 0);
    CHECK(capture.str().find("yams model download yams-static-bge-c4-256") != std::string::npos);
    CHECK(yams::daemon::ConfigResolver::readVectorSentinelDim(helper.dataDir) ==
          std::optional<size_t>{1024U});
}

// Headless setup stays offline. Static, GLiNER and reranker models all require
// an explicit choice; init does not write configuration for an absent model.
TEST_CASE("InitCommand: --auto installs no ONNX models and writes no config for them",
          "[cli][init][auto][download]") {
    const auto vectorEnv = VectorStoreEnv::onDisk();
    OfflineNetwork offline;
    CliTestHelper helper;
    IsolatedHome home(helper.tempDir / "home");
    CaptureStdout capture;
    REQUIRE(helper.runCommand({"yams", "init", "--auto", "--no-keygen", "--no-static-table"}) == 0);
    const std::string config = readFile(helper.configPath);
    CHECK(config.find("reranker_backend = \"onnx\"") == std::string::npos);
    CHECK(config.find("[plugins.glint]") == std::string::npos);
    CHECK_FALSE(fs::exists(helper.dataDir / "models" / "gliner"));
    CHECK_FALSE(fs::exists(helper.dataDir / "models" / "reranker"));
    const std::string out = capture.str();
    CHECK(out.find("yams model download --gliner") != std::string::npos);
}
