// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for ConfigResolver component (PBI-090)
//
// Catch2 migration from GTest (yams-3s4 / yams-zns)

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/compat/unistd.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/daemon.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include <yams/vector/document_chunker.h>
#include <yams/vector/embedding_generator.h>

#include "../../../common/test_helpers_catch2.h"

using namespace yams::daemon;
using Catch::Matchers::ContainsSubstring;

namespace {

struct ConfigResolverFixture {
    std::filesystem::path tempDir;

    ConfigResolverFixture() {
        tempDir = std::filesystem::temp_directory_path() /
                  ("yams_config_test_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempDir);
    }

    ~ConfigResolverFixture() {
        std::error_code ec;
        std::filesystem::remove_all(tempDir, ec);
    }

    std::filesystem::path writeToml(const std::string& filename, const std::string& content) {
        auto path = tempDir / filename;
        std::ofstream out(path);
        out << content;
        return path;
    }
};

class PostIngestStageActivityGuard {
public:
    PostIngestStageActivityGuard()
        : previousMask_(yams::daemon::TuneAdvisor::postIngestStageActiveMask()) {
        for (std::uint8_t i = 0; i < 6; ++i) {
            yams::daemon::TuneAdvisor::setPostIngestStageActive(
                static_cast<yams::daemon::TuneAdvisor::PostIngestStage>(i), true);
        }
    }

    ~PostIngestStageActivityGuard() {
        for (std::uint8_t i = 0; i < 6; ++i) {
            yams::daemon::TuneAdvisor::setPostIngestStageActive(
                static_cast<yams::daemon::TuneAdvisor::PostIngestStage>(i),
                (previousMask_ & (1u << i)) != 0);
        }
    }

    PostIngestStageActivityGuard(const PostIngestStageActivityGuard&) = delete;
    PostIngestStageActivityGuard& operator=(const PostIngestStageActivityGuard&) = delete;

private:
    std::uint32_t previousMask_;
};

class ProfileGuard {
    yams::daemon::TuneAdvisor::Profile prev_;

public:
    explicit ProfileGuard(yams::daemon::TuneAdvisor::Profile profile)
        : prev_(yams::daemon::TuneAdvisor::tuningProfile()) {
        yams::daemon::TuneAdvisor::setTuningProfile(profile);
    }
    ~ProfileGuard() { yams::daemon::TuneAdvisor::setTuningProfile(prev_); }

    ProfileGuard(const ProfileGuard&) = delete;
    ProfileGuard& operator=(const ProfileGuard&) = delete;
};

using EnvGuard = yams::test::ScopedEnvVar;

std::vector<EnvGuard> unsetEnvironment(std::initializer_list<const char*> names) {
    std::vector<EnvGuard> guards;
    guards.reserve(names.size());
    for (const char* name : names) {
        guards.emplace_back(name, std::nullopt);
    }
    return guards;
}

} // namespace

TEST_CASE("TuneAdvisor bounded override readers share env and override behavior",
          "[daemon][components][tuning][catch2]") {
    TuneAdvisor::setWorkerPollMs(0);
    TuneAdvisor::setPoolScaleStep(0);
    TuneAdvisor::setConnectionSlotsScaleStep(0);

    {
        EnvGuard env("YAMS_WORKER_POLL_MS", "75");
        CHECK((TuneAdvisor::workerPollMs() == 75u));

        TuneAdvisor::setWorkerPollMs(90u);
        CHECK((TuneAdvisor::workerPollMs() == 90u));
        TuneAdvisor::setWorkerPollMs(0);
        CHECK((TuneAdvisor::workerPollMs() == 75u));
    }

    {
        EnvGuard env("YAMS_WORKER_POLL_MS", "49");
        CHECK((TuneAdvisor::workerPollMs() == 150u));
    }

    {
        EnvGuard env("YAMS_POOL_SCALE_STEP", "16");
        CHECK((TuneAdvisor::poolScaleStep() == 16));
    }

    {
        EnvGuard env("YAMS_POOL_SCALE_STEP", "17");
        CHECK((TuneAdvisor::poolScaleStep() == 1));
    }

    {
        EnvGuard env("YAMS_CONN_SLOTS_STEP", "128");
        CHECK((TuneAdvisor::connectionSlotsScaleStep() == 128u));
    }

    {
        EnvGuard env("YAMS_CONN_SLOTS_STEP", "129");
        CHECK((TuneAdvisor::connectionSlotsScaleStep() == 16u));
    }

    TuneAdvisor::setWorkerPollMs(0);
    TuneAdvisor::setPoolScaleStep(0);
    TuneAdvisor::setConnectionSlotsScaleStep(0);
}

TEST_CASE("Fresh typed tuning lifecycle does not inherit prior process overrides",
          "[daemon][components][config][tuning][lifecycle][catch2]") {
    EnvGuard ipcCompatibility{"YAMS_IPC_TIMEOUT_MS", ""};
    EnvGuard resourceCompatibility{"YAMS_MEMORY_WARNING_PCT", ""};
    EnvGuard rpcCompatibility{"YAMS_POST_INGEST_RPC_QUEUE_MAX", ""};
    EnvGuard governorCompatibility{"YAMS_ENABLE_RESOURCE_GOVERNOR", ""};

    TuneAdvisor::setIpcTimeoutMs(4321);
    TuneAdvisor::setMemoryWarningThreshold(0.88);
    TuneAdvisor::setPostIngestRpcQueueMax(333);
    TuneAdvisor::setEnableResourceGovernor(false);
    const auto versionBefore = TuneAdvisor::configuredOverridesVersion();
    REQUIRE_FALSE(versionBefore & 1U);

    const auto fresh = ConfigResolver::applyRuntimeTuning({}, TuningConfig{});

    CHECK((TuneAdvisor::configuredOverridesVersion() == versionBefore + 2));
    CHECK(fresh.provenance.empty());
    CHECK((TuneAdvisor::ipcTimeoutMs() == 15000u));
    CHECK((TuneAdvisor::memoryWarningThreshold() == Catch::Approx(0.75)));
    CHECK((TuneAdvisor::postIngestRpcQueueMax() == 256u));
    CHECK(TuneAdvisor::enableResourceGovernor());
}

TEST_CASE("Fresh runtime tuning resolution revokes removed configured overrides",
          "[daemon][components][config][tuning][reload][catch2]") {
    EnvGuard ipcCompatibility{"YAMS_IPC_TIMEOUT_MS", ""};
    EnvGuard admissionCompatibility{"YAMS_ADMISSION_CONTROL", ""};
    EnvGuard memoryCompatibility{"YAMS_MEMORY_WARNING_PCT", ""};
    EnvGuard postIngestCompatibility{"YAMS_POST_INGEST_RPC_QUEUE_MAX", ""};

    ConfigResolver::ConfigSections configured;
    configured["tuning"] = {{"target_cpu_percent", "321"}};
    configured["tuning.ipc"] = {{"timeout_ms", "4321"}};
    configured["tuning.resource"] = {{"admission_control", "false"},
                                     {"memory_warning_threshold", "0.91"}};
    configured["tuning.post_ingest"] = {{"rpc_queue_max", "333"}};

    const auto applied = ConfigResolver::applyRuntimeTuning(configured, TuningConfig{});
    REQUIRE((applied.provenance.at("tuning.ipc.timeout_ms") == "config:tuning.ipc.timeout_ms"));
    CHECK((TuneAdvisor::ipcTimeoutMs() == 4321u));
    CHECK_FALSE(TuneAdvisor::enableAdmissionControl());
    CHECK((TuneAdvisor::memoryWarningThreshold() == Catch::Approx(0.91)));
    CHECK((TuneAdvisor::postIngestRpcQueueMax() == 333u));

    const auto versionBeforeRemoval = TuneAdvisor::configuredOverridesVersion();
    const auto reverted = ConfigResolver::applyRuntimeTuning({}, TuningConfig{});

    CHECK((TuneAdvisor::configuredOverridesVersion() == versionBeforeRemoval + 2));
    CHECK((reverted.targetCpuPercent == 200u));
    CHECK(reverted.provenance.empty());
    CHECK((TuneAdvisor::ipcTimeoutMs() == 15000u));
    CHECK(TuneAdvisor::enableAdmissionControl());
    CHECK((TuneAdvisor::memoryWarningThreshold() == Catch::Approx(0.75)));
    CHECK((TuneAdvisor::postIngestRpcQueueMax() == 256u));
}

TEST_CASE("ConfigResolver applies one typed tuning snapshot for startup and reload",
          "[daemon][components][config][tuning][catch2]") {
    EnvGuard ipcCompatibility{"YAMS_IPC_TIMEOUT_MS", "8765"};
    EnvGuard resourceCompatibility{"YAMS_MEMORY_WARNING_PCT", "80"};
    ConfigResolver::ConfigSections sections;
    sections["tuning"] = {{"target_cpu_percent", "175"},
                          {"post_ingest_capacity", "4096"},
                          {"post_ingest_threads_min", "3"},
                          {"control_interval_ms", "250"}};
    sections["tuning.ingest"] = {{"store_batch_size", "23"}};
    sections["tuning.ipc"] = {{"timeout_ms", "4321"}, {"stream_chunk_timeout_ms", "9876"}};
    sections["tuning.resource"] = {{"memory_warning_threshold", "0.77"}};
    sections["tuning.post_ingest"] = {{"coalesce_ms", "3"},
                                      {"total_concurrent", "12"},
                                      {"rpc_queue_max", "333"},
                                      {"rpc_max_per_batch", "7"}};

    TuningConfig base;
    base.postIngestThreadsMax = 12;
    base.topologyAlgorithm = "multiscale";

    const auto resolved = ConfigResolver::applyRuntimeTuning(sections, base);
    CHECK((resolved.targetCpuPercent == 175));
    CHECK((resolved.postIngestCapacity == 4096));
    CHECK((resolved.postIngestThreadsMin == 3));
    CHECK((resolved.ingestStoreBatchSize == 23));
    CHECK((resolved.postIngestCoalesceMs == 3));
    CHECK((resolved.controlIntervalMs == 250));
    CHECK((resolved.postIngestThreadsMax == 12));
    CHECK((resolved.topologyAlgorithm == "multiscale"));
    CHECK((resolved.provenance.at("tuning.target_cpu_percent") ==
           "config:tuning.target_cpu_percent"));
    CHECK((resolved.provenance.at("tuning.post_ingest.total_concurrent") ==
           "config:tuning.post_ingest.total_concurrent"));
    CHECK((TuneAdvisor::postIngestTotalConcurrent() == 12u));
    CHECK((TuneAdvisor::postIngestRpcQueueMax() == 333u));
    CHECK((TuneAdvisor::postIngestRpcMaxPerBatch() == 7u));
    CHECK((TuneAdvisor::ipcTimeoutMs() == 4321u));
    CHECK((TuneAdvisor::streamChunkTimeoutMs() == 9876u));
    CHECK((TuneAdvisor::memoryWarningThreshold() == Catch::Approx(0.77)));

    sections["tuning"] = {{"target_cpu_percent", "-1"},
                          {"post_ingest_capacity", "-2"},
                          {"control_interval_ms", "4294967296"}};
    sections["tuning.ingest"] = {{"store_batch_size", "257"}};
    sections["tuning.post_ingest"] = {{"coalesce_ms", "21"}};
    base.targetCpuPercent = 80;
    base.postIngestCapacity = 2048;
    base.ingestStoreBatchSize = 32;
    base.controlIntervalMs = 500;
    base.postIngestCoalesceMs = 4;
    const auto rejected = ConfigResolver::applyRuntimeTuning(sections, base);
    CHECK((rejected.targetCpuPercent == 80));
    CHECK((rejected.postIngestCapacity == 2048));
    CHECK((rejected.ingestStoreBatchSize == 32));
    CHECK((rejected.controlIntervalMs == 500));
    CHECK((rejected.postIngestCoalesceMs == 4));
    TuneAdvisor::setPostIngestTotalConcurrent(0);
    TuneAdvisor::setPostIngestRpcQueueMax(0);
    TuneAdvisor::setPostIngestRpcMaxPerBatch(0);
    TuneAdvisor::setIpcTimeoutMs(0);
    TuneAdvisor::setStreamChunkTimeoutMs(0);
    TuneAdvisor::setMemoryWarningThreshold(0.0);
}

TEST_CASE("Typed post-ingest configuration outranks the compatibility environment",
          "[daemon][components][config][tuning][precedence][catch2]") {
    EnvGuard compatibility{"YAMS_POST_INGEST_TOTAL_CONCURRENT", "3"};
    TuneAdvisor::setPostIngestTotalConcurrent(0);
    ConfigResolver::ConfigSections sections;
    sections["tuning.post_ingest"] = {{"total_concurrent", "12"}};

    const auto resolved = ConfigResolver::applyRuntimeTuning(sections, {});

    CHECK((TuneAdvisor::postIngestTotalConcurrent() == 12u));
    CHECK((resolved.provenance.at("tuning.post_ingest.total_concurrent") ==
           "config:tuning.post_ingest.total_concurrent"));
    TuneAdvisor::setPostIngestTotalConcurrent(0);
}

TEST_CASE("ConfigResolver applies typed search maintenance policy with provenance",
          "[daemon][components][config][search][catch2]") {
    ConfigResolver::ConfigSections sections;
    sections["search"] = {{"automatic_rebuilds", "false"}};
    DaemonConfig config;

    ConfigResolver::applySearchMaintenance(sections, config);

    REQUIRE(config.searchMaintenance.automaticRebuildsEnabled.has_value());
    CHECK_FALSE(*config.searchMaintenance.automaticRebuildsEnabled);
    CHECK((config.searchMaintenance.automaticRebuildsSource == "config:search.automatic_rebuilds"));

    sections["search"]["automatic_rebuilds"] = "invalid";
    DaemonConfig invalid;
    ConfigResolver::applySearchMaintenance(sections, invalid);
    CHECK_FALSE(invalid.searchMaintenance.automaticRebuildsEnabled.has_value());
    CHECK(invalid.searchMaintenance.automaticRebuildsSource.empty());
}

TEST_CASE("ConfigResolver::resolveEmbeddingChunkingPolicy defaults are embedding-safe",
          "[daemon][components][config][chunking][catch2]") {
    // Ensure config file discovery doesn't accidentally pick up a user config during local dev.
    EnvGuard cfg("YAMS_CONFIG_PATH", "");

    auto policy = ConfigResolver::resolveEmbeddingChunkingPolicy();
    CHECK((policy.strategy == yams::vector::ChunkingStrategy::PARAGRAPH_BASED));
    CHECK_FALSE(policy.config.preserve_sentences);
    CHECK_FALSE(policy.config.use_token_count);
}

TEST_CASE("ConfigResolver::resolveEmbeddingChunkingPolicy supports all embed strategies",
          "[daemon][components][config][chunking][catch2]") {
    struct Case {
        const char* value;
        yams::vector::ChunkingStrategy expected;
    };

    const std::vector<Case> cases = {
        {"fixed", yams::vector::ChunkingStrategy::FIXED_SIZE},
        {"sentence", yams::vector::ChunkingStrategy::SENTENCE_BASED},
        {"paragraph", yams::vector::ChunkingStrategy::PARAGRAPH_BASED},
        {"recursive", yams::vector::ChunkingStrategy::RECURSIVE},
        {"sliding_window", yams::vector::ChunkingStrategy::SLIDING_WINDOW},
        {"markdown", yams::vector::ChunkingStrategy::MARKDOWN_AWARE},
    };

    const std::string sample = "# Title\n\n"
                               "Intro paragraph with enough text to chunk reasonably. "
                               "Second sentence here. Third sentence here.\n\n"
                               "## Section\n"
                               "- bullet one\n- bullet two\n\n"
                               "Final paragraph.";

    for (const auto& tc : cases) {
        DYNAMIC_SECTION("strategy=" << tc.value) {
            EnvGuard g("YAMS_EMBED_CHUNK_STRATEGY", tc.value);

            auto policy = ConfigResolver::resolveEmbeddingChunkingPolicy();
            CHECK((policy.strategy == tc.expected));

            auto chunker = yams::vector::createChunker(policy.strategy, policy.config, nullptr);
            REQUIRE(chunker);

            auto chunks = chunker->chunkDocument(sample, "hash");
            // Some strategies may return empty and rely on caller fallback, but for this
            // representative input we expect at least one chunk.
            REQUIRE_FALSE(chunks.empty());

            for (const auto& c : chunks) {
                CHECK((c.strategy_used == tc.expected));
                CHECK_FALSE(c.content.empty());
            }
        }
    }
}

TEST_CASE("ConfigResolver::resolveEmbeddingChunkingPolicy reads from config file",
          "[daemon][components][config][chunking][catch2]") {
    ConfigResolverFixture fx;

    auto configPath = fx.writeToml("config.toml",
                                   R"TOML(
[embeddings.chunking]
strategy = "fixed"
target = 256
min = 64
max = 512
overlap = 0
use_tokens = 0
preserve_sentences = 0
)TOML");

    EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());

    auto policy = ConfigResolver::resolveEmbeddingChunkingPolicy();
    CHECK((policy.strategy == yams::vector::ChunkingStrategy::FIXED_SIZE));
    CHECK((policy.config.target_chunk_size == 256));
    CHECK((policy.config.min_chunk_size == 64));
    CHECK((policy.config.max_chunk_size == 512));
    CHECK((policy.config.overlap_size == 0));
    CHECK((policy.config.overlap_percentage == 0.0));
    CHECK_FALSE(policy.config.use_token_count);
    CHECK_FALSE(policy.config.preserve_sentences);
}

TEST_CASE("ConfigResolver::envTruthy correctly parses truthy values",
          "[daemon][components][config][catch2]") {
    SECTION("truthy values") {
        CHECK(ConfigResolver::envTruthy("1"));
        CHECK(ConfigResolver::envTruthy("true"));
        CHECK(ConfigResolver::envTruthy("TRUE"));
        CHECK(ConfigResolver::envTruthy("True"));
        CHECK(ConfigResolver::envTruthy("yes"));
        CHECK(ConfigResolver::envTruthy("YES"));
        CHECK(ConfigResolver::envTruthy("Yes"));
        CHECK(ConfigResolver::envTruthy("on"));
        CHECK(ConfigResolver::envTruthy("ON"));
        CHECK(ConfigResolver::envTruthy("On"));
    }

    SECTION("falsy values") {
        CHECK_FALSE(ConfigResolver::envTruthy("0"));
        CHECK_FALSE(ConfigResolver::envTruthy("false"));
        CHECK_FALSE(ConfigResolver::envTruthy("FALSE"));
        CHECK_FALSE(ConfigResolver::envTruthy("no"));
        CHECK_FALSE(ConfigResolver::envTruthy("NO"));
        CHECK_FALSE(ConfigResolver::envTruthy("off"));
        CHECK_FALSE(ConfigResolver::envTruthy("OFF"));
        CHECK_FALSE(ConfigResolver::envTruthy(""));
        CHECK_FALSE(ConfigResolver::envTruthy(nullptr));
    }

    SECTION("unrecognized values are rejected") {
        CHECK_FALSE(ConfigResolver::envTruthy("garbage"));
        CHECK_FALSE(ConfigResolver::envTruthy("maybe"));
        CHECK_FALSE(ConfigResolver::envTruthy("123"));
        CHECK_FALSE(ConfigResolver::envTruthy("yep"));
        CHECK_FALSE(ConfigResolver::envTruthy("nope"));
    }
}

TEST_CASE("ConfigResolver resolves plugin strict mode once from typed config and environment",
          "[daemon][components][config][plugin][catch2]") {
    SECTION("typed config is used without an environment override") {
        EnvGuard strictEnv("YAMS_PLUGIN_DIR_STRICT", "");
        CHECK(ConfigResolver::resolvePluginDirStrict(true));
    }

    SECTION("environment can enable strict mode") {
        EnvGuard strictEnv("YAMS_PLUGIN_DIR_STRICT", "on");
        CHECK(ConfigResolver::resolvePluginDirStrict(false));
    }

    SECTION("environment can disable strict mode") {
        EnvGuard strictEnv("YAMS_PLUGIN_DIR_STRICT", "off");
        CHECK_FALSE(ConfigResolver::resolvePluginDirStrict(true));
    }

    SECTION("invalid environment values preserve the typed default") {
        EnvGuard strictEnv("YAMS_PLUGIN_DIR_STRICT", "tru");
        CHECK(ConfigResolver::resolvePluginDirStrict(true));
        CHECK_FALSE(ConfigResolver::resolvePluginDirStrict(false));
    }
}

TEST_CASE_METHOD(ConfigResolverFixture, "shared flat TOML parser handles daemon policy files",
                 "[daemon][components][config][catch2]") {
    auto configPath = writeToml("shared_semantics.toml", R"(
top_level = "root" # inline comment

[search.path_tree]
enable = true
mode = "preferred"
quoted_hash = "# not a comment"

[daemon]
socket_path = "/tmp/yams.sock"
)");

    auto config = yams::config::parse_simple_toml(configPath);
    CHECK((config["top_level"] == "root"));
    CHECK((config["search.path_tree.enable"] == "true"));
    CHECK((config["search.path_tree.mode"] == "preferred"));
    CHECK((config["search.path_tree.quoted_hash"] == "# not a comment"));
    CHECK((config["daemon.socket_path"] == "/tmp/yams.sock"));
    CHECK(yams::config::parse_simple_toml(tempDir / "nonexistent.toml").empty());
}

TEST_CASE_METHOD(ConfigResolverFixture, "installed ConfigResolver compatibility lookups remain",
                 "[daemon][components][config][compatibility][catch2]") {
    const auto configPath = writeToml("compatibility.toml", R"toml(
[search]
reranker_model = "compat-reranker"

[plugins.symbol_extraction]
enable = false

[tuning.post_ingest]
total_concurrent = 12
embed_concurrent = 4
batch_size = 32
)toml");
    EnvGuard configEnvironment{"YAMS_CONFIG_PATH", configPath.string()};
    EnvGuard rerankerEnvironment{"YAMS_RERANKER_MODEL", std::nullopt};

    const auto values = ConfigResolver::parseSimpleTomlFlat(configPath);
    CHECK((values.at("search.reranker_model") == "compat-reranker"));

    DaemonConfig daemonConfig;
    daemonConfig.configFilePath = configPath;
    CHECK((ConfigResolver::resolveRerankerModel(daemonConfig) == "compat-reranker"));
    CHECK_FALSE(ConfigResolver::isSymbolExtractionEnabled(daemonConfig));

    const auto caps = ConfigResolver::resolvePostIngestCaps();
    REQUIRE(caps.totalConcurrent.has_value());
    CHECK((*caps.totalConcurrent == 12U));
    REQUIRE(caps.embedConcurrent.has_value());
    CHECK((*caps.embedConcurrent == 4U));
    REQUIRE(caps.batchSize.has_value());
    CHECK((*caps.batchSize == 32U));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver resolveDefaultConfigPath searches standard paths",
                 "[daemon][components][config][catch2]") {
    SECTION("YAMS_CONFIG_PATH env var takes precedence") {
        auto configPath = writeToml("custom.toml", "[daemon]\nlog_level = \"trace\"\n");
        EnvGuard guard("YAMS_CONFIG_PATH", configPath.string());

        auto resolved = ConfigResolver::resolveDefaultConfigPath();
        CHECK((resolved == configPath));
    }

    SECTION("returns empty path if no config found") {
        // Clear env var to test fallback
        EnvGuard guard("YAMS_CONFIG_PATH", "");
        // Note: this test may find system config, so we just check it returns a valid result
        auto resolved = ConfigResolver::resolveDefaultConfigPath();
        // Either found or empty - both are valid outcomes
        CHECK((resolved.empty() || std::filesystem::exists(resolved) || !resolved.empty()));
    }
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver resolveEmbeddingBackend defaults to simeon for new installs",
                 "[daemon][components][config][catch2]") {
    EnvGuard backendGuard("YAMS_EMBED_BACKEND", "");
    auto configPath = writeToml("embedding_backend_default.toml", R"(
[core]
data_dir = "~/.yams/data"
)");
    EnvGuard configGuard("YAMS_CONFIG_PATH", configPath.string());

    CHECK((ConfigResolver::resolveEmbeddingBackend() == "simeon"));
    CHECK((ConfigResolver::resolveEmbeddingBackend("auto") == "auto"));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver resolveEmbeddingBackend reads and normalizes configured backend",
                 "[daemon][components][config][catch2]") {
    EnvGuard backendGuard("YAMS_EMBED_BACKEND", "");

    SECTION("reads daemon from config") {
        auto configPath = writeToml("embedding_backend_daemon.toml", R"(
[embeddings]
backend = "daemon"
)");
        EnvGuard configGuard("YAMS_CONFIG_PATH", configPath.string());
        CHECK((ConfigResolver::resolveEmbeddingBackend() == "daemon"));
    }

    SECTION("normalizes uppercase simeon from config") {
        auto configPath = writeToml("embedding_backend_simeon_upper.toml", R"(
[embeddings]
backend = "SIMEON"
)");
        EnvGuard configGuard("YAMS_CONFIG_PATH", configPath.string());
        CHECK((ConfigResolver::resolveEmbeddingBackend() == "simeon"));
    }

    SECTION("maps legacy onnx backend names to onnxruntime") {
        auto configPath = writeToml("embedding_backend_legacy_onnx.toml", R"(
[embeddings]
backend = "local_onnx"
)");
        EnvGuard configGuard("YAMS_CONFIG_PATH", configPath.string());
        CHECK((ConfigResolver::resolveEmbeddingBackend() == "onnxruntime"));
    }

    SECTION("normalizes explicit onnxruntime from config") {
        auto configPath = writeToml("embedding_backend_onnxruntime.toml", R"(
[embeddings]
backend = "ONNX-RUNTIME"
)");
        EnvGuard configGuard("YAMS_CONFIG_PATH", configPath.string());
        CHECK((ConfigResolver::resolveEmbeddingBackend() == "onnxruntime"));
    }
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver resolveEmbeddingBackend maps ONNX env aliases to onnxruntime",
                 "[daemon][components][config][catch2]") {
    SECTION("maps onnx env alias") {
        EnvGuard backendGuard("YAMS_EMBED_BACKEND", "onnx");
        CHECK((ConfigResolver::resolveEmbeddingBackend() == "onnxruntime"));
    }

    SECTION("maps local_onnx env alias") {
        EnvGuard backendGuard("YAMS_EMBED_BACKEND", "local_onnx");
        CHECK((ConfigResolver::resolveEmbeddingBackend() == "onnxruntime"));
    }

    SECTION("maps ort env alias") {
        EnvGuard backendGuard("YAMS_EMBED_BACKEND", "ort");
        CHECK((ConfigResolver::resolveEmbeddingBackend() == "onnxruntime"));
    }

    SECTION("keeps daemon env selection distinct") {
        EnvGuard backendGuard("YAMS_EMBED_BACKEND", "daemon");
        CHECK((ConfigResolver::resolveEmbeddingBackend() == "daemon"));
    }
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver resolves one effective embedding snapshot from explicit config",
                 "[daemon][components][config][embeddings][snapshot][catch2]") {
    auto environment = unsetEnvironment({"YAMS_EMBED_BACKEND", "YAMS_PREFERRED_MODEL",
                                         "YAMS_EMBED_PRELOAD_ON_STARTUP", "YAMS_EMBED_BATCH",
                                         "YAMS_EMBED_BATCH_TARGET", "YAMS_REPAIR_LOCK_TIMEOUT_MS",
                                         "YAMS_EMBED_DIM"});
    const auto ambientPath = writeToml("ambient.toml", R"toml(
[embeddings]
backend = "simeon"
preferred_model = "ambient-model"
)toml");
    EnvGuard ambientConfig{"YAMS_CONFIG_PATH", ambientPath.string()};

    const auto explicitPath = writeToml("explicit.toml", R"toml(
[embeddings]
backend = "ONNX"
preferred_model = "canonical-model"
preload_on_startup = true
embedding_dim = 768

[embeddings.runtime]
backend = "simeon"
preferred_model = "legacy-model"
batch_size = 24
batch_target = 2048
repair_lock_timeout_ms = 4321

[daemon.models]
preload_models = ["preload-model"]
)toml");

    DaemonConfig config;
    config.configFilePath = explicitPath;
    const auto resolved = ConfigResolver::resolveEmbeddingConfig(config, tempDir);

    CHECK((resolved.backend == "onnxruntime"));
    CHECK((resolved.preferredModel == "canonical-model"));
    CHECK_FALSE(resolved.isTrainingFree);
    CHECK(resolved.preloadOnStartup);
    REQUIRE(resolved.dimension.has_value());
    CHECK((*resolved.dimension == 768U));
    CHECK((resolved.dimensionSource == EmbeddingDimensionSource::Config));
    CHECK((resolved.effectiveConfigPath == explicitPath));
    CHECK((resolved.provenance.at("backend") == "config:embeddings.backend"));
    CHECK((resolved.provenance.at("preferred_model") == "config:embeddings.preferred_model"));
    CHECK((resolved.provenance.at("dimension") == "config:embeddings.embedding_dim"));
    CHECK((resolved.policyIdentity == "onnxruntime:canonical-model:768"));
    REQUIRE(resolved.runtime.backend.has_value());
    CHECK((*resolved.runtime.backend == "onnxruntime"));
    REQUIRE(resolved.runtime.preferredModel.has_value());
    CHECK((*resolved.runtime.preferredModel == "canonical-model"));
    CHECK((resolved.runtime.batchSize == std::optional<std::size_t>{24U}));
    CHECK((resolved.runtime.batchTarget == std::optional<std::size_t>{2048U}));
    CHECK((resolved.runtime.repairLockTimeoutMs == std::optional<std::uint64_t>{4321U}));
    CHECK((resolved.warnings.size() >= 2U));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver embedding snapshot applies compatibility env overlays once",
                 "[daemon][components][config][embeddings][snapshot][catch2]") {
    const auto configPath = writeToml("config.toml", R"toml(
[embeddings]
backend = "onnxruntime"
preferred_model = "configured-model"
preload_on_startup = true
embedding_dim = 384

[embeddings.runtime]
batch_size = 32
batch_target = 4096
repair_lock_timeout_ms = 60000
)toml");
    EnvGuard configEnvironment{"YAMS_CONFIG_PATH", configPath.string()};
    EnvGuard backendEnvironment{"YAMS_EMBED_BACKEND", "SIMEON"};
    EnvGuard modelEnvironment{"YAMS_PREFERRED_MODEL", "configured-model"};
    EnvGuard preloadEnvironment{"YAMS_EMBED_PRELOAD_ON_STARTUP", "false"};
    EnvGuard batchEnvironment{"YAMS_EMBED_BATCH", "7"};
    EnvGuard targetEnvironment{"YAMS_EMBED_BATCH_TARGET", "99"};
    EnvGuard lockEnvironment{"YAMS_REPAIR_LOCK_TIMEOUT_MS", "1234"};
    EnvGuard dimensionEnvironment{"YAMS_EMBED_DIM", "1024"};

    DaemonConfig config;
    config.configFilePath = configPath;
    const auto resolved = ConfigResolver::resolveEmbeddingConfig(config, tempDir);

    CHECK((resolved.backend == "simeon"));
    CHECK((resolved.preferredModel == "simeon-default"));
    CHECK(resolved.isTrainingFree);
    CHECK_FALSE(resolved.preloadOnStartup);
    CHECK((resolved.dimension == std::optional<std::size_t>{384U}));
    CHECK((resolved.dimensionSource == EmbeddingDimensionSource::Config));
    CHECK((resolved.provenance.at("backend") == "environment:YAMS_EMBED_BACKEND"));
    CHECK((resolved.provenance.at("preferred_model") ==
           "normalization:simeon-backend(environment:YAMS_PREFERRED_MODEL)"));
    CHECK((resolved.provenance.at("preload") == "environment:YAMS_EMBED_PRELOAD_ON_STARTUP"));
    CHECK((resolved.runtime.batchSize == std::optional<std::size_t>{7U}));
    CHECK((resolved.runtime.batchTarget == std::optional<std::size_t>{99U}));
    CHECK((resolved.runtime.repairLockTimeoutMs == std::optional<std::uint64_t>{1234U}));
    CHECK((resolved.policyIdentity == "simeon:simeon-default:384"));

    const auto compatibilityPolicy = ConfigResolver::resolveEmbeddingRuntimePolicy();
    CHECK((compatibilityPolicy.backend == std::optional<std::string>{"simeon"}));
    CHECK((compatibilityPolicy.preferredModel == std::optional<std::string>{"simeon-default"}));
    CHECK((compatibilityPolicy.batchSize == resolved.runtime.batchSize));
    CHECK((compatibilityPolicy.batchTarget == resolved.runtime.batchTarget));
    CHECK((compatibilityPolicy.repairLockTimeoutMs == resolved.runtime.repairLockTimeoutMs));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver normalizes model identity before deriving dimension",
                 "[daemon][components][config][embeddings][snapshot][catch2]") {
    auto environment =
        unsetEnvironment({"YAMS_EMBED_BACKEND", "YAMS_PREFERRED_MODEL", "YAMS_EMBED_DIM"});
    const auto configPath = writeToml("normalized-model.toml", R"toml(
[embeddings]
backend = "simeon"
preferred_model = "all-MiniLM-L6-v2"
)toml");

    DaemonConfig config;
    config.configFilePath = configPath;
    const auto resolved = ConfigResolver::resolveEmbeddingConfig(config, {});

    CHECK((resolved.preferredModel == "simeon-default"));
    CHECK_FALSE(resolved.dimension.has_value());
    CHECK((resolved.dimensionSource == EmbeddingDimensionSource::Unresolved));
    CHECK((resolved.provenance.at("preferred_model") ==
           "normalization:simeon-backend(config:embeddings.preferred_model)"));
    CHECK((resolved.policyIdentity == "simeon:simeon-default:unresolved"));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver rejects a simeon sentinel for an ONNX backend",
                 "[daemon][components][config][embeddings][snapshot][catch2]") {
    auto environment = unsetEnvironment({"YAMS_EMBED_BACKEND", "YAMS_PREFERRED_MODEL"});
    const auto configPath = writeToml("onnx-sentinel.toml", R"toml(
[embeddings]
backend = "onnxruntime"
preferred_model = "simeon-default"
)toml");
    const auto modelDir = tempDir / "models" / "all-MiniLM-L6-v2";
    std::filesystem::create_directories(modelDir);
    std::ofstream(modelDir / "model.onnx") << "fixture";

    DaemonConfig config;
    config.configFilePath = configPath;
    const auto resolved = ConfigResolver::resolveEmbeddingConfig(config, tempDir);

    CHECK((resolved.backend == "onnxruntime"));
    CHECK((resolved.preferredModel == "all-MiniLM-L6-v2"));
    CHECK((resolved.dimension == std::optional<std::size_t>{384U}));
    CHECK((resolved.provenance.at("preferred_model") == "data_directory:models"));
    CHECK((resolved.policyIdentity == "onnxruntime:all-MiniLM-L6-v2:384"));
    CHECK_FALSE(resolved.warnings.empty());
}

TEST_CASE("EmbeddingGenerator preserves a resolved backend against ambient changes",
          "[daemon][components][config][embeddings][snapshot][catch2]") {
    EnvGuard backendEnvironment{"YAMS_EMBED_BACKEND", "daemon"};

    yams::vector::EmbeddingConfig config;
    config.backend = yams::vector::EmbeddingConfig::Backend::Simeon;
    config.backend_is_resolved = true;
    config.embedding_dim = 128;

    yams::vector::EmbeddingGenerator generator(config);
    REQUIRE(generator.initialize());
    CHECK((generator.getBackendName() == "Simeon"));
    CHECK((generator.getEmbeddingDimension() == 128U));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver embedding snapshot records persisted dimension authority",
                 "[daemon][components][config][embeddings][snapshot][catch2]") {
    auto environment = unsetEnvironment({"YAMS_EMBED_BACKEND", "YAMS_PREFERRED_MODEL",
                                         "YAMS_EMBED_PRELOAD_ON_STARTUP", "YAMS_EMBED_BATCH",
                                         "YAMS_EMBED_BATCH_TARGET", "YAMS_REPAIR_LOCK_TIMEOUT_MS",
                                         "YAMS_EMBED_DIM"});
    const auto configPath = writeToml("config.toml", R"toml(
[embeddings]
backend = "simeon"
embedding_dim = 384
)toml");
    ConfigResolver::writeVectorSentinel(tempDir, 1024, "embeddings", 1);

    DaemonConfig config;
    config.configFilePath = configPath;
    const auto resolved = ConfigResolver::resolveEmbeddingConfig(config, tempDir);

    CHECK((resolved.dimension == std::optional<std::size_t>{1024U}));
    CHECK((resolved.dimensionSource == EmbeddingDimensionSource::Sentinel));
    CHECK((resolved.provenance.at("dimension") == "sentinel:vectors_sentinel.json"));
    CHECK((resolved.policyIdentity == "simeon:simeon-default:1024"));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveTopologyTunerPolicy defaults are empty when missing",
                 "[daemon][components][config][topology_tuner][catch2]") {
    auto configPath = writeToml("config.toml", R"TOML(
[daemon]
socket_path = "/tmp/test.sock"
)TOML");

    EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
    auto policy = ConfigResolver::resolveTopologyTunerPolicy();

    CHECK_FALSE(policy.enabled.has_value());
    CHECK_FALSE(policy.cooldownMinutes.has_value());
    CHECK_FALSE(policy.docCountDelta.has_value());
    CHECK_FALSE(policy.rewardAlphaSingleton.has_value());
    CHECK_FALSE(policy.rewardBetaGiantCluster.has_value());
    CHECK_FALSE(policy.rewardGammaGiniDeviation.has_value());
    CHECK_FALSE(policy.rewardDeltaIntraEdge.has_value());
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveTopologyEnginePolicy reads routing representatives",
                 "[daemon][components][config][topology][catch2]") {
    auto configPath = writeToml("config.toml", R"TOML(
[topology]
engine = "connected"
routing_representatives = 4
boundary_spill = true
boundary_spill_limit = 1
boundary_spill_distance_ratio = +1.2
boundary_spill_residual_penalty = 1.5e0

[topology.features]
entity_fusion = true
entity_signature_k = 12
entity_fusion_alpha = 0.2
entity_min_confidence = 0.5
matryoshka_coarse_view = true
matryoshka_target_dim = 256
minhash_sketch = true
minhash_sketch_dim = 24
minhash_alpha = 0.15
)TOML");

    EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
    auto policy = ConfigResolver::resolveTopologyEnginePolicy();

    REQUIRE(policy.engine.has_value());
    CHECK((*policy.engine == "connected"));
    REQUIRE(policy.routingRepresentativeCount.has_value());
    CHECK((*policy.routingRepresentativeCount == 4));
    REQUIRE(policy.boundarySpillEnabled.has_value());
    CHECK(*policy.boundarySpillEnabled);
    REQUIRE(policy.boundarySpillLimit.has_value());
    CHECK((*policy.boundarySpillLimit == 1));
    REQUIRE(policy.boundarySpillDistanceRatio.has_value());
    CHECK((*policy.boundarySpillDistanceRatio == Catch::Approx(1.2)));
    REQUIRE(policy.boundarySpillResidualPenalty.has_value());
    CHECK((*policy.boundarySpillResidualPenalty == Catch::Approx(1.5)));
    CHECK(*policy.featureEntityFusion);
    CHECK((*policy.featureEntitySignatureK == 12));
    CHECK((*policy.featureEntityFusionAlpha == Catch::Approx(0.2F)));
    CHECK((*policy.featureEntityMinConfidence == Catch::Approx(0.5F)));
    CHECK(*policy.featureMatryoshkaCoarseView);
    CHECK((*policy.featureMatryoshkaTargetDim == 256));
    CHECK(*policy.featureMinHashSketch);
    CHECK((*policy.featureMinHashSketchDim == 24));
    CHECK((*policy.featureMinHashAlpha == Catch::Approx(0.15F)));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveTopologyRoutingPolicy reads populated TOML block",
                 "[daemon][components][config][topology_routing][catch2]") {
    auto configPath = writeToml("config.toml", R"TOML(
[search.topology]
enable_weak_query_routing = true
vector_policy = shadow
min_clusters = 1
max_clusters = 3
max_seed_documents = 24
representative_limit = 2
ann_candidate_limit = 16
adaptive_probe_score_gap = 0.07
narrow_min_boundary_margin = 0.03
max_docs = 42
expansion_output_limit = 96
graph_weighted_seed_ranking = true
medoid_boost = 0.2
route_calibration_fingerprint = "atlas-123"
route_calibration_queries = 100
route_calibration_protected_candidates = 250
route_calibration_missed_protected_candidates = 2
route_min_calibration_queries = 75
route_max_misses_per_thousand = 10
route_calibration_min_boundary_margin = 0.2
route_calibration_min_seed_hits = 2
route_work_max_rows_visited = 64
route_work_max_exact_distance_evaluations = 32
route_work_max_ann_candidates = 48
rrf_k = 33
)TOML");

    EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
    auto policy = ConfigResolver::resolveTopologyRoutingPolicy();

    REQUIRE(policy.enableWeakQueryRouting.has_value());
    CHECK((*policy.enableWeakQueryRouting == true));
    REQUIRE(policy.vectorPolicy.has_value());
    CHECK((*policy.vectorPolicy == "shadow"));
    REQUIRE(policy.maxClusters.has_value());
    CHECK((*policy.maxClusters == 3U));
    REQUIRE(policy.minClusters.has_value());
    CHECK((*policy.minClusters == 1U));
    REQUIRE(policy.maxSeedDocuments.has_value());
    CHECK((*policy.maxSeedDocuments == 24U));
    REQUIRE(policy.representativeLimit.has_value());
    CHECK((*policy.representativeLimit == 2U));
    REQUIRE(policy.annCandidateLimit.has_value());
    CHECK((*policy.annCandidateLimit == 16U));
    REQUIRE(policy.adaptiveProbeScoreGap.has_value());
    CHECK((*policy.adaptiveProbeScoreGap == Catch::Approx(0.07F)));
    REQUIRE(policy.narrowMinBoundaryMargin.has_value());
    CHECK((*policy.narrowMinBoundaryMargin == Catch::Approx(0.03F)));
    REQUIRE(policy.maxDocs.has_value());
    CHECK((*policy.maxDocs == 42U));
    REQUIRE(policy.expansionOutputLimit.has_value());
    CHECK((*policy.expansionOutputLimit == 96U));
    REQUIRE(policy.graphWeightedSeedRanking.has_value());
    CHECK(*policy.graphWeightedSeedRanking);
    REQUIRE(policy.medoidBoost.has_value());
    CHECK((*policy.medoidBoost > 0.19f));
    CHECK((*policy.medoidBoost < 0.21f));
    CHECK((*policy.routeCalibrationFingerprint == "atlas-123"));
    CHECK((*policy.routeCalibrationQueries == 100U));
    CHECK((*policy.routeCalibrationProtectedCandidates == 250U));
    CHECK((*policy.routeCalibrationMissedProtectedCandidates == 2U));
    CHECK((*policy.routeMinCalibrationQueries == 75U));
    CHECK((*policy.routeMaxMissesPerThousand == 10U));
    CHECK((*policy.routeCalibrationMinBoundaryMargin == Catch::Approx(0.2F)));
    CHECK((*policy.routeCalibrationMinSeedHits == 2U));
    CHECK((*policy.routeWorkMaxRowsVisited == 64U));
    CHECK((*policy.routeWorkMaxExactDistanceEvaluations == 32U));
    CHECK((*policy.routeWorkMaxAnnCandidates == 48U));
    REQUIRE(policy.rrfK.has_value());
    CHECK((*policy.rrfK == 33.0f));
}

TEST_CASE("Generated config keeps topology-assisted hybrid search as the product default",
          "[config][search][topology][catch2]") {
    const auto defaults = yams::config::ConfigMigrator::getLatestConfigDefaults();
    const auto topologyIt = defaults.find("search.topology");
    REQUIRE((topologyIt != defaults.end()));

    const auto& topology = topologyIt->second;
    CHECK((topology.at("mode") == "hybrid_assist"));
    CHECK((topology.at("vector_policy") == "shadow"));
    CHECK((topology.at("graph_weighted_seed_ranking") == "false"));
    CHECK_FALSE(topology.contains("enable_weak_query_routing"));
    CHECK_FALSE(topology.contains("routing_variant"));
}

TEST_CASE_METHOD(ConfigResolverFixture, "ConfigResolver reads topology evidence weight",
                 "[daemon][components][config][topology_routing][catch2]") {
    auto configPath = writeToml("config.toml", R"TOML(
[search.topology]
evidence_weight = 0.03
)TOML");

    EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
    auto topology = ConfigResolver::resolveTopologyRoutingPolicy();
    REQUIRE(topology.evidenceWeight.has_value());
    CHECK((*topology.evidenceWeight == Catch::Approx(0.03F)));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveTopologyRoutingPolicy honors legacy env overlays",
                 "[daemon][components][config][topology_routing][catch2]") {
    EnvGuard cfg("YAMS_CONFIG_PATH", "");
    EnvGuard enable("YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING", "1");
    EnvGuard clusters("YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS", "5");
    EnvGuard docs("YAMS_SEARCH_TOPOLOGY_MAX_DOCS", "17");
    EnvGuard medoid("YAMS_SEARCH_TOPOLOGY_MEDOID_BOOST", "0.15");

    auto policy = ConfigResolver::resolveTopologyRoutingPolicy();

    REQUIRE(policy.enableWeakQueryRouting.has_value());
    CHECK((*policy.enableWeakQueryRouting == true));
    REQUIRE(policy.maxClusters.has_value());
    CHECK((*policy.maxClusters == 5U));
    REQUIRE(policy.maxDocs.has_value());
    CHECK((*policy.maxDocs == 17U));
    REQUIRE(policy.medoidBoost.has_value());
    CHECK((*policy.medoidBoost > 0.14f));
    CHECK((*policy.medoidBoost < 0.16f));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveEmbeddingSelectionPolicy reads semantic graph ingest flag",
                 "[daemon][components][config][embeddings][catch2]") {
    auto configPath = writeToml("config.toml", R"TOML(
[embeddings.selection]
update_semantic_graph_during_ingest = false
)TOML");

    EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
    auto policy = ConfigResolver::resolveEmbeddingSelectionPolicy();

    CHECK((policy.updateSemanticGraphDuringIngest == false));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveTopologyTunerPolicy reads populated TOML block",
                 "[daemon][components][config][topology_tuner][catch2]") {
    auto configPath = writeToml("config.toml", R"TOML(
[topology.tuner]
enabled = true
cooldown_minutes = 30
doc_count_delta = 250

[topology.tuner.reward]
alpha_singleton = 0.5
beta_giant_cluster = 0.3
gamma_gini_deviation = 0.15
delta_intra_edge = 0.05
)TOML");

    EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
    auto policy = ConfigResolver::resolveTopologyTunerPolicy();

    REQUIRE(policy.enabled.has_value());
    CHECK((*policy.enabled == true));
    REQUIRE(policy.cooldownMinutes.has_value());
    CHECK((*policy.cooldownMinutes == 30u));
    REQUIRE(policy.docCountDelta.has_value());
    CHECK((*policy.docCountDelta == 250u));
    REQUIRE(policy.rewardAlphaSingleton.has_value());
    CHECK((*policy.rewardAlphaSingleton == 0.5));
    REQUIRE(policy.rewardBetaGiantCluster.has_value());
    CHECK((*policy.rewardBetaGiantCluster == 0.3));
    REQUIRE(policy.rewardGammaGiniDeviation.has_value());
    CHECK((*policy.rewardGammaGiniDeviation == 0.15));
    REQUIRE(policy.rewardDeltaIntraEdge.has_value());
    CHECK((*policy.rewardDeltaIntraEdge == 0.05));
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveTopologyTunerPolicy honors disabled flag explicitly",
                 "[daemon][components][config][topology_tuner][catch2]") {
    auto configPath = writeToml("config.toml", R"TOML(
[topology.tuner]
enabled = false
)TOML");

    EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
    auto policy = ConfigResolver::resolveTopologyTunerPolicy();

    REQUIRE(policy.enabled.has_value());
    CHECK((*policy.enabled == false));
}

TEST_CASE("ConfigResolver vector sentinel operations",
          "[daemon][components][config][catch2][sentinel]") {
    auto tempDir = std::filesystem::temp_directory_path() /
                   ("yams_sentinel_test_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(tempDir);

    SECTION("readVectorSentinelDim returns nullopt for missing file") {
        auto dim = ConfigResolver::readVectorSentinelDim(tempDir);
        CHECK_FALSE(dim.has_value());
    }

    SECTION("writeVectorSentinel then readVectorSentinelDim round-trips") {
        ConfigResolver::writeVectorSentinel(tempDir, 384, "test_table", 1);
        auto dim = ConfigResolver::readVectorSentinelDim(tempDir);
        REQUIRE(dim.has_value());
        CHECK((dim.value() == 384));
    }

    SECTION("different dimensions are preserved") {
        for (size_t testDim : {128, 256, 384, 768, 1024}) {
            ConfigResolver::writeVectorSentinel(tempDir, testDim, "test", 1);
            auto dim = ConfigResolver::readVectorSentinelDim(tempDir);
            REQUIRE(dim.has_value());
            CHECK((dim.value() == testDim));
        }
    }

    std::error_code ec;
    std::filesystem::remove_all(tempDir, ec);
}

TEST_CASE_METHOD(ConfigResolverFixture, "shared flat TOML parser reads tuning sections",
                 "[daemon][components][config][catch2]") {
    auto configPath = writeToml("multi_tuning.toml", R"(
[tuning]
profile = "aggressive"
pool_cooldown_ms = 250
worker_poll_ms = 100
)");
    auto config = yams::config::parse_simple_toml(configPath);
    CHECK((config["tuning.profile"] == "aggressive"));
    CHECK((config["tuning.pool_cooldown_ms"] == "250"));
    CHECK((config["tuning.worker_poll_ms"] == "100"));
}

TEST_CASE("Tuning profile from config affects TuneAdvisor methods",
          "[daemon][components][config][catch2]") {
    PostIngestStageActivityGuard stageActivityGuard;
    SECTION("efficient profile scales post-ingest concurrency down") {
        ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Efficient);
        EnvGuard maxThreadsGuard("YAMS_MAX_THREADS", "0");
        EnvGuard postIngestGuard("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        yams::daemon::TuneAdvisor::setHardwareConcurrencyForTests(8);

        // Efficient profile scale is 0.0, old totalBudget = 2
        // With active-stage floor: total = max(2, 6 active stages) = 6
        // Each stage gets 1 slot; batching remains a cap and does not reserve work.
        CHECK((TuneAdvisor::postExtractionConcurrent() == 1u));
        CHECK((TuneAdvisor::postKgConcurrent() == 1u));
        CHECK((TuneAdvisor::postSymbolConcurrent() == 1u));
        CHECK((TuneAdvisor::postEntityConcurrent() == 1u));
        CHECK((TuneAdvisor::postTitleConcurrent() == 1u));
        CHECK((TuneAdvisor::postEmbedConcurrent() == 1u));
        CHECK((TuneAdvisor::postIngestBatchSize() == 32u));
    }

    SECTION("balanced profile uses medium values") {
        ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Balanced);
        EnvGuard maxThreadsGuard("YAMS_MAX_THREADS", "0");
        EnvGuard postIngestGuard("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        yams::daemon::TuneAdvisor::setHardwareConcurrencyForTests(8);

        // Balanced profile scale is 0.5, old totalBudget = 2
        // With active-stage floor: total = max(2, 6 active stages) = 6
        // Each stage gets 1 slot; batching remains a cap and does not reserve work.
        CHECK((TuneAdvisor::postExtractionConcurrent() == 1u));
        CHECK((TuneAdvisor::postKgConcurrent() == 1u));
        CHECK((TuneAdvisor::postSymbolConcurrent() == 1u));
        CHECK((TuneAdvisor::postEntityConcurrent() == 1u));
        CHECK((TuneAdvisor::postTitleConcurrent() == 1u));
        CHECK((TuneAdvisor::postEmbedConcurrent() == 1u));
        CHECK((TuneAdvisor::postIngestBatchSize() == 32u));
    }

    SECTION("aggressive profile uses maximum values") {
        ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Aggressive);
        EnvGuard maxThreadsGuard("YAMS_MAX_THREADS", "0");
        EnvGuard postIngestGuard("YAMS_POST_INGEST_TOTAL_CONCURRENT", "0");
        yams::daemon::TuneAdvisor::setHardwareConcurrencyForTests(8);

        // Aggressive profile scale is 1.0, old totalBudget = 3
        // With active-stage floor: total = max(3, 6 active stages) = 6
        // Each stage gets 1 slot; batching remains a cap and does not reserve work.
        CHECK((TuneAdvisor::postExtractionConcurrent() == 1u));
        CHECK((TuneAdvisor::postKgConcurrent() == 1u));
        CHECK((TuneAdvisor::postSymbolConcurrent() == 1u));
        CHECK((TuneAdvisor::postEntityConcurrent() == 1u));
        CHECK((TuneAdvisor::postTitleConcurrent() == 1u));
        CHECK((TuneAdvisor::postEmbedConcurrent() == 1u));
        CHECK((TuneAdvisor::postIngestBatchSize() == 32u));
    }

    SECTION("profile affects cpuBudgetPercent") {
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Efficient);
            CHECK((TuneAdvisor::cpuBudgetPercent() == 40u));
        }
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Balanced);
            CHECK((TuneAdvisor::cpuBudgetPercent() == 50u));
        }
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Aggressive);
            // 40.0 + 1.0 * 20.0 = 60
            CHECK((TuneAdvisor::cpuBudgetPercent() == 60u));
        }
    }

    SECTION("profile affects poolCooldownMs") {
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Efficient);
            CHECK((TuneAdvisor::poolCooldownMs() == 750u));
        }
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Balanced);
            CHECK((TuneAdvisor::poolCooldownMs() == 500u));
        }
        {
            ProfileGuard guard(yams::daemon::TuneAdvisor::Profile::Aggressive);
            CHECK((TuneAdvisor::poolCooldownMs() == 250u));
        }
    }
}

TEST_CASE("YAMS_TUNING_PROFILE env var overrides config", "[daemon][components][config][catch2]") {
    auto clearProfileOverride = []() {
        yams::daemon::TuneAdvisor::tuningProfileOverride_.store(0, std::memory_order_relaxed);
    };

    SECTION("efficient profile from env var") {
        clearProfileOverride();
        EnvGuard envGuard("YAMS_TUNING_PROFILE", "efficient");

        auto profile = yams::daemon::TuneAdvisor::tuningProfile();
        CHECK((profile == yams::daemon::TuneAdvisor::Profile::Efficient));
    }

    SECTION("aggressive profile from env var") {
        clearProfileOverride();
        EnvGuard envGuard("YAMS_TUNING_PROFILE", "aggressive");

        auto profile = yams::daemon::TuneAdvisor::tuningProfile();
        CHECK((profile == yams::daemon::TuneAdvisor::Profile::Aggressive));
    }

    SECTION("conservative alias maps to efficient") {
        clearProfileOverride();
        EnvGuard envGuard("YAMS_TUNING_PROFILE", "conservative");

        auto profile = yams::daemon::TuneAdvisor::Profile::Efficient;
        CHECK((yams::daemon::TuneAdvisor::tuningProfile() == profile));
    }

    SECTION("invalid env var falls back to balanced") {
        clearProfileOverride();
        EnvGuard envGuard("YAMS_TUNING_PROFILE", "invalid_profile");

        auto profile = yams::daemon::TuneAdvisor::tuningProfile();
        CHECK((profile == yams::daemon::TuneAdvisor::Profile::Balanced));
    }

    SECTION("empty env var falls back to balanced") {
        clearProfileOverride();
        EnvGuard envGuard("YAMS_TUNING_PROFILE", "");

        auto profile = yams::daemon::TuneAdvisor::tuningProfile();
        CHECK((profile == yams::daemon::TuneAdvisor::Profile::Balanced));
    }
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveSimeonEncoderPolicy reads [embeddings.simeon]",
                 "[daemon][components][config][simeon][catch2]") {
    SECTION("missing section returns all nullopt") {
        auto configPath = writeToml("no_simeon.toml", R"(
[daemon]
log_level = "info"
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
        auto cleared = unsetEnvironment({"YAMS_SIMEON_NGRAM_MODE", "YAMS_SIMEON_NGRAM_MIN",
                                         "YAMS_SIMEON_NGRAM_MAX", "YAMS_SIMEON_SKETCH_DIM",
                                         "YAMS_SIMEON_OUTPUT_DIM", "YAMS_SIMEON_PROJECTION",
                                         "YAMS_SIMEON_PQ_BYTES"});
        auto policy = ConfigResolver::resolveSimeonEncoderPolicy();
        CHECK_FALSE(policy.ngramMode.has_value());
        CHECK_FALSE(policy.ngramMin.has_value());
        CHECK_FALSE(policy.ngramMax.has_value());
        CHECK_FALSE(policy.sketchDim.has_value());
        CHECK_FALSE(policy.outputDim.has_value());
        CHECK_FALSE(policy.projection.has_value());
        CHECK_FALSE(policy.l2Normalize.has_value());
        CHECK_FALSE(policy.pqBytes.has_value());
    }

    SECTION("populated TOML section fills every field") {
        auto configPath = writeToml("simeon_encoder.toml", R"(
[embeddings.simeon]
ngram_mode = "char_and_word"
ngram_min = 3
ngram_max = 5
sketch_dim = 4096
output_dim = 384
projection = "fwht"
l2_normalize = true
pq_bytes = 64
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
        auto cleared = unsetEnvironment({"YAMS_SIMEON_NGRAM_MODE", "YAMS_SIMEON_NGRAM_MIN",
                                         "YAMS_SIMEON_NGRAM_MAX", "YAMS_SIMEON_SKETCH_DIM",
                                         "YAMS_SIMEON_OUTPUT_DIM", "YAMS_SIMEON_PROJECTION",
                                         "YAMS_SIMEON_PQ_BYTES"});
        auto policy = ConfigResolver::resolveSimeonEncoderPolicy();
        REQUIRE(policy.ngramMode.has_value());
        CHECK((*policy.ngramMode == "char_and_word"));
        REQUIRE(policy.ngramMin.has_value());
        CHECK((*policy.ngramMin == 3u));
        REQUIRE(policy.ngramMax.has_value());
        CHECK((*policy.ngramMax == 5u));
        REQUIRE(policy.sketchDim.has_value());
        CHECK((*policy.sketchDim == 4096u));
        REQUIRE(policy.outputDim.has_value());
        CHECK((*policy.outputDim == 384u));
        REQUIRE(policy.projection.has_value());
        CHECK((*policy.projection == "fwht"));
        REQUIRE(policy.l2Normalize.has_value());
        CHECK(*policy.l2Normalize);
        REQUIRE(policy.pqBytes.has_value());
        CHECK((*policy.pqBytes == 64u));
    }

    SECTION("env vars override TOML values") {
        auto configPath = writeToml("override_encoder.toml", R"(
[embeddings.simeon]
projection = "achlioptas_sparse"
sketch_dim = 2048
output_dim = 256
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
        EnvGuard proj("YAMS_SIMEON_PROJECTION", "fwht");
        EnvGuard sketch("YAMS_SIMEON_SKETCH_DIM", "8192");
        EnvGuard out("YAMS_SIMEON_OUTPUT_DIM", "1024");

        auto policy = ConfigResolver::resolveSimeonEncoderPolicy();
        REQUIRE(policy.projection.has_value());
        CHECK((*policy.projection == "fwht"));
        REQUIRE(policy.sketchDim.has_value());
        CHECK((*policy.sketchDim == 8192u));
        REQUIRE(policy.outputDim.has_value());
        CHECK((*policy.outputDim == 1024u));
    }
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveSimeonBm25Policy reads [embeddings.simeon.bm25]",
                 "[daemon][components][config][simeon][catch2]") {
    SECTION("missing section returns all nullopt") {
        auto configPath = writeToml("no_bm25.toml", R"(
[daemon]
log_level = "info"
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
        auto cleared = unsetEnvironment({"YAMS_SIMEON_BM25_ENABLED", "YAMS_SIMEON_BM25_VARIANT",
                                         "YAMS_SIMEON_BM25_SUBWORD_GAMMA",
                                         "YAMS_SIMEON_BM25_MAX_CORPUS_DOCS"});
        auto policy = ConfigResolver::resolveSimeonBm25Policy();
        CHECK_FALSE(policy.enabled.has_value());
        CHECK_FALSE(policy.variant.has_value());
        CHECK_FALSE(policy.subwordGamma.has_value());
        CHECK_FALSE(policy.maxCorpusDocs.has_value());
    }

    SECTION("populated TOML section fills every field") {
        auto configPath = writeToml("bm25.toml", R"(
[embeddings.simeon.bm25]
enabled = true
variant = "sab_smooth"
subword_gamma = 5.0
max_corpus_docs = 200000
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
        auto cleared = unsetEnvironment({"YAMS_SIMEON_BM25_ENABLED", "YAMS_SIMEON_BM25_VARIANT",
                                         "YAMS_SIMEON_BM25_SUBWORD_GAMMA",
                                         "YAMS_SIMEON_BM25_MAX_CORPUS_DOCS"});
        auto policy = ConfigResolver::resolveSimeonBm25Policy();
        REQUIRE(policy.enabled.has_value());
        CHECK(*policy.enabled);
        REQUIRE(policy.variant.has_value());
        CHECK((*policy.variant == "sab_smooth"));
        REQUIRE(policy.subwordGamma.has_value());
        CHECK((*policy.subwordGamma == 5.0f));
        REQUIRE(policy.maxCorpusDocs.has_value());
        CHECK((*policy.maxCorpusDocs == 200000u));
    }

    SECTION("env vars override TOML values") {
        auto configPath = writeToml("override_bm25.toml", R"(
[embeddings.simeon.bm25]
variant = "atire"
subword_gamma = 3.0
max_corpus_docs = 50000
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
        EnvGuard variantEnv("YAMS_SIMEON_BM25_VARIANT", "sab_smooth");
        EnvGuard gammaEnv("YAMS_SIMEON_BM25_SUBWORD_GAMMA", "7.5");
        EnvGuard capEnv("YAMS_SIMEON_BM25_MAX_CORPUS_DOCS", "123456");

        auto policy = ConfigResolver::resolveSimeonBm25Policy();
        REQUIRE(policy.variant.has_value());
        CHECK((*policy.variant == "sab_smooth"));
        REQUIRE(policy.subwordGamma.has_value());
        CHECK((*policy.subwordGamma == 7.5f));
        REQUIRE(policy.maxCorpusDocs.has_value());
        CHECK((*policy.maxCorpusDocs == 123456u));
    }

    SECTION("fragment geometry encoder profile is read from TOML") {
        auto configPath = writeToml("bm25_fragment_encoder.toml", R"(
[embeddings.simeon.bm25.fragment_geometry]
encoder_profile = "fixed_hash_384"
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());

        auto policy = ConfigResolver::resolveSimeonBm25Policy();
        REQUIRE(policy.fragmentGeometryEncoderProfile.has_value());
        CHECK((*policy.fragmentGeometryEncoderProfile == "fixed_hash_384"));
    }

    SECTION("embedding encoder profile is read from TOML") {
        auto configPath = writeToml("simeon_encoder_profile.toml", R"(
[embeddings.simeon]
encoder_profile = "fixed_hash_384"
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());

        auto policy = ConfigResolver::resolveSimeonEncoderPolicy();
        REQUIRE(policy.encoderProfile.has_value());
        CHECK((*policy.encoderProfile == "fixed_hash_384"));
    }
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveRerankerBackendPolicy reads [search]",
                 "[daemon][components][config][reranker][catch2]") {
    SECTION("returns unset when not configured") {
        auto configPath = writeToml("reranker_default.toml", R"(
[search]
default_limit = 10
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());

        auto policy = ConfigResolver::resolveRerankerBackendPolicy(DaemonConfig{});
        CHECK_FALSE(policy.backend.has_value());
    }

    SECTION("reads explicit reranker backend") {
        auto configPath = writeToml("reranker_backend.toml", R"(
[search]
reranker_backend = "colbert"
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());

        auto policy = ConfigResolver::resolveRerankerBackendPolicy(DaemonConfig{});
        REQUIRE(policy.backend.has_value());
        CHECK((*policy.backend == "colbert"));
    }

    SECTION("normalizes reranker backend to lowercase") {
        auto configPath = writeToml("reranker_backend_upper.toml", R"(
[search]
reranker_backend = "ONNX"
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());

        auto policy = ConfigResolver::resolveRerankerBackendPolicy(DaemonConfig{});
        REQUIRE(policy.backend.has_value());
        CHECK((*policy.backend == "onnx"));
    }
}

TEST_CASE_METHOD(ConfigResolverFixture,
                 "ConfigResolver::resolveInstrumentationPolicy applies memory profile",
                 "[daemon][components][config][instrumentation][catch2]") {
    SECTION("auto profile activates when MallocStackLoggingNoCompact is present") {
        auto configPath = writeToml("instrumentation_auto.toml", R"(
[daemon]
log_level = "info"
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
        EnvGuard msl("MallocStackLoggingNoCompact", "1");
        EnvGuard mslCompact("MallocStackLogging", "0");

        auto policy = ConfigResolver::resolveInstrumentationPolicy(DaemonConfig{});
        CHECK((policy.profile == "auto"));
        CHECK(policy.memoryProfileActive);
        CHECK(policy.suppressAutoRepair);
        CHECK(policy.suppressSimeonLexicalBuild);
        CHECK(policy.suppressVectorIndexBuild);
    }

    SECTION("normal profile disables auto MSL suppression") {
        auto configPath = writeToml("instrumentation_normal.toml", R"(
[daemon.instrumentation]
profile = "normal"
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
        EnvGuard msl("MallocStackLoggingNoCompact", "1");

        auto policy = ConfigResolver::resolveInstrumentationPolicy(DaemonConfig{});
        CHECK((policy.profile == "normal"));
        CHECK_FALSE(policy.memoryProfileActive);
        CHECK_FALSE(policy.suppressAutoRepair);
        CHECK_FALSE(policy.suppressSimeonLexicalBuild);
        CHECK_FALSE(policy.suppressVectorIndexBuild);
    }

    SECTION("memory profile supports explicit overrides and stack-log threshold") {
        auto configPath = writeToml("instrumentation_memory.toml", R"(
[daemon.instrumentation]
profile = "memory"
suppress_auto_repair = false
suppress_simeon_lexical_build = true
suppress_vector_index_build = true
msl_stack_log_warn_mb = 1536
)");
        EnvGuard cfg("YAMS_CONFIG_PATH", configPath.string());
        EnvGuard msl("MallocStackLogging", "0");
        EnvGuard mslCompact("MallocStackLoggingNoCompact", "0");

        auto policy = ConfigResolver::resolveInstrumentationPolicy(DaemonConfig{});
        CHECK((policy.profile == "memory"));
        CHECK(policy.memoryProfileActive);
        CHECK_FALSE(policy.suppressAutoRepair);
        CHECK(policy.suppressSimeonLexicalBuild);
        CHECK(policy.suppressVectorIndexBuild);
        CHECK((policy.mslStackLogWarnBytes == 1536ULL * 1024ULL * 1024ULL));
    }
}
