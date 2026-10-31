#include <catch2/catch_test_macros.hpp>

#include <yams/compat/unistd.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/simeon_embedding_backend.h>

#include <array>
#include <cmath>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "../../common/test_helpers_catch2.h"

using namespace yams::vector;
using Catch::Matchers::StartsWith;

namespace {

EmbeddingConfig make_cfg(size_t dim = 256) {
    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::Simeon;
    cfg.backend_is_resolved = true;
    cfg.embedding_dim = dim;
    cfg.normalize_embeddings = true;
    cfg.max_sequence_length = 512;
    return cfg;
}

} // namespace

TEST_CASE("SimeonBackend initializes and reports its dimension", "[vector][simeon]") {
    auto backend = makeSimeonBackend(make_cfg(384));
    REQUIRE(backend != nullptr);
    REQUIRE(backend->isAvailable());
    REQUIRE(backend->initialize());
    REQUIRE(backend->isInitialized());
    REQUIRE(backend->getEmbeddingDimension() == 384u);
    REQUIRE(backend->getBackendName() == "Simeon");
}

TEST_CASE("SimeonBackend fixed hash profile exposes the frozen retrieval space",
          "[vector][simeon][identity]") {
    auto cfg = make_cfg(1024);
    cfg.simeon_encoder_profile = EmbeddingConfig::SimeonEncoderProfile::FixedHash384;

    auto backend = makeSimeonBackend(cfg);
    REQUIRE(backend != nullptr);
    REQUIRE(backend->initialize());
    CHECK(backend->getEmbeddingDimension() == 384u);
    CHECK(backend->getEmbeddingSpaceIdentity() == "simeon-v1-384");
    CHECK(simeonEmbeddingSpaceIdentity(cfg) == "simeon-v1-384");
}

TEST_CASE("SimeonBackend produces deterministic, unit-norm embeddings", "[vector][simeon]") {
    auto backend = makeSimeonBackend(make_cfg(256));
    REQUIRE(backend->initialize());

    const std::string text = "The quick brown fox jumps over the lazy dog.";
    auto r1 = backend->generateEmbedding(text);
    auto r2 = backend->generateEmbedding(text);
    REQUIRE(r1);
    REQUIRE(r2);
    REQUIRE(r1.value().size() == 256u);
    REQUIRE(r1.value() == r2.value());

    double acc = 0.0;
    for (float f : r1.value())
        acc += static_cast<double>(f) * static_cast<double>(f);
    REQUIRE(std::fabs(std::sqrt(acc) - 1.0) < 1e-4);
}

TEST_CASE("SimeonBackend batch matches per-item encoding", "[vector][simeon]") {
    auto backend = makeSimeonBackend(make_cfg(128));
    REQUIRE(backend->initialize());

    const std::vector<std::string> texts = {"alpha bravo charlie", "delta echo foxtrot",
                                            "golf hotel india"};
    auto per = std::vector<std::vector<float>>{};
    for (const auto& t : texts) {
        auto r = backend->generateEmbedding(t);
        REQUIRE(r);
        per.push_back(std::move(r).value());
    }
    auto batch = backend->generateEmbeddings(std::span<const std::string>(texts));
    REQUIRE(batch);
    REQUIRE(batch.value().size() == per.size());
    for (size_t i = 0; i < per.size(); ++i) {
        REQUIRE(batch.value()[i] == per[i]);
    }
}

TEST_CASE("EmbeddingGenerator trusts typed backend over ambient environment",
          "[vector][simeon][config]") {
    yams::test::ScopedEnvVar backend{"YAMS_EMBED_BACKEND", std::string{"daemon"}};

    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::Simeon;
    cfg.embedding_dim = 384;
    cfg.normalize_embeddings = true;
    EmbeddingGenerator gen(cfg);
    REQUIRE(gen.initialize());
    REQUIRE(gen.getBackendName() == "Simeon");
    REQUIRE(gen.getEmbeddingDimension() == 384u);

    auto v = gen.generateEmbedding("hello world");
    REQUIRE(v.size() == 384u);
}

TEST_CASE("EmbeddingGenerator selects Simeon when configured directly", "[vector][simeon]") {
    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::Simeon;
    cfg.backend_is_resolved = true;
    cfg.embedding_dim = 128;
    cfg.normalize_embeddings = true;

    EmbeddingGenerator gen(cfg);
    REQUIRE(gen.initialize());
    REQUIRE(gen.getBackendName() == "Simeon");
    REQUIRE(gen.getEmbeddingDimension() == 128u);
}

TEST_CASE("EmbeddingGenerator moved-from observers report neutral state",
          "[vector][embedding-generator][move][catch2]") {
    EmbeddingGenerator source(make_cfg(128));
    EmbeddingGenerator destination(std::move(source));

    CHECK_FALSE(source.isInitialized());
    CHECK((source.estimateTokenCount("moved-from") == 0));
    CHECK((source.getLastError().empty()));
    CHECK_FALSE(source.hasError());
    CHECK((destination.getConfig().embedding_dim == 128));
}

TEST_CASE("EmbeddingGenerator maps OnnxRuntime config to Daemon path", "[vector][daemon]") {
    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::OnnxRuntime;
    cfg.backend_is_resolved = true;

    EmbeddingGenerator gen(cfg);
    REQUIRE_THAT(gen.getBackendName(), StartsWith("Daemon"));
}

TEST_CASE("EmbeddingGenerator maps legacy Hybrid config to Daemon path", "[vector][daemon]") {
    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::Hybrid;
    cfg.backend_is_resolved = true;

    EmbeddingGenerator gen(cfg);
    REQUIRE_THAT(gen.getBackendName(), StartsWith("Daemon"));
}

TEST_CASE("EmbeddingGenerator selects Daemon when configured directly", "[vector][daemon]") {
    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::Daemon;
    cfg.backend_is_resolved = true;

    EmbeddingGenerator gen(cfg);
    REQUIRE_THAT(gen.getBackendName(), StartsWith("Daemon"));
}

#include <simeon/static_embedding.hpp>
#include <yams/config/static_embedding_table.h>

#include <filesystem>
#include <fstream>
#include <random>

namespace {

// Minimal SMSTAT01 table: dim 4, rows with scale 1; `bump` changes one row so contents differ.
void writeTinyStaticTable(const std::filesystem::path& path, std::int8_t bump = 0) {
    const std::vector<std::string> tokens = {"[UNK]", "alpha", "beta", "gamma"};
    const std::vector<std::array<std::int8_t, 4>> rows = {
        {{0, 0, 0, 0}},
        {{100, 0, 0, 0}},
        {{0, 100, 0, 0}},
        {{0, 0, static_cast<std::int8_t>(100 - bump), 0}}};
    std::string bytes = "SMSTAT01";
    auto u32 = [&](std::uint32_t v) {
        for (int k = 0; k < 4; ++k)
            bytes.push_back(static_cast<char>((v >> (8 * k)) & 0xFF));
    };
    u32(1);
    u32(4);
    u32(static_cast<std::uint32_t>(tokens.size()));
    u32(3);
    u32(0);
    u32(100);
    for (const auto& t : tokens) {
        bytes.push_back(static_cast<char>(t.size()));
        bytes.push_back(0);
        bytes += t;
    }
    for (std::size_t i = 0; i < tokens.size(); ++i)
        u32(0x3F800000u);
    for (const auto& r : rows)
        for (auto v : r)
            bytes.push_back(static_cast<char>(v));
    std::ofstream(path, std::ios::binary)
        .write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

std::filesystem::path uniqueTempDir(const char* stem) {
    auto dir = std::filesystem::temp_directory_path() /
               (std::string(stem) + std::to_string(std::random_device{}()));
    std::filesystem::create_directories(dir);
    return dir;
}

} // namespace

TEST_CASE("SimeonBackend static profile encodes with the configured table",
          "[vector][simeon][static]") {
    const auto dir = uniqueTempDir("yams_static_profile_");
    const auto table = dir / "tiny.smstat";
    writeTinyStaticTable(table);
    const auto cfgPath = dir / "config.toml";
    std::ofstream(cfgPath) << "[embeddings.simeon]\nencoder_profile = \"static\"\nstatic_table = \""
                           << table.string() << "\"\n";
    yams::test::ScopedEnvVar cfgEnv{"YAMS_CONFIG", cfgPath.string()};
    yams::test::ScopedEnvVar compatEnv{"YAMS_CONFIG_PATH", std::nullopt};

    auto backend = makeSimeonBackend(EmbeddingConfig{});
    REQUIRE(backend->initialize());
    CHECK(backend->getEmbeddingDimension() == 4);
    CHECK_THAT(backend->getEmbeddingSpaceIdentity(), StartsWith("simeon-static-v1:tiny.smstat:"));
    CHECK(backend->getEmbeddingSpaceIdentity().ends_with(":dim=4"));

    auto v = backend->generateEmbedding("alpha beta");
    REQUIRE(v.has_value());
    REQUIRE(v.value().size() == 4);
    CHECK(std::abs(v.value()[0] - 0.70710677f) < 1e-5f);
    CHECK(std::abs(v.value()[1] - 0.70710677f) < 1e-5f);
    CHECK(v.value()[2] == 0.0f);

    // A table with different contents has a different identity (it forces re-embedding).
    const auto other = dir / "tiny2.smstat";
    writeTinyStaticTable(other, /*bump=*/1);
    CHECK(staticEmbeddingSpaceIdentity(other) != staticEmbeddingSpaceIdentity(table));
    CHECK(loadSharedStaticEmbeddingTable(table) == loadSharedStaticEmbeddingTable(table));

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST_CASE("SimeonBackend static profile without a table falls back to the hash encoder",
          "[vector][simeon][static]") {
    const auto dir = uniqueTempDir("yams_static_missing_");
    const auto cfgPath = dir / "config.toml";
    std::ofstream(cfgPath) << "[embeddings.simeon]\nencoder_profile = \"static\"\nstatic_table = \""
                           << (dir / "missing.smstat").string() << "\"\n";
    yams::test::ScopedEnvVar cfgEnv{"YAMS_CONFIG", cfgPath.string()};
    yams::test::ScopedEnvVar compatEnv{"YAMS_CONFIG_PATH", std::nullopt};
    yams::test::ScopedEnvVar dataEnv{"YAMS_DATA_DIR", dir.string()};

    auto backend = makeSimeonBackend(EmbeddingConfig{});
    REQUIRE(backend->initialize());
    CHECK_THAT(backend->getEmbeddingSpaceIdentity(), StartsWith("simeon-config-v1:"));

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST_CASE("findStaticEmbeddingTable prefers the configured path, then the data dir",
          "[vector][simeon][static]") {
    using yams::config::findStaticEmbeddingTable;
    const auto dir = uniqueTempDir("yams_static_lookup_");
    const auto models = dir / "models" / "static";
    std::filesystem::create_directories(models);
    writeTinyStaticTable(models / "b.smstat");
    writeTinyStaticTable(models / "a.smstat");
    std::ofstream(models / "notes.txt") << "not a table";
    const auto configured = dir / "chosen.smstat";
    writeTinyStaticTable(configured);

    CHECK(findStaticEmbeddingTable(configured, dir) == configured);
    CHECK(findStaticEmbeddingTable(std::nullopt, dir) == models / "a.smstat");
    // A configured path that does not exist falls through to the data dir.
    CHECK(findStaticEmbeddingTable(dir / "missing.smstat", dir) == models / "a.smstat");

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST_CASE("SimeonBackend defaults to the static table when the profile is unset",
          "[vector][simeon][static]") {
    const auto dir = uniqueTempDir("yams_static_default_");
    std::filesystem::create_directories(dir / "models" / "static");
    writeTinyStaticTable(dir / "models" / "static" / "shipped.smstat");
    const auto cfgPath = dir / "config.toml";
    std::ofstream(cfgPath) << "[core]\ndata_dir = \"" << dir.string() << "\"\n";
    yams::test::ScopedEnvVar cfgEnv{"YAMS_CONFIG", cfgPath.string()};
    yams::test::ScopedEnvVar compatEnv{"YAMS_CONFIG_PATH", std::nullopt};
    yams::test::ScopedEnvVar dataEnv{"YAMS_DATA_DIR", dir.string()};

    auto backend = makeSimeonBackend(EmbeddingConfig{});
    REQUIRE(backend->initialize());
    CHECK_THAT(backend->getEmbeddingSpaceIdentity(),
               StartsWith("simeon-static-v1:shipped.smstat:"));
    CHECK(backend->getEmbeddingDimension() == 4);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST_CASE("SimeonBackend keeps the hash encoder when the profile is set explicitly",
          "[vector][simeon][static]") {
    const auto dir = uniqueTempDir("yams_static_explicit_");
    std::filesystem::create_directories(dir / "models" / "static");
    writeTinyStaticTable(dir / "models" / "static" / "shipped.smstat");
    const auto cfgPath = dir / "config.toml";
    std::ofstream(cfgPath) << "[core]\ndata_dir = \"" << dir.string()
                           << "\"\n[embeddings.simeon]\nencoder_profile = \"configurable\"\n";
    yams::test::ScopedEnvVar cfgEnv{"YAMS_CONFIG", cfgPath.string()};
    yams::test::ScopedEnvVar compatEnv{"YAMS_CONFIG_PATH", std::nullopt};
    yams::test::ScopedEnvVar dataEnv{"YAMS_DATA_DIR", dir.string()};

    auto backend = makeSimeonBackend(EmbeddingConfig{});
    REQUIRE(backend->initialize());
    CHECK_THAT(backend->getEmbeddingSpaceIdentity(), StartsWith("simeon-config-v1:"));

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

TEST_CASE("Static table lookup honours the daemon's resolved data dir",
          "[vector][simeon][static]") {
    const auto daemonDir = uniqueTempDir("yams_static_hint_");
    std::filesystem::create_directories(daemonDir / "models" / "static");
    writeTinyStaticTable(daemonDir / "models" / "static" / "daemon.smstat");
    const auto otherDir = uniqueTempDir("yams_static_other_");
    const auto cfgPath = otherDir / "config.toml";
    std::ofstream(cfgPath) << "[core]\ndata_dir = \"" << otherDir.string() << "\"\n";
    yams::test::ScopedEnvVar cfgEnv{"YAMS_CONFIG", cfgPath.string()};
    yams::test::ScopedEnvVar compatEnv{"YAMS_CONFIG_PATH", std::nullopt};
    yams::test::ScopedEnvVar dataEnv{"YAMS_DATA_DIR", otherDir.string()};

    setStaticEmbeddingDataDirHint(daemonDir);
    auto backend = makeSimeonBackend(EmbeddingConfig{});
    REQUIRE(backend->initialize());
    CHECK_THAT(backend->getEmbeddingSpaceIdentity(), StartsWith("simeon-static-v1:daemon.smstat:"));
    setStaticEmbeddingDataDirHint({});

    std::error_code ec;
    std::filesystem::remove_all(daemonDir, ec);
    std::filesystem::remove_all(otherDir, ec);
}
