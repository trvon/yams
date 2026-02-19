// Daemon-only embedding backend regression tests

#include <catch2/catch_test_macros.hpp>

#include <yams/vector/embedding_generator.h>

#include "../../common/test_helpers_catch2.h"

using namespace yams::vector;

TEST_CASE("EmbeddingGenerator defaults to daemon backend", "[vector][embedding][backend]") {
    EmbeddingConfig cfg;
    CHECK(cfg.backend == EmbeddingConfig::Backend::Daemon);

    EmbeddingGenerator gen(cfg);
    CHECK(gen.getBackendName().find("Daemon") != std::string::npos);
}

TEST_CASE("EmbeddingGenerator treats hybrid as daemon alias", "[vector][embedding][backend]") {
    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::Hybrid;

    EmbeddingGenerator gen(cfg);
    CHECK(gen.getBackendName().find("Daemon") != std::string::npos);
}

TEST_CASE("EmbeddingGenerator daemon backend works with mock provider fallback",
          "[vector][embedding][backend][synthetic]") {
    yams::test::ScopedEnvVar inDaemon{"YAMS_IN_DAEMON", std::string{"1"}};
    yams::test::ScopedEnvVar mockProvider{"YAMS_USE_MOCK_PROVIDER", std::string{"1"}};

    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::Daemon;
    cfg.model_name = "all-MiniLM-L6-v2";
    cfg.batch_size = 8;

    EmbeddingGenerator gen(cfg);
    REQUIRE(gen.initialize());

    auto vectors = gen.generateEmbeddings({"hello", "world"});
    REQUIRE(vectors.size() == 2);
    REQUIRE_FALSE(vectors[0].empty());
    CHECK(gen.getEmbeddingDimension() > 0);
}
