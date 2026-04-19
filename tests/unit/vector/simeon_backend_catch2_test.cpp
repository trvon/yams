#include <catch2/catch_test_macros.hpp>

#include <yams/vector/embedding_generator.h>
#include <yams/vector/simeon_embedding_backend.h>

#include <array>
#include <cmath>
#include <cstdlib>
#include <string>
#include <vector>

using namespace yams::vector;

namespace {

EmbeddingConfig make_cfg(size_t dim = 256) {
    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::Simeon;
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

TEST_CASE("EmbeddingGenerator selects Simeon via env override", "[vector][simeon]") {
    setenv("YAMS_EMBED_BACKEND", "simeon", 1);

    EmbeddingConfig cfg;
    cfg.embedding_dim = 384;
    cfg.normalize_embeddings = true;
    EmbeddingGenerator gen(cfg);
    REQUIRE(gen.initialize());
    REQUIRE(gen.getBackendName() == "Simeon");
    REQUIRE(gen.getEmbeddingDimension() == 384u);

    auto v = gen.generateEmbedding("hello world");
    REQUIRE(v.size() == 384u);

    unsetenv("YAMS_EMBED_BACKEND");
}
