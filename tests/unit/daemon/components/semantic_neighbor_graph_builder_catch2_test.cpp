// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// SemanticNeighborGraphBuilder against a real in-memory vector database and a real sqlite KG
// store, with the edge sink captured instead of a WriteCoordinator.

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/semantic_neighbor_graph_builder.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/vector/vector_database.h>

#include "../../../common/test_helpers_catch2.h"

#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

using yams::daemon::SemanticNeighborGraphBuilder;
using yams::daemon::SemanticNeighborGraphConfig;

namespace {

struct Fixture {
    yams::test::ScopedEnvVar enableVectors{"YAMS_DISABLE_VECTORS", std::string("0")};
    yams::test::ScopedEnvVar enableSqliteVecInit{"YAMS_SQLITE_VEC_SKIP_INIT", std::string("0")};
    std::filesystem::path dir = yams::test::make_temp_dir("yams_semantic_graph_");
    std::shared_ptr<yams::vector::VectorDatabase> vdb;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg;
    std::vector<yams::metadata::KGEdge> captured;
    std::vector<std::string> sources;

    // Unit vectors so cosine similarity is the dot product: a·b=0.8, a·d=0.6, b·d=0.96,
    // c·d=0.8, b·c=0.6, a·c=0.
    const std::map<std::string, std::vector<float>> corpus{
        {"a", {1.0F, 0.0F}},
        {"b", {0.8F, 0.6F}},
        {"c", {0.0F, 1.0F}},
        {"d", {0.6F, 0.8F}},
    };

    Fixture() {
        yams::vector::VectorDatabaseConfig dbConfig;
        dbConfig.database_path = ":memory:";
        dbConfig.embedding_dim = 2;
        dbConfig.use_in_memory = true;
        vdb = std::make_shared<yams::vector::VectorDatabase>(dbConfig);
        REQUIRE(vdb->initialize());
        std::vector<yams::metadata::KGNode> nodes;
        for (const auto& [hash, embedding] : corpus) {
            yams::vector::VectorRecord record;
            record.chunk_id = "chunk-" + hash;
            record.document_hash = hash;
            record.embedding = embedding;
            record.level = yams::vector::EmbeddingLevel::DOCUMENT;
            record.metadata["path"] = "/docs/" + hash + ".md";
            REQUIRE(vdb->insertVector(record));
            nodes.push_back({.nodeKey = "doc:" + hash, .label = hash, .type = "document"});
        }
        auto store = yams::metadata::makeSqliteKnowledgeGraphStore((dir / "kg.db").string());
        REQUIRE(store.has_value());
        kg = std::move(store.value());
        REQUIRE(kg->upsertNodes(nodes).has_value());
    }

    ~Fixture() {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    std::unique_ptr<SemanticNeighborGraphBuilder> builder(SemanticNeighborGraphConfig cfg,
                                                          bool withSink = true) {
        auto b = std::make_unique<SemanticNeighborGraphBuilder>(cfg);
        if (withSink) {
            b->setEdgeSink([this](std::vector<yams::metadata::KGEdge> edges, std::string_view src) {
                sources.emplace_back(src);
                captured.insert(captured.end(), edges.begin(), edges.end());
                return true;
            });
        }
        return b;
    }

    std::vector<std::pair<std::string, std::string>> allSources() const {
        std::vector<std::pair<std::string, std::string>> out;
        for (const auto& [hash, _] : corpus)
            out.emplace_back(hash, "");
        return out;
    }

    float weight(const std::string& src, const std::string& dst) const {
        const auto srcId = kg->getNodeByKey("doc:" + src).value()->id;
        const auto dstId = kg->getNodeByKey("doc:" + dst).value()->id;
        for (const auto& e : captured) {
            if (e.srcNodeId == srcId && e.dstNodeId == dstId)
                return e.weight;
        }
        return -1.0F;
    }
};

} // namespace

TEST_CASE("semantic edge capacity checks multiplication without allocating",
          "[daemon][semantic-graph][bounds]") {
    using Builder = yams::daemon::SemanticNeighborGraphBuilder;
    CHECK(Builder::checkedEdgeCapacity(0, 8) == std::optional<std::size_t>{0});
    CHECK(Builder::checkedEdgeCapacity(1, 256) == std::optional<std::size_t>{512});
    CHECK_FALSE(Builder::checkedEdgeCapacity(1, 0));
    CHECK_FALSE(Builder::checkedEdgeCapacity(1, 257));
    CHECK_FALSE(Builder::checkedEdgeCapacity(SIZE_MAX, 256));
    const auto largest = std::vector<yams::metadata::KGEdge>{}.max_size() / 512;
    REQUIRE(Builder::checkedEdgeCapacity(largest, 256));
    CHECK(*Builder::checkedEdgeCapacity(largest, 256) == largest * 512);
    CHECK_FALSE(Builder::checkedEdgeCapacity(largest + 1, 256));
}

TEST_CASE("semantic neighbor builder rejects invalid direct top-K before work",
          "[daemon][semantic-graph][bounds]") {
    for (std::size_t topK : {std::size_t{0}, std::size_t{257}, SIZE_MAX}) {
        yams::daemon::SemanticNeighborGraphConfig config;
        config.topK = topK;
        yams::daemon::SemanticNeighborGraphBuilder builder(config);
        builder.update({}, {}, "test", {}, true);
        CHECK(builder.updateErrors() == 1);
        CHECK(builder.docsProcessed() == 0);
        CHECK(builder.edgesCreated() == 0);
    }
}

TEST_CASE("semantic neighbor builder emits top-K forward and reverse edges above the threshold",
          "[daemon][components][semantic-graph][catch2]") {
    Fixture fx;
    SemanticNeighborGraphConfig cfg;
    cfg.topK = 2;
    cfg.similarityThreshold = 0.5F;
    cfg.useHnsw = false;
    auto builder = fx.builder(cfg);

    SECTION("streaming update over the four documents") {
        builder->update(fx.kg, fx.vdb, "model-x", fx.allSources(), /*sourceAllCorpus=*/false);
        REQUIRE(fx.sources == std::vector<std::string>{"EmbeddingService::semanticNeighborStream"});
    }
    SECTION("corpus-wide rebuild") {
        builder->update(fx.kg, fx.vdb, "model-x", {}, /*sourceAllCorpus=*/true);
        REQUIRE(fx.sources == std::vector<std::string>{"EmbeddingService::semanticNeighborCorpus"});
    }

    // 4 sources x 2 neighbors x (forward + reverse).
    CHECK(fx.captured.size() == 16);
    CHECK(builder->edgesCreated() == 16);
    CHECK(builder->docsProcessed() == 4);
    CHECK(builder->updateErrors() == 0);
    for (const auto& e : fx.captured) {
        CHECK(e.relation == "semantic_neighbor");
        CHECK(e.weight >= 0.5F);
        REQUIRE(e.properties.has_value());
        CHECK(e.properties->find("\"model\":\"model-x\"") != std::string::npos);
    }
    CHECK(fx.weight("a", "b") == Catch::Approx(0.8).margin(1e-4));
    CHECK(fx.weight("a", "d") == Catch::Approx(0.6).margin(1e-4));
    CHECK(fx.weight("b", "d") == Catch::Approx(0.96).margin(1e-4));
    CHECK(fx.weight("c", "d") == Catch::Approx(0.8).margin(1e-4));
    // a·c = 0 is below the threshold: no edge in either direction.
    CHECK(fx.weight("a", "c") == -1.0F);
    CHECK(fx.weight("c", "a") == -1.0F);
}

TEST_CASE("semantic neighbor builder honours top-K and the adaptive threshold",
          "[daemon][components][semantic-graph][catch2]") {
    // Streaming candidates come from the builder's own corpus cache, which only knows the
    // sources it has seen, so every document is passed as a source here.
    Fixture fx;
    SemanticNeighborGraphConfig cfg;
    cfg.topK = 1;
    cfg.useHnsw = false;
    auto builder = fx.builder(cfg);
    builder->update(fx.kg, fx.vdb, "model-x", fx.allSources(), /*sourceAllCorpus=*/false);

    // One neighbor per source, forward + reverse: a->b, b->d, c->d, d->b.
    REQUIRE(fx.captured.size() == 8);
    CHECK(fx.weight("a", "b") == Catch::Approx(0.8).margin(1e-4));
    CHECK(fx.weight("b", "a") == Catch::Approx(0.8).margin(1e-4));
    CHECK(fx.weight("b", "d") == Catch::Approx(0.96).margin(1e-4));
    CHECK(fx.weight("a", "d") == -1.0F);
}

TEST_CASE("semantic neighbor builder counts a missing writer as an update error",
          "[daemon][components][semantic-graph][catch2]") {
    Fixture fx;
    SemanticNeighborGraphConfig cfg;
    cfg.topK = 2;
    cfg.useHnsw = false;
    auto builder = fx.builder(cfg, /*withSink=*/false);
    builder->update(fx.kg, fx.vdb, "model-x", fx.allSources(), /*sourceAllCorpus=*/false);
    CHECK(fx.captured.empty());
    CHECK(builder->edgesCreated() == 0);
    // Pre-existing shape, pinned rather than changed here: when the streaming write is dropped
    // the update falls through to the corpus-wide pass, which is dropped too (two errors).
    CHECK(builder->updateErrors() == 2);
}
