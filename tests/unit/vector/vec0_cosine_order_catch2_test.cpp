// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Similarity is cosine on every vector search engine. The vec0 engine ranks by L2
// distance, which matches cosine order only when both sides are unit length. Stored
// rows need not be unit length (providers with normalization off, imported or synced
// vectors, old data), so vec0 must rank by cosine regardless of stored magnitude.

#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"

#include <memory>
#include <string>
#include <vector>

#include <yams/vector/vector_database.h>

using namespace yams::vector;

namespace {

// CI runs unit tests with vectors disabled and sqlite-vec init skipped. These cases
// exercise the vec0 engine itself, so pin a vector-enabled environment for their scope.
struct VectorStoreEnv {
    yams::test::ScopedEnvVar disable{"YAMS_DISABLE_VECTORS", std::nullopt};
    yams::test::ScopedEnvVar disableSingular{"YAMS_DISABLE_VECTOR", std::nullopt};
    yams::test::ScopedEnvVar disableDb{"YAMS_DISABLE_VECTOR_DB", std::nullopt};
    yams::test::ScopedEnvVar skipVecInit{"YAMS_SQLITE_VEC_SKIP_INIT", std::nullopt};
    yams::test::ScopedEnvVar inMemory{"YAMS_VDB_IN_MEMORY", std::nullopt};
};

constexpr std::size_t kDim = 4;

std::unique_ptr<VectorDatabase> makeDb(VectorSearchEngine engine) {
    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = kDim;
    config.create_if_missing = true;
    config.use_in_memory = true;
    config.search_engine = engine;
    auto db = std::make_unique<VectorDatabase>(config);
    REQUIRE(db->initializeChecked().has_value());
    return db;
}

VectorRecord makeRecord(const std::string& id, std::vector<float> embedding) {
    VectorRecord record;
    record.chunk_id = id;
    record.document_hash = "doc_" + id;
    record.embedding = std::move(embedding);
    record.content = id;
    return record;
}

std::vector<std::string> ids(const std::vector<VectorRecord>& records) {
    std::vector<std::string> out;
    out.reserve(records.size());
    for (const auto& record : records) {
        out.push_back(record.chunk_id);
    }
    return out;
}

VectorSearchParams params(std::size_t k) {
    VectorSearchParams p;
    p.k = k;
    p.similarity_threshold = -1.0F;
    return p;
}

const std::vector<float> kQuery{1.0F, 0.0F, 0.0F, 0.0F};

// Against kQuery:
//   near  cosine 0.995, L2 4.03
//   wide  cosine 0.707, L2 13.45
//   small cosine 0.447, L2 0.92
// Cosine order is near, wide, small. L2 order is small, near, wide.
std::vector<VectorRecord> nonUnitCorpus() {
    return {
        makeRecord("wide", {10.0F, 10.0F, 0.0F, 0.0F}),
        makeRecord("small", {0.3F, 0.6F, 0.0F, 0.0F}),
        makeRecord("near", {5.0F, 0.5F, 0.0F, 0.0F}),
    };
}

} // namespace

TEST_CASE("vec0 search ranks non-unit stored vectors by cosine", "[vector][vec0][cosine][catch2]") {
    const VectorStoreEnv env;
    auto db = makeDb(VectorSearchEngine::Vec0L2);
    for (const auto& record : nonUnitCorpus()) {
        REQUIRE(db->insertVectorChecked(record).has_value());
    }

    SECTION("top-k selects the cosine nearest rows") {
        CHECK(ids(db->search(kQuery, params(2))) == std::vector<std::string>{"near", "wide"});
    }
    SECTION("full ranking follows cosine order") {
        CHECK(ids(db->search(kQuery, params(3))) ==
              std::vector<std::string>{"near", "wide", "small"});
    }
    SECTION("a non-unit query ranks the same as its unit direction") {
        const std::vector<float> scaledQuery{7.5F, 0.0F, 0.0F, 0.0F};
        CHECK(ids(db->search(scaledQuery, params(3))) ==
              std::vector<std::string>{"near", "wide", "small"});
    }
}

TEST_CASE("vec0 search agrees with the exact cosine scan on non-unit rows",
          "[vector][vec0][cosine][catch2]") {
    const VectorStoreEnv env;
    auto vec0 = makeDb(VectorSearchEngine::Vec0L2);
    auto exact = makeDb(VectorSearchEngine::ExactScan);

    // Batch path for one store, single inserts for the other: both feed vec0 the same way.
    auto corpus = nonUnitCorpus();
    corpus.push_back(makeRecord("tilted", {0.2F, -3.0F, 1.0F, 0.0F}));
    corpus.push_back(makeRecord("opposite", {-0.4F, 0.1F, 0.0F, 0.0F}));
    REQUIRE(vec0->insertVectorsBatchChecked(corpus).has_value());
    for (const auto& record : corpus) {
        REQUIRE(exact->insertVectorChecked(record).has_value());
    }

    const std::vector<std::vector<float>> queries{
        kQuery,
        {0.0F, -2.0F, 0.5F, 0.0F},
        {-3.0F, 0.2F, 0.0F, 0.1F},
    };
    for (const auto& query : queries) {
        for (std::size_t k : {std::size_t{1}, std::size_t{3}, corpus.size()}) {
            const auto expected = ids(exact->search(query, params(k)));
            REQUIRE(expected.size() == k);
            CHECK(ids(vec0->search(query, params(k))) == expected);
        }
    }
}

TEST_CASE("vec0 search follows a vector updated to a new magnitude",
          "[vector][vec0][cosine][catch2]") {
    const VectorStoreEnv env;
    auto db = makeDb(VectorSearchEngine::Vec0L2);
    for (const auto& record : nonUnitCorpus()) {
        REQUIRE(db->insertVectorChecked(record).has_value());
    }
    // Same direction as before for "small" is irrelevant; move it onto the query axis
    // with a large norm. L2 would now rank it last, cosine ranks it first.
    REQUIRE(db->updateVectorChecked("small", makeRecord("small", {40.0F, 0.0F, 0.0F, 0.0F}))
                .has_value());
    CHECK(ids(db->search(kQuery, params(2))) == std::vector<std::string>{"small", "near"});
}

TEST_CASE("vec0 search never matches a zero stored vector",
          "[vector][vec0][cosine][zero][catch2]") {
    const VectorStoreEnv env;
    auto db = makeDb(VectorSearchEngine::Vec0L2);
    // The zero row sits at L2 distance 1 from the query, nearer than "near" (4.03).
    REQUIRE(db->insertVectorChecked(makeRecord("zero", {0.0F, 0.0F, 0.0F, 0.0F})).has_value());
    REQUIRE(db->insertVectorChecked(makeRecord("near", {5.0F, 0.5F, 0.0F, 0.0F})).has_value());

    CHECK(ids(db->search(kQuery, params(2))) == std::vector<std::string>{"near"});
}

TEST_CASE("vec0 search returns nothing for a zero query", "[vector][vec0][cosine][zero][catch2]") {
    const VectorStoreEnv env;
    auto db = makeDb(VectorSearchEngine::Vec0L2);
    for (const auto& record : nonUnitCorpus()) {
        REQUIRE(db->insertVectorChecked(record).has_value());
    }
    CHECK(db->search(std::vector<float>(kDim, 0.0F), params(3)).empty());
}
