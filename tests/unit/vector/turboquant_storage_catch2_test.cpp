// Catch2 tests for TurboQuant storage integration with VectorDatabase
// Tests the insert/dequantize plumbing when enable_turboquant_storage is set
//
// NOTE: This tests the CURRENT implementation:
// - TurboQuant codes are computed on insert
// - Backend still persists float embeddings (not packed codes)
// - Search uses full float embeddings in HNSW
// - This is NOT end-to-end compressed storage yet

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

#include <yams/vector/vector_database.h>

using namespace yams::vector;

namespace {

struct TurboQuantStorageFixture {
    TurboQuantStorageFixture() : rng(42) {
        // Check if vector tests should be skipped
        if (const char* skipEnv = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
            std::string v(skipEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping (YAMS_SQLITE_VEC_SKIP_INIT=1)";
            }
        }
        if (const char* disableEnv = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disableEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping (YAMS_DISABLE_VECTORS=1)";
            }
        }
    }

    void skipIfNeeded() {
        if (!skipReason.empty()) {
            SKIP(skipReason);
        }
    }

    // Generate random unit vector
    std::vector<float> generateUnitVector(size_t dim) {
        std::vector<float> v(dim);
        std::normal_distribution<float> dist(0.0f, 1.0f);
        float norm_sq = 0.0f;
        for (size_t i = 0; i < dim; ++i) {
            v[i] = dist(rng);
            norm_sq += v[i] * v[i];
        }
        float norm = std::sqrt(norm_sq);
        for (size_t i = 0; i < dim; ++i) {
            v[i] /= norm;
        }
        return v;
    }

    // Compute MSE between two vectors
    double computeMSE(const std::vector<float>& a, const std::vector<float>& b) {
        double sum = 0.0;
        for (size_t i = 0; i < a.size(); ++i) {
            double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
            sum += diff * diff;
        }
        return sum / a.size();
    }

    std::mt19937 rng;
    std::string skipReason;
};

} // namespace

// =============================================================================
// Test Group: TurboQuant Storage Integration
// =============================================================================

TEST_CASE_METHOD(TurboQuantStorageFixture,
                 "TurboQuant storage insert and retrieve with compression",
                 "[turboquant][storage][integration][catch2]") {
    skipIfNeeded();

    const size_t dim = 128;
    const uint8_t bits = 4;

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = dim;
    config.create_if_missing = true;
    config.use_in_memory = true;
    config.enable_turboquant_storage = true;
    config.turboquant_bits = bits;
    config.turboquant_seed = 42;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Insert a vector
    std::vector<float> original = generateUnitVector(dim);
    VectorRecord rec;
    rec.chunk_id = "turbo_test_001";
    rec.document_hash = "turbo_doc_001";
    rec.embedding = original;
    rec.content = "TurboQuant test content";

    REQUIRE(db.insertVector(rec));

    // Retrieve it
    auto retrieved = db.getVector("turbo_test_001");
    REQUIRE(retrieved.has_value());

    // Check that retrieval worked
    CHECK(retrieved->chunk_id == "turbo_test_001");
    CHECK(retrieved->embedding.size() == dim);

    // Verify the embedding was compressed (should be recovered via dequantization)
    // The recovered vector should be similar to the original
    double mse = computeMSE(original, retrieved->embedding);

    INFO("TurboQuant storage MSE: " << mse);
    REQUIRE(mse < 0.01); // MSE should be low
}

TEST_CASE_METHOD(TurboQuantStorageFixture, "TurboQuant storage batch insert and retrieve",
                 "[turboquant][storage][integration][catch2]") {
    skipIfNeeded();

    const size_t dim = 128;
    const size_t num_vectors = 10;

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = dim;
    config.create_if_missing = true;
    config.use_in_memory = true;
    config.enable_turboquant_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 42;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Insert multiple vectors
    std::vector<std::vector<float>> originals;
    std::vector<VectorRecord> records;

    for (size_t i = 0; i < num_vectors; ++i) {
        auto vec = generateUnitVector(dim);
        originals.push_back(vec);

        VectorRecord rec;
        rec.chunk_id = "turbo_batch_" + std::to_string(i);
        rec.document_hash = "turbo_doc_batch";
        rec.embedding = vec;
        rec.content = "Batch test content " + std::to_string(i);
        records.push_back(rec);
    }

    REQUIRE(db.insertVectorsBatch(records));

    // Retrieve and verify all
    for (size_t i = 0; i < num_vectors; ++i) {
        auto id = "turbo_batch_" + std::to_string(i);
        auto retrieved = db.getVector(id);
        REQUIRE(retrieved.has_value());

        double mse = computeMSE(originals[i], retrieved->embedding);
        INFO("Vector " << i << " MSE: " << mse);
        REQUIRE(mse < 0.01);
    }
}

TEST_CASE_METHOD(TurboQuantStorageFixture, "TurboQuant storage different bit widths",
                 "[turboquant][storage][integration][bits][catch2]") {
    skipIfNeeded();

    const size_t dim = 128;

    for (uint8_t bits : {2, 3, 4}) {
        VectorDatabaseConfig config;
        config.database_path = ":memory:";
        config.embedding_dim = dim;
        config.create_if_missing = true;
        config.use_in_memory = true;
        config.enable_turboquant_storage = true;
        config.turboquant_bits = bits;
        config.turboquant_seed = 42;

        VectorDatabase db(config);
        REQUIRE(db.initialize());

        auto original = generateUnitVector(dim);
        VectorRecord rec;
        rec.chunk_id = "turbo_bits_" + std::to_string(bits);
        rec.document_hash = "turbo_doc_bits";
        rec.embedding = original;
        rec.content = "Bit width test";

        REQUIRE(db.insertVector(rec));

        auto retrieved = db.getVector(rec.chunk_id);
        REQUIRE(retrieved.has_value());

        double mse = computeMSE(original, retrieved->embedding);
        INFO("Bits=" << (int)bits << " MSE: " << mse);

        // Lower bits = higher MSE, but should still be recoverable
        if (bits == 2) {
            REQUIRE(mse < 0.02); // 2-bit has higher MSE
        } else if (bits == 3) {
            REQUIRE(mse < 0.01);
        } else {
            REQUIRE(mse < 0.005);
        }
    }
}

TEST_CASE_METHOD(TurboQuantStorageFixture, "TurboQuant storage search quality",
                 "[turboquant][storage][integration][search][catch2]") {
    skipIfNeeded();

    const size_t dim = 128;
    const size_t num_vectors = 100;
    const size_t k = 5;

    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = dim;
    config.create_if_missing = true;
    config.use_in_memory = true;
    config.enable_turboquant_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 42;

    VectorDatabase db(config);
    REQUIRE(db.initialize());

    // Insert random vectors
    std::vector<std::vector<float>> originals;
    for (size_t i = 0; i < num_vectors; ++i) {
        auto vec = generateUnitVector(dim);
        originals.push_back(vec);

        VectorRecord rec;
        rec.chunk_id = "turbo_search_" + std::to_string(i);
        rec.document_hash = "turbo_doc_search";
        rec.embedding = vec;
        rec.content = "Search test " + std::to_string(i);
        REQUIRE(db.insertVector(rec));
    }

    // Search for a random query (should find itself as top-1)
    std::vector<float> query = originals[42];

    VectorSearchParams params;
    params.k = k;
    params.similarity_threshold = 0.0f; // Accept all results
    auto results = db.search(query, params);
    REQUIRE(results.size() == k);

    // The query vector should match itself in top results
    bool found_self = false;
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].chunk_id == "turbo_search_42") {
            found_self = true;
            INFO("Self match found at position " << i);
            break;
        }
    }
    CHECK(found_self);
}

TEST_CASE_METHOD(TurboQuantStorageFixture, "TurboQuant storage vs baseline comparison",
                 "[turboquant][storage][integration][baseline][catch2]") {
    skipIfNeeded();

    const size_t dim = 128;
    const size_t num_vectors = 20;

    // Baseline (no compression)
    VectorDatabaseConfig baseline_config;
    baseline_config.database_path = ":memory:";
    baseline_config.embedding_dim = dim;
    baseline_config.create_if_missing = true;
    baseline_config.use_in_memory = true;
    baseline_config.enable_turboquant_storage = false;

    VectorDatabase baseline_db(baseline_config);
    REQUIRE(baseline_db.initialize());

    // TurboQuant (with compression)
    VectorDatabaseConfig tq_config;
    tq_config.database_path = ":memory:";
    tq_config.embedding_dim = dim;
    tq_config.create_if_missing = true;
    tq_config.use_in_memory = true;
    tq_config.enable_turboquant_storage = true;
    tq_config.turboquant_bits = 4;
    tq_config.turboquant_seed = 42;

    VectorDatabase tq_db(tq_config);
    REQUIRE(tq_db.initialize());

    // Insert same vectors in both
    for (size_t i = 0; i < num_vectors; ++i) {
        auto vec = generateUnitVector(dim);

        VectorRecord rec;
        rec.chunk_id = "compare_" + std::to_string(i);
        rec.document_hash = "compare_doc";
        rec.embedding = vec;
        rec.content = "Compare test";

        REQUIRE(baseline_db.insertVector(rec));
        REQUIRE(tq_db.insertVector(rec));
    }

    // Search in both
    std::vector<float> query = generateUnitVector(dim);

    VectorSearchParams params;
    params.k = 10;
    params.similarity_threshold = 0.0f; // Accept all results

    auto baseline_results = baseline_db.search(query, params);
    auto tq_results = tq_db.search(query, params);

    REQUIRE(baseline_results.size() == tq_results.size());

    // Compare top-1 results
    INFO("Baseline top-1: " << baseline_results[0].chunk_id);
    INFO("TurboQuant top-1: " << tq_results[0].chunk_id);

    // Both should return the same chunks (order may vary due to compression artifacts)
    CHECK(baseline_results[0].chunk_id == tq_results[0].chunk_id);
}
