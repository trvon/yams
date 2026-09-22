// HNSW Entity Search Benchmark
// Tests HNSW search performance with entity vectors (symbol embeddings)
// Validates search quality and latency for semantic symbol search

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <map>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/compat/dlfcn.h>
#include <yams/plugins/abi.h>
#include <yams/vector/vector_database.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace yams::vector;

namespace {

// ============================================================================
// Benchmark Results
// ============================================================================

struct HNSWBenchmarkResult {
    std::string name;
    size_t corpus_size = 0;
    size_t dimension = 0;
    size_t k = 0;
    double mean_latency_us = 0.0;
    double p50_latency_us = 0.0;
    double p95_latency_us = 0.0;
    double p99_latency_us = 0.0;
    double qps = 0.0;
    double recall = 0.0;
};

// ============================================================================
// Vector Generation Helpers
// ============================================================================

std::vector<float> generateRandomVector(size_t dim, std::mt19937& rng, bool normalize = true) {
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    std::vector<float> vec(dim);
    for (auto& v : vec) {
        v = dist(rng);
    }
    if (normalize) {
        float norm = 0.0f;
        for (float v : vec) {
            norm += v * v;
        }
        norm = std::sqrt(norm);
        if (norm > 0) {
            for (float& v : vec) {
                v /= norm;
            }
        }
    }
    return vec;
}

std::vector<std::vector<float>> generateRandomVectors(size_t count, size_t dim, std::mt19937& rng) {
    std::vector<std::vector<float>> vectors;
    vectors.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        vectors.push_back(generateRandomVector(dim, rng));
    }
    return vectors;
}

// Compute exact k-NN using brute force with cosine similarity (ground truth)
// For normalized vectors, cosine similarity = dot product
std::vector<size_t> bruteForceKNN(const std::vector<float>& query,
                                  const std::vector<std::vector<float>>& corpus, size_t k) {
    std::vector<std::pair<size_t, float>> similarities;
    similarities.reserve(corpus.size());

    for (size_t i = 0; i < corpus.size(); ++i) {
        float dot = 0.0f;
        for (size_t j = 0; j < query.size(); ++j) {
            dot += query[j] * corpus[i][j];
        }
        similarities.emplace_back(i, dot);
    }

    // Sort by descending similarity (highest first)
    std::partial_sort(similarities.begin(), similarities.begin() + std::min(k, similarities.size()),
                      similarities.end(),
                      [](const auto& a, const auto& b) { return a.second > b.second; });

    std::vector<size_t> result;
    result.reserve(k);
    for (size_t i = 0; i < std::min(k, similarities.size()); ++i) {
        result.push_back(similarities[i].first);
    }
    return result;
}

// Calculate recall@k
double calculateRecall(const std::vector<std::string>& retrieved,
                       const std::vector<size_t>& ground_truth_indices,
                       const std::vector<std::string>& all_chunk_ids) {
    std::set<std::string> gt_set;
    for (size_t idx : ground_truth_indices) {
        if (idx < all_chunk_ids.size()) {
            gt_set.insert(all_chunk_ids[idx]);
        }
    }

    size_t hits = 0;
    for (const auto& id : retrieved) {
        if (gt_set.count(id)) {
            hits++;
        }
    }

    return gt_set.empty() ? 0.0 : static_cast<double>(hits) / gt_set.size();
}

// Write benchmark results to JSON
void writeBenchmarkJSON(const fs::path& output, const std::vector<HNSWBenchmarkResult>& results) {
    json arr = json::array();
    for (const auto& r : results) {
        arr.push_back({{"name", r.name},
                       {"corpus_size", r.corpus_size},
                       {"dimension", r.dimension},
                       {"k", r.k},
                       {"mean_latency_us", r.mean_latency_us},
                       {"p50_latency_us", r.p50_latency_us},
                       {"p95_latency_us", r.p95_latency_us},
                       {"p99_latency_us", r.p99_latency_us},
                       {"qps", r.qps},
                       {"recall", r.recall}});
    }

    fs::create_directories(output.parent_path());
    std::ofstream out(output);
    out << arr.dump(2);
}

// ============================================================================
// Benchmark Fixtures
// ============================================================================

struct HNSWSearchFixture {
    VectorDatabaseConfig createConfig(size_t dim) {
        VectorDatabaseConfig config;
        config.database_path = ":memory:";
        config.embedding_dim = dim;
        config.create_if_missing = true;
        config.use_in_memory = true;
        return config;
    }

    VectorRecord createRecord(const std::string& chunk_id, const std::string& doc_hash,
                              const std::vector<float>& embedding) {
        VectorRecord rec;
        rec.chunk_id = chunk_id;
        rec.document_hash = doc_hash;
        rec.embedding = embedding;
        rec.content = "Test content for " + chunk_id;
        rec.start_offset = 0;
        rec.end_offset = rec.content.size();
        return rec;
    }
};

} // namespace

// ============================================================================
// HNSW Search Speed Benchmarks
// ============================================================================

TEST_CASE_METHOD(HNSWSearchFixture, "HNSW Search Speed Benchmark",
                 "[benchmark][hnsw][search][!benchmark]") {
    std::vector<HNSWBenchmarkResult> all_results;
    std::mt19937 rng(42);

    SECTION("Search Latency vs Corpus Size") {
        // Larger corpus sizes for meaningful benchmarks
        std::vector<size_t> corpus_sizes = {1000, 5000, 10000, 50000};
        size_t dim = 384;
        size_t k = 10;

        for (size_t corpus_size : corpus_sizes) {
            INFO("Testing corpus size: " << corpus_size);

            auto config = createConfig(dim);
            VectorDatabase db(config);
            REQUIRE(db.initialize());

            // Generate and insert vectors
            auto vectors = generateRandomVectors(corpus_size, dim, rng);
            std::vector<std::string> chunk_ids;
            chunk_ids.reserve(corpus_size);

            for (size_t i = 0; i < corpus_size; ++i) {
                std::string chunk_id = "chunk_" + std::to_string(i);
                chunk_ids.push_back(chunk_id);
                db.insertVector(createRecord(chunk_id, "doc_" + std::to_string(i), vectors[i]));
            }

            // Generate query and compute ground truth
            auto query = generateRandomVector(dim, rng);
            auto gt_indices = bruteForceKNN(query, vectors, k);

            // Benchmark search
            std::vector<double> latencies_us;
            size_t num_iterations = 100;

            for (size_t iter = 0; iter < num_iterations; ++iter) {
                auto start = std::chrono::high_resolution_clock::now();

                VectorSearchParams params;
                params.k = k;
                params.similarity_threshold = -2.0f; // Include all results
                auto results = db.search(query, params);

                auto end = std::chrono::high_resolution_clock::now();
                auto duration_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                latencies_us.push_back(static_cast<double>(duration_us));
            }

            // Calculate statistics
            std::sort(latencies_us.begin(), latencies_us.end());

            HNSWBenchmarkResult result;
            result.name = "HNSW_Search_" + std::to_string(corpus_size);
            result.corpus_size = corpus_size;
            result.dimension = dim;
            result.k = k;
            result.mean_latency_us =
                std::accumulate(latencies_us.begin(), latencies_us.end(), 0.0) /
                latencies_us.size();
            result.p50_latency_us = latencies_us[latencies_us.size() / 2];
            result.p95_latency_us = latencies_us[latencies_us.size() * 95 / 100];
            result.p99_latency_us = latencies_us[latencies_us.size() * 99 / 100];
            result.qps = 1000000.0 / result.mean_latency_us;

            // Calculate recall
            VectorSearchParams params;
            params.k = k;
            params.similarity_threshold = -2.0f; // Include all results
            auto final_results = db.search(query, params);
            std::vector<std::string> retrieved;
            for (const auto& r : final_results) {
                retrieved.push_back(r.chunk_id);
            }
            result.recall = calculateRecall(retrieved, gt_indices, chunk_ids);

            all_results.push_back(result);

            INFO("  Mean latency: " << result.mean_latency_us << " μs");
            INFO("  P95 latency: " << result.p95_latency_us << " μs");
            INFO("  QPS: " << result.qps);
            INFO("  Recall@" << k << ": " << result.recall);
            INFO("  Retrieved " << retrieved.size() << " results");

            // Performance requirements (relaxed for TSan builds)
            // Normal: <10ms, TSan: <100ms
            CHECK(result.mean_latency_us < 100000); // <100ms mean latency (TSan overhead)
            CHECK(result.recall >= 0.5);            // >50% recall (approximate search)
        }

        writeBenchmarkJSON("bench_results/hnsw_search_corpus_scaling.json", all_results);
    }

    SECTION("Search Latency vs k") {
        size_t corpus_size = 10000;
        size_t dim = 384;
        std::vector<size_t> k_values = {1, 5, 10, 20, 50, 100};

        auto config = createConfig(dim);
        VectorDatabase db(config);
        REQUIRE(db.initialize());

        auto vectors = generateRandomVectors(corpus_size, dim, rng);
        std::vector<std::string> chunk_ids;

        for (size_t i = 0; i < corpus_size; ++i) {
            std::string chunk_id = "chunk_" + std::to_string(i);
            chunk_ids.push_back(chunk_id);
            db.insertVector(createRecord(chunk_id, "doc_" + std::to_string(i), vectors[i]));
        }

        auto query = generateRandomVector(dim, rng);

        std::vector<HNSWBenchmarkResult> k_results;

        for (size_t k : k_values) {
            INFO("Testing k=" << k);

            auto gt_indices = bruteForceKNN(query, vectors, k);

            std::vector<double> latencies_us;
            size_t num_iterations = 100;

            for (size_t iter = 0; iter < num_iterations; ++iter) {
                auto start = std::chrono::high_resolution_clock::now();

                VectorSearchParams params;
                params.k = k;
                params.similarity_threshold = -2.0f; // Include all results
                auto results = db.search(query, params);

                auto end = std::chrono::high_resolution_clock::now();
                auto duration_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                latencies_us.push_back(static_cast<double>(duration_us));
            }

            std::sort(latencies_us.begin(), latencies_us.end());

            HNSWBenchmarkResult result;
            result.name = "HNSW_Search_k" + std::to_string(k);
            result.corpus_size = corpus_size;
            result.dimension = dim;
            result.k = k;
            result.mean_latency_us =
                std::accumulate(latencies_us.begin(), latencies_us.end(), 0.0) /
                latencies_us.size();
            result.p50_latency_us = latencies_us[latencies_us.size() / 2];
            result.p95_latency_us = latencies_us[latencies_us.size() * 95 / 100];
            result.p99_latency_us = latencies_us[latencies_us.size() * 99 / 100];
            result.qps = 1000000.0 / result.mean_latency_us;

            VectorSearchParams params;
            params.k = k;
            params.similarity_threshold = -2.0f; // Include all results
            auto final_results = db.search(query, params);
            std::vector<std::string> retrieved;
            for (const auto& r : final_results) {
                retrieved.push_back(r.chunk_id);
            }
            result.recall = calculateRecall(retrieved, gt_indices, chunk_ids);

            k_results.push_back(result);

            INFO("  k=" << k << " Mean latency: " << result.mean_latency_us << " μs");
            INFO("  Recall: " << result.recall << " Retrieved: " << retrieved.size());
        }

        writeBenchmarkJSON("bench_results/hnsw_search_k_scaling.json", k_results);
    }

    SECTION("Search Latency vs Dimension") {
        size_t corpus_size = 10000;
        std::vector<size_t> dimensions = {64, 128, 256, 384, 512, 768};
        size_t k = 10;

        std::vector<HNSWBenchmarkResult> dim_results;

        for (size_t dim : dimensions) {
            INFO("Testing dimension: " << dim);

            auto config = createConfig(dim);
            VectorDatabase db(config);
            REQUIRE(db.initialize());

            auto vectors = generateRandomVectors(corpus_size, dim, rng);
            std::vector<std::string> chunk_ids;

            for (size_t i = 0; i < corpus_size; ++i) {
                std::string chunk_id = "chunk_" + std::to_string(i);
                chunk_ids.push_back(chunk_id);
                db.insertVector(createRecord(chunk_id, "doc_" + std::to_string(i), vectors[i]));
            }

            auto query = generateRandomVector(dim, rng);
            auto gt_indices = bruteForceKNN(query, vectors, k);

            std::vector<double> latencies_us;
            size_t num_iterations = 100;

            for (size_t iter = 0; iter < num_iterations; ++iter) {
                auto start = std::chrono::high_resolution_clock::now();

                VectorSearchParams params;
                params.k = k;
                params.similarity_threshold = -2.0f; // Include all results
                auto results = db.search(query, params);

                auto end = std::chrono::high_resolution_clock::now();
                auto duration_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                latencies_us.push_back(static_cast<double>(duration_us));
            }

            std::sort(latencies_us.begin(), latencies_us.end());

            HNSWBenchmarkResult result;
            result.name = "HNSW_Search_dim" + std::to_string(dim);
            result.corpus_size = corpus_size;
            result.dimension = dim;
            result.k = k;
            result.mean_latency_us =
                std::accumulate(latencies_us.begin(), latencies_us.end(), 0.0) /
                latencies_us.size();
            result.p50_latency_us = latencies_us[latencies_us.size() / 2];
            result.p95_latency_us = latencies_us[latencies_us.size() * 95 / 100];
            result.p99_latency_us = latencies_us[latencies_us.size() * 99 / 100];
            result.qps = 1000000.0 / result.mean_latency_us;

            VectorSearchParams params;
            params.k = k;
            params.similarity_threshold = -2.0f; // Include all results
            auto final_results = db.search(query, params);
            std::vector<std::string> retrieved;
            for (const auto& r : final_results) {
                retrieved.push_back(r.chunk_id);
            }
            result.recall = calculateRecall(retrieved, gt_indices, chunk_ids);

            dim_results.push_back(result);

            INFO("  dim=" << dim << " Mean latency: " << result.mean_latency_us << " μs");
            INFO("  Recall: " << result.recall << " Retrieved: " << retrieved.size());
        }

        writeBenchmarkJSON("bench_results/hnsw_search_dim_scaling.json", dim_results);
    }
}

// ============================================================================
// Batch Insert Performance
// ============================================================================

TEST_CASE_METHOD(HNSWSearchFixture, "HNSW Batch Insert Performance",
                 "[benchmark][hnsw][insert][!benchmark]") {
    std::mt19937 rng(42);
    size_t dim = 384;

    SECTION("Batch Insert Throughput") {
        std::vector<size_t> batch_sizes = {10, 50, 100, 500, 1000};
        size_t total_vectors = 10000;

        json results = json::array();

        for (size_t batch_size : batch_sizes) {
            INFO("Testing batch size: " << batch_size);

            auto config = createConfig(dim);
            VectorDatabase db(config);
            REQUIRE(db.initialize());

            auto vectors = generateRandomVectors(total_vectors, dim, rng);

            auto start = std::chrono::high_resolution_clock::now();

            for (size_t i = 0; i < total_vectors; i += batch_size) {
                std::vector<VectorRecord> batch;
                for (size_t j = i; j < std::min(i + batch_size, total_vectors); ++j) {
                    batch.push_back(createRecord("chunk_" + std::to_string(j),
                                                 "doc_" + std::to_string(j), vectors[j]));
                }
                db.insertVectorsBatch(batch);
            }

            auto end = std::chrono::high_resolution_clock::now();
            double duration_ms = static_cast<double>(
                std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
            double throughput = total_vectors / (duration_ms / 1000.0);

            INFO("  Batch size " << batch_size << ": " << duration_ms << " ms, " << throughput
                                 << " vectors/sec");

            results.push_back({{"batch_size", batch_size},
                               {"total_vectors", total_vectors},
                               {"duration_ms", duration_ms},
                               {"throughput_vectors_per_sec", throughput}});

            CHECK(throughput > 10); // >10 vectors/sec minimum (relaxed for TSan)
        }

        fs::create_directories("bench_results");
        std::ofstream out("bench_results/hnsw_batch_insert.json");
        out << results.dump(2);
    }
}
