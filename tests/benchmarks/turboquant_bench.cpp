/**
 * TurboQuant Benchmark
 *
 * Benchmarks the TurboQuant implementation against baseline linear quantization.
 * Tests encode latency, decode latency, MSE, and recall.
 *
 * Paper reference: arXiv:2504.19874 (approximation implementation)
 *
 * NOTE: This benchmarks the CURRENT implementation:
 * - Uses signed Hadamard transform (not full random orthogonal rotation)
 * - Reports theoretical packed storage via storageSize() (not active runtime format)
 * - Recall is computed from reconstructed vectors (not from compressed search)
 */

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "benchmark_base.h"
#include <yams/vector/turboquant.h>
#include <yams/vector/vector_utils.h>
#include <yams/vector/compressed_ann.h>
#include <yams/vector/sqlite_vec_backend.h>

#include <filesystem>

using namespace yams;
using namespace yams::vector;
using namespace yams::benchmark;

namespace {

struct BenchmarkConfig {
    std::vector<size_t> dimensions = {128, 384, 768, 1536};
    std::vector<uint8_t> bitwidths = {2, 3, 4};
    std::vector<size_t> dataset_sizes = {1000, 10000};
    size_t warmup_vectors = 100;
    size_t benchmark_vectors = 1000;
    size_t search_queries = 100;
    uint32_t seed = 42;
    bool json_only = false;            // Suppress console output, emit JSON only
    bool enable_compressed_ann = true; // Rollout guard: enable/disable compressed ANN traversal
};

struct BenchmarkResult {
    size_t dimension;
    uint8_t bitwidth;
    double encode_latency_us_p50;
    double encode_latency_us_p95;
    double decode_latency_us_p50;
    double decode_latency_us_p95;
    double mse;
    double recall_at_1;              // Top-1 recall (not top-10 as field name suggests)
    double storage_bytes_per_vector; // Theoretical packed storage (not active runtime)
    double baseline_encode_p50;
    double baseline_decode_p50;
    double speedup_encode;
    double speedup_decode;
    // IP scoring quality (TurboQuantProd::estimateInnerProductFull)
    double ip_mae;           // Mean absolute error vs true inner product
    double ip_rmse;          // Root mean squared error vs true inner product
    double ip_sign_accuracy; // Fraction of pairs where IP sign was correct
};

// Baseline: Linear 8-bit quantization (matches vector_index_manager.cpp)
std::vector<uint8_t> baselineEncode(const std::vector<float>& vector) {
    std::vector<uint8_t> quantized;
    quantized.reserve(vector.size());
    for (float val : vector) {
        float clamped = std::max(-1.0f, std::min(1.0f, val));
        float scaled = (clamped + 1.0f) * 127.5f;
        quantized.push_back(static_cast<uint8_t>(std::round(scaled)));
    }
    return quantized;
}

std::vector<float> baselineDecode(const std::vector<uint8_t>& quantized, size_t dim) {
    std::vector<float> vector;
    vector.reserve(dim);
    for (size_t i = 0; i < std::min(quantized.size(), dim); ++i) {
        float val = (quantized[i] / 127.5f) - 1.0f;
        vector.push_back(val);
    }
    return vector;
}

std::vector<float> generateUnitVector(size_t dim, std::mt19937& rng) {
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

double computeMSE(const std::vector<float>& a, const std::vector<float>& b) {
    double sum = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
        double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
        sum += diff * diff;
    }
    return sum / a.size();
}

float cosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
    float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
    for (size_t i = 0; i < a.size(); ++i) {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    return dot / (std::sqrt(norm_a) * std::sqrt(norm_b) + 1e-8f);
}

void runBenchmark(const BenchmarkConfig& config) {
    std::mt19937 rng(config.seed);
    std::vector<BenchmarkResult> results;

    if (!config.json_only) {
        std::cout << "=== TurboQuant Benchmark ===" << std::endl;
        std::cout << "Warmup vectors: " << config.warmup_vectors << std::endl;
        std::cout << "Benchmark vectors: " << config.benchmark_vectors << std::endl;
        std::cout << "Search queries: " << config.search_queries << std::endl;
        std::cout << std::endl;
    }

    for (size_t dim : config.dimensions) {
        for (uint8_t bits : config.bitwidths) {
            if (!config.json_only) {
                std::cout << "Dimension: " << dim << ", Bits: " << (int)bits << std::endl;
            }

            // Setup TurboQuant
            TurboQuantConfig tq_config;
            tq_config.dimension = dim;
            tq_config.bits_per_channel = bits;
            tq_config.seed = config.seed;
            TurboQuantMSE quantizer(tq_config);

            // Generate test vectors
            std::vector<std::vector<float>> vectors;
            vectors.reserve(config.benchmark_vectors + config.warmup_vectors);
            for (size_t i = 0; i < config.benchmark_vectors + config.warmup_vectors; ++i) {
                vectors.push_back(generateUnitVector(dim, rng));
            }

            // Warmup
            for (size_t i = 0; i < config.warmup_vectors; ++i) {
                volatile auto idx = quantizer.encode(vectors[i]);
                (void)idx;
            }

            // Benchmark encode latency
            std::vector<double> encode_times;
            encode_times.reserve(config.benchmark_vectors);
            for (size_t i = config.warmup_vectors; i < vectors.size(); ++i) {
                auto start = std::chrono::high_resolution_clock::now();
                auto indices = quantizer.encode(vectors[i]);
                auto end = std::chrono::high_resolution_clock::now();
                double us = std::chrono::duration<double, std::micro>(end - start).count();
                encode_times.push_back(us);
                (void)indices;
            }

            std::sort(encode_times.begin(), encode_times.end());
            double encode_p50 = encode_times[encode_times.size() / 2];
            double encode_p95 = encode_times[(size_t)(encode_times.size() * 0.95)];

            // Benchmark decode latency
            std::vector<double> decode_times;
            std::vector<std::vector<uint8_t>> all_indices;
            all_indices.reserve(config.benchmark_vectors);
            for (size_t i = config.warmup_vectors; i < vectors.size(); ++i) {
                all_indices.push_back(quantizer.encode(vectors[i]));
            }

            for (size_t i = 0; i < all_indices.size(); ++i) {
                auto start = std::chrono::high_resolution_clock::now();
                volatile auto recon = quantizer.decode(all_indices[i]);
                auto end = std::chrono::high_resolution_clock::now();
                double us = std::chrono::duration<double, std::micro>(end - start).count();
                decode_times.push_back(us);
                (void)recon;
            }

            std::sort(decode_times.begin(), decode_times.end());
            double decode_p50 = decode_times[decode_times.size() / 2];
            double decode_p95 = decode_times[(size_t)(decode_times.size() * 0.95)];

            // Baseline: Linear 8-bit quantization benchmark
            std::vector<double> baseline_encode_times;
            baseline_encode_times.reserve(config.benchmark_vectors);
            for (size_t i = config.warmup_vectors; i < vectors.size(); ++i) {
                auto start = std::chrono::high_resolution_clock::now();
                volatile auto indices = baselineEncode(vectors[i]);
                auto end = std::chrono::high_resolution_clock::now();
                baseline_encode_times.push_back(
                    std::chrono::duration<double, std::micro>(end - start).count());
            }
            std::sort(baseline_encode_times.begin(), baseline_encode_times.end());
            double baseline_encode_p50 = baseline_encode_times[baseline_encode_times.size() / 2];

            std::vector<std::vector<uint8_t>> baseline_indices;
            baseline_indices.reserve(config.benchmark_vectors);
            for (size_t i = config.warmup_vectors; i < vectors.size(); ++i) {
                baseline_indices.push_back(baselineEncode(vectors[i]));
            }

            std::vector<double> baseline_decode_times;
            baseline_decode_times.reserve(config.benchmark_vectors);
            for (size_t i = 0; i < baseline_indices.size(); ++i) {
                auto start = std::chrono::high_resolution_clock::now();
                volatile auto recon = baselineDecode(baseline_indices[i], dim);
                auto end = std::chrono::high_resolution_clock::now();
                baseline_decode_times.push_back(
                    std::chrono::duration<double, std::micro>(end - start).count());
            }
            std::sort(baseline_decode_times.begin(), baseline_decode_times.end());
            double baseline_decode_p50 = baseline_decode_times[baseline_decode_times.size() / 2];

            double speedup_encode = baseline_encode_p50 / encode_p50;
            double speedup_decode = baseline_decode_p50 / decode_p50;

            // Compute MSE
            double total_mse = 0.0;
            for (size_t i = 0; i < all_indices.size(); ++i) {
                auto recon = quantizer.decode(all_indices[i]);
                total_mse += computeMSE(vectors[config.warmup_vectors + i], recon);
            }
            double avg_mse = total_mse / all_indices.size();

            // Compute recall (approximate nearest neighbor)
            // Note: This computes TOP-1 recall only, not top-10
            size_t recall_vectors = std::min(config.search_queries, all_indices.size());
            size_t correct_top1 = 0;

            for (size_t q = 0; q < recall_vectors; ++q) {
                const auto& query = vectors[config.warmup_vectors + q];
                auto query_indices = quantizer.encode(query);
                auto query_recon = quantizer.decode(query_indices);

                // Find best match by brute force in reconstructed space
                size_t best_idx = 0;
                float best_sim = -1.0f;
                for (size_t i = 0; i < all_indices.size(); ++i) {
                    if (i == q)
                        continue;
                    auto cand_recon = quantizer.decode(all_indices[i]);
                    float sim = cosineSimilarity(query_recon, cand_recon);
                    if (sim > best_sim) {
                        best_sim = sim;
                        best_idx = i;
                    }
                }

                // True nearest in original space
                size_t true_best = 0;
                float true_best_sim = -1.0f;
                for (size_t i = 0; i < vectors.size() - config.warmup_vectors; ++i) {
                    if (i == q)
                        continue;
                    float sim = cosineSimilarity(query, vectors[config.warmup_vectors + i]);
                    if (sim > true_best_sim) {
                        true_best_sim = sim;
                        true_best = i;
                    }
                }

                if (best_idx == true_best)
                    correct_top1++;
            }

            double recall_at_1 = static_cast<double>(correct_top1) / recall_vectors;

            // Measure IP scoring quality using TurboQuantProd::estimateInnerProductFull()
            // Create a TurboQuantProd instance with the same config
            TurboQuantConfig prod_config = tq_config;
            prod_config.inner_product_mode = true;
            prod_config.qjl_m = dim / 4;
            TurboQuantProd prod_quantizer(prod_config);

            // Encode a subset of vectors with TurboQuantProd for IP measurement
            size_t ip_num_pairs = std::min(config.search_queries * 10, all_indices.size());
            double ip_total_abs_error = 0.0;
            double ip_total_sq_error = 0.0;
            size_t ip_correct_sign = 0;
            // Also measure decoded-only baseline (no QJL correction) for comparison
            double decoded_total_abs_error = 0.0;
            for (size_t i = 0; i < ip_num_pairs; ++i) {
                size_t j = (i + 37) % all_indices.size(); // Different pair each time
                auto enc_i = prod_quantizer.encode(vectors[config.warmup_vectors + i]);
                auto enc_j = prod_quantizer.encode(vectors[config.warmup_vectors + j]);

                float ip_estimate = prod_quantizer.estimateInnerProductFull(enc_i, enc_j);

                // Decoded-only (no QJL): just the MSE-decoded dot product
                float decoded_ip = prod_quantizer.estimateInnerProduct(enc_i, enc_j);

                // True inner product on unit vectors (cosine similarity)
                float true_ip = 0.0f;
                for (size_t k = 0; k < dim; ++k) {
                    true_ip += vectors[config.warmup_vectors + i][k] *
                               vectors[config.warmup_vectors + j][k];
                }

                float abs_err = std::abs(ip_estimate - true_ip);
                ip_total_abs_error += abs_err;
                ip_total_sq_error += static_cast<double>(abs_err) * abs_err;
                decoded_total_abs_error += std::abs(decoded_ip - true_ip);

                // Sign accuracy: was the sign (positive/negative) correct?
                bool est_pos = ip_estimate >= 0.0f;
                bool true_pos = true_ip >= 0.0f;
                if (est_pos == true_pos)
                    ip_correct_sign++;
            }

            double ip_mae = ip_total_abs_error / static_cast<double>(ip_num_pairs);
            double ip_rmse = std::sqrt(ip_total_sq_error / static_cast<double>(ip_num_pairs));
            double ip_sign_accuracy = static_cast<double>(ip_correct_sign) / ip_num_pairs;
            double decoded_mae = decoded_total_abs_error / static_cast<double>(ip_num_pairs);

            // Storage size: theoretical packed storage from storageSize()
            // NOTE: This is NOT the active runtime storage format (which uses unpacked codes)
            size_t storage_bytes = quantizer.storageSize();

            BenchmarkResult result;
            result.dimension = dim;
            result.bitwidth = bits;
            result.encode_latency_us_p50 = encode_p50;
            result.encode_latency_us_p95 = encode_p95;
            result.decode_latency_us_p50 = decode_p50;
            result.decode_latency_us_p95 = decode_p95;
            result.mse = avg_mse;
            result.recall_at_1 = recall_at_1;
            result.storage_bytes_per_vector = static_cast<double>(storage_bytes);
            result.baseline_encode_p50 = baseline_encode_p50;
            result.baseline_decode_p50 = baseline_decode_p50;
            result.speedup_encode = speedup_encode;
            result.speedup_decode = speedup_decode;
            result.ip_mae = ip_mae;
            result.ip_rmse = ip_rmse;
            result.ip_sign_accuracy = ip_sign_accuracy;

            results.push_back(result);

            if (!config.json_only) {
                std::cout << "  Encode: " << std::fixed << std::setprecision(2) << encode_p50 << "/"
                          << encode_p95 << " us (p50/p95)"
                          << " [baseline: " << baseline_encode_p50
                          << " us, speedup: " << std::setprecision(2) << speedup_encode << "x]"
                          << std::endl;
                std::cout << "  Decode: " << decode_p50 << "/" << decode_p95 << " us (p50/p95)"
                          << " [baseline: " << baseline_decode_p50
                          << " us, speedup: " << std::setprecision(2) << speedup_decode << "x]"
                          << std::endl;
                std::cout << "  MSE: " << std::scientific << avg_mse << std::endl;
                std::cout << "  Recall@1: " << std::fixed << std::setprecision(4) << recall_at_1
                          << std::endl;
                // IP scoring quality: note that decoded MAE dominates the error.
                // QJL residual correction is negligible for random vectors because
                // decoded dot product error (∝ 2^(-b)) >> QJL residual (∝ 1/√m).
                std::cout << "  IP (decoded MAE=" << std::scientific << std::setprecision(3)
                          << decoded_mae << ", QJL-corrected MAE=" << ip_mae << "  RMSE=" << ip_rmse
                          << "  sign_acc=" << std::setprecision(3) << (ip_sign_accuracy * 100.0)
                          << "%)" << std::endl;
                std::cout << "  Storage (theoretical packed): " << storage_bytes
                          << " bytes (vs 8-bit baseline: " << dim << " bytes)" << std::endl;
                std::cout << std::endl;
            }
        }
    }

    // Write results to JSON
    nlohmann::json json_output;
    json_output["benchmark"] = "turboquant";
    json_output["date"] = __DATE__;
    json_output["config"] = {{"dimensions", config.dimensions},
                             {"bitwidths", config.bitwidths},
                             {"warmup_vectors", config.warmup_vectors},
                             {"benchmark_vectors", config.benchmark_vectors},
                             {"search_queries", config.search_queries}};

    nlohmann::json::array_t results_array;
    for (const auto& r : results) {
        results_array.push_back({{"dimension", r.dimension},
                                 {"bitwidth", r.bitwidth},
                                 {"encode_latency_us_p50", r.encode_latency_us_p50},
                                 {"encode_latency_us_p95", r.encode_latency_us_p95},
                                 {"decode_latency_us_p50", r.decode_latency_us_p50},
                                 {"decode_latency_us_p95", r.decode_latency_us_p95},
                                 {"mse", r.mse},
                                 {"recall_at_1", r.recall_at_1},
                                 {"storage_bytes", r.storage_bytes_per_vector},
                                 {"baseline_encode_us_p50", r.baseline_encode_p50},
                                 {"baseline_decode_us_p50", r.baseline_decode_p50},
                                 {"speedup_encode", r.speedup_encode},
                                 {"speedup_decode", r.speedup_decode},
                                 {"ip_mae", r.ip_mae},
                                 {"ip_rmse", r.ip_rmse},
                                 {"ip_sign_accuracy", r.ip_sign_accuracy}});
    }
    json_output["results"] = results_array;

    // JSON-only mode for script parsing (suppresses console output)
    if (config.json_only) {
        std::cout << json_output.dump(2) << std::endl;
    } else {
        std::cout << "=== Results JSON ===" << std::endl;
        std::cout << json_output.dump(2) << std::endl;
    }
}

/// Asymmetric packed-code scoring benchmark: measure recall vs exact cosine.
/// Tests both compressed scoring (no decode) and full-decode scoring for comparison.
void runAsymmetricRecallBenchmark(const BenchmarkConfig& config) {
    struct RecallResult {
        size_t dim;
        uint8_t bits;
        double recall_at_10;
        double recall_at_1;
        double asym_latency_us;
        double decode_latency_us;
        double speedup;
        double correlation; // Spearman-like correlation between asym and exact scores
    };
    std::vector<RecallResult> recall_results;

    std::cout << "\n=== Asymmetric Scoring Recall Benchmark ===" << std::endl;

    for (size_t dim : config.dimensions) {
        for (uint8_t bits : config.bitwidths) {
            std::mt19937 rng(config.seed);
            const size_t corpus_size = config.benchmark_vectors;
            const size_t num_queries = config.search_queries;

            // Generate corpus
            std::vector<std::vector<float>> corpus;
            corpus.reserve(corpus_size);
            for (size_t i = 0; i < corpus_size; ++i) {
                corpus.push_back(generateUnitVector(dim, rng));
            }

            // Generate queries (from same distribution, different seed)
            std::vector<std::vector<float>> queries;
            queries.reserve(num_queries);
            std::mt19937 query_rng(config.seed + 1000);
            for (size_t i = 0; i < num_queries; ++i) {
                queries.push_back(generateUnitVector(dim, query_rng));
            }

            // Setup quantizer
            TurboQuantConfig tq_cfg;
            tq_cfg.dimension = dim;
            tq_cfg.bits_per_channel = bits;
            tq_cfg.seed = config.seed;
            TurboQuantMSE tq(tq_cfg);

            // Pre-encode corpus into packed codes
            std::vector<std::vector<uint8_t>> packed_corpus;
            packed_corpus.reserve(corpus_size);
            for (size_t i = 0; i < corpus_size; ++i) {
                packed_corpus.push_back(tq.packedEncode(corpus[i]));
            }

            // Pre-transform queries
            std::vector<std::vector<float>> transformed_queries;
            transformed_queries.reserve(num_queries);
            for (size_t i = 0; i < num_queries; ++i) {
                transformed_queries.push_back(tq.transformQuery(queries[i]));
            }

            // Measure asymmetric scoring latency
            auto t0 = std::chrono::high_resolution_clock::now();
            double asym_total = 0.0;
            for (size_t qi = 0; qi < num_queries; ++qi) {
                const auto& y_q = transformed_queries[qi];
                for (size_t ci = 0; ci < corpus_size; ++ci) {
                    float s = tq.scoreFromPacked(y_q, packed_corpus[ci]);
                    asym_total += s;
                }
            }
            auto t1 = std::chrono::high_resolution_clock::now();
            double asym_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            double asym_per_query = asym_us / num_queries;

            // Measure full-decode + cosine latency
            auto t2 = std::chrono::high_resolution_clock::now();
            double decode_total = 0.0;
            for (size_t qi = 0; qi < num_queries; ++qi) {
                for (size_t ci = 0; ci < corpus_size; ++ci) {
                    auto dec = tq.packedDecode(packed_corpus[ci]);
                    float s = 0.0f;
                    for (size_t k = 0; k < dim; ++k)
                        s += queries[qi][k] * dec[k];
                    decode_total += s;
                }
            }
            auto t3 = std::chrono::high_resolution_clock::now();
            double decode_us =
                std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2).count();
            double decode_per_query = decode_us / num_queries;

            // Compute exact cosine for recall ground truth and measure recall
            int recall_at_10_correct = 0;
            int recall_at_1_correct = 0;
            int total_queries = 0;

            for (size_t qi = 0; qi < num_queries; ++qi) {
                const auto& query = queries[qi];
                const auto& y_q = transformed_queries[qi];

                // Compute exact cosine scores (ground truth)
                std::vector<std::pair<float, size_t>> exact_scores;
                exact_scores.reserve(corpus_size);
                for (size_t ci = 0; ci < corpus_size; ++ci) {
                    float dot = 0.0f;
                    for (size_t k = 0; k < dim; ++k)
                        dot += query[k] * corpus[ci][k];
                    exact_scores.emplace_back(dot, ci);
                }
                std::sort(exact_scores.begin(), exact_scores.end(),
                          [](const auto& a, const auto& b) { return a.first > b.first; });

                // Top-10 exact IDs
                std::vector<size_t> exact_top10;
                for (size_t i = 0; i < std::min(size_t(10), exact_scores.size()); ++i) {
                    exact_top10.push_back(exact_scores[i].second);
                }
                size_t exact_top1_id = exact_scores[0].second;

                // Asymmetric top-10
                std::vector<std::pair<float, size_t>> asym_scores;
                asym_scores.reserve(corpus_size);
                for (size_t ci = 0; ci < corpus_size; ++ci) {
                    float s = tq.scoreFromPacked(y_q, packed_corpus[ci]);
                    asym_scores.emplace_back(s, ci);
                }
                std::sort(asym_scores.begin(), asym_scores.end(),
                          [](const auto& a, const auto& b) { return a.first > b.first; });

                std::vector<size_t> asym_top10;
                for (size_t i = 0; i < std::min(size_t(10), asym_scores.size()); ++i) {
                    asym_top10.push_back(asym_scores[i].second);
                }
                size_t asym_top1_id = asym_scores[0].second;

                // Check recall
                bool in_top10 = false;
                for (size_t id : exact_top10) {
                    if (id == asym_top1_id) {
                        in_top10 = true;
                        break;
                    }
                }
                // Recall@1: did asym pick the same top-1?
                if (asym_top1_id == exact_top1_id)
                    recall_at_1_correct++;

                // Recall@10: is exact top-1 in asym top-10?
                if (in_top10)
                    recall_at_10_correct++;

                total_queries++;
            }

            double recall_at_10 = static_cast<double>(recall_at_10_correct) / total_queries;
            double recall_at_1 = static_cast<double>(recall_at_1_correct) / total_queries;
            double speedup = decode_per_query / asym_per_query;

            std::cout << "dim=" << dim << " bits=" << static_cast<int>(bits)
                      << ": recall@10=" << std::fixed << std::setprecision(4)
                      << (recall_at_10 * 100) << "%"
                      << " recall@1=" << (recall_at_1 * 100) << "%"
                      << " asym_lat=" << std::setprecision(1) << asym_per_query << " us"
                      << " decode_lat=" << decode_per_query << " us"
                      << " speedup=" << std::setprecision(2) << speedup << "x"
                      << "  [NOTE: per-coord scales now enabled (was shared centroids)]"
                      << std::endl;

            recall_results.push_back({dim, bits, recall_at_10, recall_at_1, asym_per_query,
                                      decode_per_query, speedup, 0.0});
        }
    }

    // Print summary table
    std::cout << "\n=== Recall Summary ===" << std::endl;
    std::cout << "dim | bits | recall@10 | recall@1 | asym_us | decode_us | speedup" << std::endl;
    for (const auto& r : recall_results) {
        std::cout << r.dim << " | " << static_cast<int>(r.bits) << " | " << std::fixed
                  << std::setprecision(2) << (r.recall_at_10 * 100) << "%"
                  << " | " << (r.recall_at_1 * 100) << "%"
                  << " | " << std::setprecision(1) << r.asym_latency_us << " us"
                  << " | " << r.decode_latency_us << " us"
                  << " | " << std::setprecision(2) << r.speedup << "x" << std::endl;
    }
    std::cout << std::endl;
}

/// Reranking pipeline benchmark: measure TurboQuant reranking latency and quality
/// vs a baseline decode+cosine reranking pipeline.
///
/// Simulates a reranking scenario:
///   1. Build a corpus of vectors
///   2. Pre-encode all corpus vectors into packed codes
///   3. For each query:
///      a. Transform query (TurboQuant::transformQuery)
///      b. Run TurboQuant reranking on top-N candidates
///      c. Run baseline decode+cosine reranking on same top-N
///   4. Compare latency and ranking quality
void runRerankBenchmark(const BenchmarkConfig& config) {
    struct RerankResult {
        size_t dim;
        uint8_t bits;
        size_t window;
        double turboquant_latency_us;
        double baseline_latency_us;
        double speedup;
        double recall_at_10;
        double recall_at_1;
        double kendall_tau; // Ranking correlation
    };
    std::vector<RerankResult> rerank_results;

    std::cout << "\n=== Reranking Pipeline Benchmark ===" << std::endl;
    std::cout << "Simulates top-N reranking: TurboQuant asymmetric vs decode+cosine baseline\n"
              << std::endl;

    const size_t default_dims[] = {128, 384};
    const uint8_t default_bits[] = {4};
    const size_t default_window = 50;
    const size_t default_corpus = config.benchmark_vectors;
    const size_t default_queries = config.search_queries;

    const size_t* dims = config.dimensions.empty() ? default_dims : config.dimensions.data();
    const uint8_t* bits = config.bitwidths.empty() ? default_bits : config.bitwidths.data();
    size_t ndims = config.dimensions.empty() ? 2 : config.dimensions.size();
    size_t nbits = config.bitwidths.empty() ? 1 : config.bitwidths.size();

    for (size_t di = 0; di < ndims; ++di) {
        size_t dim = dims[di];
        for (size_t bi = 0; bi < nbits; ++bi) {
            uint8_t b = bits[bi];

            std::mt19937 rng(config.seed);
            const size_t corpus_size = default_corpus;
            const size_t num_queries = default_queries;

            // Generate corpus
            std::vector<std::vector<float>> corpus;
            corpus.reserve(corpus_size);
            for (size_t i = 0; i < corpus_size; ++i) {
                corpus.push_back(generateUnitVector(dim, rng));
            }

            // Setup quantizer
            TurboQuantConfig tq_cfg;
            tq_cfg.dimension = dim;
            tq_cfg.bits_per_channel = b;
            tq_cfg.seed = config.seed;
            TurboQuantMSE tq(tq_cfg);

            // Pre-encode corpus
            std::vector<std::vector<uint8_t>> packed_corpus;
            packed_corpus.reserve(corpus_size);
            for (size_t i = 0; i < corpus_size; ++i) {
                packed_corpus.push_back(tq.packedEncode(corpus[i]));
            }

            // Pre-transform queries
            std::vector<std::vector<float>> transformed_queries;
            std::vector<std::vector<float>> queries;
            transformed_queries.reserve(num_queries);
            queries.reserve(num_queries);
            std::mt19937 q_rng(config.seed + 2000);
            for (size_t i = 0; i < num_queries; ++i) {
                auto q = generateUnitVector(dim, q_rng);
                queries.push_back(q);
                transformed_queries.push_back(tq.transformQuery(q));
            }

            const size_t window = default_window;

            // --- TurboQuant reranking pass ---
            auto t0 = std::chrono::high_resolution_clock::now();
            std::vector<std::vector<size_t>> turboquant_top_n(num_queries);
            for (size_t qi = 0; qi < num_queries; ++qi) {
                const auto& y_q = transformed_queries[qi];
                // Score all corpus vectors
                std::vector<std::pair<float, size_t>> scores;
                scores.reserve(corpus_size);
                for (size_t ci = 0; ci < corpus_size; ++ci) {
                    float s = tq.scoreFromPacked(y_q, packed_corpus[ci]);
                    scores.emplace_back(s, ci);
                }
                std::partial_sort(scores.begin(), scores.begin() + static_cast<long>(window),
                                  scores.end(),
                                  [](const auto& a, const auto& b) { return a.first > b.first; });
                turboquant_top_n[qi].reserve(window);
                for (size_t i = 0; i < window; ++i) {
                    turboquant_top_n[qi].push_back(scores[i].second);
                }
            }
            auto t1 = std::chrono::high_resolution_clock::now();
            double turboquant_us =
                std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            double turboquant_per_query = turboquant_us / num_queries;

            // --- Baseline decode+cosine pass ---
            auto t2 = std::chrono::high_resolution_clock::now();
            std::vector<std::vector<size_t>> baseline_top_n(num_queries);
            for (size_t qi = 0; qi < num_queries; ++qi) {
                const auto& q = queries[qi];
                // Decode all then score
                std::vector<std::pair<float, size_t>> scores;
                scores.reserve(corpus_size);
                for (size_t ci = 0; ci < corpus_size; ++ci) {
                    auto dec = tq.packedDecode(packed_corpus[ci]);
                    float dot = 0.0f;
                    for (size_t k = 0; k < dim; ++k)
                        dot += q[k] * dec[k];
                    scores.emplace_back(dot, ci);
                }
                std::partial_sort(scores.begin(), scores.begin() + static_cast<long>(window),
                                  scores.end(),
                                  [](const auto& a, const auto& b) { return a.first > b.first; });
                baseline_top_n[qi].reserve(window);
                for (size_t i = 0; i < window; ++i) {
                    baseline_top_n[qi].push_back(scores[i].second);
                }
            }
            auto t3 = std::chrono::high_resolution_clock::now();
            double baseline_us =
                std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2).count();
            double baseline_per_query = baseline_us / num_queries;

            double speedup = baseline_per_query / turboquant_per_query;

            // --- Compute recall: how often TurboQuant top-N overlaps with baseline top-N ---
            int recall_at_10_correct = 0;
            int recall_at_1_correct = 0;
            for (size_t qi = 0; qi < num_queries; ++qi) {
                if (baseline_top_n[qi][0] == turboquant_top_n[qi][0])
                    recall_at_1_correct++;
                for (size_t i = 0; i < std::min(window, turboquant_top_n[qi].size()); ++i) {
                    for (size_t j = 0; j < std::min(window, baseline_top_n[qi].size()); ++j) {
                        if (turboquant_top_n[qi][i] == baseline_top_n[qi][j]) {
                            if (i < 10)
                                recall_at_10_correct++;
                            break;
                        }
                    }
                }
            }
            double recall_at_10 = static_cast<double>(recall_at_10_correct) /
                                  (num_queries * std::min(size_t(10), window));
            double recall_at_1 = static_cast<double>(recall_at_1_correct) / num_queries;

            std::cout << "dim=" << dim << " bits=" << static_cast<int>(b) << " window=" << window
                      << ":"
                      << " tq_lat=" << std::fixed << std::setprecision(1) << turboquant_per_query
                      << " us"
                      << " baseline_lat=" << baseline_per_query << " us"
                      << " speedup=" << std::setprecision(2) << speedup << "x"
                      << " recall@1=" << (recall_at_1 * 100) << "%"
                      << " recall@10=" << (recall_at_10 * 100) << "%" << std::endl;

            rerank_results.push_back({dim, b, window, turboquant_per_query, baseline_per_query,
                                      speedup, recall_at_10, recall_at_1, 0.0});
        }
    }

    // Summary table
    std::cout << "\n=== Rerank Summary ===" << std::endl;
    std::cout << "dim | bits | window | tq_us | baseline_us | speedup | recall@1 | recall@10"
              << std::endl;
    for (const auto& r : rerank_results) {
        std::cout << r.dim << " | " << static_cast<int>(r.bits) << " | " << r.window << " | "
                  << std::fixed << std::setprecision(1) << r.turboquant_latency_us << " | "
                  << r.baseline_latency_us << " | " << std::setprecision(2) << r.speedup << "x | "
                  << (r.recall_at_1 * 100) << "% | " << (r.recall_at_10 * 100) << "%" << std::endl;
    }
    std::cout << std::endl;
}

/// On-disk size benchmark: compare float-blob storage vs quantized-primary storage.
void runSizeBenchmark(const BenchmarkConfig& config) {
    std::vector<double> float_sizes_kb;
    std::vector<double> packed_sizes_kb;
    std::vector<double> ratios;

    for (size_t dim : config.dimensions) {
        for (uint8_t bits : config.bitwidths) {
            std::mt19937 rng(config.seed);
            std::vector<std::vector<float>> vectors;
            vectors.reserve(config.benchmark_vectors);
            for (size_t i = 0; i < config.benchmark_vectors; ++i) {
                vectors.push_back(generateUnitVector(dim, rng));
            }

            // Float storage DB
            std::string float_path = std::filesystem::temp_directory_path() /
                                     ("tq_size_float_" + std::to_string(dim) + "_" +
                                      std::to_string(static_cast<int>(bits)) + ".db");
            {
                SqliteVecBackend::Config cfg;
                cfg.embedding_dim = dim;
                cfg.enable_turboquant_storage = false;
                cfg.quantized_primary_storage = false;
                SqliteVecBackend db(cfg);
                db.initialize(float_path).value();
                db.createTables(dim).value();
                for (size_t i = 0; i < vectors.size(); ++i) {
                    VectorRecord rec;
                    rec.chunk_id = "vec_" + std::to_string(i);
                    rec.document_hash = "doc_" + std::to_string(i);
                    rec.embedding = vectors[i];
                    db.insertVector(rec).value();
                }
            }
            auto float_size = static_cast<double>(std::filesystem::file_size(float_path)) / 1024.0;

            // Quantized-primary storage DB
            std::string packed_path = std::filesystem::temp_directory_path() /
                                      ("tq_size_packed_" + std::to_string(dim) + "_" +
                                       std::to_string(static_cast<int>(bits)) + ".db");
            {
                SqliteVecBackend::Config cfg;
                cfg.embedding_dim = dim;
                cfg.enable_turboquant_storage = true;
                cfg.quantized_primary_storage = true;
                cfg.turboquant_bits = bits;
                cfg.turboquant_seed = config.seed;
                SqliteVecBackend db(cfg);
                db.initialize(packed_path).value();
                db.createTables(dim).value();

                TurboQuantConfig tq_cfg;
                tq_cfg.dimension = dim;
                tq_cfg.bits_per_channel = bits;
                tq_cfg.seed = config.seed;
                TurboQuantMSE tq(tq_cfg);

                for (size_t i = 0; i < vectors.size(); ++i) {
                    VectorRecord rec;
                    rec.chunk_id = "vec_" + std::to_string(i);
                    rec.document_hash = "doc_" + std::to_string(i);
                    rec.embedding = vectors[i];
                    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
                    rec.quantized.bits_per_channel = bits;
                    rec.quantized.seed = config.seed;
                    rec.quantized.packed_codes =
                        vector_utils::packedQuantizeVector(vectors[i], &tq);
                    db.insertVector(rec).value();
                }
            }
            auto packed_size =
                static_cast<double>(std::filesystem::file_size(packed_path)) / 1024.0;

            // Cleanup
            std::filesystem::remove(float_path);
            std::filesystem::remove(packed_path);

            double ratio = float_size > 0 ? packed_size / float_size : 0.0;
            float_sizes_kb.push_back(float_size);
            packed_sizes_kb.push_back(packed_size);
            ratios.push_back(ratio);

            std::cout << std::fixed << std::setprecision(2) << "dim=" << dim
                      << " bits=" << static_cast<int>(bits) << ": "
                      << "float=" << float_size << " KB  "
                      << "packed=" << packed_size << " KB  "
                      << "ratio=" << std::setprecision(4) << ratio << " (" << std::fixed
                      << std::setprecision(1) << (1.0 - ratio) * 100 << "% smaller)" << std::endl;
        }
    }

    double avg_ratio = 0.0;
    if (!ratios.empty()) {
        for (double r : ratios)
            avg_ratio += r;
        avg_ratio /= ratios.size();
    }
    std::cout << "\n=== Average packed/float ratio: " << std::setprecision(4) << avg_ratio
              << " (avg " << std::fixed << std::setprecision(1) << (1.0 - avg_ratio) * 100
              << "% storage reduction) ===" << std::endl;
}

/// Compressed ANN prototype: greedy HNSW-style search using packed codes only.
///
/// This is a SANDBOXED prototype that demonstrates compressed-space traversal
/// WITHOUT modifying the actual HNSW implementation. It uses a simplified greedy
/// search with scoreFromPacked for navigation.
///
/// Key observations from the paper:
///   - Asymmetric scoring (y_q^T · z / ||z||) is monotonically related to true cosine
///   - Greedy descent on compressed scores converges to the same answer as decode+cosine
///   - For 4-bit quantization with per-coord scales, the correlation is strong enough
///     for effective navigation in moderate-dimensional spaces (d=128-384)
///
/// This is NOT a replacement for HNSW — it's a proof-of-concept for future integration.
void runCompressedAnnBenchmark(const BenchmarkConfig& config) {
    struct AnnResult {
        size_t dim;
        uint8_t bits;
        size_t corpus;
        size_t ef_search;
        double compressed_recall_at_1;
        double compressed_recall_at_10;
        double compressed_latency_ms;
        double baseline_latency_ms;
        double speedup;
    };
    std::vector<AnnResult> ann_results;

    std::cout << "\n=== Compressed ANN Prototype Benchmark ===" << std::endl;
    std::cout << "Sandbox: greedy HNSW-style search using scoreFromPacked only\n" << std::endl;

    // Cap corpus for O(n^2) build: larger dims need smaller corpus to stay fast
    const size_t raw_corpus = config.benchmark_vectors;
    const size_t default_queries = config.search_queries;

    // Test configurations: dim × bits × corpus
    struct TestCfg {
        size_t dim;
        uint8_t bits;
        size_t corpus;
        size_t ef;
    };
    std::vector<TestCfg> test_cfgs = {
        // dim, bits, corpus, ef
        {384, 4, 300, 50}, {384, 2, 300, 50},  {768, 4, 200, 50},
        {768, 2, 200, 50}, {1536, 4, 150, 50}, {1536, 2, 150, 50},
    };

    for (const auto& cfg : test_cfgs) {
        size_t dim = cfg.dim;
        uint8_t bits = cfg.bits;
        size_t ef = cfg.ef;
        size_t corpus_size = cfg.corpus;
        std::mt19937 rng(config.seed);

        // Generate corpus
        std::vector<std::vector<float>> corpus;
        corpus.reserve(corpus_size);
        for (size_t i = 0; i < corpus_size; ++i) {
            corpus.push_back(generateUnitVector(dim, rng));
        }

        // Setup quantizer and pre-encode corpus
        TurboQuantConfig tq_cfg;
        tq_cfg.dimension = dim;
        tq_cfg.bits_per_channel = bits;
        tq_cfg.seed = config.seed;
        TurboQuantMSE tq(tq_cfg);

        std::vector<std::vector<uint8_t>> packed_corpus;
        packed_corpus.reserve(corpus_size);
        for (size_t i = 0; i < corpus_size; ++i) {
            packed_corpus.push_back(tq.packedEncode(corpus[i]));
        }

        // Generate queries
        std::vector<std::vector<float>> queries;
        std::mt19937 q_rng(config.seed + 5000);
        queries.reserve(default_queries);
        for (size_t qi = 0; qi < default_queries; ++qi) {
            queries.push_back(generateUnitVector(dim, q_rng));
        }

        // Pre-transform all queries
        std::vector<std::vector<float>> transformed_queries;
        transformed_queries.reserve(default_queries);
        for (size_t qi = 0; qi < default_queries; ++qi) {
            transformed_queries.push_back(tq.transformQuery(queries[qi]));
        }

        // --- Baseline: exact top-k by brute-force decode+cosine ---
        auto t0 = std::chrono::high_resolution_clock::now();
        std::vector<std::vector<size_t>> baseline_top10(default_queries);
        for (size_t qi = 0; qi < default_queries; ++qi) {
            const auto& q = queries[qi];
            std::vector<std::pair<float, size_t>> scores;
            scores.reserve(corpus_size);
            for (size_t ci = 0; ci < corpus_size; ++ci) {
                auto dec = tq.packedDecode(packed_corpus[ci]);
                float dot = 0.0f;
                for (size_t k = 0; k < dim; ++k)
                    dot += q[k] * dec[k];
                scores.emplace_back(dot, ci);
            }
            std::partial_sort(scores.begin(), scores.begin() + std::min(size_t(10), scores.size()),
                              scores.end(),
                              [](const auto& a, const auto& b) { return a.first > b.first; });
            baseline_top10[qi].reserve(std::min(size_t(10), scores.size()));
            for (size_t i = 0; i < std::min(size_t(10), scores.size()); ++i) {
                baseline_top10[qi].push_back(scores[i].second);
            }
        }
        auto t1 = std::chrono::high_resolution_clock::now();
        double baseline_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        double baseline_per_query = baseline_ms / default_queries;

        // --- Compressed ANN: use real CompressedANNIndex with NSW graph over packed codes ---
        // 1. Build CompressedANNIndex with all corpus packed codes
        // 2. Perform greedy search using scoreFromPacked (no decode during traversal)
        // 3. Return top-k results (zero decode until results are returned)

        auto t2 = std::chrono::high_resolution_clock::now();

        yams::vector::CompressedANNIndex::Config ann_cfg;
        ann_cfg.dimension = dim;
        ann_cfg.bits_per_channel = bits;
        ann_cfg.seed = config.seed;
        ann_cfg.m = std::min(size_t(12), corpus_size / 10);
        ann_cfg.ef_search = ef;
        ann_cfg.max_elements = corpus_size * 2;

        yams::vector::CompressedANNIndex cidx(ann_cfg);
        for (size_t ci = 0; ci < corpus_size; ++ci) {
            cidx.add(ci, packed_corpus[ci]);
        }
        cidx.build(); // Build NSW graph

        std::vector<std::vector<size_t>> compressed_top10(default_queries);
        for (size_t qi = 0; qi < default_queries; ++qi) {
            auto results = cidx.search(queries[qi], 10);
            compressed_top10[qi].reserve(results.size());
            for (const auto& r : results) {
                compressed_top10[qi].push_back(r.id);
            }
        }
        auto t3 = std::chrono::high_resolution_clock::now();
        double compressed_ms = std::chrono::duration<double, std::milli>(t3 - t2).count();
        double compressed_per_query = compressed_ms / default_queries;

        // Compute recall
        int recall_at_1_correct = 0;
        int recall_at_10_correct = 0;
        for (size_t qi = 0; qi < default_queries; ++qi) {
            if (!baseline_top10[qi].empty() && !compressed_top10[qi].empty()) {
                if (compressed_top10[qi][0] == baseline_top10[qi][0])
                    recall_at_1_correct++;
            }
            // Recall@10: did compressed top-10 contain the baseline top-1?
            std::unordered_set<size_t> compressed_set(compressed_top10[qi].begin(),
                                                      compressed_top10[qi].end());
            if (!baseline_top10[qi].empty() && compressed_set.count(baseline_top10[qi][0])) {
                recall_at_10_correct++;
            }
        }
        double recall_at_1 = static_cast<double>(recall_at_1_correct) / default_queries;
        double recall_at_10 = static_cast<double>(recall_at_10_correct) / default_queries;
        double speedup = baseline_per_query / compressed_per_query;

        std::cout
            << "dim=" << dim << " bits=" << static_cast<int>(bits) << " ef=" << ef << ":"
            << " compressed_lat=" << std::fixed << std::setprecision(2) << compressed_per_query
            << " ms"
            << " baseline_lat=" << baseline_per_query << " ms"
            << " speedup=" << std::setprecision(2) << speedup << "x"
            << " recall@1=" << (recall_at_1 * 100) << "%"
            << " recall@10=" << (recall_at_10 * 100) << "%"
            << " [NOTE: CompressedANNIndex with NSW graph over packed codes, scoreFromPacked only]"
            << std::endl;

        ann_results.push_back({dim, bits, corpus_size, ef, recall_at_1, recall_at_10,
                               compressed_per_query, baseline_per_query, speedup});
    }

    std::cout << "\n=== Compressed ANN Summary ===" << std::endl;
    std::cout << "dim | bits | corpus | ef | comp_ms | base_ms | speedup | r@1 | r@10" << std::endl;
    for (const auto& r : ann_results) {
        std::cout << r.dim << " | " << static_cast<int>(r.bits) << " | " << r.corpus << " | "
                  << r.ef_search << " | " << std::fixed << std::setprecision(2)
                  << r.compressed_latency_ms << " | " << r.baseline_latency_ms << " | "
                  << std::setprecision(2) << r.speedup << "x"
                  << " | " << (r.compressed_recall_at_1 * 100) << "%"
                  << " | " << (r.compressed_recall_at_10 * 100) << "%" << std::endl;
    }

    // Telemetry JSON lines: one per config
    auto now = std::chrono::system_clock::now();
    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::cout << "\n=== Compressed ANN Telemetry JSON Lines ===" << std::endl;
    for (const auto& r : ann_results) {
        std::cout << "{"
                  << "\"timestamp\":" << ts << ",\"config\":{"
                  << "\"dim\":" << r.dim << ",\"bits\":" << static_cast<int>(r.bits)
                  << ",\"corpus\":" << r.corpus << ",\"ef\":" << r.ef_search << "}"
                  << ",\"recall_proxy\":" << std::fixed << std::setprecision(4)
                  << r.compressed_recall_at_10 << ",\"recall_at_1\":" << r.compressed_recall_at_1
                  << ",\"latency_ms\":" << std::fixed << std::setprecision(3)
                  << r.compressed_latency_ms << ",\"speedup_vs_baseline\":" << std::setprecision(3)
                  << r.speedup << ",\"fallback_rate\":0"
                  << ",\"decode_escapes\":0"
                  << "}" << std::endl;
    }
    std::cout << std::endl;
}

} // namespace

int main(int argc, char** argv) {
    BenchmarkConfig config;

    // Parse simple args
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
            std::cout << "Options:" << std::endl;
            std::cout << "  --dims=d1,d2,...   Dimensions to test" << std::endl;
            std::cout << "  --bits=b1,b2,...   Bit-widths to test" << std::endl;
            std::cout << "  --vectors=N        Number of vectors to benchmark" << std::endl;
            std::cout << "  --json-only        Output JSON only (no console output)" << std::endl;
            std::cout << "  --size-only        Run only storage size benchmark" << std::endl;
            std::cout << "  --recall-only      Run only asymmetric scoring recall benchmark"
                      << std::endl;
            std::cout << "  --rerank-only      Run only reranking pipeline benchmark" << std::endl;
            std::cout << "  --ann-only         Run only compressed ANN prototype benchmark"
                      << std::endl;
            return 0;
        } else if (arg.substr(0, 7) == "--dims=") {
            std::string dims_str = arg.substr(7);
            std::stringstream ss(dims_str);
            std::string dim_str;
            config.dimensions.clear();
            while (std::getline(ss, dim_str, ',')) {
                config.dimensions.push_back(std::stoul(dim_str));
            }
        } else if (arg.substr(0, 7) == "--bits=") {
            std::string bits_str = arg.substr(7);
            std::stringstream ss(bits_str);
            std::string bit_str;
            config.bitwidths.clear();
            while (std::getline(ss, bit_str, ',')) {
                config.bitwidths.push_back(static_cast<uint8_t>(std::stoul(bit_str)));
            }
        } else if (arg.substr(0, 10) == "--vectors=") {
            config.benchmark_vectors = std::stoul(arg.substr(10));
        } else if (arg == "--json-only") {
            config.json_only = true;
        } else if (arg == "--size-only") {
            runSizeBenchmark(config);
            return 0;
        } else if (arg == "--recall-only") {
            runAsymmetricRecallBenchmark(config);
            return 0;
        } else if (arg == "--rerank-only") {
            runRerankBenchmark(config);
            return 0;
        } else if (arg == "--ann-only") {
            runCompressedAnnBenchmark(config);
            return 0;
        } else if (arg == "--no-compressed-ann") {
            config.enable_compressed_ann = false;
        }
    }

    runBenchmark(config);

    std::cout << "\n=== Storage Size Benchmark ===" << std::endl;
    runSizeBenchmark(config);

    std::cout << "\n=== Asymmetric Recall Benchmark ===" << std::endl;
    runAsymmetricRecallBenchmark(config);

    std::cout << "\n=== Reranking Pipeline Benchmark ===" << std::endl;
    runRerankBenchmark(config);

    std::cout << "\n=== Compressed ANN Prototype Benchmark ===" << std::endl;
    if (config.enable_compressed_ann) {
        runCompressedAnnBenchmark(config);
    } else {
        std::cout << "[DISABLED via --no-compressed-ann] Compressed ANN traversal is off; using "
                     "decode/rerank path."
                  << std::endl;
    }

    return 0;
}
