#include <yams/vector/simeon_embedding_backend.h>

#include <simeon/simeon.hpp>

#include <spdlog/spdlog.h>

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>

namespace yams::vector {

namespace {

simeon::NGramMode parse_ngram_mode(const char* s) {
    if (!s)
        return simeon::NGramMode::CharOnly;
    if (std::strcmp(s, "word") == 0)
        return simeon::NGramMode::WordOnly;
    if (std::strcmp(s, "both") == 0 || std::strcmp(s, "char_and_word") == 0)
        return simeon::NGramMode::CharAndWord;
    return simeon::NGramMode::CharOnly;
}

simeon::ProjectionMode parse_projection_mode(const char* s) {
    if (!s)
        return simeon::ProjectionMode::AchlioptasSparse;
    if (std::strcmp(s, "none") == 0)
        return simeon::ProjectionMode::None;
    if (std::strcmp(s, "dense_gaussian") == 0 || std::strcmp(s, "gaussian") == 0)
        return simeon::ProjectionMode::DenseGaussian;
    if (std::strcmp(s, "very_sparse") == 0)
        return simeon::ProjectionMode::VerySparse;
    if (std::strcmp(s, "achlioptas_sparse") == 0 || std::strcmp(s, "achlioptas") == 0)
        return simeon::ProjectionMode::AchlioptasSparse;
    return simeon::ProjectionMode::AchlioptasSparse;
}

const char* projection_mode_label(simeon::ProjectionMode m) noexcept {
    switch (m) {
        case simeon::ProjectionMode::None:
            return "none";
        case simeon::ProjectionMode::AchlioptasSparse:
            return "achlioptas_sparse";
        case simeon::ProjectionMode::DenseGaussian:
            return "dense_gaussian";
        case simeon::ProjectionMode::VerySparse:
            return "very_sparse";
    }
    return "achlioptas_sparse";
}

uint32_t env_u32(const char* name, uint32_t fallback) {
    const char* raw = std::getenv(name);
    if (!raw || !*raw)
        return fallback;
    try {
        return static_cast<uint32_t>(std::stoul(raw));
    } catch (...) {
        return fallback;
    }
}

simeon::EncoderConfig build_encoder_config(const EmbeddingConfig& yams_cfg) {
    simeon::EncoderConfig cfg;
    cfg.ngram_mode = parse_ngram_mode(std::getenv("YAMS_SIMEON_NGRAM_MODE"));
    cfg.ngram_min = env_u32("YAMS_SIMEON_NGRAM_MIN", 3);
    cfg.ngram_max = env_u32("YAMS_SIMEON_NGRAM_MAX", 5);
    cfg.sketch_dim = env_u32("YAMS_SIMEON_SKETCH_DIM", 4096);
    cfg.output_dim =
        env_u32("YAMS_SIMEON_OUTPUT_DIM", static_cast<uint32_t>(yams_cfg.embedding_dim));
    cfg.projection = parse_projection_mode(std::getenv("YAMS_SIMEON_PROJECTION"));
    cfg.l2_normalize = yams_cfg.normalize_embeddings;
    return cfg;
}

// One-line recipe identity for telemetry. Format:
//   <projection>_<sketch>_<output>[+pq<bytes>]
// Read at process start; YAMS_SIMEON_PQ_BYTES is reserved for the post-encode
// PQ compression tier (PQ wiring lives in the storage layer, not the encoder).
std::string compute_simeon_recipe_label() {
    const auto proj = parse_projection_mode(std::getenv("YAMS_SIMEON_PROJECTION"));
    const auto sketch = env_u32("YAMS_SIMEON_SKETCH_DIM", 4096);
    const auto out = env_u32("YAMS_SIMEON_OUTPUT_DIM", 384);
    const auto pq = env_u32("YAMS_SIMEON_PQ_BYTES", 0);
    std::string s = projection_mode_label(proj);
    s += '_';
    s += std::to_string(sketch);
    s += '_';
    s += std::to_string(out);
    if (pq > 0) {
        s += "+pq";
        s += std::to_string(pq);
    }
    return s;
}

class SimeonBackend final : public IEmbeddingBackend {
public:
    explicit SimeonBackend(const EmbeddingConfig& config) : config_(config) {}

    bool initialize() override {
        if (initialized_.load())
            return true;
        std::lock_guard<std::mutex> lock(mu_);
        if (initialized_.load())
            return true;
        try {
            encoder_ = std::make_unique<simeon::Encoder>(build_encoder_config(config_));
        } catch (const std::exception& e) {
            spdlog::error("SimeonBackend init failed: {}", e.what());
            return false;
        }
        dim_ = encoder_->output_dim();
        initialized_.store(true);
        spdlog::info("SimeonBackend initialized: dim={} tier={}", dim_,
                     simeon::simd_tier_name(simeon::active_simd_tier()));
        return true;
    }

    void shutdown() override {
        std::lock_guard<std::mutex> lock(mu_);
        encoder_.reset();
        initialized_.store(false);
        dim_ = 0;
    }

    bool isInitialized() const override { return initialized_.load(); }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        if (!initialized_.load())
            return Error{ErrorCode::NotInitialized, "SimeonBackend not initialized"};
        const auto start = std::chrono::steady_clock::now();
        std::vector<float> out(dim_, 0.0f);
        encoder_->encode(text, out.data());
        recordOne(text.size(), start);
        return out;
    }

    Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        if (!initialized_.load())
            return Error{ErrorCode::NotInitialized, "SimeonBackend not initialized"};
        const auto start = std::chrono::steady_clock::now();
        std::vector<std::vector<float>> out;
        out.reserve(texts.size());
        size_t total_bytes = 0;
        for (const auto& t : texts) {
            std::vector<float> v(dim_, 0.0f);
            encoder_->encode(t, v.data());
            total_bytes += t.size();
            out.push_back(std::move(v));
        }
        recordBatch(texts.size(), total_bytes, start);
        return out;
    }

    size_t getEmbeddingDimension() const override { return dim_; }
    size_t getMaxSequenceLength() const override { return config_.max_sequence_length; }
    std::string getBackendName() const override { return "Simeon"; }
    bool isAvailable() const override { return true; }

    GenerationStats getStats() const override { return stats_; }
    void resetStats() override {
        stats_.total_texts_processed.store(0);
        stats_.total_tokens_processed.store(0);
        stats_.total_inference_time.store(0);
        stats_.avg_inference_time.store(0);
        stats_.batch_count.store(0);
        stats_.total_batches.store(0);
        stats_.throughput_texts_per_sec.store(0.0);
        stats_.throughput_tokens_per_sec.store(0.0);
    }

private:
    void recordOne(size_t bytes, std::chrono::steady_clock::time_point start) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();
        stats_.total_texts_processed.fetch_add(1);
        stats_.total_tokens_processed.fetch_add(bytes);
        stats_.total_inference_time.fetch_add(elapsed);
        stats_.batch_count.fetch_add(1);
        stats_.total_batches.fetch_add(1);
        updateAverages();
    }

    void recordBatch(size_t count, size_t bytes, std::chrono::steady_clock::time_point start) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();
        stats_.total_texts_processed.fetch_add(count);
        stats_.total_tokens_processed.fetch_add(bytes);
        stats_.total_inference_time.fetch_add(elapsed);
        stats_.batch_count.fetch_add(1);
        stats_.total_batches.fetch_add(1);
        updateAverages();
    }

    void updateAverages() {
        const auto total_texts = stats_.total_texts_processed.load();
        if (total_texts > 0) {
            stats_.avg_inference_time.store(stats_.total_inference_time.load() /
                                            static_cast<long long>(total_texts));
        }
        stats_.updateThroughput();
    }

    EmbeddingConfig config_;
    std::unique_ptr<simeon::Encoder> encoder_;
    std::mutex mu_;
    std::atomic<bool> initialized_{false};
    size_t dim_ = 0;
    mutable GenerationStats stats_;
};

} // namespace

std::unique_ptr<IEmbeddingBackend> makeSimeonBackend(const EmbeddingConfig& config) {
    return std::make_unique<SimeonBackend>(config);
}

std::string simeonRecipeLabel() {
    return compute_simeon_recipe_label();
}

} // namespace yams::vector
