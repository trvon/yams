#include <yams/vector/simeon_embedding_backend.h>

#include <yams/daemon/components/ConfigResolver.h>

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

simeon::NGramMode parse_ngram_mode(const std::string& s) {
    if (s == "word")
        return simeon::NGramMode::WordOnly;
    if (s == "both" || s == "char_and_word")
        return simeon::NGramMode::CharAndWord;
    return simeon::NGramMode::CharOnly;
}

simeon::ProjectionMode parse_projection_mode(const std::string& s) {
    if (s == "none")
        return simeon::ProjectionMode::None;
    if (s == "sparse_jl" || s == "sparse-jl")
        return simeon::ProjectionMode::SparseJL;
    if (s == "fwht" || s == "hadamard")
        return simeon::ProjectionMode::Fwht;
    if (s == "dense_gaussian" || s == "gaussian")
        return simeon::ProjectionMode::DenseGaussian;
    if (s == "very_sparse")
        return simeon::ProjectionMode::VerySparse;
    if (s == "achlioptas_sparse" || s == "achlioptas")
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
        case simeon::ProjectionMode::SparseJL:
            return "sparse_jl";
        case simeon::ProjectionMode::Fwht:
            return "fwht";
    }
    return "achlioptas_sparse";
}

const char* ngram_mode_label(simeon::NGramMode m) noexcept {
    switch (m) {
        case simeon::NGramMode::CharOnly:
            return "char";
        case simeon::NGramMode::WordOnly:
            return "word";
        case simeon::NGramMode::CharAndWord:
            return "char_and_word";
        default:
            return "unknown";
    }
}

simeon::EncoderConfig build_encoder_config(const EmbeddingConfig& yams_cfg) {
    const auto policy = daemon::ConfigResolver::resolveSimeonEncoderPolicy();
    simeon::EncoderConfig cfg;
    cfg.ngram_mode = parse_ngram_mode(policy.ngramMode.value_or(std::string{}));
    cfg.ngram_min = policy.ngramMin.value_or(3);
    cfg.ngram_max = policy.ngramMax.value_or(5);
    cfg.sketch_dim = policy.sketchDim.value_or(4096);
    cfg.output_dim = policy.outputDim.value_or(static_cast<uint32_t>(yams_cfg.embedding_dim));
    cfg.projection = parse_projection_mode(policy.projection.value_or(std::string{}));
    cfg.l2_normalize = policy.l2Normalize.value_or(yams_cfg.normalize_embeddings);
    return cfg;
}

std::string compute_simeon_recipe_label() {
    const auto policy = daemon::ConfigResolver::resolveSimeonEncoderPolicy();
    const auto proj = parse_projection_mode(policy.projection.value_or(std::string{}));
    const auto sketch = policy.sketchDim.value_or(4096);
    const auto out = policy.outputDim.value_or(384);
    const auto pq = policy.pqBytes.value_or(0);
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
        auto enc_cfg = build_encoder_config(config_);
        spdlog::info("SimeonBackend encoder_config: ngram_mode={} ngram_min={} ngram_max={} "
                     "sketch_dim={} output_dim={} projection={} l2_normalize={}",
                     ngram_mode_label(enc_cfg.ngram_mode), enc_cfg.ngram_min, enc_cfg.ngram_max,
                     enc_cfg.sketch_dim, enc_cfg.output_dim,
                     projection_mode_label(enc_cfg.projection), enc_cfg.l2_normalize);
        try {
            encoder_ = std::make_unique<simeon::Encoder>(enc_cfg);
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
