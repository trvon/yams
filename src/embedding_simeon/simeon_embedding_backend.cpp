#include <yams/vector/simeon_embedding_backend.h>

#include <yams/daemon/components/ConfigResolver.h>
#include <yams/vector/embedding_generator.h>

#include <simeon/simeon.hpp>
#include <simeon/static_embedding.hpp>
#include <yams/config/config_helpers.h>
#include <yams/config/static_embedding_table.h>

#include <spdlog/spdlog.h>

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <map>
#include <mutex>
#include <string>

namespace yams::vector {

namespace {

simeon::NGramMode parse_ngram_mode(const std::string& s) {
    if (s == "char")
        return simeon::NGramMode::CharOnly;
    if (s == "word")
        return simeon::NGramMode::WordOnly;
    if (s == "both" || s == "char_and_word")
        return simeon::NGramMode::CharAndWord;
    return simeon::NGramMode::CharAndWord;
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
    return simeon::ProjectionMode::Fwht;
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

struct ResolvedSimeonEncoder {
    simeon::EncoderConfig config;
    std::string spaceIdentity;
    // Set for encoder_profile = "static": encode with this table instead of the hash encoder.
    std::shared_ptr<const simeon::StaticEmbedding> staticTable;
    // The static table was wanted but not loaded; this hash space is a stand-in.
    bool fallback{false};
};

struct CachedStaticTable {
    std::shared_ptr<const simeon::StaticEmbedding> table;
    std::string identity;
};

std::mutex& staticTableMutex() {
    static std::mutex mu;
    return mu;
}

std::map<std::string, CachedStaticTable>& staticTableCache() {
    static std::map<std::string, CachedStaticTable> cache;
    return cache;
}

const CachedStaticTable* cachedStaticTable(const std::filesystem::path& path) {
    std::lock_guard<std::mutex> lock(staticTableMutex());
    auto& cache = staticTableCache();
    const std::string key = path.string();
    if (auto it = cache.find(key); it != cache.end()) {
        return it->second.table ? &it->second : nullptr;
    }
    CachedStaticTable entry;
    try {
        std::ifstream in(path, std::ios::binary);
        std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                        std::istreambuf_iterator<char>());
        std::uint64_t h = 1469598103934665603ULL;
        for (std::uint8_t b : bytes) {
            h = (h ^ b) * 1099511628211ULL;
        }
        auto table = std::make_shared<const simeon::StaticEmbedding>(
            simeon::StaticEmbedding::from_bytes(bytes));
        char hex[17];
        std::snprintf(hex, sizeof hex, "%016llx", static_cast<unsigned long long>(h));
        entry.identity = "simeon-static-v1:" + path.filename().string() + ":" + hex +
                         ":dim=" + std::to_string(table->dim());
        entry.table = std::move(table);
        spdlog::info("Simeon static embedding table {} (dim={}, vocab={})", key, entry.table->dim(),
                     entry.table->vocab_size());
    } catch (const std::exception& e) {
        spdlog::warn("Simeon static embedding table {} not loaded: {}", key, e.what());
    }
    auto [it, inserted] = cache.emplace(key, std::move(entry));
    return it->second.table ? &it->second : nullptr;
}

std::mutex& dataDirHintMutex() {
    static std::mutex mu;
    return mu;
}

std::filesystem::path& dataDirHint() {
    static std::filesystem::path hint;
    return hint;
}

// "static" explicitly, or no profile configured: the static table is the default when one is
// found. Any other explicit profile keeps the hash encoder.
bool wantsStaticProfile(const EmbeddingConfig& yamsConfig,
                        const daemon::ConfigResolver::SimeonEncoderPolicy& policy) {
    if (yamsConfig.simeon_encoder_profile == EmbeddingConfig::SimeonEncoderProfile::FixedHash384) {
        return false;
    }
    return !policy.encoderProfile.has_value() || policy.encoderProfile->empty() ||
           *policy.encoderProfile == "static";
}

bool explicitStaticProfile(const daemon::ConfigResolver::SimeonEncoderPolicy& policy) {
    return policy.encoderProfile.has_value() && *policy.encoderProfile == "static";
}

std::optional<std::filesystem::path>
resolveStaticTablePath(const daemon::ConfigResolver::SimeonEncoderPolicy& policy) {
    std::optional<std::filesystem::path> configured;
    if (policy.staticTable) {
        configured = std::filesystem::path(*policy.staticTable);
    }
    std::filesystem::path dataDir;
    {
        std::lock_guard<std::mutex> lock(dataDirHintMutex());
        dataDir = dataDirHint();
    }
    if (dataDir.empty()) {
        try {
            dataDir = yams::config::resolve_data_dir_from_config();
        } catch (const std::exception&) {
            if (auto env = yams::config::getenv_nonempty("YAMS_DATA_DIR")) {
                dataDir = *env;
            }
        }
    }
    return yams::config::findStaticEmbeddingTable(configured, dataDir);
}

bool usesFixedHash384Profile(const EmbeddingConfig& yamsConfig,
                             const daemon::ConfigResolver::SimeonEncoderPolicy& policy) {
    if (yamsConfig.simeon_encoder_profile == EmbeddingConfig::SimeonEncoderProfile::FixedHash384) {
        return true;
    }
    if (!policy.encoderProfile.has_value()) {
        return false;
    }
    if (*policy.encoderProfile == "fixed_hash_384" || *policy.encoderProfile == "simeon-v1-384") {
        return true;
    }
    if (*policy.encoderProfile != "configurable" && *policy.encoderProfile != "static") {
        spdlog::warn("Ignoring unknown embeddings.simeon.encoder_profile='{}'",
                     *policy.encoderProfile);
    }
    return false;
}

std::string configurableSpaceIdentity(const simeon::EncoderConfig& config) {
    std::string identity = "simeon-config-v1:";
    identity += ngram_mode_label(config.ngram_mode);
    identity += ':';
    identity += std::to_string(config.ngram_min);
    identity += '-';
    identity += std::to_string(config.ngram_max);
    identity += ":sketch=";
    identity += std::to_string(config.sketch_dim);
    identity += ":output=";
    identity += std::to_string(config.output_dim);
    identity += ":projection=";
    identity += projection_mode_label(config.projection);
    identity += ":l2=";
    identity += config.l2_normalize ? '1' : '0';
    return identity;
}

ResolvedSimeonEncoder resolveEncoder(const EmbeddingConfig& yamsConfig) {
    const auto policy = daemon::ConfigResolver::resolveSimeonEncoderPolicy();
    const bool wantedStatic = wantsStaticProfile(yamsConfig, policy);
    if (wantedStatic) {
        if (auto path = resolveStaticTablePath(policy)) {
            if (const auto* cached = cachedStaticTable(*path)) {
                return ResolvedSimeonEncoder{
                    .config = {}, .spaceIdentity = cached->identity, .staticTable = cached->table};
            }
        }
        if (explicitStaticProfile(policy)) {
            spdlog::warn("embeddings.simeon.encoder_profile='static' but no static table was "
                         "found; using the configurable hash encoder");
        }
    }
    if (usesFixedHash384Profile(yamsConfig, policy)) {
        return ResolvedSimeonEncoder{.config = simeon::simeon_v1_384_config(),
                                     .spaceIdentity = std::string{simeon::simeon_v1_384_identity},
                                     .fallback = wantedStatic};
    }

    simeon::EncoderConfig config;
    config.ngram_mode = parse_ngram_mode(policy.ngramMode.value_or(std::string{}));
    config.ngram_min = policy.ngramMin.value_or(3);
    config.ngram_max = policy.ngramMax.value_or(5);
    config.sketch_dim = policy.sketchDim.value_or(4096);
    config.output_dim = policy.outputDim.value_or(static_cast<uint32_t>(yamsConfig.embedding_dim));
    config.projection = parse_projection_mode(policy.projection.value_or(std::string{}));
    config.l2_normalize = policy.l2Normalize.value_or(yamsConfig.normalize_embeddings);
    return ResolvedSimeonEncoder{.config = config,
                                 .spaceIdentity = configurableSpaceIdentity(config),
                                 .fallback = wantedStatic};
}

std::string compute_simeon_recipe_label() {
    return resolveEncoder(EmbeddingConfig{}).spaceIdentity;
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
        auto resolved = resolveEncoder(config_);
        if (resolved.staticTable) {
            staticTable_ = std::move(resolved.staticTable);
            dim_ = staticTable_->dim();
            spaceIdentity_ = std::move(resolved.spaceIdentity);
            initialized_.store(true);
            spdlog::info("SimeonBackend initialized: static table dim={} identity={}", dim_,
                         spaceIdentity_);
            return true;
        }
        auto& enc_cfg = resolved.config;
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
        spaceIdentity_ = std::move(resolved.spaceIdentity);
        fallback_ = resolved.fallback;
        initialized_.store(true);
        spdlog::info("SimeonBackend initialized: dim={} tier={}", dim_,
                     simeon::simd_tier_name(simeon::active_simd_tier()));
        return true;
    }

    void shutdown() override {
        std::lock_guard<std::mutex> lock(mu_);
        encoder_.reset();
        staticTable_.reset();
        initialized_.store(false);
        dim_ = 0;
        spaceIdentity_.clear();
    }

    bool isInitialized() const override { return initialized_.load(); }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        if (!initialized_.load())
            return Error{ErrorCode::NotInitialized, "SimeonBackend not initialized"};
        const auto start = std::chrono::steady_clock::now();
        std::vector<float> out(dim_, 0.0f);
        encodeInto(text, out);
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
            encodeInto(t, v);
            total_bytes += t.size();
            out.push_back(std::move(v));
        }
        recordBatch(texts.size(), total_bytes, start);
        return out;
    }

    size_t getEmbeddingDimension() const override { return dim_; }
    size_t getMaxSequenceLength() const override { return config_.max_sequence_length; }
    std::string getBackendName() const override { return "Simeon"; }
    std::string getEmbeddingSpaceIdentity() const override { return spaceIdentity_; }
    bool isFallbackEmbeddingSpace() const override { return fallback_; }
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
    void encodeInto(const std::string& text, std::vector<float>& out) const {
        if (staticTable_) {
            staticTable_->encode(text, out);
        } else {
            encoder_->encode(text, out.data());
        }
    }

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
    std::shared_ptr<const simeon::StaticEmbedding> staticTable_;
    std::mutex mu_;
    std::atomic<bool> initialized_{false};
    size_t dim_ = 0;
    std::string spaceIdentity_;
    bool fallback_{false};
    mutable GenerationStats stats_;
};

} // namespace

std::unique_ptr<IEmbeddingBackend> makeSimeonBackend(const EmbeddingConfig& config) {
    return std::make_unique<SimeonBackend>(config);
}

void setStaticEmbeddingDataDirHint(const std::filesystem::path& dataDir) {
    std::lock_guard<std::mutex> lock(dataDirHintMutex());
    dataDirHint() = dataDir;
}

std::shared_ptr<const simeon::StaticEmbedding>
loadSharedStaticEmbeddingTable(const std::filesystem::path& path) {
    const auto* cached = cachedStaticTable(path);
    return cached ? cached->table : nullptr;
}

std::string staticEmbeddingSpaceIdentity(const std::filesystem::path& path) {
    const auto* cached = cachedStaticTable(path);
    return cached ? cached->identity : std::string{};
}

std::string simeonEmbeddingSpaceIdentity(const EmbeddingConfig& config) {
    return resolveEncoder(config).spaceIdentity;
}

std::string simeonRecipeLabel() {
    return compute_simeon_recipe_label();
}

} // namespace yams::vector
