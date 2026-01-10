#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <span>
#include <yams/daemon/resource/abi_model_provider_adapter.h>
#include <yams/vector/embedding_generator.h>

namespace yams::daemon {

/**
 * ModelProviderBackend - Custom IEmbeddingBackend that wraps IModelProvider
 *
 * This backend directly uses the IModelProvider interface for embedding generation,
 * avoiding the need to go through the daemon IPC or mock providers.
 */
class ModelProviderBackend : public vector::IEmbeddingBackend {
public:
    ModelProviderBackend(IModelProvider* provider, std::string modelName, size_t dim)
        : provider_(provider), modelName_(std::move(modelName)), dim_(dim), initialized_(false) {}

    bool initialize() override {
        if (!provider_) {
            spdlog::warn("[ModelProviderBackend] No provider");
            return false;
        }
        initialized_ = true;
        spdlog::info("[ModelProviderBackend] Initialized with model '{}' dim={}", modelName_, dim_);
        return true;
    }

    void shutdown() override { initialized_ = false; }

    bool isInitialized() const override { return initialized_; }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        if (!provider_) {
            return Error{ErrorCode::NotInitialized, "Provider not available"};
        }
        return provider_->generateEmbeddingFor(modelName_, text);
    }

    Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        if (!provider_) {
            return Error{ErrorCode::NotInitialized, "Provider not available"};
        }
        std::vector<std::string> textVec(texts.begin(), texts.end());
        return provider_->generateBatchEmbeddingsFor(modelName_, textVec);
    }

    size_t getEmbeddingDimension() const override { return dim_; }

    size_t getMaxSequenceLength() const override { return 512; }

    std::string getBackendName() const override { return "ModelProviderBackend"; }

    bool isAvailable() const override { return provider_ != nullptr && initialized_; }

    vector::GenerationStats getStats() const override { return stats_; }

    void resetStats() override { stats_ = vector::GenerationStats{}; }

private:
    IModelProvider* provider_;
    std::string modelName_;
    size_t dim_;
    bool initialized_;
    vector::GenerationStats stats_;
};

AbiModelProviderAdapter::AbiModelProviderAdapter(yams_model_provider_v1* table) : table_(table) {}

void AbiModelProviderAdapter::setProgressCallback(std::function<void(const ModelLoadEvent&)> cb) {
    progress_ = std::move(cb);
    if (table_ && table_->set_progress_callback) {
        table_->set_progress_callback(
            table_->self,
            // Bridge C callback to C++ functor
            [](void* user, const char* model_id, int phase, uint64_t current, uint64_t total,
               const char* message) {
                auto* fn = reinterpret_cast<std::function<void(const ModelLoadEvent&)>*>(user);
                if (!fn || !(*fn))
                    return;
                ModelLoadEvent ev{};
                ev.modelName = model_id ? std::string(model_id) : std::string();
                ev.phase = mapPhaseName(phase);
                ev.bytesLoaded = current;
                ev.bytesTotal = total;
                ev.message = message ? std::string(message) : std::string();
                (*fn)(ev);
            },
            &progress_);
    }
}

Result<std::vector<float>> AbiModelProviderAdapter::generateEmbedding(const std::string& text) {
    std::string model = pickDefaultModel();
    if (model.empty())
        return Error{ErrorCode::NotFound, "No model loaded"};
    return generateEmbeddingFor(model, text);
}

Result<std::vector<std::vector<float>>>
AbiModelProviderAdapter::generateBatchEmbeddings(const std::vector<std::string>& texts) {
    std::string model = pickDefaultModel();
    if (model.empty())
        return Error{ErrorCode::NotFound, "No model loaded"};
    return generateBatchEmbeddingsFor(model, texts);
}

Result<std::vector<float>>
AbiModelProviderAdapter::generateEmbeddingFor(const std::string& modelName,
                                              const std::string& text) {
    if (!table_ || !table_->generate_embedding)
        return ErrorCode::NotImplemented;
    const auto* bytes = reinterpret_cast<const uint8_t*>(text.data());
    size_t len = text.size();
    float* vec = nullptr;
    size_t dim = 0;
    auto st = table_->generate_embedding(table_->self, modelName.c_str(), bytes, len, &vec, &dim);
    if (st != YAMS_OK)
        return mapStatus(st, "generate_embedding");
    std::vector<float> out;
    try {
        out.assign(vec, vec + dim);
    } catch (...) {
        if (table_->free_embedding)
            table_->free_embedding(table_->self, vec, dim);
        return ErrorCode::InternalError;
    }
    if (table_->free_embedding)
        table_->free_embedding(table_->self, vec, dim);
    return out;
}

Result<std::vector<std::vector<float>>>
AbiModelProviderAdapter::generateBatchEmbeddingsFor(const std::string& modelName,
                                                    const std::vector<std::string>& texts) {
    if (!table_)
        return ErrorCode::NotImplemented;
    if (texts.empty())
        return std::vector<std::vector<float>>{};
    if (!table_->generate_embedding_batch) {
        // Fallback: call single repeatedly
        std::vector<std::vector<float>> res;
        res.reserve(texts.size());
        for (const auto& t : texts) {
            auto r = generateEmbeddingFor(modelName, t);
            if (!r)
                return r.error();
            res.emplace_back(std::move(r.value()));
        }
        return res;
    }
    std::vector<const uint8_t*> input_ptrs;
    std::vector<size_t> input_lens;
    input_ptrs.reserve(texts.size());
    input_lens.reserve(texts.size());
    for (auto& s : texts) {
        input_ptrs.push_back(reinterpret_cast<const uint8_t*>(s.data()));
        input_lens.push_back(s.size());
    }
    float* vecs = nullptr;
    size_t out_batch = 0;
    size_t out_dim = 0;
    auto st = table_->generate_embedding_batch(table_->self, modelName.c_str(), input_ptrs.data(),
                                               input_lens.data(), texts.size(), &vecs, &out_batch,
                                               &out_dim);
    if (st != YAMS_OK) {
        spdlog::warn("[ABI Adapter] generate_embedding_batch failed for model '{}' (batch_size={}, "
                     "status={})",
                     modelName, texts.size(), static_cast<int>(st));
        return mapStatus(st, "generate_embedding_batch");
    }
    std::vector<std::vector<float>> result;
    try {
        result.resize(out_batch);
        for (size_t i = 0; i < out_batch; ++i) {
            const float* row = vecs + i * out_dim;
            result[i].assign(row, row + out_dim);
        }
    } catch (...) {
        if (table_->free_embedding_batch)
            table_->free_embedding_batch(table_->self, vecs, out_batch, out_dim);
        return ErrorCode::InternalError;
    }
    if (table_->free_embedding_batch)
        table_->free_embedding_batch(table_->self, vecs, out_batch, out_dim);
    return result;
}

Result<void> AbiModelProviderAdapter::loadModel(const std::string& modelName) {
    spdlog::info("[ABI Adapter] loadModel() called for: {}", modelName);
    if (!table_) {
        spdlog::error("[ABI Adapter] table_ is null!");
        return ErrorCode::NotImplemented;
    }
    if (!table_->load_model) {
        spdlog::error("[ABI Adapter] table_->load_model function pointer is null!");
        return ErrorCode::NotImplemented;
    }

    auto st = table_->load_model(table_->self, modelName.c_str(), nullptr, nullptr);
    spdlog::info("[ABI Adapter] table_->load_model() returned status: {}", static_cast<int>(st));

    if (st != YAMS_OK)
        return mapStatus(st, "load_model");
    return Result<void>();
}

Result<void> AbiModelProviderAdapter::loadModelWithOptions(const std::string& modelName,
                                                           const std::string& optionsJson) {
    if (!table_ || !table_->load_model)
        return ErrorCode::NotImplemented;
    const char* opt = optionsJson.empty() ? nullptr : optionsJson.c_str();
    auto st = table_->load_model(table_->self, modelName.c_str(), nullptr, opt);
    if (st != YAMS_OK)
        return mapStatus(st, "load_model");
    return Result<void>();
}

Result<void> AbiModelProviderAdapter::unloadModel(const std::string& modelName) {
    if (!table_ || !table_->unload_model)
        return ErrorCode::NotImplemented;
    auto st = table_->unload_model(table_->self, modelName.c_str());
    if (st != YAMS_OK)
        return mapStatus(st, "unload_model");
    return Result<void>();
}

bool AbiModelProviderAdapter::isModelLoaded(const std::string& modelName) const {
    if (!table_ || !table_->is_model_loaded)
        return false;
    bool loaded = false;
    auto st = table_->is_model_loaded(table_->self, modelName.c_str(), &loaded);
    return st == YAMS_OK && loaded;
}

std::vector<std::string> AbiModelProviderAdapter::getLoadedModels() const {
    std::vector<std::string> out;
    if (!table_ || !table_->get_loaded_models)
        return out;
    const char** ids = nullptr;
    size_t count = 0;
    if (table_->get_loaded_models(table_->self, &ids, &count) != YAMS_OK)
        return out;
    try {
        out.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            if (ids && ids[i])
                out.emplace_back(ids[i]);
        }
    } catch (...) {
    }
    if (table_->free_model_list)
        table_->free_model_list(table_->self, ids, count);
    return out;
}

size_t AbiModelProviderAdapter::getLoadedModelCount() const {
    if (!table_ || !table_->get_loaded_models)
        return 0;
    const char** ids = nullptr;
    size_t count = 0;
    if (table_->get_loaded_models(table_->self, &ids, &count) != YAMS_OK)
        return 0;
    // Free the list immediately - we only wanted the count
    if (table_->free_model_list)
        table_->free_model_list(table_->self, ids, count);
    return count;
}

Result<ModelInfo> AbiModelProviderAdapter::getModelInfo(const std::string& modelName) const {
    if (!isModelLoaded(modelName))
        return Error{ErrorCode::NotFound, "Model not loaded"};
    ModelInfo info;
    info.name = modelName;
    // Use v1.2 JSON if available
    if (table_ && table_->get_runtime_info_json && table_->free_string) {
        char* json_c = nullptr;
        if (table_->get_runtime_info_json(table_->self, modelName.c_str(), &json_c) == YAMS_OK &&
            json_c) {
            try {
                nlohmann::json j = nlohmann::json::parse(json_c);
                if (j.contains("model"))
                    info.name = j.value("model", info.name);
                if (j.contains("dim"))
                    info.embeddingDim = j.value("dim", 0ull);
                if (j.contains("runtime_version")) {
                    (void)j["runtime_version"]; // could be surfaced later
                }
            } catch (...) {
            }
        }
        if (json_c)
            table_->free_string(table_->self, json_c);
    }
    if (info.embeddingDim == 0)
        info.embeddingDim = getEmbeddingDim(modelName);
    return info;
}

size_t AbiModelProviderAdapter::getEmbeddingDim(const std::string& modelName) const {
    if (table_ && table_->get_embedding_dim) {
        size_t dim = 0;
        if (table_->get_embedding_dim(table_->self, modelName.c_str(), &dim) == YAMS_OK)
            return dim;
    }
    return 0;
}

std::shared_ptr<vector::EmbeddingGenerator>
AbiModelProviderAdapter::getEmbeddingGenerator(const std::string& modelName) {
    // Use our custom ModelProviderBackend that wraps IModelProvider directly.
    // This bypasses the broken ml::createEmbeddingProvider() factory that returns MockProvider.

    std::string model = modelName.empty() ? "all-MiniLM-L6-v2" : modelName;

    // Get dimension from our provider - DO NOT use hardcoded fallback
    // to avoid dimension mismatch when user switches models
    size_t dim = 0;
    if (table_ && table_->get_embedding_dim) {
        size_t pluginDim = 0;
        if (table_->get_embedding_dim(table_->self, model.c_str(), &pluginDim) == YAMS_OK &&
            pluginDim > 0) {
            dim = pluginDim;
        }
    }

    // If we can't get the dimension, fail early rather than using wrong default
    if (dim == 0) {
        spdlog::warn("[AbiModelProviderAdapter] Cannot determine embedding dimension for '{}' - "
                     "model may not be loaded yet",
                     model);
        return nullptr;
    }

    // Create our custom backend that wraps this IModelProvider
    auto backend = std::make_unique<ModelProviderBackend>(this, model, dim);
    if (!backend->initialize()) {
        spdlog::warn("[AbiModelProviderAdapter] Failed to initialize ModelProviderBackend for '{}'",
                     model);
        return nullptr;
    }

    // Build config for the EmbeddingGenerator
    vector::EmbeddingConfig config;
    config.model_name = model;
    config.embedding_dim = dim;

    // Create EmbeddingGenerator with our custom backend
    auto gen = std::make_shared<vector::EmbeddingGenerator>(std::move(backend), config);
    if (gen->initialize()) {
        spdlog::info(
            "[AbiModelProviderAdapter] Created EmbeddingGenerator with ModelProviderBackend "
            "for model '{}' dim={}",
            model, dim);
        return gen;
    }

    spdlog::warn("[AbiModelProviderAdapter] Failed to initialize EmbeddingGenerator for model '{}'",
                 model);
    return nullptr;
}

std::string AbiModelProviderAdapter::getProviderName() const {
    return "ABIModelProvider";
}

std::string AbiModelProviderAdapter::getProviderVersion() const {
    return "v1.2";
}

bool AbiModelProviderAdapter::isAvailable() const {
    return table_ != nullptr;
}

size_t AbiModelProviderAdapter::getMemoryUsage() const {
    return 0;
}

void AbiModelProviderAdapter::releaseUnusedResources() {}

void AbiModelProviderAdapter::shutdown() {}

Error AbiModelProviderAdapter::mapStatus(yams_status_t st, const std::string& context) {
    using EC = ErrorCode;
    EC code = EC::Unknown;
    switch (st) {
        case YAMS_OK:
            return Error{EC::Success, ""};
        case YAMS_ERR_INVALID_ARG:
            code = EC::InvalidArgument;
            break;
        case YAMS_ERR_NOT_FOUND:
            code = EC::NotFound;
            break;
        case YAMS_ERR_IO:
            code = EC::IOError;
            break;
        case YAMS_ERR_INTERNAL:
            code = EC::InternalError;
            break;
        case YAMS_ERR_UNSUPPORTED:
            code = EC::NotImplemented;
            break;
        default:
            code = EC::Unknown;
            break;
    }
    std::string msg = context.empty() ? "plugin error" : ("plugin error: " + context);
    return Error{code, std::move(msg)};
}

const char* AbiModelProviderAdapter::mapPhaseName(int phase) {
    switch (phase) {
        case YAMS_MODEL_PHASE_PROBE:
            return "probe";
        case YAMS_MODEL_PHASE_DOWNLOAD:
            return "downloading";
        case YAMS_MODEL_PHASE_LOAD:
            return "initializing";
        case YAMS_MODEL_PHASE_WARMUP:
            return "warming";
        case YAMS_MODEL_PHASE_READY:
            return "completed";
        default:
            return "started";
    }
}

std::string AbiModelProviderAdapter::pickDefaultModel() const {
    auto models = getLoadedModels();
    if (!models.empty())
        return models.front();
    return {};
}

} // namespace yams::daemon
