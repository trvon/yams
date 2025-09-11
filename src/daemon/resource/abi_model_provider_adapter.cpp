#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <yams/daemon/resource/abi_model_provider_adapter.h>

namespace yams::daemon {

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
    if (st != YAMS_OK)
        return mapStatus(st, "generate_embedding_batch");
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
    if (!table_ || !table_->load_model)
        return ErrorCode::NotImplemented;
    auto st = table_->load_model(table_->self, modelName.c_str(), nullptr, nullptr);
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

Result<ModelInfo> AbiModelProviderAdapter::getModelInfo(const std::string& modelName) const {
    // Not part of v1; return minimal info if loaded
    if (!isModelLoaded(modelName))
        return Error{ErrorCode::NotFound, "Model not loaded"};
    ModelInfo info;
    info.name = modelName;
    info.embeddingDim = getEmbeddingDim(modelName);
    return info;
}

size_t AbiModelProviderAdapter::getEmbeddingDim(const std::string& /*modelName*/) const {
    // Not part of v1; unknown
    return 0;
}

std::string AbiModelProviderAdapter::getProviderName() const {
    return "ABIModelProvider";
}

std::string AbiModelProviderAdapter::getProviderVersion() const {
    return "v1";
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
