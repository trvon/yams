#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <future>
#include <thread>
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
    spdlog::info("[ABI Adapter] loadModel() called for: {}", modelName);
    if (!table_) {
        spdlog::error("[ABI Adapter] table_ is null!");
        return ErrorCode::NotImplemented;
    }
    if (!table_->load_model) {
        spdlog::error("[ABI Adapter] table_->load_model function pointer is null!");
        return ErrorCode::NotImplemented;
    }
    spdlog::info(
        "[ABI Adapter] table_ = {}, load_model = {}, self = {}", static_cast<const void*>(table_),
        reinterpret_cast<const void*>(table_->load_model), static_cast<const void*>(table_->self));
    spdlog::info("[ABI Adapter] Calling table_->load_model({})...", modelName);

    // Run plugin load_model() asynchronously with a timeout.
    // NOTE: On MSVC/Windows, std::async(std::launch::async, ...) can throw
    // std::system_error("resource deadlock would occur") in some environments.
    // Use an explicit std::thread + std::promise instead.
    auto read_timeout_ms = [](const char* env, int def_ms) {
        int ms = def_ms;
        if (const char* s = std::getenv(env)) {
            try {
                ms = std::stoi(s);
            } catch (...) {
            }
        }
        if (ms < 100)
            ms = 100;
        return ms;
    };
    const int timeout_ms = read_timeout_ms("YAMS_ABI_LOAD_MODEL_TIMEOUT_MS", 90000); // 90s default

    auto promise = std::make_shared<std::promise<yams_status_t>>();
    auto fut = promise->get_future();

    std::thread worker;
    try {
        worker = std::thread([this, modelName, promise]() {
            try {
                const auto st =
                    table_->load_model(table_->self, modelName.c_str(), nullptr, nullptr);
                promise->set_value(st);
            } catch (...) {
                try {
                    promise->set_exception(std::current_exception());
                } catch (...) {
                }
            }
        });
    } catch (const std::system_error& e) {
        spdlog::error(
            "[ABI Adapter] failed to start model load thread: {} (category='{}' value={})",
            e.what(), e.code().category().name(), e.code().value());
        return Error{ErrorCode::InternalError,
                     std::string("load_model thread start failed: ") + e.what()};
    } catch (const std::exception& e) {
        spdlog::error("[ABI Adapter] failed to start model load thread: {}", e.what());
        return Error{ErrorCode::InternalError,
                     std::string("load_model thread start failed: ") + e.what()};
    }

    if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) != std::future_status::ready) {
        spdlog::warn("[ABI Adapter] load_model timeout after {} ms for: {}", timeout_ms, modelName);
        // Match prior behavior: return timeout and let the background thread continue.
        worker.detach();
        return Error{ErrorCode::Timeout, "Model load timed out"};
    }

    yams_status_t st = YAMS_ERR_INTERNAL;
    try {
        st = fut.get();
    } catch (const std::exception& e) {
        spdlog::error("[ABI Adapter] load_model async exception: {}", e.what());
        if (worker.joinable())
            worker.join();
        return Error{ErrorCode::InternalError, std::string("load_model exception: ") + e.what()};
    } catch (...) {
        spdlog::error("[ABI Adapter] load_model async unknown exception");
        if (worker.joinable())
            worker.join();
        return Error{ErrorCode::InternalError, "load_model unknown exception"};
    }

    if (worker.joinable())
        worker.join();
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
    // For now, return nullptr - plugins provide embeddings via generateEmbedding()
    // ServiceManager should use modelProvider directly, not create separate EmbeddingGenerator
    (void)modelName;
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
