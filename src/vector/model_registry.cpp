#include <yams/vector/model_registry.h>
#include <yams/core/format.h>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <regex>
#include <shared_mutex>

namespace yams::vector {

namespace fs = std::filesystem;

class ModelRegistry::Impl {
public:
    Impl() = default;
    
    Result<void> registerModel(const ModelInfo& info) {
        if (!info.validate()) {
            return Error{ErrorCode::InvalidArgument, "Invalid model info"};
        }
        
        std::unique_lock lock(mutex_);
        
        if (models_.find(info.model_id) != models_.end()) {
            return Error{ErrorCode::InvalidArgument, 
                yams::format("Model {} already registered", info.model_id)};
        }
        
        models_[info.model_id] = info;
        models_[info.model_id].registered_at = std::chrono::system_clock::now();
        
        // Update dimension index
        dimension_index_[info.embedding_dimension].push_back(info.model_id);
        
        // Update version index
        version_index_[info.name].push_back(info.model_id);
        
        spdlog::info("Registered model: {} v{} ({}D)", 
            info.name, info.version, info.embedding_dimension);
        
        return Result<void>();
    }
    
    Result<void> unregisterModel(const std::string& model_id) {
        std::unique_lock lock(mutex_);
        
        auto it = models_.find(model_id);
        if (it == models_.end()) {
            return Error{ErrorCode::NotFound, 
                std::format("Model {} not found", model_id)};
        }
        
        // Remove from indices
        auto& dim_models = dimension_index_[it->second.embedding_dimension];
        dim_models.erase(
            std::remove(dim_models.begin(), dim_models.end(), model_id),
            dim_models.end()
        );
        
        auto& ver_models = version_index_[it->second.name];
        ver_models.erase(
            std::remove(ver_models.begin(), ver_models.end(), model_id),
            ver_models.end()
        );
        
        models_.erase(it);
        metrics_.erase(model_id);
        
        return Result<void>();
    }
    
    Result<void> discoverModels(const std::string& directory) {
        if (!fs::exists(directory)) {
            return Error{ErrorCode::NotFound, 
                std::format("Directory {} not found", directory)};
        }
        
        size_t discovered = 0;
        
        for (const auto& entry : fs::recursive_directory_iterator(directory)) {
            if (!entry.is_regular_file()) continue;
            
            auto ext = entry.path().extension().string();
            if (ext != ".onnx" && ext != ".pb" && ext != ".pt") continue;
            
            auto result = discovery::parseModelMetadata(entry.path().string());
            if (result.has_value()) {
                auto reg_result = registerModel(result.value());
                if (reg_result.has_value()) {
                    discovered++;
                }
            }
        }
        
        spdlog::info("Discovered {} models in {}", discovered, directory);
        return Result<void>();
    }
    
    Result<ModelInfo> getModel(const std::string& model_id) const {
        std::shared_lock lock(mutex_);
        
        auto it = models_.find(model_id);
        if (it == models_.end()) {
            return Error{ErrorCode::NotFound, 
                std::format("Model {} not found", model_id)};
        }
        
        return it->second;
    }
    
    Result<std::vector<ModelInfo>> getModelsByDimension(size_t dimension) const {
        std::shared_lock lock(mutex_);
        
        auto it = dimension_index_.find(dimension);
        if (it == dimension_index_.end()) {
            return std::vector<ModelInfo>{};
        }
        
        std::vector<ModelInfo> result;
        for (const auto& model_id : it->second) {
            auto model_it = models_.find(model_id);
            if (model_it != models_.end()) {
                result.push_back(model_it->second);
            }
        }
        
        return result;
    }
    
    Result<ModelInfo> getDefaultModel(size_t dimension) const {
        std::shared_lock lock(mutex_);
        
        auto it = default_models_.find(dimension);
        if (it == default_models_.end()) {
            // Find any available model for this dimension
            auto dim_it = dimension_index_.find(dimension);
            if (dim_it == dimension_index_.end() || dim_it->second.empty()) {
                return Error{ErrorCode::NotFound, 
                    std::format("No models found for dimension {}", dimension)};
            }
            
            // Return first available model
            auto model_it = models_.find(dim_it->second.front());
            if (model_it != models_.end()) {
                return model_it->second;
            }
        } else {
            auto model_it = models_.find(it->second);
            if (model_it != models_.end()) {
                return model_it->second;
            }
        }
        
        return Error{ErrorCode::NotFound, 
            std::format("No default model for dimension {}", dimension)};
    }
    
    std::vector<ModelInfo> getAllModels() const {
        std::shared_lock lock(mutex_);
        
        std::vector<ModelInfo> result;
        result.reserve(models_.size());
        
        for (const auto& [id, info] : models_) {
            result.push_back(info);
        }
        
        return result;
    }
    
    bool hasModel(const std::string& model_id) const {
        std::shared_lock lock(mutex_);
        return models_.find(model_id) != models_.end();
    }
    
    Result<std::string> selectBestModel(
        size_t required_dimension,
        const std::map<std::string, std::string>& requirements
    ) const {
        std::shared_lock lock(mutex_);
        
        auto candidates_result = getModelsByDimension(required_dimension);
        if (!candidates_result.has_value()) {
            return candidates_result.error();
        }
        
        auto candidates = candidates_result.value();
        if (candidates.empty()) {
            return Error{ErrorCode::NotFound, 
                std::format("No models for dimension {}", required_dimension)};
        }
        
        // Filter by requirements
        candidates.erase(
            std::remove_if(candidates.begin(), candidates.end(),
                [&requirements](const ModelInfo& info) {
                    return !ModelCompatibilityChecker::meetsRequirements(info, requirements);
                }),
            candidates.end()
        );
        
        if (candidates.empty()) {
            return Error{ErrorCode::NotFound, "No models meet requirements"};
        }
        
        // Sort by performance (throughput)
        std::sort(candidates.begin(), candidates.end(),
            [](const ModelInfo& a, const ModelInfo& b) {
                return a.throughput_per_sec > b.throughput_per_sec;
            });
        
        return candidates.front().model_id;
    }
    
    Result<void> updateMetrics(
        const std::string& model_id,
        double inference_time_ms,
        bool success
    ) {
        std::unique_lock lock(mutex_);
        
        auto& metrics = metrics_[model_id];
        metrics.model_id = model_id;
        
        if (success) {
            metrics.total_inferences++;
            metrics.total_inference_time_ms += inference_time_ms;
            metrics.min_inference_time_ms = std::min(metrics.min_inference_time_ms, inference_time_ms);
            metrics.max_inference_time_ms = std::max(metrics.max_inference_time_ms, inference_time_ms);
        } else {
            metrics.failed_inferences++;
        }
        
        metrics.last_use = std::chrono::system_clock::now();
        if (metrics.total_inferences == 1) {
            metrics.first_use = metrics.last_use;
        }
        
        // Update model info with average
        auto it = models_.find(model_id);
        if (it != models_.end()) {
            it->second.avg_inference_time_ms = metrics.getAverageInferenceTime();
            it->second.last_used = metrics.last_use;
        }
        
        return Result<void>();
    }
    
    ModelRegistry::RegistryStats getStats() const {
        std::shared_lock lock(mutex_);
        
        RegistryStats stats;
        stats.total_models = models_.size();
        
        for (const auto& [id, info] : models_) {
            if (info.is_available) {
                stats.available_models++;
            }
            stats.models_by_dimension[info.embedding_dimension]++;
            stats.models_by_format[info.format]++;
            stats.total_model_size_bytes += info.model_size_bytes;
        }
        
        return stats;
    }
    
private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, ModelInfo> models_;
    std::unordered_map<std::string, ModelMetrics> metrics_;
    std::unordered_map<size_t, std::vector<std::string>> dimension_index_;
    std::unordered_map<std::string, std::vector<std::string>> version_index_;
    std::unordered_map<size_t, std::string> default_models_;
};

// ModelRegistry implementation
ModelRegistry::ModelRegistry() : pImpl(std::make_unique<Impl>()) {}
ModelRegistry::~ModelRegistry() = default;
ModelRegistry::ModelRegistry(ModelRegistry&&) noexcept = default;
ModelRegistry& ModelRegistry::operator=(ModelRegistry&&) noexcept = default;

Result<void> ModelRegistry::registerModel(const ModelInfo& info) {
    return pImpl->registerModel(info);
}

Result<void> ModelRegistry::unregisterModel(const std::string& model_id) {
    return pImpl->unregisterModel(model_id);
}

Result<void> ModelRegistry::discoverModels(const std::string& directory) {
    return pImpl->discoverModels(directory);
}

Result<ModelInfo> ModelRegistry::getModel(const std::string& model_id) const {
    return pImpl->getModel(model_id);
}

Result<std::vector<ModelInfo>> ModelRegistry::getModelsByDimension(size_t dimension) const {
    return pImpl->getModelsByDimension(dimension);
}

Result<ModelInfo> ModelRegistry::getDefaultModel(size_t dimension) const {
    return pImpl->getDefaultModel(dimension);
}

std::vector<ModelInfo> ModelRegistry::getAllModels() const {
    return pImpl->getAllModels();
}

bool ModelRegistry::hasModel(const std::string& model_id) const {
    return pImpl->hasModel(model_id);
}

Result<std::string> ModelRegistry::selectBestModel(
    size_t required_dimension,
    const std::map<std::string, std::string>& requirements
) const {
    return pImpl->selectBestModel(required_dimension, requirements);
}

Result<void> ModelRegistry::updateMetrics(
    const std::string& model_id,
    double inference_time_ms,
    bool success
) {
    return pImpl->updateMetrics(model_id, inference_time_ms, success);
}

ModelRegistry::RegistryStats ModelRegistry::getStats() const {
    return pImpl->getStats();
}

// ModelCompatibilityChecker implementation
bool ModelCompatibilityChecker::areCompatible(const ModelInfo& model1, const ModelInfo& model2) {
    // Same dimension is required
    if (model1.embedding_dimension != model2.embedding_dimension) {
        return false;
    }
    
    // Check explicit compatibility
    auto it = std::find(model1.compatible_with.begin(), 
                        model1.compatible_with.end(), 
                        model2.model_id);
    if (it != model1.compatible_with.end()) {
        return true;
    }
    
    // Check if one replaces the other
    it = std::find(model1.replaces.begin(), 
                   model1.replaces.end(), 
                   model2.model_id);
    if (it != model1.replaces.end()) {
        return true;
    }
    
    // Same model family with different versions
    if (model1.name == model2.name) {
        return true;
    }
    
    return false;
}

bool ModelCompatibilityChecker::meetsRequirements(
    const ModelInfo& model,
    const std::map<std::string, std::string>& requirements
) {
    for (const auto& [key, value] : requirements) {
        if (key == "gpu" && value == "true" && !model.requires_gpu) {
            return false;
        }
        if (key == "max_memory" && model.memory_usage_bytes > std::stoull(value)) {
            return false;
        }
        if (key == "min_throughput" && model.throughput_per_sec < std::stod(value)) {
            return false;
        }
        
        // Check tags
        auto tag_it = model.tags.find(key);
        if (tag_it != model.tags.end() && tag_it->second != value) {
            return false;
        }
    }
    
    return model.is_available;
}

// Discovery utilities
namespace discovery {

std::vector<ModelInfo> discoverONNXModels(const std::string& directory) {
    std::vector<ModelInfo> models;
    
    if (!fs::exists(directory)) {
        return models;
    }
    
    for (const auto& entry : fs::recursive_directory_iterator(directory)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() != ".onnx") continue;
        
        auto result = parseModelMetadata(entry.path().string());
        if (result.has_value()) {
            models.push_back(result.value());
        }
    }
    
    return models;
}

Result<ModelInfo> parseModelMetadata(const std::string& model_path) {
    if (!fs::exists(model_path)) {
        return Error{ErrorCode::NotFound, "Model file not found"};
    }
    
    ModelInfo info;
    info.path = model_path;
    info.format = getModelFormat(model_path);
    info.model_size_bytes = fs::file_size(model_path);
    
    // Extract name and version from filename
    fs::path p(model_path);
    std::string filename = p.stem().string();
    
    // Try to parse version from filename (e.g., model-v1.2.3.onnx)
    std::regex version_regex(R"((.+)-v?(\d+\.\d+\.\d+))");
    std::smatch match;
    
    if (std::regex_match(filename, match, version_regex)) {
        info.name = match[1];
        info.version = match[2];
    } else {
        info.name = filename;
        info.version = "1.0.0";
    }
    
    info.model_id = std::format("{}_{}", info.name, info.version);
    
    // Default values (would be parsed from model metadata in real implementation)
    info.embedding_dimension = 384;  // Default for all-MiniLM-L6-v2
    info.max_sequence_length = 512;
    
    // Check for metadata file
    auto metadata_path = p.parent_path() / (p.stem().string() + ".json");
    if (fs::exists(metadata_path)) {
        // Parse JSON metadata (simplified - would use proper JSON parser)
        std::ifstream file(metadata_path);
        std::string line;
        while (std::getline(file, line)) {
            if (line.find("\"dimension\"") != std::string::npos) {
                std::regex dim_regex(R"(.*"dimension"\s*:\s*(\d+).*)");
                if (std::regex_match(line, match, dim_regex)) {
                    info.embedding_dimension = std::stoul(match[1]);
                }
            }
        }
    }
    
    info.created_at = std::chrono::system_clock::now();
    
    return info;
}

std::string getModelFormat(const std::string& path) {
    fs::path p(path);
    std::string ext = p.extension().string();
    
    if (ext == ".onnx") return "ONNX";
    if (ext == ".pb") return "TensorFlow";
    if (ext == ".pt" || ext == ".pth") return "PyTorch";
    if (ext == ".tflite") return "TFLite";
    
    return "Unknown";
}

std::string extractVersion(const std::string& filename) {
    std::regex version_regex(R"(v?(\d+\.\d+\.\d+))");
    std::smatch match;
    
    if (std::regex_search(filename, match, version_regex)) {
        return match[1];
    }
    
    return "1.0.0";
}

} // namespace discovery

} // namespace yams::vector