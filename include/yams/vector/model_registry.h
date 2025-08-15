#pragma once

#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>
#include <yams/core/types.h>

namespace yams::vector {

/**
 * Model metadata and information
 */
struct ModelInfo {
    std::string model_id; // Unique model identifier
    std::string name;     // Human-readable name
    std::string version;  // Semantic version (e.g., "1.2.3")
    std::string path;     // Path to model file
    std::string format;   // Model format (ONNX, TorchScript, etc.)

    // Model characteristics
    size_t embedding_dimension = 0;   // Output dimension
    size_t max_sequence_length = 512; // Maximum input length
    size_t model_size_bytes = 0;      // Model file size

    // Performance characteristics
    double avg_inference_time_ms = 0.0; // Average inference time
    double throughput_per_sec = 0.0;    // Throughput capability
    size_t memory_usage_bytes = 0;      // Runtime memory usage

    // Metadata
    std::string description;                 // Model description
    std::string author;                      // Model author/source
    std::string license;                     // Model license
    std::map<std::string, std::string> tags; // Additional tags

    // Timestamps
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_used;
    std::chrono::system_clock::time_point registered_at;

    // Compatibility
    std::vector<std::string> compatible_with; // Compatible model IDs
    std::vector<std::string> replaces;        // Models this can replace

    // Status
    bool is_available = true;  // Model is available for use
    bool is_default = false;   // Default model for dimension
    bool requires_gpu = false; // GPU requirement

    // Validation
    bool validate() const {
        return !model_id.empty() && !path.empty() && embedding_dimension > 0 &&
               std::filesystem::exists(path);
    }

    // Comparison for sorting
    bool operator<(const ModelInfo& other) const { return model_id < other.model_id; }
};

/**
 * Model performance metrics
 */
struct ModelMetrics {
    std::string model_id;
    size_t total_inferences = 0;
    size_t failed_inferences = 0;
    double total_inference_time_ms = 0.0;
    double min_inference_time_ms = std::numeric_limits<double>::max();
    double max_inference_time_ms = 0.0;
    size_t total_tokens_processed = 0;
    std::chrono::system_clock::time_point first_use;
    std::chrono::system_clock::time_point last_use;

    double getAverageInferenceTime() const {
        return total_inferences > 0 ? total_inference_time_ms / total_inferences : 0.0;
    }

    double getSuccessRate() const {
        size_t total = total_inferences + failed_inferences;
        return total > 0 ? static_cast<double>(total_inferences) / total : 0.0;
    }
};

/**
 * Model registry for managing available embedding models
 */
class ModelRegistry {
public:
    ModelRegistry();
    ~ModelRegistry();

    // Non-copyable but movable
    ModelRegistry(const ModelRegistry&) = delete;
    ModelRegistry& operator=(const ModelRegistry&) = delete;
    ModelRegistry(ModelRegistry&&) noexcept;
    ModelRegistry& operator=(ModelRegistry&&) noexcept;

    // Model registration and discovery
    Result<void> registerModel(const ModelInfo& info);
    Result<void> unregisterModel(const std::string& model_id);
    Result<void> discoverModels(const std::string& directory);
    Result<void> refreshRegistry();

    // Model queries
    Result<ModelInfo> getModel(const std::string& model_id) const;
    Result<std::vector<ModelInfo>> getModelsByDimension(size_t dimension) const;
    Result<std::vector<ModelInfo>> getCompatibleModels(const std::string& model_id) const;
    Result<ModelInfo> getDefaultModel(size_t dimension) const;
    std::vector<ModelInfo> getAllModels() const;
    bool hasModel(const std::string& model_id) const;

    // Model selection
    Result<std::string>
    selectBestModel(size_t required_dimension,
                    const std::map<std::string, std::string>& requirements = {}) const;

    // Version management
    Result<std::vector<ModelInfo>> getModelVersions(const std::string& name) const;
    Result<ModelInfo> getLatestVersion(const std::string& name) const;
    Result<bool> isVersionCompatible(const std::string& model_id,
                                     const std::string& required_version) const;

    // Metrics and monitoring
    Result<void> updateMetrics(const std::string& model_id, double inference_time_ms,
                               bool success = true);
    Result<ModelMetrics> getMetrics(const std::string& model_id) const;
    std::vector<ModelMetrics> getAllMetrics() const;

    // Configuration
    Result<void> setDefaultModel(const std::string& model_id, size_t dimension);
    Result<void> updateModelInfo(const std::string& model_id, const ModelInfo& info);

    // Persistence
    Result<void> saveRegistry(const std::string& path) const;
    Result<void> loadRegistry(const std::string& path);

    // Statistics
    struct RegistryStats {
        size_t total_models = 0;
        size_t available_models = 0;
        std::map<size_t, size_t> models_by_dimension;
        std::map<std::string, size_t> models_by_format;
        size_t total_model_size_bytes = 0;
    };

    RegistryStats getStats() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Model compatibility checker
 */
class ModelCompatibilityChecker {
public:
    // Check if two models are compatible for swapping
    static bool areCompatible(const ModelInfo& model1, const ModelInfo& model2);

    // Check if a model meets requirements
    static bool meetsRequirements(const ModelInfo& model,
                                  const std::map<std::string, std::string>& requirements);

    // Validate model migration path
    static Result<std::vector<std::string>>
    getMigrationPath(const std::string& from_version, const std::string& to_version,
                     const std::vector<ModelInfo>& available_models);

    // Check dimension compatibility
    static bool areDimensionsCompatible(size_t dim1, size_t dim2);
};

/**
 * Model discovery utilities
 */
namespace discovery {
// Discover ONNX models in directory
std::vector<ModelInfo> discoverONNXModels(const std::string& directory);

// Parse model metadata from file
Result<ModelInfo> parseModelMetadata(const std::string& model_path);

// Validate model file
Result<void> validateModelFile(const std::string& model_path);

// Get model format from file extension
std::string getModelFormat(const std::string& path);

// Extract version from filename
std::string extractVersion(const std::string& filename);
} // namespace discovery

} // namespace yams::vector