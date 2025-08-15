#pragma once

#include <yams/core/types.h>
#include <yams/vector/model_registry.h>

#include <functional>
#include <future>
#include <memory>
#include <string>
#include <vector>

namespace yams::vector {

/**
 * Model loading options
 */
struct LoadOptions {
    bool enable_optimization = true;        // Enable model optimizations
    bool use_gpu = false;                   // Use GPU if available
    int num_threads = -1;                   // -1 for auto-detect
    bool enable_profiling = false;          // Enable performance profiling
    bool validate_model = true;             // Validate model on load
    size_t initial_batch_size = 1;          // Initial batch size for optimization
    std::string execution_provider = "CPU"; // ONNX execution provider
};

/**
 * Loaded model information
 */
struct LoadedModelInfo {
    std::string model_id;
    std::string format;
    size_t memory_usage_bytes = 0;
    bool is_optimized = false;
    bool uses_gpu = false;
    std::vector<std::string> input_names;
    std::vector<std::string> output_names;
    std::map<std::string, std::vector<int64_t>> input_shapes;
    std::map<std::string, std::vector<int64_t>> output_shapes;
    std::chrono::milliseconds load_time{0};
};

/**
 * Model validation result
 */
struct ValidationResult {
    bool is_valid = false;
    std::vector<std::string> errors;
    std::vector<std::string> warnings;

    // Model properties discovered during validation
    size_t embedding_dimension = 0;
    size_t max_sequence_length = 0;
    std::string model_type; // BERT, GPT, etc.

    operator bool() const { return is_valid; }
};

/**
 * Model loader interface
 */
class IModelLoader {
public:
    virtual ~IModelLoader() = default;

    // Load model from file
    virtual Result<std::shared_ptr<void>> loadModel(const std::string& path,
                                                    const LoadOptions& options = {}) = 0;

    // Validate model without loading
    virtual Result<ValidationResult> validateModel(const std::string& path) = 0;

    // Get loader capabilities
    virtual std::vector<std::string> getSupportedFormats() const = 0;
    virtual bool supportsFormat(const std::string& format) const = 0;
    virtual bool supportsGPU() const = 0;
};

/**
 * ONNX model loader
 */
class ONNXModelLoader : public IModelLoader {
public:
    ONNXModelLoader();
    ~ONNXModelLoader() override;

    // IModelLoader interface
    Result<std::shared_ptr<void>> loadModel(const std::string& path,
                                            const LoadOptions& options = {}) override;

    Result<ValidationResult> validateModel(const std::string& path) override;

    std::vector<std::string> getSupportedFormats() const override;
    bool supportsFormat(const std::string& format) const override;
    bool supportsGPU() const override;

    // ONNX-specific methods
    Result<void> optimizeModel(std::shared_ptr<void> model);
    Result<LoadedModelInfo> getModelInfo(std::shared_ptr<void> model) const;

    // Session options configuration
    void setSessionOptions(const std::map<std::string, std::string>& options);

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Main model loader with format detection
 */
class ModelLoader {
public:
    ModelLoader();
    ~ModelLoader();

    // Non-copyable but movable
    ModelLoader(const ModelLoader&) = delete;
    ModelLoader& operator=(const ModelLoader&) = delete;
    ModelLoader(ModelLoader&&) noexcept;
    ModelLoader& operator=(ModelLoader&&) noexcept;

    // Register format-specific loaders
    void registerLoader(const std::string& format, std::shared_ptr<IModelLoader> loader);
    void unregisterLoader(const std::string& format);

    // Load model with automatic format detection
    Result<std::shared_ptr<void>> loadModel(const std::string& path,
                                            const LoadOptions& options = {});

    // Load model from registry
    Result<std::shared_ptr<void>> loadModelFromRegistry(const ModelInfo& info,
                                                        const LoadOptions& options = {});

    // Async loading
    std::future<Result<std::shared_ptr<void>>> loadModelAsync(const std::string& path,
                                                              const LoadOptions& options = {});

    // Validation
    Result<ValidationResult> validateModel(const std::string& path);
    Result<void> validateAllModels(const std::string& directory);

    // Model information
    Result<LoadedModelInfo> getModelInfo(std::shared_ptr<void> model) const;
    Result<std::string> detectFormat(const std::string& path) const;

    // Supported formats
    std::vector<std::string> getSupportedFormats() const;
    bool supportsFormat(const std::string& format) const;

    // Resource management
    Result<void> releaseModel(std::shared_ptr<void> model);
    size_t getLoadedModelCount() const;

    // Performance monitoring
    struct LoaderStats {
        size_t total_loads = 0;
        size_t successful_loads = 0;
        size_t failed_loads = 0;
        std::chrono::milliseconds total_load_time{0};
        std::chrono::milliseconds avg_load_time{0};
        std::map<std::string, size_t> loads_by_format;
    };

    LoaderStats getStats() const;
    void resetStats();

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Model optimization utilities
 */
namespace optimization {
// Optimize ONNX model for inference
Result<void> optimizeONNXModel(const std::string& input_path, const std::string& output_path,
                               const std::map<std::string, std::string>& options = {});

// Quantize model to reduce size
Result<void> quantizeModel(const std::string& input_path, const std::string& output_path,
                           const std::string& quantization_type = "dynamic" // dynamic, static, qat
);

// Convert model format
Result<void> convertModel(const std::string& input_path, const std::string& output_path,
                          const std::string& target_format);

// Profile model performance
struct ProfilingResult {
    std::map<std::string, double> layer_times_ms;
    double total_time_ms = 0.0;
    size_t memory_usage_bytes = 0;
    std::vector<std::string> bottlenecks;
};

Result<ProfilingResult> profileModel(std::shared_ptr<void> model,
                                     const std::vector<std::vector<float>>& sample_inputs);
} // namespace optimization

/**
 * Model migration utilities
 */
namespace migration {
// Migrate embeddings from one model to another
Result<std::vector<float>> migrateEmbedding(const std::vector<float>& embedding,
                                            const std::string& from_model_id,
                                            const std::string& to_model_id);

// Create migration matrix between models
Result<std::vector<std::vector<float>>>
createMigrationMatrix(const std::string& from_model_id, const std::string& to_model_id,
                      const std::vector<std::pair<std::string, std::string>>& training_pairs = {});

// Validate migration compatibility
Result<bool> canMigrate(const ModelInfo& from_model, const ModelInfo& to_model);
} // namespace migration

} // namespace yams::vector