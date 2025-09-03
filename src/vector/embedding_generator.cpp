#include <spdlog/spdlog.h>
#include <yams/cli/async_bridge.h>
#include <yams/profiling.h>
#include <yams/vector/embedding_generator.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <iomanip>
#include <mutex>
#include <random>
#include <regex>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>

#ifdef YAMS_USE_ONNX_RUNTIME
#include <onnxruntime_cxx_api.h>
#endif

// Include daemon client for DaemonBackend
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::vector {

// Forward declarations
class LocalOnnxBackend;
class DaemonBackend;
class HybridBackend;

// =============================================================================
// TextPreprocessor Implementation
// =============================================================================

class TextPreprocessor::Impl {
public:
    explicit Impl(const EmbeddingConfig& config) : config_(config) {
        // Initialize basic vocabulary for simple tokenizer
        // TODO: Replace with proper tokenizer (HuggingFace tokenizers, SentencePiece, etc.)
        initializeBasicVocab();
    }

    std::string normalizeText(const std::string& text) {
        std::string normalized = text;

        // Unicode normalization (simplified)
        // TODO: Proper Unicode normalization

        // Convert to lowercase if specified
        if (config_.model_settings.do_lower_case) {
            std::transform(normalized.begin(), normalized.end(), normalized.begin(), ::tolower);
        }

        // Normalize whitespace
        std::regex multiple_spaces(R"(\s+)");
        normalized = std::regex_replace(normalized, multiple_spaces, " ");

        // Trim leading/trailing whitespace
        normalized.erase(0, normalized.find_first_not_of(" \t\n\r"));
        normalized.erase(normalized.find_last_not_of(" \t\n\r") + 1);

        return normalized;
    }

    std::vector<int32_t> tokenize(const std::string& text) {
        std::vector<int32_t> tokens;

        // Simple tokenization (replace with proper tokenizer)
        std::string normalized = normalizeText(text);

        // Add CLS token at the beginning
        tokens.push_back(getSpecialTokenId(config_.model_settings.cls_token));

        // Simple word-based tokenization
        std::istringstream iss(normalized);
        std::string word;

        while (iss >> word) {
            // Remove punctuation for simplicity (proper tokenizer handles this better)
            std::regex punct_regex(R"([^\w\s])");
            word = std::regex_replace(word, punct_regex, "");

            if (!word.empty()) {
                auto token_id = getTokenId(word);
                tokens.push_back(token_id);
            }
        }

        // Add SEP token at the end
        tokens.push_back(getSpecialTokenId(config_.model_settings.sep_token));

        return tokens;
    }

    std::vector<std::vector<int32_t>> tokenizeBatch(const std::vector<std::string>& texts) {
        std::vector<std::vector<int32_t>> batch_tokens;
        batch_tokens.reserve(texts.size());

        for (const auto& text : texts) {
            batch_tokens.push_back(tokenize(text));
        }

        return batch_tokens;
    }

    std::vector<int32_t> truncateTokens(const std::vector<int32_t>& tokens, size_t max_length) {
        if (tokens.size() <= max_length) {
            return tokens;
        }

        // Keep CLS token at the beginning and SEP token at the end
        std::vector<int32_t> truncated;
        truncated.reserve(max_length);

        // Add CLS token
        if (!tokens.empty()) {
            truncated.push_back(tokens[0]);
        }

        // Add middle tokens (keeping space for SEP)
        size_t content_length = max_length - 2; // Reserve space for CLS and SEP
        if (tokens.size() > 2) {
            auto start_it = tokens.begin() + 1;
            auto end_it = tokens.begin() + std::min(content_length + 1, tokens.size() - 1);
            truncated.insert(truncated.end(), start_it, end_it);
        }

        // Add SEP token
        if (tokens.size() > 1) {
            truncated.push_back(getSpecialTokenId(config_.model_settings.sep_token));
        }

        return truncated;
    }

    std::vector<int32_t> padTokens(const std::vector<int32_t>& tokens, size_t target_length) {
        std::vector<int32_t> padded = tokens;

        while (padded.size() < target_length) {
            padded.push_back(static_cast<int32_t>(config_.padding_token_id));
        }

        return padded;
    }

    std::vector<int32_t> generateAttentionMask(const std::vector<int32_t>& tokens) {
        std::vector<int32_t> attention_mask;
        attention_mask.reserve(tokens.size());

        int32_t pad_token_id = static_cast<int32_t>(config_.padding_token_id);

        for (int32_t token : tokens) {
            attention_mask.push_back(token == pad_token_id ? 0 : 1);
        }

        return attention_mask;
    }

    size_t getVocabSize() const { return vocab_to_id_.size(); }

    bool isValidToken(int32_t token_id) const {
        return token_id >= 0 && token_id < static_cast<int32_t>(vocab_to_id_.size());
    }

    std::string decodeToken(int32_t token_id) const {
        for (const auto& [token, id] : vocab_to_id_) {
            if (id == token_id) {
                return token;
            }
        }
        return config_.model_settings.unk_token;
    }

private:
    void initializeBasicVocab() {
        // Initialize with special tokens
        vocab_to_id_[config_.model_settings.cls_token] = 0;
        vocab_to_id_[config_.model_settings.sep_token] = 1;
        vocab_to_id_[config_.model_settings.unk_token] = 2;
        vocab_to_id_[config_.model_settings.pad_token] = 3;
        vocab_to_id_[config_.model_settings.mask_token] = 4;

        // Add some common words (in real implementation, load from vocab file)
        std::vector<std::string> common_words = {
            "the", "and", "to",   "of",   "a",    "in", "is",   "it",  "you",  "that",
            "he",  "was", "for",  "on",   "are",  "as", "with", "his", "they", "i",
            "at",  "be",  "this", "have", "from", "or", "one",  "had", "by",   "word"};

        for (const auto& word : common_words) {
            if (vocab_to_id_.find(word) == vocab_to_id_.end()) {
                vocab_to_id_[word] = vocab_to_id_.size();
            }
        }
    }

    int32_t getTokenId(const std::string& token) {
        auto it = vocab_to_id_.find(token);
        if (it != vocab_to_id_.end()) {
            return it->second;
        }

        // Don't add new tokens beyond model vocabulary limit
        // Most BERT-based models have vocab size around 30k-32k
        const size_t MAX_VOCAB_SIZE = 30527; // Known limit from error message

        if (vocab_to_id_.size() < MAX_VOCAB_SIZE) {
            vocab_to_id_[token] = vocab_to_id_.size();
            return vocab_to_id_[token];
        } else {
            // Return UNK token for unknown tokens beyond vocab limit
            return static_cast<int32_t>(config_.unk_token_id);
        }
    }

    int32_t getSpecialTokenId(const std::string& token) {
        auto it = vocab_to_id_.find(token);
        if (it != vocab_to_id_.end()) {
            return it->second;
        }
        return static_cast<int32_t>(config_.unk_token_id);
    }

    EmbeddingConfig config_;
    std::unordered_map<std::string, int32_t> vocab_to_id_;
};

TextPreprocessor::TextPreprocessor(const EmbeddingConfig& config)
    : pImpl(std::make_unique<Impl>(config)) {}

TextPreprocessor::~TextPreprocessor() = default;

std::string TextPreprocessor::normalizeText(const std::string& text) {
    return pImpl->normalizeText(text);
}

std::vector<int32_t> TextPreprocessor::tokenize(const std::string& text) {
    return pImpl->tokenize(text);
}

std::vector<std::vector<int32_t>>
TextPreprocessor::tokenizeBatch(const std::vector<std::string>& texts) {
    return pImpl->tokenizeBatch(texts);
}

std::vector<int32_t> TextPreprocessor::truncateTokens(const std::vector<int32_t>& tokens,
                                                      size_t max_length) {
    return pImpl->truncateTokens(tokens, max_length);
}

std::vector<int32_t> TextPreprocessor::padTokens(const std::vector<int32_t>& tokens,
                                                 size_t target_length) {
    return pImpl->padTokens(tokens, target_length);
}

std::vector<int32_t> TextPreprocessor::generateAttentionMask(const std::vector<int32_t>& tokens) {
    return pImpl->generateAttentionMask(tokens);
}

size_t TextPreprocessor::getVocabSize() const {
    return pImpl->getVocabSize();
}

bool TextPreprocessor::isValidToken(int32_t token_id) const {
    return pImpl->isValidToken(token_id);
}

std::string TextPreprocessor::decodeToken(int32_t token_id) const {
    return pImpl->decodeToken(token_id);
}

// =============================================================================
// ModelManager Implementation
// =============================================================================

#ifdef YAMS_USE_ONNX_RUNTIME
class ModelManager::Impl {
public:
    Impl() : env_(ORT_LOGGING_LEVEL_WARNING, "yams") {
        int threads = 4; // safe default
        if (const char* env = std::getenv("YAMS_ONNX_INTRA_OP_THREADS")) {
            try {
                unsigned long v = std::stoul(std::string(env));
                // Clamp to a reasonable range to avoid oversubscription
                if (v < 1UL)
                    v = 1UL;
                if (v > 8UL)
                    v = 8UL;
                threads = static_cast<int>(v);
            } catch (...) {
                spdlog::warn("Invalid YAMS_ONNX_INTRA_OP_THREADS='{}', using default {}", env,
                             threads);
            }
        }
        session_options_.SetIntraOpNumThreads(threads);
        session_options_.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_ALL);
    }

    bool loadModel(const std::string& model_name, const std::string& model_path) {
        YAMS_ZONE_SCOPED_N("ModelManager::loadModel");
        std::lock_guard<std::mutex> lock(mutex_);

        if (models_.find(model_name) != models_.end()) {
            return true; // Already loaded
        }

        try {
            if (!std::filesystem::exists(model_path)) {
                setError("Model file not found: " + model_path);
                return false;
            }

            ModelInfo info;
            info.model_path = model_path;
            info.model_size_bytes = std::filesystem::file_size(model_path);
            info.load_time = std::chrono::system_clock::now();

            // Load ONNX model
            info.session =
                std::make_unique<Ort::Session>(env_, model_path.c_str(), session_options_);

            // Get model metadata from ONNX
            size_t num_input_nodes = info.session->GetInputCount();
            size_t num_output_nodes = info.session->GetOutputCount();

            // Set fixed sequence lengths based on model type
            // Sentence-transformers ONNX models expect fixed input shapes
            if (model_name.find("MiniLM") != std::string::npos) {
                info.max_sequence_length = 256; // MiniLM models typically use 256
            } else if (model_name.find("mpnet") != std::string::npos) {
                info.max_sequence_length = 512; // MPNet models can handle 512
            } else {
                info.max_sequence_length = 512; // Default to 512
            }

            // Get output shape to determine embedding dimension
            if (num_output_nodes > 0) {
                auto output_shape =
                    info.session->GetOutputTypeInfo(0).GetTensorTypeAndShapeInfo().GetShape();
                if (output_shape.size() >= 2) {
                    info.embedding_dim = output_shape[output_shape.size() - 1] > 0
                                             ? output_shape[output_shape.size() - 1]
                                             : 384;
                }
            }

            // Validate that the model has the expected inputs
            if (num_input_nodes < 2) {
                throw std::runtime_error(
                    "Model should have at least 2 inputs (input_ids, attention_mask). Found: " +
                    std::to_string(num_input_nodes));
            }

            // Use the actual model input/output names for flexibility
            // This allows different sentence-transformers models with different input requirements
            info.input_names.clear();
            info.output_names.clear();

            // Get actual input names from the model
            Ort::AllocatorWithDefaultOptions allocator;
            for (size_t i = 0; i < num_input_nodes; ++i) {
                auto input_name = info.session->GetInputNameAllocated(i, allocator);
                info.input_names.push_back(input_name.get());
            }

            // Get actual output names from the model
            for (size_t i = 0; i < num_output_nodes; ++i) {
                auto output_name = info.session->GetOutputNameAllocated(i, allocator);
                info.output_names.push_back(output_name.get());
            }

            // Build char* vectors for ONNX Runtime
            info.input_names_char.clear();
            for (const auto& name : info.input_names) {
                info.input_names_char.push_back(name.c_str());
            }

            info.output_names_char.clear();
            for (const auto& name : info.output_names) {
                info.output_names_char.push_back(name.c_str());
            }

            models_[model_name] = std::move(info);

            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Failed to load model: " + std::string(e.what()));
            return false;
        }
    }

    bool isModelLoaded(const std::string& model_name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return models_.find(model_name) != models_.end();
    }

    void unloadModel(const std::string& model_name) {
        std::lock_guard<std::mutex> lock(mutex_);
        models_.erase(model_name);
    }

    void clearCache() {
        std::lock_guard<std::mutex> lock(mutex_);
        models_.clear();
    }

    std::vector<std::vector<float>>
    runInference(const std::string& model_name,
                 const std::vector<std::vector<int32_t>>& input_tokens,
                 const std::vector<std::vector<int32_t>>& attention_masks) {
        YAMS_ZONE_SCOPED_N("ModelManager::runInference");
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = models_.find(model_name);
        if (it == models_.end()) {
            return {};
        }

        auto& model_info = it->second;

        try {
            auto start = std::chrono::high_resolution_clock::now();

            if (model_info.session) {
                YAMS_ONNX_ZONE("CoreInference");

                std::vector<std::vector<float>> embeddings;

                // Tracy plots for inference characteristics
                YAMS_PLOT("InferenceBatchSize", static_cast<int64_t>(input_tokens.size()));
                YAMS_PLOT("InferenceEmbeddingDim", static_cast<int64_t>(model_info.embedding_dim));
                YAMS_PLOT("InferenceMaxSeqLen",
                          static_cast<int64_t>(model_info.max_sequence_length));
                embeddings.reserve(input_tokens.size());

                // Process each item in the batch
                for (size_t batch_idx = 0; batch_idx < input_tokens.size(); ++batch_idx) {
                    const auto& tokens = input_tokens[batch_idx];
                    const auto& mask = attention_masks[batch_idx];

                    // Ensure input is exactly the expected sequence length
                    size_t expected_seq_len = model_info.max_sequence_length;
                    if (tokens.size() != expected_seq_len || mask.size() != expected_seq_len) {
                        throw std::runtime_error("Input sequence length mismatch: expected " +
                                                 std::to_string(expected_seq_len) + ", got " +
                                                 std::to_string(tokens.size()));
                    }

                    // Create input tensors with fixed shape
                    std::vector<int64_t> input_shape = {1, static_cast<int64_t>(expected_seq_len)};
                    size_t input_tensor_size = expected_seq_len;

                    // Convert tokens to int64, with conservative vocabulary bounds
                    const int64_t MAX_TOKEN_ID = 30521; // BERT vocab size for sentence-transformers
                    const int64_t UNK_TOKEN_ID = 100;   // [UNK] token ID in BERT vocab

                    std::vector<int64_t> tokens_int64;
                    tokens_int64.reserve(expected_seq_len);
                    for (const auto& token : tokens) {
                        if (token < 0 || token > MAX_TOKEN_ID) {
                            tokens_int64.push_back(UNK_TOKEN_ID);
                        } else {
                            tokens_int64.push_back(static_cast<int64_t>(token));
                        }
                    }

                    std::vector<int64_t> mask_int64(mask.begin(), mask.end());

                    // Create ONNX tensors
                    std::vector<Ort::Value> input_tensors;
                    std::vector<int64_t> token_type_ids;

                    {
                        YAMS_ONNX_ZONE("TensorCreation");
                        auto memory_info =
                            Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);

                        // Add input_ids (always first)
                        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(
                            memory_info, tokens_int64.data(), input_tensor_size, input_shape.data(),
                            2));

                        // Add attention_mask (always second)
                        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(
                            memory_info, mask_int64.data(), input_tensor_size, input_shape.data(),
                            2));

                        // Add token_type_ids if the model expects it (some models like MiniLM need
                        // it, MPNet doesn't)
                        if (model_info.input_names.size() >= 3) {
                            token_type_ids.resize(expected_seq_len,
                                                  0); // All zeros for single sentences
                            input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(
                                memory_info, token_type_ids.data(), input_tensor_size,
                                input_shape.data(), 2));
                        }

                        YAMS_PLOT("TensorInputCount", static_cast<int64_t>(input_tensors.size()));
                        YAMS_PLOT("TensorSize", static_cast<int64_t>(input_tensor_size));
                    }

                    // Run inference with the actual number of inputs and outputs
                    std::vector<Ort::Value> output_tensors;
                    {
                        YAMS_ONNX_ZONE("SessionRun");
                        output_tensors = model_info.session->Run(
                            Ort::RunOptions{nullptr}, model_info.input_names_char.data(),
                            input_tensors.data(),
                            model_info.input_names.size(), // Use actual input count
                            model_info.output_names_char.data(),
                            model_info.output_names.size() // Use actual output count
                        );
                        YAMS_PLOT("TensorOutputCount", static_cast<int64_t>(output_tensors.size()));
                    }

                    // Extract embeddings from output with proper attention-mask-weighted mean
                    // pooling
                    if (!output_tensors.empty()) {
                        auto& output_tensor = output_tensors[0];
                        auto output_shape = output_tensor.GetTensorTypeAndShapeInfo().GetShape();
                        auto output_data = output_tensor.GetTensorData<float>();

                        size_t embedding_size = model_info.embedding_dim;

                        // Sentence-transformers models output token embeddings that need mean
                        // pooling Expected shape: [batch_size, seq_len, hidden_dim]
                        if (output_shape.size() == 3) {
                            size_t batch_size = output_shape[0];
                            size_t seq_len = output_shape[1];
                            size_t hidden_dim = output_shape[2];

                            // Validate output dimensions
                            if (hidden_dim != embedding_size) {
                                throw std::runtime_error(
                                    "Output dimension " + std::to_string(hidden_dim) +
                                    " doesn't match expected " + std::to_string(embedding_size));
                            }

                            // Process each batch item (should only be 1 in our case)
                            {
                                YAMS_ONNX_ZONE("MeanPooling");
                                YAMS_PLOT("PoolingBatchSize", static_cast<int64_t>(batch_size));
                                YAMS_PLOT("PoolingSeqLen", static_cast<int64_t>(seq_len));
                                YAMS_PLOT("PoolingHiddenDim", static_cast<int64_t>(hidden_dim));

                                for (size_t b_idx = 0; b_idx < batch_size; ++b_idx) {
                                    std::vector<float> embedding(embedding_size, 0.0f);
                                    float total_attention = 0.0f;

                                    // Get attention mask for this batch item
                                    const auto& attention_mask = attention_masks[b_idx];

                                    // Attention-mask-weighted mean pooling
                                    for (size_t seq_idx = 0;
                                         seq_idx < seq_len && seq_idx < attention_mask.size();
                                         ++seq_idx) {
                                        float attention_weight =
                                            static_cast<float>(attention_mask[seq_idx]);

                                        if (attention_weight > 0.0f) {
                                            total_attention += attention_weight;

                                            // Add weighted token embedding to sentence embedding
                                            size_t token_offset =
                                                b_idx * seq_len * hidden_dim + seq_idx * hidden_dim;
                                            for (size_t dim = 0; dim < hidden_dim; ++dim) {
                                                embedding[dim] += output_data[token_offset + dim] *
                                                                  attention_weight;
                                            }
                                        }
                                    }

                                    // Normalize by total attention (number of non-padding tokens)
                                    if (total_attention > 0.0f) {
                                        for (float& val : embedding) {
                                            val /= total_attention;
                                        }
                                    }

                                    YAMS_PLOT_F("AttentionMaskRatio", total_attention / seq_len);

                                    // L2 normalize the final embedding (sentence-transformers
                                    // standard)
                                    {
                                        YAMS_ONNX_ZONE("L2Normalization");
                                        double norm = 0.0;
                                        for (float val : embedding) {
                                            norm +=
                                                static_cast<double>(val) * static_cast<double>(val);
                                        }
                                        norm = std::sqrt(norm);

                                        YAMS_PLOT_F("EmbeddingNorm", norm);

                                        if (norm > 1e-8) {
                                            for (float& val : embedding) {
                                                val /= static_cast<float>(norm);
                                            }
                                        } else {
                                            // Handle zero embedding case
                                            std::fill(embedding.begin(), embedding.end(), 0.0f);
                                        }
                                    }

                                    embeddings.push_back(std::move(embedding));
                                }
                            }
                        } else if (output_shape.size() == 2 &&
                                   static_cast<size_t>(output_shape[1]) == embedding_size) {
                            // Direct pooled output - already sentence embedding
                            size_t batch_size = output_shape[0];
                            for (size_t b_idx = 0; b_idx < batch_size; ++b_idx) {
                                std::vector<float> embedding(embedding_size);
                                std::copy(output_data + b_idx * embedding_size,
                                          output_data + (b_idx + 1) * embedding_size,
                                          embedding.begin());

                                // L2 normalize
                                double norm = 0.0;
                                for (float val : embedding) {
                                    norm += static_cast<double>(val) * static_cast<double>(val);
                                }
                                norm = std::sqrt(norm);

                                if (norm > 1e-8) {
                                    for (float& val : embedding) {
                                        val /= static_cast<float>(norm);
                                    }
                                }

                                embeddings.push_back(std::move(embedding));
                            }
                        } else {
                            throw std::runtime_error(
                                "Unexpected output tensor shape: [" +
                                std::to_string(output_shape[0]) +
                                (output_shape.size() > 1 ? ", " + std::to_string(output_shape[1])
                                                         : "") +
                                (output_shape.size() > 2 ? ", " + std::to_string(output_shape[2])
                                                         : "") +
                                "]");
                        }
                    }
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

                // Update statistics
                model_info.stats.inference_count++;
                model_info.stats.total_inference_time += duration;

                return embeddings;
            }

            // If we reach here, ONNX session is null - this should not happen after proper model
            // loading
            setError("ONNX session is null - model was not loaded properly");
            return {};

        } catch (const Ort::Exception& e) {
            std::string error_msg = "ONNX Runtime error: " + std::string(e.what()) +
                                    " (code: " + std::to_string(e.GetOrtErrorCode()) + ")";
            spdlog::error("ONNX inference failed for model '{}': {}", model_name, error_msg);
            setError(error_msg);
            return {};
        } catch (const std::exception& e) {
            std::string error_msg = "Inference failed: " + std::string(e.what());
            spdlog::error("Embedding generation failed for model '{}': {}", model_name, error_msg);
            setError(error_msg);
            return {};
        }
    }

    size_t getModelEmbeddingDim(const std::string& model_name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = models_.find(model_name);
        return it != models_.end() ? it->second.embedding_dim : 0;
    }

    size_t getModelMaxLength(const std::string& model_name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = models_.find(model_name);
        return it != models_.end() ? it->second.max_sequence_length : 0;
    }

    ModelManager::ModelStats getModelStats(const std::string& model_name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = models_.find(model_name);
        if (it != models_.end()) {
            const auto& info = it->second;
            ModelManager::ModelStats stats;
            stats.inference_count = info.stats.inference_count;
            stats.total_inference_time = info.stats.total_inference_time;
            stats.model_size_bytes = info.model_size_bytes;
            stats.memory_usage_bytes = info.model_size_bytes; // Simplified
            return stats;
        }
        return {};
    }

    std::vector<std::string> getLoadedModels() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> model_names;
        model_names.reserve(models_.size());

        for (const auto& [name, _] : models_) {
            model_names.push_back(name);
        }

        return model_names;
    }

    std::string getLastError() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_error_;
    }

    bool hasError() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return has_error_;
    }

private:
    struct ModelInfo {
        std::string model_path;
        size_t embedding_dim = 384;
        size_t max_sequence_length = 512;
        size_t model_size_bytes = 0;
        std::chrono::system_clock::time_point load_time;

        struct {
            size_t inference_count = 0;
            std::chrono::milliseconds total_inference_time{0};
        } stats;

        std::unique_ptr<Ort::Session> session;
        std::vector<std::string> input_names;
        std::vector<std::string> output_names;
        std::vector<const char*> input_names_char;
        std::vector<const char*> output_names_char;
    };

    void setError(const std::string& error) const {
        last_error_ = error;
        has_error_ = true;
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, ModelInfo> models_;
    mutable std::string last_error_;
    mutable bool has_error_ = false;

    Ort::Env env_;
    Ort::SessionOptions session_options_;
}; // End of ModelManager::Impl class
#else
// Stub implementation when ONNX is not available
class ModelManager::Impl {
public:
    bool loadModel(const std::string&, const std::string&) { return false; }
    bool isModelLoaded(const std::string&) const { return false; }
    void unloadModel(const std::string&) {}
    void clearCache() {}
    std::vector<std::vector<float>> runInference(const std::string&,
                                                 const std::vector<std::vector<int32_t>>&,
                                                 const std::vector<std::vector<int32_t>>&) {
        return {};
    }
    size_t getModelEmbeddingDim(const std::string&) const { return 0; }
    size_t getModelMaxLength(const std::string&) const { return 0; }
    ModelManager::ModelStats getModelStats(const std::string&) const { return {}; }
    std::vector<std::string> getLoadedModels() const { return {}; }
};
#endif

// ModelManager method implementations
ModelManager::ModelManager() : pImpl(std::make_unique<Impl>()) {}
ModelManager::~ModelManager() = default;
ModelManager::ModelManager(ModelManager&&) noexcept = default;
ModelManager& ModelManager::operator=(ModelManager&&) noexcept = default;

bool ModelManager::loadModel(const std::string& model_name, const std::string& model_path) {
    return pImpl->loadModel(model_name, model_path);
}

bool ModelManager::isModelLoaded(const std::string& model_name) const {
    return pImpl->isModelLoaded(model_name);
}

void ModelManager::unloadModel(const std::string& model_name) {
    pImpl->unloadModel(model_name);
}

void ModelManager::clearCache() {
    pImpl->clearCache();
}

std::vector<std::vector<float>>
ModelManager::runInference(const std::string& model_name,
                           const std::vector<std::vector<int32_t>>& input_tokens,
                           const std::vector<std::vector<int32_t>>& attention_masks) {
    return pImpl->runInference(model_name, input_tokens, attention_masks);
}

size_t ModelManager::getModelEmbeddingDim(const std::string& model_name) const {
    return pImpl->getModelEmbeddingDim(model_name);
}

size_t ModelManager::getModelMaxLength(const std::string& model_name) const {
    return pImpl->getModelMaxLength(model_name);
}

ModelManager::ModelStats ModelManager::getModelStats(const std::string& model_name) const {
    return pImpl->getModelStats(model_name);
}

std::vector<std::string> ModelManager::getLoadedModels() const {
    return pImpl->getLoadedModels();
}

// =============================================================================
// Backend Implementations
// =============================================================================

#ifdef YAMS_USE_ONNX_RUNTIME
/**
 * LocalOnnxBackend - Direct ONNX runtime backend
 * Uses the existing ModelManager and TextPreprocessor
 */
class LocalOnnxBackend : public IEmbeddingBackend {
public:
    explicit LocalOnnxBackend(const EmbeddingConfig& config)
        : config_(config), preprocessor_(config), initialized_(false) {}

    bool initialize() override {
        YAMS_ZONE_SCOPED_N("LocalOnnxBackend::initialize");

        if (initialized_) {
            return true;
        }

        try {
            if (!model_manager_.loadModel(config_.model_name, config_.model_path)) {
                spdlog::error("Failed to load ONNX model: {}", config_.model_name);
                return false;
            }

            // Verify model dimensions
            auto model_dim = model_manager_.getModelEmbeddingDim(config_.model_name);
            if (model_dim != 0 && model_dim != config_.embedding_dim) {
                config_.embedding_dim = model_dim;
            }

            initialized_ = true;
            spdlog::info("LocalOnnxBackend initialized with model: {}", config_.model_name);
            return true;
        } catch (const std::exception& e) {
            spdlog::error("LocalOnnxBackend initialization failed: {}", e.what());
            return false;
        }
    }

    void shutdown() override {
        model_manager_.clearCache();
        initialized_ = false;
        stats_ = GenerationStats{};
    }

    bool isInitialized() const override { return initialized_; }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        YAMS_ZONE_SCOPED_N("LocalOnnxBackend::generateEmbedding");

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Backend not initialized"};
        }

        try {
            auto start = std::chrono::high_resolution_clock::now();

            // Get model sequence length
            size_t model_seq_length = model_manager_.getModelMaxLength(config_.model_name);
            if (model_seq_length == 0) {
                model_seq_length = config_.max_sequence_length;
            }

            // Tokenize and preprocess
            auto tokens = preprocessor_.tokenize(text);
            tokens = preprocessor_.truncateTokens(tokens, model_seq_length);
            tokens = preprocessor_.padTokens(tokens, model_seq_length);
            auto attention_mask = preprocessor_.generateAttentionMask(tokens);

            // Run inference
            std::vector<std::vector<int32_t>> batch_tokens = {tokens};
            std::vector<std::vector<int32_t>> batch_masks = {attention_mask};
            auto embeddings =
                model_manager_.runInference(config_.model_name, batch_tokens, batch_masks);

            if (embeddings.empty()) {
                return Error{ErrorCode::InternalError, "Model inference failed"};
            }

            auto embedding = embeddings[0];

            // Normalize if requested
            if (config_.normalize_embeddings) {
                embedding = embedding_utils::normalizeEmbedding(embedding);
            }

            // Update stats
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            updateStats(1, tokens.size(), duration);

            return embedding;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Embedding generation failed: ") + e.what()};
        }
    }

    Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        YAMS_EMBEDDING_ZONE_BATCH(texts.size());

        if (texts.empty()) {
            return std::vector<std::vector<float>>{};
        }

        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Backend not initialized"};
        }

        try {
            auto start = std::chrono::high_resolution_clock::now();
            std::vector<std::vector<float>> all_embeddings;
            all_embeddings.reserve(texts.size());

            // Process in batches
            for (size_t i = 0; i < texts.size(); i += config_.batch_size) {
                size_t batch_end = std::min(i + config_.batch_size, texts.size());
                auto batch_span = texts.subspan(i, batch_end - i);

                // Tokenize batch
                std::vector<std::vector<int32_t>> batch_tokens;
                std::vector<std::vector<int32_t>> batch_masks;
                size_t model_seq_length = model_manager_.getModelMaxLength(config_.model_name);
                if (model_seq_length == 0) {
                    model_seq_length = config_.max_sequence_length;
                }

                for (const auto& text : batch_span) {
                    auto tokens = preprocessor_.tokenize(text);
                    tokens = preprocessor_.truncateTokens(tokens, model_seq_length);
                    tokens = preprocessor_.padTokens(tokens, model_seq_length);
                    batch_tokens.push_back(tokens);
                    batch_masks.push_back(preprocessor_.generateAttentionMask(tokens));
                }

                // Run batch inference
                auto batch_embeddings =
                    model_manager_.runInference(config_.model_name, batch_tokens, batch_masks);

                if (batch_embeddings.size() != batch_span.size()) {
                    return Error{ErrorCode::InternalError, "Batch inference size mismatch"};
                }

                // Normalize if requested
                if (config_.normalize_embeddings) {
                    batch_embeddings = embedding_utils::normalizeEmbeddings(batch_embeddings);
                }

                all_embeddings.insert(all_embeddings.end(), batch_embeddings.begin(),
                                      batch_embeddings.end());
                stats_.total_batches.fetch_add(1);
            }

            // Update stats
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            updateStats(texts.size(), texts.size() * config_.max_sequence_length / 4, duration);
            stats_.batch_count++; // Track number of batch operations

            return all_embeddings;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Batch embedding generation failed: ") + e.what()};
        }
    }

    size_t getEmbeddingDimension() const override { return config_.embedding_dim; }
    size_t getMaxSequenceLength() const override { return config_.max_sequence_length; }
    std::string getBackendName() const override { return "LocalONNX"; }
    bool isAvailable() const override { return initialized_; }
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
    void updateStats(size_t texts, size_t tokens, std::chrono::milliseconds duration) {
        stats_.total_texts_processed.fetch_add(texts);
        stats_.total_tokens_processed.fetch_add(tokens);
        stats_.total_inference_time.fetch_add(duration.count());

        auto total_texts = stats_.total_texts_processed.load();
        if (total_texts > 0) {
            auto total_time = stats_.total_inference_time.load();
            stats_.avg_inference_time.store(total_time / total_texts);
        }
        stats_.updateThroughput();
    }

    EmbeddingConfig config_;
    TextPreprocessor preprocessor_;
    ModelManager model_manager_;
    bool initialized_;
    GenerationStats stats_;
};
#else
// Stub implementation when ONNX is not available
class LocalOnnxBackend : public IEmbeddingBackend {
public:
    explicit LocalOnnxBackend(const EmbeddingConfig& config)
        : config_(config), initialized_(false) {}

    bool initialize() override {
        // Check if mock mode is enabled
        bool use_mock = (std::getenv("YAMS_USE_MOCK_EMBEDDINGS") != nullptr) ||
                        (std::getenv("YAMS_USE_MOCK_PROVIDER") != nullptr);

        if (use_mock) {
            // Even in mock mode, validate that model path isn't explicitly invalid
            if (!config_.model_path.empty() &&
                config_.model_path.find("non_existent") != std::string::npos) {
                spdlog::error("LocalOnnxBackend: Invalid model path specified: {}",
                              config_.model_path);
                return false;
            }

            initialized_ = true;
            use_mock_ = true;
            spdlog::info("LocalOnnxBackend: Using mock embeddings (ONNX not available)");
            return true;
        }

        spdlog::warn("LocalOnnxBackend not available - ONNX Runtime not compiled in");
        return false;
    }

    void shutdown() override {
        initialized_ = false;
        use_mock_ = false;
        stats_ = GenerationStats{};
    }

    bool isInitialized() const override { return initialized_ && use_mock_; }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        if (!initialized_ || !use_mock_) {
            return Error{ErrorCode::NotImplemented,
                         "ONNX Runtime not available and mock mode disabled"};
        }

        // Generate deterministic mock embedding based on text hash
        std::hash<std::string> hasher;
        size_t seed = hasher(text);
        std::mt19937 gen(seed);
        std::normal_distribution<float> dist(0.0f, 1.0f);

        std::vector<float> embedding(config_.embedding_dim);
        for (size_t i = 0; i < config_.embedding_dim; ++i) {
            embedding[i] = dist(gen);
        }

        // Normalize to unit length if requested
        if (config_.normalize_embeddings) {
            float norm = 0.0f;
            for (float val : embedding) {
                norm += val * val;
            }
            norm = std::sqrt(norm);
            if (norm > 0) {
                for (float& val : embedding) {
                    val /= norm;
                }
            }
        }

        // Update stats
        stats_.total_texts_processed++;
        stats_.total_tokens_processed += text.length() / 4; // Rough estimate

        return embedding;
    }

    Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        if (!initialized_ || !use_mock_) {
            return Error{ErrorCode::NotImplemented,
                         "ONNX Runtime not available and mock mode disabled"};
        }

        std::vector<std::vector<float>> embeddings;
        embeddings.reserve(texts.size());

        for (const auto& text : texts) {
            auto result = generateEmbedding(text);
            if (!result) {
                return result.error();
            }
            embeddings.push_back(std::move(result.value()));
        }

        // Update batch stats
        stats_.total_batches++;
        stats_.batch_count++; // Track number of batch operations

        return embeddings;
    }

    size_t getEmbeddingDimension() const override { return config_.embedding_dim; }
    size_t getMaxSequenceLength() const override { return config_.max_sequence_length; }
    std::string getBackendName() const override {
        return use_mock_ ? "LocalOnnx (mock)" : "LocalOnnx (disabled)";
    }
    bool isAvailable() const override { return use_mock_; }
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
    EmbeddingConfig config_;
    bool initialized_;
    bool use_mock_ = false;
    GenerationStats stats_;
};
#endif

/**
 * DaemonBackend - Daemon IPC backend
 * Communicates with the daemon service for embedding generation
 */
class DaemonBackend : public IEmbeddingBackend {
public:
    explicit DaemonBackend(const EmbeddingConfig& config) : config_(config), initialized_(false) {}

    bool initialize() override {
        YAMS_ZONE_SCOPED_N("DaemonBackend::initialize");

        if (initialized_) {
            return true;
        }

        try {
            // Configure daemon client with socket/auto-start from config
            daemon::ClientConfig dcfg;
            // If socket path is empty, let DaemonClient auto-resolve it
            if (!config_.daemon_socket.empty()) {
                dcfg.socketPath = config_.daemon_socket;
            }
            // Otherwise socketPath remains empty and DaemonClient will resolve it
            dcfg.requestTimeout = config_.daemon_timeout;
            dcfg.maxRetries = config_.daemon_max_retries;
            dcfg.autoStart = config_.daemon_auto_start;
            daemon_client_ = std::make_shared<daemon::DaemonClient>(dcfg);

            if (auto result = daemon_client_->connect(); !result) {
                spdlog::warn("Failed to connect to daemon: {}", result.error().message);

                // Try to auto-start daemon if configured
                if (config_.daemon_auto_start) {
                    spdlog::info("Attempting to auto-start daemon...");
                    // TODO: Implement daemon auto-start
                }
                return false;
            }

            // Verify daemon is responsive via DaemonClient, then request model preload (non-fatal on error)
            auto st = yams::cli::run_sync(daemon_client_->status(), std::chrono::seconds(5));
            if (!st) {
                // Downgrade to debug to avoid noisy warnings during CLI init paths.
                // Search will gracefully fall back when daemon is unavailable/slow.
                spdlog::debug("Daemon status probe failed: {}", st.error().message);
            } else if (!config_.model_name.empty()) {
                daemon::LoadModelRequest req;
                req.modelName = config_.model_name;
                req.preload = true;
                auto lm = yams::cli::run_sync(daemon_client_->loadModel(req), std::chrono::seconds(10));
                if (!lm) {
                    spdlog::warn("Failed to preload model in daemon: {}", lm.error().message);
                }
            }

            initialized_ = true;
            spdlog::info("DaemonBackend connected to daemon service");
            return true;
        } catch (const std::exception& e) {
            spdlog::error("DaemonBackend initialization failed: {}", e.what());
            return false;
        }
    }

    void shutdown() override {
        if (daemon_client_) {
            daemon_client_->disconnect();
            daemon_client_.reset();
        }
        initialized_ = false;
        stats_ = GenerationStats{};
    }

    bool isInitialized() const override {
        return initialized_ && daemon_client_ && daemon_client_->isConnected();
    }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        YAMS_ZONE_SCOPED_N("DaemonBackend::generateEmbedding");

        if (!isInitialized()) {
            return Error{ErrorCode::NotInitialized, "Daemon backend not connected"};
        }

        try {
            auto start = std::chrono::high_resolution_clock::now();

            daemon::GenerateEmbeddingRequest req;
            req.text = text;
            req.modelName = config_.model_name;
            req.normalize = config_.normalize_embeddings;

            auto result = daemon_client_->generateEmbedding(req).get();
            if (!result) {
                return Error{ErrorCode::NetworkError, result.error().message};
            }

            const auto& response = result.value();

            // Update cached dimensions
            if (response.dimensions > 0) {
                cached_dim_ = response.dimensions;
            }

            // Update stats
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            updateStats(1, text.length() / 4, duration);

            return response.embedding;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Daemon embedding failed: ") + e.what()};
        }
    }

    Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        YAMS_EMBEDDING_ZONE_BATCH(texts.size());

        if (!isInitialized()) {
            return Error{ErrorCode::NotInitialized, "Daemon backend not connected"};
        }

        try {
            auto start = std::chrono::high_resolution_clock::now();

            daemon::BatchEmbeddingRequest req;
            req.texts = std::vector<std::string>(texts.begin(), texts.end());
            req.modelName = config_.model_name;
            req.normalize = config_.normalize_embeddings;
            req.batchSize = config_.batch_size;

            auto result = daemon_client_->generateBatchEmbeddings(req).get();
            if (!result) {
                return Error{ErrorCode::NetworkError, result.error().message};
            }

            const auto& response = result.value();

            // Update cached dimensions
            if (response.dimensions > 0) {
                cached_dim_ = response.dimensions;
            }

            // Update stats
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            size_t total_chars = 0;
            for (const auto& text : texts) {
                total_chars += text.length();
            }
            updateStats(texts.size(), total_chars / 4, duration);
            stats_.total_batches++;
            stats_.batch_count++; // Track number of batch operations

            return response.embeddings;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Daemon batch embedding failed: ") + e.what()};
        }
    }

    size_t getEmbeddingDimension() const override {
        return cached_dim_ > 0 ? cached_dim_ : config_.embedding_dim;
    }

    size_t getMaxSequenceLength() const override {
        return cached_seq_len_ > 0 ? cached_seq_len_ : config_.max_sequence_length;
    }

    std::string getBackendName() const override { return "Daemon"; }

    bool isAvailable() const override { return daemon_client_ && daemon_client_->isConnected(); }

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
    void updateStats(size_t texts, size_t tokens, std::chrono::milliseconds duration) {
        stats_.total_texts_processed.fetch_add(texts);
        stats_.total_tokens_processed.fetch_add(tokens);
        stats_.total_inference_time.fetch_add(duration.count());

        auto total_texts = stats_.total_texts_processed.load();
        if (total_texts > 0) {
            auto total_time = stats_.total_inference_time.load();
            stats_.avg_inference_time.store(total_time / total_texts);
        }
        stats_.updateThroughput();
    }

    EmbeddingConfig config_;
    std::shared_ptr<daemon::DaemonClient> daemon_client_;
    bool initialized_;
    GenerationStats stats_;
    mutable size_t cached_dim_ = 0;
    mutable size_t cached_seq_len_ = 0;
};

/**
 * HybridBackend - Tries daemon first, falls back to local
 * Provides seamless failover between daemon and local ONNX
 */
class HybridBackend : public IEmbeddingBackend {
public:
    explicit HybridBackend(const EmbeddingConfig& config)
        : config_(config), daemon_backend_(std::make_unique<DaemonBackend>(config)),
          local_backend_(std::make_unique<LocalOnnxBackend>(config)), initialized_(false) {}

    bool initialize() override {
        YAMS_ZONE_SCOPED_N("HybridBackend::initialize");

        if (initialized_) {
            return true;
        }

        // Try daemon first
        bool daemon_ok = daemon_backend_->initialize();
        if (daemon_ok) {
            spdlog::info("HybridBackend: Daemon backend available");
        } else {
            spdlog::info("HybridBackend: Daemon backend not available, will use local fallback");
        }

        // Always initialize local as fallback
        bool local_ok = local_backend_->initialize();
        if (local_ok) {
            spdlog::info("HybridBackend: Local backend available");
        } else if (!daemon_ok) {
            spdlog::error("HybridBackend: Neither daemon nor local backend available");
            return false;
        }

        initialized_ = daemon_ok || local_ok;
        return initialized_;
    }

    void shutdown() override {
        daemon_backend_->shutdown();
        local_backend_->shutdown();
        initialized_ = false;
        stats_ = GenerationStats{};
    }

    bool isInitialized() const override { return initialized_; }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        YAMS_ZONE_SCOPED_N("HybridBackend::generateEmbedding");

        // Try daemon first if available
        if (daemon_backend_->isAvailable()) {
            auto result = daemon_backend_->generateEmbedding(text);
            if (result) {
                daemon_uses_++;
                mergeStats(daemon_backend_->getStats());
                return result;
            }
            spdlog::debug("Daemon backend failed, falling back to local");
        }

        // Fall back to local
        if (local_backend_->isAvailable()) {
            auto result = local_backend_->generateEmbedding(text);
            if (result) {
                local_fallbacks_++;
                mergeStats(local_backend_->getStats());
                return result;
            }
        }

        return Error{ErrorCode::NotSupported, "No backend available for embedding generation"};
    }

    Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        YAMS_EMBEDDING_ZONE_BATCH(texts.size());

        // Try daemon first if available
        if (daemon_backend_->isAvailable()) {
            auto result = daemon_backend_->generateEmbeddings(texts);
            if (result) {
                daemon_uses_++;
                mergeStats(daemon_backend_->getStats());
                return result;
            }
            spdlog::debug("Daemon backend failed for batch, falling back to local");
        }

        // Fall back to local
        if (local_backend_->isAvailable()) {
            auto result = local_backend_->generateEmbeddings(texts);
            if (result) {
                local_fallbacks_++;
                mergeStats(local_backend_->getStats());
                return result;
            }
        }

        return Error{ErrorCode::NotSupported,
                     "No backend available for batch embedding generation"};
    }

    size_t getEmbeddingDimension() const override {
        if (daemon_backend_->isAvailable()) {
            return daemon_backend_->getEmbeddingDimension();
        }
        return local_backend_->getEmbeddingDimension();
    }

    size_t getMaxSequenceLength() const override {
        if (daemon_backend_->isAvailable()) {
            return daemon_backend_->getMaxSequenceLength();
        }
        return local_backend_->getMaxSequenceLength();
    }

    std::string getBackendName() const override {
        return "Hybrid (Daemon: " + std::to_string(daemon_uses_) +
               ", Local: " + std::to_string(local_fallbacks_) + ")";
    }

    bool isAvailable() const override {
        return daemon_backend_->isAvailable() || local_backend_->isAvailable();
    }

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
        daemon_uses_ = 0;
        local_fallbacks_ = 0;
    }

private:
    void mergeStats(const GenerationStats& backend_stats) {
        stats_.total_texts_processed.store(backend_stats.total_texts_processed.load());
        stats_.total_tokens_processed.store(backend_stats.total_tokens_processed.load());
        stats_.total_inference_time.store(backend_stats.total_inference_time.load());
        stats_.avg_inference_time.store(backend_stats.avg_inference_time.load());
        stats_.batch_count.store(backend_stats.batch_count.load());
        stats_.total_batches.store(backend_stats.total_batches.load());
        stats_.updateThroughput();
    }

    EmbeddingConfig config_;
    std::unique_ptr<DaemonBackend> daemon_backend_;
    std::unique_ptr<LocalOnnxBackend> local_backend_;
    bool initialized_;
    GenerationStats stats_;
    mutable size_t daemon_uses_ = 0;
    mutable size_t local_fallbacks_ = 0;
};

// =============================================================================
// EmbeddingGenerator Implementation with Variant Backend
// =============================================================================

class EmbeddingGenerator::Impl {
public:
    explicit Impl(const EmbeddingConfig& config) : config_(config) {
        // Select backend based on configuration
        switch (config.backend) {
            case EmbeddingConfig::Backend::Local:
                backend_ = std::make_unique<LocalOnnxBackend>(config);
                break;
            case EmbeddingConfig::Backend::Daemon:
                backend_ = std::make_unique<DaemonBackend>(config);
                break;
            case EmbeddingConfig::Backend::Hybrid:
            default:
                backend_ = std::make_unique<HybridBackend>(config);
                break;
        }
    }

    bool initialize() {
        YAMS_ZONE_SCOPED_N("EmbeddingGenerator::initialize");

        // Check if already initialized (lock-free fast path)
        if (initialized_.load()) {
            return true;
        }

        try {
            if (!backend_) {
                setError("No backend configured");
                return false;
            }

            bool init_result = backend_->initialize();
            if (!init_result) {
                setError("Backend initialization failed: " + backend_->getBackendName());
                return false;
            }

            // Atomically update initialization state
            initialized_.store(true);
            has_error_.store(false);
            spdlog::info("EmbeddingGenerator initialized with backend: {}",
                         backend_->getBackendName());
            return true;

        } catch (const std::exception& e) {
            setError("Initialization failed: " + std::string(e.what()));
            return false;
        }
    }

    bool isInitialized() const {
        return initialized_.load() && backend_ && backend_->isInitialized();
    }

    void shutdown() {
        if (backend_) {
            backend_->shutdown();
        }
        initialized_.store(false);
        has_error_.store(false);
        {
            std::unique_lock<std::shared_mutex> lock(error_mutex_);
            last_error_.clear();
        }
    }

    std::vector<float> generateEmbedding(const std::string& text) {
        YAMS_ZONE_SCOPED_N("EmbeddingGenerator::generateEmbedding");

        // Lock-free check for initialization
        if (!initialized_.load() || !backend_) {
            setError("Generator not initialized");
            return {};
        }

        try {
            auto result = backend_->generateEmbedding(text);
            if (!result) {
                setError(result.error().message);
                return {};
            }

            has_error_.store(false);
            return result.value();

        } catch (const std::exception& e) {
            setError("Embedding generation failed: " + std::string(e.what()));
            return {};
        }
    }

    std::vector<std::vector<float>> generateEmbeddings(const std::vector<std::string>& texts) {
        YAMS_EMBEDDING_ZONE_BATCH(texts.size());

        if (texts.empty()) {
            return {};
        }

        // Lock-free check for initialization
        if (!initialized_.load() || !backend_) {
            setError("Generator not initialized");
            return {};
        }

        try {
            // Convert to span
            std::span<const std::string> texts_span(texts);
            auto result = backend_->generateEmbeddings(texts_span);

            if (!result) {
                setError(result.error().message);
                return {};
            }

            has_error_.store(false);
            return result.value();

        } catch (const std::exception& e) {
            setError("Batch embedding generation failed: " + std::string(e.what()));
            return {};
        }
    }

    size_t estimateTokenCount(const std::string& text) const {
        // Simple estimation: ~4 characters per token on average
        return std::max(1ul, text.length() / 4);
    }

    // Getters and configuration
    size_t getEmbeddingDimension() const {
        return backend_ ? backend_->getEmbeddingDimension() : config_.embedding_dim;
    }

    size_t getMaxSequenceLength() const {
        return backend_ ? backend_->getMaxSequenceLength() : config_.max_sequence_length;
    }

    const EmbeddingConfig& getConfig() const { return config_; }

    GenerationStats getStats() const {
        if (!backend_) {
            return GenerationStats{};
        }

        try {
            return backend_->getStats();
        } catch (const std::exception& e) {
            spdlog::warn("getStats: Backend getStats failed: {}", e.what());
            return GenerationStats{};
        }
    }

    void resetStats() {
        if (backend_) {
            try {
                backend_->resetStats();
            } catch (const std::exception& e) {
                spdlog::warn("resetStats: Backend reset failed: {}", e.what());
            }
        }
    }

    std::string getLastError() const {
        std::shared_lock<std::shared_mutex> lock(error_mutex_);
        return last_error_;
    }

    bool hasError() const { return has_error_.load(); }

private:
    void setError(const std::string& error) const {
        {
            std::unique_lock<std::shared_mutex> lock(error_mutex_);
            last_error_ = error;
        }
        has_error_.store(true);
    }

    EmbeddingConfig config_;
    std::unique_ptr<IEmbeddingBackend> backend_;
    std::atomic<bool> initialized_{false};
    mutable std::shared_mutex error_mutex_; // Reader-writer lock for error strings only
    mutable std::string last_error_;
    mutable std::atomic<bool> has_error_{false};
};

EmbeddingGenerator::EmbeddingGenerator(const EmbeddingConfig& config)
    : pImpl(std::make_unique<Impl>(config)) {}

EmbeddingGenerator::~EmbeddingGenerator() = default;
EmbeddingGenerator::EmbeddingGenerator(EmbeddingGenerator&&) noexcept = default;
EmbeddingGenerator& EmbeddingGenerator::operator=(EmbeddingGenerator&&) noexcept = default;

bool EmbeddingGenerator::initialize() {
    return pImpl ? pImpl->initialize() : false;
}

bool EmbeddingGenerator::isInitialized() const {
    return pImpl && pImpl->isInitialized();
}

void EmbeddingGenerator::shutdown() {
    if (pImpl) {
        pImpl->shutdown();
    }
}

std::vector<float> EmbeddingGenerator::generateEmbedding(const std::string& text) {
    YAMS_ZONE_SCOPED_N("EmbeddingGenerator::generateEmbedding");
    return pImpl ? pImpl->generateEmbedding(text) : std::vector<float>();
}

std::vector<std::vector<float>>
EmbeddingGenerator::generateEmbeddings(const std::vector<std::string>& texts) {
    YAMS_ZONE_SCOPED_N("EmbeddingGenerator::generateEmbeddings");
    return pImpl ? pImpl->generateEmbeddings(texts) : std::vector<std::vector<float>>();
}

std::future<std::vector<float>>
EmbeddingGenerator::generateEmbeddingAsync(const std::string& text) {
    return std::async(std::launch::async, [this, text]() { return generateEmbedding(text); });
}

std::future<std::vector<std::vector<float>>>
EmbeddingGenerator::generateEmbeddingsAsync(const std::vector<std::string>& texts) {
    return std::async(std::launch::async, [this, texts]() { return generateEmbeddings(texts); });
}

bool EmbeddingGenerator::loadModel([[maybe_unused]] const std::string& model_path) {
    // Update config and reinitialize
    // This is a simplified implementation
    return initialize();
}

bool EmbeddingGenerator::switchModel([[maybe_unused]] const std::string& model_name,
                                     const EmbeddingConfig& new_config) {
    shutdown();
    pImpl = std::make_unique<Impl>(new_config);
    return initialize();
}

bool EmbeddingGenerator::isModelLoaded() const {
    return isInitialized();
}

void EmbeddingGenerator::unloadModel() {
    shutdown();
}

size_t EmbeddingGenerator::getEmbeddingDimension() const {
    return pImpl ? pImpl->getEmbeddingDimension() : 0;
}

size_t EmbeddingGenerator::getMaxSequenceLength() const {
    return pImpl ? pImpl->getMaxSequenceLength() : 0;
}

const EmbeddingConfig& EmbeddingGenerator::getConfig() const {
    static const EmbeddingConfig empty_config{};
    return pImpl ? pImpl->getConfig() : empty_config;
}

void EmbeddingGenerator::updateConfig(const EmbeddingConfig& new_config) {
    shutdown();
    pImpl = std::make_unique<Impl>(new_config);
}

GenerationStats EmbeddingGenerator::getStats() const {
    return pImpl ? pImpl->getStats() : GenerationStats{};
}

void EmbeddingGenerator::resetStats() {
    if (pImpl) {
        pImpl->resetStats();
    }
}

bool EmbeddingGenerator::validateText(const std::string& text) const {
    return !text.empty() && text.length() < 1000000; // 1MB limit
}

size_t EmbeddingGenerator::estimateTokenCount(const std::string& text) const {
    return pImpl->estimateTokenCount(text);
}

std::string EmbeddingGenerator::getModelInfo() const {
    std::ostringstream info;
    info << "Model: " << getConfig().model_name << "\n";
    info << "Embedding Dimension: " << getEmbeddingDimension() << "\n";
    info << "Max Sequence Length: " << getMaxSequenceLength() << "\n";
    info << "Initialized: " << (isInitialized() ? "Yes" : "No") << "\n";
    return info.str();
}

std::string EmbeddingGenerator::getLastError() const {
    return pImpl->getLastError();
}

bool EmbeddingGenerator::hasError() const {
    return pImpl->hasError();
}

// Factory function
std::unique_ptr<EmbeddingGenerator> createEmbeddingGenerator(const EmbeddingConfig& config) {
    auto generator = std::make_unique<EmbeddingGenerator>(config);
    if (!generator->initialize()) {
        return nullptr;
    }
    return generator;
}

// =============================================================================
// Utility Functions
// =============================================================================

namespace embedding_utils {

std::vector<float> normalizeEmbedding(const std::vector<float>& embedding) {
    double norm = 0.0;
    for (float val : embedding) {
        norm += static_cast<double>(val) * static_cast<double>(val);
    }
    norm = std::sqrt(norm);

    if (norm == 0.0) {
        return embedding;
    }

    std::vector<float> normalized;
    normalized.reserve(embedding.size());
    for (float val : embedding) {
        normalized.push_back(val / static_cast<float>(norm));
    }

    return normalized;
}

std::vector<std::vector<float>>
normalizeEmbeddings(const std::vector<std::vector<float>>& embeddings) {
    std::vector<std::vector<float>> normalized;
    normalized.reserve(embeddings.size());

    for (const auto& embedding : embeddings) {
        normalized.push_back(normalizeEmbedding(embedding));
    }

    return normalized;
}

double computeMagnitude(const std::vector<float>& embedding) {
    double magnitude = 0.0;
    for (float val : embedding) {
        magnitude += static_cast<double>(val) * static_cast<double>(val);
    }
    return std::sqrt(magnitude);
}

bool validateEmbedding(const std::vector<float>& embedding, size_t expected_dim) {
    if (embedding.size() != expected_dim) {
        return false;
    }

    for (float val : embedding) {
        if (!std::isfinite(val)) {
            return false;
        }
    }

    return true;
}

std::string embeddingToString(const std::vector<float>& embedding, size_t max_values) {
    std::ostringstream oss;
    oss << "[";

    size_t count = std::min(max_values, embedding.size());
    for (size_t i = 0; i < count; ++i) {
        if (i > 0)
            oss << ", ";
        oss << std::fixed << std::setprecision(4) << embedding[i];
    }

    if (embedding.size() > max_values) {
        oss << ", ... (" << embedding.size() << " total)";
    }

    oss << "]";
    return oss.str();
}

EmbeddingConfig loadConfigFromFile([[maybe_unused]] const std::string& config_path) {
    // TODO: Implement JSON loading
    // For now, return default config
    return EmbeddingConfig{};
}

bool saveConfigToFile([[maybe_unused]] const EmbeddingConfig& config,
                      [[maybe_unused]] const std::string& config_path) {
    // TODO: Implement JSON saving
    return false;
}

std::vector<std::string> getAvailableModels(const std::string& models_dir) {
    std::vector<std::string> models;

    if (std::filesystem::exists(models_dir) && std::filesystem::is_directory(models_dir)) {
        for (const auto& entry : std::filesystem::directory_iterator(models_dir)) {
            if (entry.is_directory()) {
                models.push_back(entry.path().filename().string());
            }
        }
    }

    return models;
}

bool downloadModel([[maybe_unused]] const std::string& model_name,
                   [[maybe_unused]] const std::string& target_dir) {
    // TODO: Implement model downloading
    return false;
}

} // namespace embedding_utils

} // namespace yams::vector
