#include <yams/vector/embedding_generator.h>

#include <algorithm>
#include <cmath>
#include <sstream>
#include <regex>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <fstream>
#include <filesystem>
#include <random>
#include <iomanip>

// TODO: Replace with actual ONNX Runtime includes when available
// #include <onnxruntime_cxx_api.h>

namespace yams::vector {

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

    size_t getVocabSize() const {
        return vocab_to_id_.size();
    }

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
            "the", "and", "to", "of", "a", "in", "is", "it", "you", "that",
            "he", "was", "for", "on", "are", "as", "with", "his", "they", "i",
            "at", "be", "this", "have", "from", "or", "one", "had", "by", "word"
        };
        
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
        
        // Add new tokens dynamically (in real implementation, this wouldn't happen)
        vocab_to_id_[token] = vocab_to_id_.size();
        return vocab_to_id_[token];
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

std::vector<std::vector<int32_t>> TextPreprocessor::tokenizeBatch(const std::vector<std::string>& texts) {
    return pImpl->tokenizeBatch(texts);
}

std::vector<int32_t> TextPreprocessor::truncateTokens(const std::vector<int32_t>& tokens, size_t max_length) {
    return pImpl->truncateTokens(tokens, max_length);
}

std::vector<int32_t> TextPreprocessor::padTokens(const std::vector<int32_t>& tokens, size_t target_length) {
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

class ModelManager::Impl {
public:
    Impl() = default;

    bool loadModel(const std::string& model_name, const std::string& model_path) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (models_.find(model_name) != models_.end()) {
            return true; // Already loaded
        }
        
        try {
            // TODO: Load actual ONNX model
            // For now, simulate model loading
            
            if (!std::filesystem::exists(model_path)) {
                setError("Model file not found: " + model_path);
                return false;
            }
            
            ModelInfo info;
            info.model_path = model_path;
            info.embedding_dim = 384; // Default for all-MiniLM-L6-v2
            info.max_sequence_length = 512;
            info.model_size_bytes = std::filesystem::file_size(model_path);
            info.load_time = std::chrono::system_clock::now();
            
            // Detect embedding dimension from model name
            if (model_name.find("all-mpnet-base-v2") != std::string::npos) {
                info.embedding_dim = 768;
            } else if (model_name.find("multilingual") != std::string::npos) {
                info.embedding_dim = 384;
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

    std::vector<std::vector<float>> runInference(
        const std::string& model_name,
        const std::vector<std::vector<int32_t>>& input_tokens,
        const std::vector<std::vector<int32_t>>& attention_masks) {
        
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = models_.find(model_name);
        if (it == models_.end()) {
            return {};
        }
        
        auto& model_info = it->second;
        
        try {
            auto start = std::chrono::high_resolution_clock::now();
            
            // TODO: Run actual ONNX inference
            // For now, generate mock embeddings
            std::vector<std::vector<float>> embeddings;
            embeddings.reserve(input_tokens.size());
            
            std::random_device rd;
            std::mt19937 gen(rd());
            std::normal_distribution<float> dist(0.0f, 1.0f);
            
            for (size_t i = 0; i < input_tokens.size(); ++i) {
                std::vector<float> embedding(model_info.embedding_dim);
                
                // Generate deterministic but varied embeddings based on input
                std::mt19937 token_gen(input_tokens[i].empty() ? 0 : input_tokens[i][0]);
                std::normal_distribution<float> token_dist(0.0f, 1.0f);
                
                for (size_t j = 0; j < model_info.embedding_dim; ++j) {
                    embedding[j] = token_dist(token_gen);
                }
                
                // Normalize embedding
                double norm = 0.0;
                for (float val : embedding) {
                    norm += val * val;
                }
                norm = std::sqrt(norm);
                
                if (norm > 0.0) {
                    for (float& val : embedding) {
                        val /= norm;
                    }
                }
                
                embeddings.push_back(std::move(embedding));
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            // Update statistics
            model_info.stats.inference_count++;
            model_info.stats.total_inference_time += duration;
            
            return embeddings;

        } catch (const std::exception& e) {
            setError("Inference failed: " + std::string(e.what()));
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
        
        // TODO: Add ONNX runtime session
        // Ort::Session session;
    };

    void setError(const std::string& error) const {
        last_error_ = error;
        has_error_ = true;
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, ModelInfo> models_;
    mutable std::string last_error_;
    mutable bool has_error_ = false;
};

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

std::vector<std::vector<float>> ModelManager::runInference(
    const std::string& model_name,
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
// EmbeddingGenerator Implementation
// =============================================================================

class EmbeddingGenerator::Impl {
public:
    explicit Impl(const EmbeddingConfig& config) 
        : config_(config)
        , preprocessor_(config)
        , initialized_(false)
        , has_error_(false) {}

    bool initialize() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (initialized_) {
            return true;
        }

        try {
            // Load the model
            if (!model_manager_.loadModel(config_.model_name, config_.model_path)) {
                setError("Failed to load embedding model");
                return false;
            }

            // Verify model dimensions match config
            auto model_dim = model_manager_.getModelEmbeddingDim(config_.model_name);
            if (model_dim != 0 && model_dim != config_.embedding_dim) {
                config_.embedding_dim = model_dim; // Update config to match model
            }

            initialized_ = true;
            has_error_ = false;
            return true;

        } catch (const std::exception& e) {
            setError("Initialization failed: " + std::string(e.what()));
            return false;
        }
    }

    bool isInitialized() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return initialized_;
    }

    void shutdown() {
        std::lock_guard<std::mutex> lock(mutex_);
        model_manager_.clearCache();
        initialized_ = false;
        has_error_ = false;
        last_error_.clear();
    }

    std::vector<float> generateEmbedding(const std::string& text) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (!initialized_) {
            setError("Generator not initialized");
            return {};
        }

        try {
            auto start = std::chrono::high_resolution_clock::now();
            
            // Tokenize and preprocess
            auto tokens = preprocessor_.tokenize(text);
            tokens = preprocessor_.truncateTokens(tokens, config_.max_sequence_length);
            tokens = preprocessor_.padTokens(tokens, config_.max_sequence_length);
            
            auto attention_mask = preprocessor_.generateAttentionMask(tokens);
            
            // Run inference
            std::vector<std::vector<int32_t>> batch_tokens = {tokens};
            std::vector<std::vector<int32_t>> batch_masks = {attention_mask};
            
            auto embeddings = model_manager_.runInference(config_.model_name, batch_tokens, batch_masks);
            
            if (embeddings.empty()) {
                setError("Model inference failed");
                return {};
            }
            
            auto embedding = embeddings[0];
            
            // Normalize if requested
            if (config_.normalize_embeddings) {
                embedding = embedding_utils::normalizeEmbedding(embedding);
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            // Update statistics
            stats_.total_texts_processed++;
            stats_.total_tokens_processed += tokens.size();
            stats_.total_inference_time += duration;
            stats_.avg_inference_time = stats_.total_inference_time / stats_.total_texts_processed;
            stats_.updateThroughput();
            
            has_error_ = false;
            return embedding;

        } catch (const std::exception& e) {
            setError("Embedding generation failed: " + std::string(e.what()));
            return {};
        }
    }

    std::vector<std::vector<float>> generateEmbeddings(const std::vector<std::string>& texts) {
        if (texts.empty()) {
            return {};
        }

        std::lock_guard<std::mutex> lock(mutex_);
        
        if (!initialized_) {
            setError("Generator not initialized");
            return {};
        }

        try {
            auto start = std::chrono::high_resolution_clock::now();
            
            // Process texts in batches
            std::vector<std::vector<float>> all_embeddings;
            all_embeddings.reserve(texts.size());
            
            for (size_t i = 0; i < texts.size(); i += config_.batch_size) {
                size_t batch_end = std::min(i + config_.batch_size, texts.size());
                std::vector<std::string> batch_texts(texts.begin() + i, texts.begin() + batch_end);
                
                // Tokenize batch
                auto batch_tokens = preprocessor_.tokenizeBatch(batch_texts);
                std::vector<std::vector<int32_t>> batch_masks;
                
                // Truncate and pad
                for (auto& tokens : batch_tokens) {
                    tokens = preprocessor_.truncateTokens(tokens, config_.max_sequence_length);
                    tokens = preprocessor_.padTokens(tokens, config_.max_sequence_length);
                }
                
                // Generate attention masks
                for (const auto& tokens : batch_tokens) {
                    batch_masks.push_back(preprocessor_.generateAttentionMask(tokens));
                }
                
                // Run batch inference
                auto batch_embeddings = model_manager_.runInference(config_.model_name, batch_tokens, batch_masks);
                
                if (batch_embeddings.size() != batch_texts.size()) {
                    setError("Batch inference size mismatch");
                    return {};
                }
                
                // Normalize if requested
                if (config_.normalize_embeddings) {
                    batch_embeddings = embedding_utils::normalizeEmbeddings(batch_embeddings);
                }
                
                // Add to results
                all_embeddings.insert(all_embeddings.end(), 
                                    batch_embeddings.begin(), 
                                    batch_embeddings.end());
                
                stats_.total_batches++;
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            // Update statistics
            stats_.total_texts_processed += texts.size();
            for (const auto& text : texts) {
                stats_.total_tokens_processed += estimateTokenCount(text);
            }
            stats_.total_inference_time += duration;
            stats_.avg_inference_time = stats_.total_inference_time / stats_.total_texts_processed;
            stats_.batch_count++;
            stats_.updateThroughput();
            
            has_error_ = false;
            return all_embeddings;

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
        return config_.embedding_dim;
    }

    size_t getMaxSequenceLength() const {
        return config_.max_sequence_length;
    }

    const EmbeddingConfig& getConfig() const {
        return config_;
    }

    GenerationStats getStats() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return stats_;
    }

    void resetStats() {
        std::lock_guard<std::mutex> lock(mutex_);
        stats_ = GenerationStats{};
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
    void setError(const std::string& error) const {
        last_error_ = error;
        has_error_ = true;
    }

    EmbeddingConfig config_;
    TextPreprocessor preprocessor_;
    ModelManager model_manager_;
    bool initialized_;
    GenerationStats stats_;
    mutable std::mutex mutex_;
    mutable std::string last_error_;
    mutable bool has_error_;
};

EmbeddingGenerator::EmbeddingGenerator(const EmbeddingConfig& config)
    : pImpl(std::make_unique<Impl>(config)) {}

EmbeddingGenerator::~EmbeddingGenerator() = default;
EmbeddingGenerator::EmbeddingGenerator(EmbeddingGenerator&&) noexcept = default;
EmbeddingGenerator& EmbeddingGenerator::operator=(EmbeddingGenerator&&) noexcept = default;

bool EmbeddingGenerator::initialize() {
    return pImpl->initialize();
}

bool EmbeddingGenerator::isInitialized() const {
    return pImpl->isInitialized();
}

void EmbeddingGenerator::shutdown() {
    pImpl->shutdown();
}

std::vector<float> EmbeddingGenerator::generateEmbedding(const std::string& text) {
    return pImpl->generateEmbedding(text);
}

std::vector<std::vector<float>> EmbeddingGenerator::generateEmbeddings(const std::vector<std::string>& texts) {
    return pImpl->generateEmbeddings(texts);
}

std::future<std::vector<float>> EmbeddingGenerator::generateEmbeddingAsync(const std::string& text) {
    return std::async(std::launch::async, [this, text]() {
        return generateEmbedding(text);
    });
}

std::future<std::vector<std::vector<float>>> EmbeddingGenerator::generateEmbeddingsAsync(const std::vector<std::string>& texts) {
    return std::async(std::launch::async, [this, texts]() {
        return generateEmbeddings(texts);
    });
}

bool EmbeddingGenerator::loadModel(const std::string& model_path) {
    // Update config and reinitialize
    // This is a simplified implementation
    return initialize();
}

bool EmbeddingGenerator::switchModel(const std::string& model_name, const EmbeddingConfig& new_config) {
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
    return pImpl->getEmbeddingDimension();
}

size_t EmbeddingGenerator::getMaxSequenceLength() const {
    return pImpl->getMaxSequenceLength();
}

const EmbeddingConfig& EmbeddingGenerator::getConfig() const {
    return pImpl->getConfig();
}

void EmbeddingGenerator::updateConfig(const EmbeddingConfig& new_config) {
    shutdown();
    pImpl = std::make_unique<Impl>(new_config);
}

GenerationStats EmbeddingGenerator::getStats() const {
    return pImpl->getStats();
}

void EmbeddingGenerator::resetStats() {
    pImpl->resetStats();
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
        norm += val * val;
    }
    norm = std::sqrt(norm);
    
    if (norm == 0.0) {
        return embedding;
    }
    
    std::vector<float> normalized;
    normalized.reserve(embedding.size());
    for (float val : embedding) {
        normalized.push_back(static_cast<float>(val / norm));
    }
    
    return normalized;
}

std::vector<std::vector<float>> normalizeEmbeddings(const std::vector<std::vector<float>>& embeddings) {
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
        magnitude += val * val;
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
        if (i > 0) oss << ", ";
        oss << std::fixed << std::setprecision(4) << embedding[i];
    }
    
    if (embedding.size() > max_values) {
        oss << ", ... (" << embedding.size() << " total)";
    }
    
    oss << "]";
    return oss.str();
}

EmbeddingConfig loadConfigFromFile(const std::string& config_path) {
    // TODO: Implement JSON loading
    // For now, return default config
    return EmbeddingConfig{};
}

bool saveConfigToFile(const EmbeddingConfig& config, const std::string& config_path) {
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

bool downloadModel(const std::string& model_name, const std::string& target_dir) {
    // TODO: Implement model downloading
    return false;
}

} // namespace embedding_utils

} // namespace yams::vector