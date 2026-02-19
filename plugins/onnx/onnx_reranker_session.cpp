#include <yams/daemon/resource/onnx_reranker_session.h>
#include <yams/vector/embedding_generator.h>

#include "onnx_gpu_provider.h"
#include <yams/daemon/resource/gpu_info.h>

#include <nlohmann/json.hpp>
#include <onnxruntime_cxx_api.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <unordered_set>

namespace fs = std::filesystem;

// Global ONNX Runtime environment - shared singleton (same pattern as onnx_model_pool.cpp)
// Using leaking singleton to avoid static destruction issues with ONNX Runtime
static std::recursive_mutex* g_reranker_onnx_mutex = new std::recursive_mutex();
static Ort::Env* g_reranker_onnx_env = nullptr;
static bool g_reranker_onnx_env_initialized = false;

static Ort::Env& get_reranker_ort_env() {
    std::lock_guard<std::recursive_mutex> lock(*g_reranker_onnx_mutex);
    if (!g_reranker_onnx_env_initialized) {
        spdlog::info("[Reranker] Initializing global Ort::Env");

        OrtThreadingOptions* threading_options = nullptr;
        (void)Ort::GetApi().CreateThreadingOptions(&threading_options);
        if (threading_options) {
            (void)Ort::GetApi().SetGlobalIntraOpNumThreads(threading_options, 1);
            (void)Ort::GetApi().SetGlobalInterOpNumThreads(threading_options, 1);
            (void)Ort::GetApi().SetGlobalSpinControl(threading_options, 0);

            g_reranker_onnx_env =
                new Ort::Env(threading_options, ORT_LOGGING_LEVEL_WARNING, "YamsReranker");
            Ort::GetApi().ReleaseThreadingOptions(threading_options);
        } else {
            g_reranker_onnx_env = new Ort::Env(ORT_LOGGING_LEVEL_WARNING, "YamsReranker");
        }
        g_reranker_onnx_env_initialized = true;
    }
    return *g_reranker_onnx_env;
}

namespace yams::daemon {

// ============================================================================
// OnnxRerankerSession Implementation
// ============================================================================

class OnnxRerankerSession::Impl {
public:
    Impl(const std::string& modelPath, const std::string& modelName, const RerankerConfig& config)
        : modelPath_(modelPath), modelName_(modelName), config_(config),
          preprocessor_(vector::EmbeddingConfig{}) {
        // Check for mock/test mode
        if (std::getenv("YAMS_USE_MOCK_PROVIDER") || std::getenv("YAMS_SKIP_MODEL_LOADING") ||
            std::getenv("YAMS_TEST_MODE")) {
            testMode_ = true;
            spdlog::warn("[Reranker] Mock mode enabled via env; skipping ONNX init");
            return;
        }

        // Get global ONNX environment (shared across all reranker sessions)
        spdlog::info("[Reranker] Getting global Ort::Env for '{}'", modelName);
        env_ = &get_reranker_ort_env();

        // Configure session options
        sessionOptions_ = std::make_unique<Ort::SessionOptions>();

        // Thread configuration
        int intra = config.num_threads > 0 ? config.num_threads : 4;
        if (const char* s = std::getenv("YAMS_ONNX_INTRA_OP_THREADS")) {
            try {
                int v = std::stoi(s);
                if (v > 0 && v <= 64)
                    intra = v;
            } catch (...) {
            }
        }
        sessionOptions_->SetIntraOpNumThreads(intra);
        sessionOptions_->SetInterOpNumThreads(1);

        // Disable spinning for better resource sharing
        sessionOptions_->AddConfigEntry("session.intra_op.allow_spinning", "0");
        sessionOptions_->AddConfigEntry("session.inter_op.allow_spinning", "0");

        // Graph optimization
        sessionOptions_->SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_BASIC);
        sessionOptions_->EnableMemPattern();
        sessionOptions_->EnableCpuMemArena();

        // Attach GPU provider only if GPU is detected
        const auto& gpuInfo = resource::detectGpu();
        if (gpuInfo.detected) {
            std::string ep = onnx_util::appendGpuProvider(*sessionOptions_);
            spdlog::info("[Reranker] GPU detected ({}), using {} execution provider", gpuInfo.name,
                         ep);
        } else {
            spdlog::debug("[Reranker] No GPU detected, using CPU execution provider");
        }

        spdlog::info("[Reranker] SessionOptions configured: intra-op={}", intra);
    }

    Result<void> loadModel() {
        if (testMode_) {
            maxSequenceLength_ = config_.max_sequence_length;
            isLoaded_ = true;
            return Result<void>();
        }

        try {
            spdlog::info("[Reranker] Creating Ort::Session for '{}' at {}", modelName_, modelPath_);

            auto options = sessionOptions_->Clone();
            options.SetIntraOpNumThreads(1);
            options.SetInterOpNumThreads(1);

            session_ = std::make_unique<Ort::Session>(*env_, fs::path(modelPath_).c_str(), options);
            spdlog::info("[Reranker] Ort::Session created successfully");

            // Get input/output names
            Ort::AllocatorWithDefaultOptions allocator;
            size_t numInputs = session_->GetInputCount();
            size_t numOutputs = session_->GetOutputCount();

            for (size_t i = 0; i < numInputs; ++i) {
                auto name = session_->GetInputNameAllocated(i, allocator);
                inputNames_.push_back(name.get());
            }

            for (size_t i = 0; i < numOutputs; ++i) {
                auto name = session_->GetOutputNameAllocated(i, allocator);
                outputNames_.push_back(name.get());
            }

            // Parse config for max sequence length
            parseModelConfig();

            isLoaded_ = true;
            spdlog::info("[Reranker] Session ready (inputs={}, outputs={}, max_seq={})", numInputs,
                         numOutputs, maxSequenceLength_);
            return Result<void>();

        } catch (const Ort::Exception& e) {
            spdlog::error("[Reranker] Failed to load '{}': {}", modelName_, e.what());
            return Error{ErrorCode::InternalError,
                         std::string("Failed to load reranker model: ") + e.what()};
        } catch (const std::exception& e) {
            spdlog::error("[Reranker] Failed to load '{}': {}", modelName_, e.what());
            return Error{ErrorCode::InternalError,
                         std::string("Failed to load reranker model: ") + e.what()};
        }
    }

    Result<float> score(const std::string& query, const std::string& document) {
        if (testMode_) {
            // Mock scoring based on simple text overlap
            return computeMockScore(query, document);
        }

        if (!isLoaded_) {
            if (auto r = loadModel(); !r)
                return r.error();
        }

        return runOnnx(query, document);
    }

    Result<std::vector<float>> scoreBatch(const std::string& query,
                                          const std::vector<std::string>& documents) {
        if (testMode_) {
            std::vector<float> scores;
            scores.reserve(documents.size());
            for (const auto& doc : documents) {
                scores.push_back(computeMockScore(query, doc));
            }
            return scores;
        }

        if (!isLoaded_) {
            if (auto r = loadModel(); !r)
                return r.error();
        }

        return runOnnxBatch(query, documents);
    }

    bool isValid() const { return testMode_ ? isLoaded_ : (isLoaded_ && session_ != nullptr); }

    size_t getMaxSequenceLength() const { return maxSequenceLength_; }

private:
    // Tokenize a (query, document) pair for cross-encoder
    // Format: [CLS] query [SEP] document [SEP]
    std::vector<int32_t> tokenizePair(const std::string& query, const std::string& document,
                                      std::vector<int32_t>& tokenTypeIds) {
        const int32_t CLS = 101;
        const int32_t SEP = 102;
        const int32_t PAD = 0;

        auto queryTokens = preprocessor_.tokenize(query);
        auto docTokens = preprocessor_.tokenize(document);

        // Reserve space: [CLS] query [SEP] document [SEP]
        const size_t maxQueryLen = maxSequenceLength_ / 3;
        const size_t maxDocLen = maxSequenceLength_ - maxQueryLen - 3;

        if (queryTokens.size() > maxQueryLen) {
            queryTokens.resize(maxQueryLen);
        }
        if (docTokens.size() > maxDocLen) {
            docTokens.resize(maxDocLen);
        }

        std::vector<int32_t> inputIds;
        inputIds.reserve(maxSequenceLength_);
        tokenTypeIds.clear();
        tokenTypeIds.reserve(maxSequenceLength_);

        // [CLS] query [SEP]
        inputIds.push_back(CLS);
        tokenTypeIds.push_back(0);

        for (auto t : queryTokens) {
            inputIds.push_back(t);
            tokenTypeIds.push_back(0);
        }

        inputIds.push_back(SEP);
        tokenTypeIds.push_back(0);

        // document [SEP]
        for (auto t : docTokens) {
            inputIds.push_back(t);
            tokenTypeIds.push_back(1);
        }

        inputIds.push_back(SEP);
        tokenTypeIds.push_back(1);

        // Pad to max sequence length
        while (inputIds.size() < maxSequenceLength_) {
            inputIds.push_back(PAD);
            tokenTypeIds.push_back(0);
        }

        return inputIds;
    }

    Result<float> runOnnx(const std::string& query, const std::string& document) {
        std::vector<int32_t> tokenTypeIds;
        auto inputIds = tokenizePair(query, document, tokenTypeIds);

        std::vector<int64_t> inputShape = {1, static_cast<int64_t>(maxSequenceLength_)};
        auto memoryInfo = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);

        // Convert to int64
        std::vector<int64_t> inputIds64(inputIds.begin(), inputIds.end());
        std::vector<int64_t> tokenTypeIds64(tokenTypeIds.begin(), tokenTypeIds.end());

        // Generate attention mask
        std::vector<int64_t> attentionMask(maxSequenceLength_, 0);
        for (size_t i = 0; i < inputIds.size(); ++i) {
            attentionMask[i] = (inputIds[i] != 0) ? 1 : 0;
        }

        std::vector<Ort::Value> inputs;
        inputs.push_back(Ort::Value::CreateTensor<int64_t>(
            memoryInfo, inputIds64.data(), inputIds64.size(), inputShape.data(), 2));
        inputs.push_back(Ort::Value::CreateTensor<int64_t>(
            memoryInfo, attentionMask.data(), attentionMask.size(), inputShape.data(), 2));
        if (inputNames_.size() >= 3) {
            inputs.push_back(Ort::Value::CreateTensor<int64_t>(
                memoryInfo, tokenTypeIds64.data(), tokenTypeIds64.size(), inputShape.data(), 2));
        }

        std::vector<const char*> inNames;
        for (auto& n : inputNames_)
            inNames.push_back(n.c_str());

        std::vector<const char*> outNames;
        for (auto& n : outputNames_)
            outNames.push_back(n.c_str());
        if (outNames.empty()) {
            static const char* defaultOut = "logits";
            outNames.push_back(defaultOut);
        }

        try {
            auto outputs = session_->Run(Ort::RunOptions{nullptr}, inNames.data(), inputs.data(),
                                         inputs.size(), outNames.data(), outNames.size());

            if (outputs.empty()) {
                return Error{ErrorCode::InternalError, "Reranker returned no outputs"};
            }

            const float* data = outputs[0].GetTensorData<float>();
            auto shape = outputs[0].GetTensorTypeAndShapeInfo().GetShape();

            // BGE reranker outputs logits; apply sigmoid for [0,1] score
            float logit = data[0];
            float score = 1.0f / (1.0f + std::exp(-logit));

            return score;

        } catch (const Ort::Exception& e) {
            return Error{ErrorCode::InternalError, std::string("ONNX error: ") + e.what()};
        }
    }

    Result<std::vector<float>> runOnnxBatch(const std::string& query,
                                            const std::vector<std::string>& documents) {
        if (documents.empty())
            return std::vector<float>{};

        const size_t batchSize = documents.size();
        std::vector<int64_t> inputShape = {static_cast<int64_t>(batchSize),
                                           static_cast<int64_t>(maxSequenceLength_)};
        const size_t tensorSize = batchSize * maxSequenceLength_;

        auto memoryInfo = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);

        std::vector<int64_t> inputIdsBatch;
        std::vector<int64_t> attentionMaskBatch;
        std::vector<int64_t> tokenTypeIdsBatch;
        inputIdsBatch.reserve(tensorSize);
        attentionMaskBatch.reserve(tensorSize);
        tokenTypeIdsBatch.reserve(tensorSize);

        for (const auto& doc : documents) {
            std::vector<int32_t> tokenTypeIds;
            auto inputIds = tokenizePair(query, doc, tokenTypeIds);

            for (auto id : inputIds) {
                inputIdsBatch.push_back(static_cast<int64_t>(id));
                attentionMaskBatch.push_back(id != 0 ? 1 : 0);
            }
            for (auto t : tokenTypeIds) {
                tokenTypeIdsBatch.push_back(static_cast<int64_t>(t));
            }
        }

        std::vector<Ort::Value> inputs;
        inputs.push_back(Ort::Value::CreateTensor<int64_t>(
            memoryInfo, inputIdsBatch.data(), inputIdsBatch.size(), inputShape.data(), 2));
        inputs.push_back(Ort::Value::CreateTensor<int64_t>(memoryInfo, attentionMaskBatch.data(),
                                                           attentionMaskBatch.size(),
                                                           inputShape.data(), 2));
        if (inputNames_.size() >= 3) {
            inputs.push_back(Ort::Value::CreateTensor<int64_t>(memoryInfo, tokenTypeIdsBatch.data(),
                                                               tokenTypeIdsBatch.size(),
                                                               inputShape.data(), 2));
        }

        std::vector<const char*> inNames;
        for (auto& n : inputNames_)
            inNames.push_back(n.c_str());

        std::vector<const char*> outNames;
        for (auto& n : outputNames_)
            outNames.push_back(n.c_str());
        if (outNames.empty()) {
            static const char* defaultOut = "logits";
            outNames.push_back(defaultOut);
        }

        try {
            auto outputs = session_->Run(Ort::RunOptions{nullptr}, inNames.data(), inputs.data(),
                                         inputs.size(), outNames.data(), outNames.size());

            if (outputs.empty()) {
                return Error{ErrorCode::InternalError, "Reranker returned no outputs"};
            }

            const float* data = outputs[0].GetTensorData<float>();
            auto shape = outputs[0].GetTensorTypeAndShapeInfo().GetShape();

            std::vector<float> scores;
            scores.reserve(batchSize);

            for (size_t i = 0; i < batchSize; ++i) {
                float logit = data[i];
                float score = 1.0f / (1.0f + std::exp(-logit));
                scores.push_back(score);
            }

            return scores;

        } catch (const Ort::Exception& e) {
            return Error{ErrorCode::InternalError, std::string("ONNX batch error: ") + e.what()};
        }
    }

    float computeMockScore(const std::string& query, const std::string& document) {
        // Simple mock: score based on word overlap
        auto tokenize = [](const std::string& text) {
            std::vector<std::string> words;
            std::string word;
            for (char c : text) {
                if (std::isalnum(static_cast<unsigned char>(c))) {
                    word += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
                } else if (!word.empty()) {
                    words.push_back(word);
                    word.clear();
                }
            }
            if (!word.empty())
                words.push_back(word);
            return words;
        };

        auto queryWords = tokenize(query);
        auto docWords = tokenize(document);

        if (queryWords.empty() || docWords.empty())
            return 0.0f;

        std::unordered_set<std::string> docSet(docWords.begin(), docWords.end());
        size_t overlap = 0;
        for (const auto& w : queryWords) {
            if (docSet.count(w))
                ++overlap;
        }

        // Jaccard-like score normalized to [0, 1]
        float score = static_cast<float>(overlap) / static_cast<float>(queryWords.size());
        return std::min(1.0f, score);
    }

    void parseModelConfig() {
        try {
            fs::path mp(modelPath_);
            std::vector<fs::path> candidates;
            if (mp.has_parent_path()) {
                candidates.push_back(mp.parent_path() / "config.json");
                if (mp.parent_path().has_parent_path()) {
                    candidates.push_back(mp.parent_path().parent_path() / "config.json");
                }
            }

            for (const auto& c : candidates) {
                if (!fs::exists(c))
                    continue;
                std::ifstream in(c);
                if (!in.good())
                    continue;

                nlohmann::json j;
                in >> j;

                if (j.contains("max_position_embeddings") &&
                    j["max_position_embeddings"].is_number_integer()) {
                    maxSequenceLength_ =
                        static_cast<size_t>(j["max_position_embeddings"].get<int>());
                }
                if (j.contains("max_length") && j["max_length"].is_number_integer()) {
                    maxSequenceLength_ = static_cast<size_t>(j["max_length"].get<int>());
                }
                break;
            }
        } catch (...) {
        }

        // Apply config override
        if (config_.max_sequence_length > 0 && config_.max_sequence_length < maxSequenceLength_) {
            maxSequenceLength_ = config_.max_sequence_length;
        }

        // Default to 512 for BGE rerankers
        if (maxSequenceLength_ == 0) {
            maxSequenceLength_ = 512;
        }
    }

    std::string modelPath_;
    std::string modelName_;
    RerankerConfig config_;
    vector::TextPreprocessor preprocessor_;

    Ort::Env* env_ = nullptr;
    std::unique_ptr<Ort::SessionOptions> sessionOptions_;
    std::unique_ptr<Ort::Session> session_;

    std::vector<std::string> inputNames_;
    std::vector<std::string> outputNames_;

    size_t maxSequenceLength_ = 512;
    bool isLoaded_ = false;
    bool testMode_ = false;
};

// ============================================================================
// OnnxRerankerSession Public Interface
// ============================================================================

OnnxRerankerSession::OnnxRerankerSession(const std::string& modelPath, const std::string& modelName,
                                         const RerankerConfig& config)
    : modelPath_(modelPath), modelName_(modelName), config_(config),
      pImpl_(std::make_unique<Impl>(modelPath, modelName, config)) {
    // Eagerly load the model
    spdlog::info("[Reranker] Created session for '{}' - eager loading...", modelName);
    if (auto r = pImpl_->loadModel(); !r) {
        spdlog::error("[Reranker] Eager load failed for '{}': {}", modelName, r.error().message);
    } else {
        maxSequenceLength_ = pImpl_->getMaxSequenceLength();
        spdlog::info("[Reranker] Session initialized: max_seq_len={}", maxSequenceLength_);
    }
}

OnnxRerankerSession::~OnnxRerankerSession() = default;

Result<float> OnnxRerankerSession::score(const std::string& query, const std::string& document) {
    return pImpl_->score(query, document);
}

Result<std::vector<float>>
OnnxRerankerSession::scoreBatch(const std::string& query,
                                const std::vector<std::string>& documents) {
    return pImpl_->scoreBatch(query, documents);
}

Result<std::vector<RerankerResult>>
OnnxRerankerSession::rerank(const std::string& query, const std::vector<std::string>& documents,
                            size_t topK) {
    auto scoresResult = pImpl_->scoreBatch(query, documents);
    if (!scoresResult)
        return scoresResult.error();

    auto& scores = scoresResult.value();

    // Build results with indices
    std::vector<RerankerResult> results;
    results.reserve(scores.size());
    for (size_t i = 0; i < scores.size(); ++i) {
        results.push_back({scores[i], i});
    }

    // Sort by score descending
    std::sort(results.begin(), results.end(),
              [](const RerankerResult& a, const RerankerResult& b) { return a.score > b.score; });

    // Limit to topK if specified
    if (topK > 0 && topK < results.size()) {
        results.resize(topK);
    }

    return results;
}

bool OnnxRerankerSession::isValid() const {
    return pImpl_->isValid();
}

} // namespace yams::daemon
