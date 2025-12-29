#include <yams/common/test_utils.h>
#include <yams/daemon/resource/onnx_model_pool.h>
#include <yams/vector/embedding_generator.h>

#include <nlohmann/json.hpp>
#include <onnxruntime_cxx_api.h>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <mutex>

#ifdef YAMS_ENABLE_ONNX_GENAI
#include <yams/daemon/resource/onnx_genai_adapter.h>
#endif

// Global ONNX Runtime environment - single instance per process, intentionally leaked.
// Using leaking singleton pattern because ONNX Runtime's LoggingManager destructor
// tries to lock a mutex that may be destroyed during static destruction, causing
// "mutex lock failed: Invalid argument" crashes.
static std::recursive_mutex* g_onnx_init_mutex = new std::recursive_mutex();
static Ort::Env* g_onnx_env = nullptr; // Intentionally leaked to avoid static destruction crash
static bool g_onnx_env_initialized = false;

static Ort::Env& get_global_ort_env() {
    std::lock_guard<std::recursive_mutex> lock(*g_onnx_init_mutex);
    if (!g_onnx_env_initialized) {
        spdlog::info("[ONNX] Initializing global Ort::Env (thread-safe, single instance)");

        OrtThreadingOptions* threading_options = nullptr;
        Ort::GetApi().CreateThreadingOptions(&threading_options);
        if (threading_options) {
            Ort::GetApi().SetGlobalIntraOpNumThreads(threading_options, 1);
            Ort::GetApi().SetGlobalInterOpNumThreads(threading_options, 1);
            Ort::GetApi().SetGlobalSpinControl(threading_options, 0);

            g_onnx_env = new Ort::Env(threading_options, ORT_LOGGING_LEVEL_WARNING, "YamsDaemon");
            Ort::GetApi().ReleaseThreadingOptions(threading_options);
            spdlog::info("[ONNX] Global Ort::Env initialized with custom threading options");
        } else {
            g_onnx_env = new Ort::Env(ORT_LOGGING_LEVEL_WARNING, "YamsDaemon");
            spdlog::info("[ONNX] Global Ort::Env initialized (default threading)");
        }
        g_onnx_env_initialized = true;
    }
    return *g_onnx_env;
}

// Check if we should skip model loading for tests
#ifdef GTEST_API_
#include <yams/common/test_utils.h>
#endif

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <thread>

namespace yams::daemon {

namespace fs = std::filesystem;

// ============================================================================
// OnnxModelSession Implementation
// ============================================================================

class OnnxModelSession::Impl {
public:
    Impl(const std::string& modelPath, const std::string& modelName,
         const vector::EmbeddingConfig& config)
        : modelPath_(modelPath), modelName_(modelName), config_(config), preprocessor_(config) {
#ifdef GTEST_API_
        // Check if we should skip model loading in test mode
        if (yams::test::shouldSkipModelLoading()) {
            test_mode_ = true;
            spdlog::debug("[ONNX] Test mode enabled - skipping actual model loading");
            return;
        }
#endif

        // Lightweight, environment-driven mock mode for CI and constrained hosts
        // (works regardless of gtest being present in this translation unit).
        if (std::getenv("YAMS_USE_MOCK_PROVIDER") || std::getenv("YAMS_SKIP_MODEL_LOADING") ||
            std::getenv("YAMS_TEST_MODE")) {
            test_mode_ = true;
            spdlog::warn("[ONNX] Mock provider mode enabled via env; skipping ONNX init");
            return;
        }

        // Optional: initialize GenAI adapter when enabled at build + requested at runtime
#ifdef YAMS_ENABLE_ONNX_GENAI
        // Always attempt to allocate GenAI adapter (unified policy: ONNX present => try GenAI)
        try {
            genai_ = std::make_unique<OnnxGenAIAdapter>();
            spdlog::info("[GenAI] (plugin) attempting adapter init for '{}'", modelName);
        } catch (...) {
            genai_.reset();
        }
#endif

        // Initialize ONNX Runtime environment
        // Use a single global Ort::Env per process (best practice)
        // The global env is protected by g_onnx_init_mutex during initialization.
        spdlog::info("[ONNX] Impl constructor: getting global Ort::Env for '{}'", modelName);
        env_ = &get_global_ort_env();
        spdlog::info("[ONNX] Impl constructor: global Ort::Env obtained for '{}'", modelName);

        // Configure session options
        sessionOptions_ = std::make_unique<Ort::SessionOptions>();
        // Allow env overrides for thread tuning
        auto detect_threads = [](const char* name, int fallback) {
            if (const char* s = std::getenv(name)) {
                try {
                    int v = std::stoi(s);
                    if (v > 0 && v <= 64)
                        return v;
                } catch (...) {
                }
            }
            return fallback;
        };
        auto detect_bool = [](const char* name, bool fallback) {
            if (const char* s = std::getenv(name)) {
                std::string val(s);
                std::transform(val.begin(), val.end(), val.begin(), ::tolower);
                if (val == "0" || val == "false" || val == "no")
                    return false;
                if (val == "1" || val == "true" || val == "yes")
                    return true;
            }
            return fallback;
        };

#ifdef _WIN32
        // Windows-specific: ALWAYS use minimal thread counts to avoid thread pool
        // exhaustion and "resource deadlock would occur" errors. The Windows thread
        // scheduler behaves differently than Linux. Do NOT use config.num_threads
        // here as it defaults to hardware_concurrency() which causes issues.
        int intra_default = 1;               // Single thread for intra-op
        int inter_default = 1;               // Single thread for inter-op
        bool allow_spinning_default = false; // No spinning on Windows
#else
        int intra_default = 4;
        int inter_default = 1;
        bool allow_spinning_default = true;
#endif

        // On Windows, ignore config.num_threads to enforce safe defaults
        // On other platforms, respect the config
#ifdef _WIN32
        int intra = detect_threads("YAMS_ONNX_INTRA_OP_THREADS", intra_default);
#else
        int intra = config.num_threads > 0 ? config.num_threads : intra_default;
        intra = detect_threads("YAMS_ONNX_INTRA_OP_THREADS", intra);
#endif
        int inter = detect_threads("YAMS_ONNX_INTER_OP_THREADS", inter_default);
        sessionOptions_->SetIntraOpNumThreads(intra);
        sessionOptions_->SetInterOpNumThreads(inter);

        // Thread spinning control - spinning threads consume CPU cycles waiting for work
        // which can cause contention issues on Windows with concurrent embedding requests
        bool allow_spinning = detect_bool("YAMS_ONNX_ALLOW_SPINNING", allow_spinning_default);
        sessionOptions_->AddConfigEntry("session.intra_op.allow_spinning",
                                        allow_spinning ? "1" : "0");
        sessionOptions_->AddConfigEntry("session.inter_op.allow_spinning",
                                        allow_spinning ? "1" : "0");

        spdlog::info("[ONNX] SessionOptions threads: intra-op={} inter-op={} spinning={}", intra,
                     inter, allow_spinning);

#ifdef _WIN32
        // On Windows, use sequential execution mode to avoid thread pool issues
        // This ensures ONNX Runtime doesn't spawn additional threads internally
        sessionOptions_->SetExecutionMode(ExecutionMode::ORT_SEQUENTIAL);
        spdlog::info("[ONNX] Windows: using sequential execution mode");
#endif

        // Graph optimization level: BASIC for fast startup, ALL for production
        // ORT_ENABLE_ALL performs expensive graph transformations (minutes for large models)
        // ORT_ENABLE_BASIC is much faster and sufficient for most embeddings models
        GraphOptimizationLevel optLevel = GraphOptimizationLevel::ORT_ENABLE_BASIC;
        if (const char* opt = std::getenv("YAMS_ONNX_OPT_LEVEL")) {
            std::string level(opt);
            std::transform(level.begin(), level.end(), level.begin(), ::tolower);
            if (level == "all" || level == "extended") {
                optLevel = GraphOptimizationLevel::ORT_ENABLE_ALL;
                spdlog::info("[ONNX] Using ORT_ENABLE_ALL optimization (slow first load)");
            } else if (level == "extended") {
                optLevel = GraphOptimizationLevel::ORT_ENABLE_EXTENDED;
            }
        }
        sessionOptions_->SetGraphOptimizationLevel(optLevel);

        // Enable memory pattern optimization for faster inference
        sessionOptions_->EnableMemPattern();
        // Enable CPU memory arena for better memory reuse
        sessionOptions_->EnableCpuMemArena();

        if (config.enable_gpu) {
            // TODO: Add GPU provider when available
            spdlog::warn("GPU support requested but not yet implemented");
        }
    }

    Result<void> loadModel() {
        if (test_mode_) {
            // In test mode, pretend loading succeeded
            // Derive embeddingDim_/maxSequenceLength_/pooling/normalize from nearby configs when
            // available to better match real model characteristics.
            try {
                parseModelConfigHints();
            } catch (...) {
            }
            // Heuristics based on common model families when config hints are absent
            auto lname = modelName_;
            std::transform(lname.begin(), lname.end(), lname.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            // Dimension defaults
            if (embeddingDim_ == 0) {
                auto has = [&](const char* s) { return lname.find(s) != std::string::npos; };
                // OpenAI TE3 sizes (mock support for external references)
                if (has("text-embedding-3-large")) {
                    embeddingDim_ = 3072;
                } else if (has("text-embedding-3-small")) {
                    embeddingDim_ = 1536;
                } else if (has("e5")) {
                    if (has("large"))
                        embeddingDim_ = 1024;
                    else if (has("small"))
                        embeddingDim_ = 384;
                    else
                        embeddingDim_ = 768; // base/default
                } else if (has("gte")) {
                    if (has("large"))
                        embeddingDim_ = 1024;
                    else if (has("small"))
                        embeddingDim_ = 384;
                    else
                        embeddingDim_ = 768; // base/default
                } else if (has("bge")) {
                    if (has("-m3") || has("m3") || has("large"))
                        embeddingDim_ = 1024;
                    else if (has("small"))
                        embeddingDim_ = 384;
                    else
                        embeddingDim_ = 768; // base/default
                } else if (has("nomic") || has("mpnet")) {
                    embeddingDim_ = 768;
                } else if (has("minilm")) {
                    embeddingDim_ = 384;
                }
            }
            // Sequence length defaults
            if (maxSequenceLength_ == 0 || maxSequenceLength_ == 512) {
                auto has = [&](const char* s) { return lname.find(s) != std::string::npos; };
                if (has("nomic") || has("bge-m3") || has("text-embedding-3-")) {
                    maxSequenceLength_ = 8192;
                } else if (has("minilm")) {
                    maxSequenceLength_ = 256;
                } else if (has("mpnet") || has("bge") || has("e5") || has("gte")) {
                    maxSequenceLength_ = 512;
                }
            }
            // Respect explicit config cap
            if (config_.max_sequence_length > 0 &&
                maxSequenceLength_ > config_.max_sequence_length) {
                maxSequenceLength_ = config_.max_sequence_length;
            }
            isLoaded_ = true;
            return Result<void>();
        }

        try {
#ifdef YAMS_ENABLE_ONNX_GENAI
            if (genai_) {
                OnnxGenAIAdapter::Options o;
                o.intra_op_threads = config_.num_threads > 0 ? config_.num_threads : 4;
                o.normalize = config_.normalize_embeddings;
                const std::string id_or_path = (!modelPath_.empty() ? modelPath_ : modelName_);
                if (genai_->init(id_or_path, o)) {
                    auto d = genai_->embedding_dim();
                    if (d > 0)
                        embeddingDim_ = d;
                    spdlog::info("[ONNX] Using GenAI adapter for '{}'", modelName_);
                    isLoaded_ = true;
                    return Result<void>();
                } else {
                    spdlog::warn("[ONNX] GenAI init failed; falling back to raw ORT");
                }
            }
#endif
            spdlog::info("[ONNX] Creating Ort::Session for model '{}' at {}", modelName_.c_str(),
                         modelPath_.c_str());

            int retries = 3;
            while (retries-- > 0) {
                try {
                    // NOTE: Removed g_onnx_init_mutex lock here - it may have been conflicting
                    // with ONNX Runtime's internal locking. We rely on:
                    // 1. Single-flight pattern in loadModel() to prevent concurrent model loads
                    // 2. ONNX Runtime configured for single-threaded operation
                    spdlog::info("[ONNX] Configuring session options for '{}'", modelName_);

                    // Create local session options with single thread
                    auto options = sessionOptions_->Clone();
                    options.SetIntraOpNumThreads(1);
                    options.SetInterOpNumThreads(1);

                    // Create session directly - no async wrapper needed for local file operations
                    spdlog::info("[ONNX] Creating Ort::Session for '{}' at '{}'", modelName_,
                                 modelPath_);
                    session_ = std::make_unique<Ort::Session>(
                        *env_, std::filesystem::path(modelPath_).c_str(), options);
                    spdlog::info("[ONNX] Ort::Session created successfully for '{}'", modelName_);

                    // Success!
                    break;
                } catch (const std::system_error& e) {
                    spdlog::warn("[ONNX] loadModel: system_error '{}' (code={}). Retries left: {}",
                                 e.what(), e.code().value(), retries);
                    if (retries == 0)
                        throw;
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
                } catch (const Ort::Exception& e) {
                    spdlog::warn("[ONNX] loadModel: Ort::Exception '{}'. Retries left: {}",
                                 e.what(), retries);
                    if (retries == 0)
                        throw;
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
                }
            }

            // Get input/output information
            size_t numInputs = session_->GetInputCount();
            size_t numOutputs = session_->GetOutputCount();

            if (numInputs < 2) {
                return Error{ErrorCode::InvalidData,
                             "Model must have at least 2 inputs (input_ids, attention_mask)"};
            }

            // Get input/output names
            Ort::AllocatorWithDefaultOptions allocator;
            for (size_t i = 0; i < numInputs; ++i) {
                auto inputName = session_->GetInputNameAllocated(i, allocator);
                inputNames_.push_back(inputName.get());
                // Inspect first input shape to detect model sequence length when available
                if (i == 0) {
                    try {
                        auto inInfo = session_->GetInputTypeInfo(i);
                        auto inTensor = inInfo.GetTensorTypeAndShapeInfo();
                        auto inShape = inTensor.GetShape();
                        // Expect shape like [batch, seq_len]; seq_len > 0 means fixed
                        if (inShape.size() >= 2) {
                            auto seq = inShape[1];
                            if (seq > 0) {
                                maxSequenceLength_ = static_cast<size_t>(seq);
                            }
                        }
                    } catch (const std::exception& e) {
                        spdlog::debug("[ONNX] Failed to read input shape: {}", e.what());
                    }
                }
            }

            for (size_t i = 0; i < numOutputs; ++i) {
                auto outputName = session_->GetOutputNameAllocated(i, allocator);
                outputNames_.push_back(outputName.get());
            }

            // Get output shape to determine embedding dimension
            if (numOutputs > 0) {
                auto outputInfo = session_->GetOutputTypeInfo(0);
                auto tensorInfo = outputInfo.GetTensorTypeAndShapeInfo();
                auto shape = tensorInfo.GetShape();
                if (shape.size() >= 2 && shape.back() > 0) {
                    embeddingDim_ = static_cast<size_t>(shape.back());
                }
            }

            // Try to parse nearby config (Sentence-Transformers) for pooling/max
            // length/normalization
            parseModelConfigHints();

            // If input shape and config did not give a positive seq_len, fall back to a heuristic.
            if (maxSequenceLength_ == 0) {
                if (modelName_.find("MiniLM") != std::string::npos) {
                    maxSequenceLength_ = 256;
                } else if (modelName_.find("mpnet") != std::string::npos) {
                    maxSequenceLength_ = 512;
                } else if (modelName_.find("nomic") != std::string::npos) {
                    // Nomic BERT supports very long contexts (e.g., 8192). Default to 2048 unless
                    // user config overrides to avoid excessive memory on defaults.
                    maxSequenceLength_ = 2048;
                } else {
                    maxSequenceLength_ = 512;
                }
            }
            // Respect configured max from EmbeddingConfig if provided
            if (config_.max_sequence_length > 0) {
                maxSequenceLength_ = std::min(maxSequenceLength_, config_.max_sequence_length);
            }

            isLoaded_ = true;
            spdlog::info(
                "[ONNX] Session ready for '{}' (inputs={}, outputs={}, dim={}, max_seq_len={})",
                modelName_.c_str(), numInputs, numOutputs, embeddingDim_, maxSequenceLength_);
            return Result<void>();

        } catch (const Ort::Exception& e) {
            spdlog::error("[ONNX] Failed to load '{}' (Ort::Exception): {}", modelName_.c_str(),
                          e.what());
            return Error{ErrorCode::InternalError,
                         std::string("Failed to load ONNX model: ") + e.what()};
        } catch (const std::system_error& e) {
            spdlog::error("[ONNX] Failed to load '{}' (std::system_error): {}", modelName_.c_str(),
                          e.what());
            return Error{ErrorCode::InternalError,
                         std::string("Failed to load ONNX model (system error): ") + e.what()};
        }
    }

    Result<std::vector<float>> generateEmbedding(const std::string& text) {
        if (test_mode_) {
            // Representative mock: deterministic per-text embedding derived from tokens.
            // - Uses same preprocessing (tokenize/truncate/pad + attention mask)
            // - Pseudo-random per-token vector seeded by token id and position
            // - Pooling respects configured pooling_mode_
            // - Optional L2 normalization
            const size_t seq_len = maxSequenceLength_ > 0 ? maxSequenceLength_ : 512;
            auto toks = preprocessor_.tokenize(text);
            toks = preprocessor_.truncateTokens(toks, seq_len);
            toks = preprocessor_.padTokens(toks, seq_len);
            auto mask = preprocessor_.generateAttentionMask(toks);

            const size_t D = embeddingDim_ > 0 ? embeddingDim_ : 0;
            auto prng_val = [](uint32_t seed) -> float {
                // xorshift32 → map to [-1, 1]
                uint32_t x = seed ? seed : 0x9e3779b9u;
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                // Scale to [0,1)
                float u = (x & 0x00FFFFFFu) / static_cast<float>(0x01000000u);
                return 2.0f * u - 1.0f;
            };

            std::vector<float> emb(D, 0.0f);
            if (pooling_mode_ == Pooling::CLS) {
                const int32_t t = toks.empty() ? 0 : toks[0];
                for (size_t d = 0; d < D; ++d) {
                    emb[d] = prng_val(static_cast<uint32_t>(t * 1315423911u + d * 2654435761u));
                }
            } else if (pooling_mode_ == Pooling::MAX) {
                for (size_t i = 0; i < std::min(seq_len, toks.size()); ++i) {
                    if (i >= mask.size() || mask[i] <= 0)
                        continue;
                    const int32_t t = toks[i];
                    for (size_t d = 0; d < D; ++d) {
                        float v = prng_val(static_cast<uint32_t>(t * 2246822519u + i * 3266489917u +
                                                                 d * 668265263u));
                        emb[d] = (i == 0) ? v : std::max(emb[d], v);
                    }
                }
            } else { // MEAN pooling
                double denom = 0.0;
                for (size_t i = 0; i < std::min(seq_len, toks.size()); ++i) {
                    float w = (i < mask.size() && mask[i] > 0) ? 1.0f : 0.0f;
                    if (w <= 0.0f)
                        continue;
                    denom += w;
                    const int32_t t = toks[i];
                    for (size_t d = 0; d < D; ++d) {
                        float v = prng_val(
                            static_cast<uint32_t>(t * 374761393u + i * 1664525u + d * 1013904223u));
                        emb[d] += v * w;
                    }
                }
                if (denom > 0.0) {
                    for (size_t d = 0; d < D; ++d)
                        emb[d] = static_cast<float>(emb[d] / denom);
                }
            }

            if (normalize_) {
                double n2 = 0.0;
                for (auto v : emb) {
                    double dv = static_cast<double>(v);
                    n2 += dv * dv;
                }
                n2 = std::sqrt(n2);
                if (n2 > 1e-8) {
                    for (auto& v : emb)
                        v = static_cast<float>(v / n2);
                } else {
                    std::fill(emb.begin(), emb.end(), 0.0f);
                }
            }
            return emb;
        }

        if (!isLoaded_) {
            if (auto result = loadModel(); !result) {
                return result.error();
            }
        }

        // Try real ONNX path first (preferred)
        if (auto r = runOnnx(text); r) {
            return r.value();
        }

        try {
#ifdef YAMS_ENABLE_ONNX_GENAI
            if (genai_ && genai_->available()) {
                auto v = genai_->embed(text);
                if (!v.empty())
                    return v;
            }
#endif
            // TODO: Implement tokenization and actual embedding generation
            // For now, return a dummy embedding
            std::vector<float> embedding(embeddingDim_, 0.0f);

            // Simple hash-based dummy embedding for testing
            std::hash<std::string> hasher;
            size_t hash = hasher(text);
            for (size_t i = 0; i < embeddingDim_; ++i) {
                embedding[i] = static_cast<float>((hash >> i) & 1) * 0.1f;
            }

            return embedding;

        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Failed to generate embedding: ") + e.what()};
        }
    }

    // Public batch API that uses the optimized batched ONNX path
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) {
        if (test_mode_) {
            std::vector<std::vector<float>> res;
            res.reserve(texts.size());
            for (const auto& t : texts) {
                auto one = generateEmbedding(t);
                if (!one)
                    return one.error();
                res.emplace_back(std::move(one.value()));
            }
            return res;
        }
        if (!isLoaded_) {
            if (auto result = loadModel(); !result) {
                return result.error();
            }
        }
        return runOnnxBatch(texts);
    }

    bool isValid() const { return test_mode_ ? isLoaded_ : (isLoaded_ && session_ != nullptr); }

    size_t getEmbeddingDim() const { return embeddingDim_; }
    size_t getMaxSequenceLength() const { return maxSequenceLength_; }

private:
    // GenAI adapter (optional)
#ifdef YAMS_ENABLE_ONNX_GENAI
    std::unique_ptr<OnnxGenAIAdapter> genai_;
#endif
    // Helper that performs tokenization + ONNX run + pooling for single input
    Result<std::vector<float>> runOnnx(const std::string& t) {
        const size_t seq_len = maxSequenceLength_ > 0 ? maxSequenceLength_ : 512;
        auto tokens = preprocessor_.tokenize(t);
        tokens = preprocessor_.truncateTokens(tokens, seq_len);
        tokens = preprocessor_.padTokens(tokens, seq_len);
        auto attention_mask = preprocessor_.generateAttentionMask(tokens);

        std::vector<int64_t> input_shape = {1, static_cast<int64_t>(seq_len)};
        const size_t input_tensor_size = seq_len;
        auto memory_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);

        // Cap token ids to model vocab; Nomic uses 30528, MiniLM/mpnet ~30522
        const int64_t MAX_TOKEN_ID = 30527;
        const int64_t UNK_TOKEN_ID = 100;
        std::vector<int64_t> tokens_i64;
        tokens_i64.reserve(seq_len);
        for (auto v : tokens) {
            tokens_i64.push_back((v < 0 || v > MAX_TOKEN_ID) ? UNK_TOKEN_ID
                                                             : static_cast<int64_t>(v));
        }
        std::vector<int64_t> mask_i64(attention_mask.begin(), attention_mask.end());

        std::vector<Ort::Value> input_tensors;
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(
            memory_info, tokens_i64.data(), input_tensor_size, input_shape.data(), 2));
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(
            memory_info, mask_i64.data(), input_tensor_size, input_shape.data(), 2));
        std::vector<int64_t> token_type_ids;
        if (inputNames_.size() >= 3) {
            token_type_ids.resize(seq_len, 0);
            input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(
                memory_info, token_type_ids.data(), input_tensor_size, input_shape.data(), 2));
        }

        std::vector<const char*> input_names_c;
        for (auto& n : inputNames_)
            input_names_c.push_back(n.c_str());
        std::vector<const char*> output_names_c;
        for (auto& n : outputNames_)
            output_names_c.push_back(n.c_str());
        if (output_names_c.empty()) {
            static const char* default_out = "sentence_embedding";
            output_names_c.push_back(default_out);
        }

        auto outputs =
            session_->Run(Ort::RunOptions{nullptr}, input_names_c.data(), input_tensors.data(),
                          input_tensors.size(), output_names_c.data(), output_names_c.size());
        if (outputs.empty()) {
            return Error{ErrorCode::InternalError, "ONNX session returned no outputs"};
        }

        auto& out = outputs[0];
        auto out_shape = out.GetTensorTypeAndShapeInfo().GetShape();
        const size_t hidden_dim = embeddingDim_;

        if (out_shape.size() == 3) {
            const size_t seq = static_cast<size_t>(out_shape[1]);
            const size_t dim = static_cast<size_t>(out_shape[2]);
            if (dim != hidden_dim) {
                return Error{ErrorCode::InvalidData, "Output dim mismatch"};
            }
            const float* data = out.GetTensorData<float>();
            std::vector<float> embedding(hidden_dim, 0.0f);
            if (pooling_mode_ == Pooling::CLS) {
                // CLS token (first token)
                const size_t off = 0;
                for (size_t d = 0; d < dim; ++d)
                    embedding[d] = data[off + d];
            } else if (pooling_mode_ == Pooling::MAX) {
                for (size_t i = 0; i < std::min(seq, attention_mask.size()); ++i) {
                    if (attention_mask[i] <= 0)
                        continue;
                    const size_t off = i * dim;
                    for (size_t d = 0; d < dim; ++d)
                        embedding[d] = std::max(embedding[d], data[off + d]);
                }
            } else { // MEAN
                float total_attn = 0.0f;
                for (size_t i = 0; i < std::min(seq, attention_mask.size()); ++i) {
                    const float w = static_cast<float>(attention_mask[i]);
                    if (w <= 0.0f)
                        continue;
                    total_attn += w;
                    const size_t off = i * dim;
                    for (size_t d = 0; d < dim; ++d)
                        embedding[d] += data[off + d] * w;
                }
                if (total_attn > 0.0f) {
                    for (auto& v : embedding)
                        v = static_cast<float>(v / total_attn);
                }
            }
            if (normalize_) {
                double norm = 0.0;
                for (auto v : embedding) {
                    double dv = static_cast<double>(v);
                    norm += dv * dv;
                }
                norm = std::sqrt(norm);
                if (norm > 1e-8) {
                    for (auto& v : embedding)
                        v = static_cast<float>(v / norm);
                } else {
                    std::fill(embedding.begin(), embedding.end(), 0.0f);
                }
            }
            return embedding;
        }

        if (out_shape.size() == 2) {
            const size_t dim = static_cast<size_t>(out_shape[1]);
            if (dim != hidden_dim) {
                return Error{ErrorCode::InvalidData, "Output dim mismatch"};
            }
            const float* data = out.GetTensorData<float>();
            std::vector<float> embedding(data, data + dim);
            if (normalize_) {
                double norm = 0.0;
                for (auto v : embedding) {
                    double dv = static_cast<double>(v);
                    norm += dv * dv;
                }
                norm = std::sqrt(norm);
                if (norm > 1e-8) {
                    for (auto& v : embedding)
                        v = static_cast<float>(v / norm);
                } else {
                    std::fill(embedding.begin(), embedding.end(), 0.0f);
                }
            }
            return embedding;
        }

        return Error{ErrorCode::InvalidData, "Unexpected output tensor rank"};
    }

    // Helper that performs batched tokenization + single ONNX run + pooling
    Result<std::vector<std::vector<float>>> runOnnxBatch(const std::vector<std::string>& texts) {
        if (texts.empty())
            return std::vector<std::vector<float>>{};
        const size_t seq_len = maxSequenceLength_ > 0 ? maxSequenceLength_ : 512;

        // Tokenize all inputs
        std::vector<std::vector<int32_t>> token_seqs;
        token_seqs.reserve(texts.size());
        std::vector<std::vector<int32_t>> masks;
        masks.reserve(texts.size());
        for (const auto& t : texts) {
            auto tokens = preprocessor_.tokenize(t);
            tokens = preprocessor_.truncateTokens(tokens, seq_len);
            tokens = preprocessor_.padTokens(tokens, seq_len);
            auto mask = preprocessor_.generateAttentionMask(tokens);
            token_seqs.push_back(std::move(tokens));
            masks.push_back(std::move(mask));
        }

        // Prepare batched tensors [B, S]
        const size_t B = texts.size();
        std::vector<int64_t> input_shape = {static_cast<int64_t>(B), static_cast<int64_t>(seq_len)};
        const size_t tensor_size = B * seq_len;
        auto memory_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);

        const int64_t MAX_TOKEN_ID = 30527;
        const int64_t UNK_TOKEN_ID = 100;
        std::vector<int64_t> tokens_batched;
        tokens_batched.reserve(tensor_size);
        std::vector<int64_t> masks_batched;
        masks_batched.reserve(tensor_size);
        for (size_t i = 0; i < B; ++i) {
            const auto& toks = token_seqs[i];
            const auto& m = masks[i];
            for (size_t j = 0; j < seq_len && j < toks.size(); ++j) {
                int64_t v = toks[j];
                tokens_batched.push_back((v < 0 || v > MAX_TOKEN_ID) ? UNK_TOKEN_ID : v);
                masks_batched.push_back(static_cast<int64_t>(m[j]));
            }
            for (size_t j = toks.size(); j < seq_len; ++j) {
                tokens_batched.push_back(UNK_TOKEN_ID);
                masks_batched.push_back(0);
            }
        }

        std::vector<Ort::Value> inputs;
        inputs.push_back(Ort::Value::CreateTensor<int64_t>(
            memory_info, tokens_batched.data(), tokens_batched.size(), input_shape.data(), 2));
        inputs.push_back(Ort::Value::CreateTensor<int64_t>(
            memory_info, masks_batched.data(), masks_batched.size(), input_shape.data(), 2));
        std::vector<int64_t> token_type_ids;
        if (inputNames_.size() >= 3) {
            token_type_ids.assign(tensor_size, 0);
            inputs.push_back(Ort::Value::CreateTensor<int64_t>(
                memory_info, token_type_ids.data(), token_type_ids.size(), input_shape.data(), 2));
        }

        std::vector<const char*> in_names;
        for (auto& n : inputNames_)
            in_names.push_back(n.c_str());
        std::vector<const char*> out_names;
        for (auto& n : outputNames_)
            out_names.push_back(n.c_str());
        if (out_names.empty()) {
            static const char* def = "sentence_embedding";
            out_names.push_back(def);
        }

        // Wrap ONNX Run in try/catch to prevent exceptions from bubbling through ABI boundary
        // On Windows, ORT can throw std::system_error("resource deadlock would occur") when
        // thread pool resources are exhausted (PBI-1c1)
        std::vector<Ort::Value> outputs;
        int retries = 3;
        while (retries-- > 0) {
            try {
                outputs = session_->Run(Ort::RunOptions{nullptr}, in_names.data(), inputs.data(),
                                        inputs.size(), out_names.data(), out_names.size());
                break; // synchronous success
            } catch (const Ort::Exception& e) {
                if (retries == 0) {
                    fprintf(stderr, "[ONNX Plugin] Ort::Exception in Run: %s\n", e.what());
                    return Error{ErrorCode::InternalError,
                                 std::string("ONNX runtime error: ") + e.what()};
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            } catch (const std::system_error& e) {
                if (std::string(e.what()).find("resource deadlock") != std::string::npos ||
                    retries > 0) {
                    spdlog::warn("[ONNX Plugin] Deadlock/system error caught, retrying... ({})",
                                 e.what());
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    if (retries == 0) {
                        fprintf(stderr, "[ONNX Plugin] std::system_error in Run (exhausted): %s\n",
                                e.what());
                        return Error{ErrorCode::InternalError,
                                     std::string("System error during ONNX inference: ") +
                                         e.what()};
                    }
                    continue;
                }
                fprintf(stderr, "[ONNX Plugin] std::system_error in Run: %s\n", e.what());
                return Error{ErrorCode::InternalError,
                             std::string("System error during ONNX inference: ") + e.what()};
            } catch (const std::exception& e) {
                fprintf(stderr, "[ONNX Plugin] std::exception in Run: %s\n", e.what());
                return Error{ErrorCode::InternalError,
                             std::string("Exception during ONNX inference: ") + e.what()};
            }
        }
        if (outputs.empty())
            return Error{ErrorCode::InternalError, "ONNX session returned no outputs"};

        auto& out = outputs[0];
        auto shape = out.GetTensorTypeAndShapeInfo().GetShape();
        const size_t hidden_dim = embeddingDim_;
        std::vector<std::vector<float>> result;
        if (shape.size() == 3) {
            // [B, S, D]
            const size_t BB = static_cast<size_t>(shape[0]);
            const size_t SS = static_cast<size_t>(shape[1]);
            const size_t DD = static_cast<size_t>(shape[2]);
            if (BB != B || DD != hidden_dim)
                return Error{ErrorCode::InvalidData, "Output shape mismatch"};
            const float* data = out.GetTensorData<float>();
            result.resize(B, std::vector<float>(hidden_dim, 0.0f));
            for (size_t b = 0; b < B; ++b) {
                auto& emb = result[b];
                if (pooling_mode_ == Pooling::CLS) {
                    const size_t off = b * SS * DD;
                    for (size_t d = 0; d < DD; ++d)
                        emb[d] = data[off + d];
                } else if (pooling_mode_ == Pooling::MAX) {
                    for (size_t i = 0; i < std::min(SS, masks[b].size()); ++i) {
                        if (masks[b][i] <= 0)
                            continue;
                        const size_t off = b * SS * DD + i * DD;
                        for (size_t d = 0; d < DD; ++d)
                            emb[d] = std::max(emb[d], data[off + d]);
                    }
                } else {
                    float total = 0.0f;
                    for (size_t i = 0; i < std::min(SS, masks[b].size()); ++i) {
                        float w = static_cast<float>(masks[b][i]);
                        if (w <= 0.0f) {
                            continue;
                        }
                        total += w;
                        const size_t off = b * SS * DD + i * DD;
                        for (size_t d = 0; d < DD; ++d)
                            emb[d] += data[off + d] * w;
                    }
                    if (total > 0.0f) {
                        for (auto& v : emb)
                            v = static_cast<float>(v / total);
                    }
                }
                if (normalize_) {
                    double norm = 0.0;
                    for (auto v : emb) {
                        double dv = static_cast<double>(v);
                        norm += dv * dv;
                    }
                    norm = std::sqrt(norm);
                    if (norm > 1e-8) {
                        for (auto& v : emb)
                            v = static_cast<float>(v / norm);
                    } else {
                        std::fill(emb.begin(), emb.end(), 0.0f);
                    }
                }
            }
            return result;
        }
        if (shape.size() == 2) {
            // [B, D]
            const size_t BB = static_cast<size_t>(shape[0]);
            const size_t DD = static_cast<size_t>(shape[1]);
            if (BB != B || DD != hidden_dim)
                return Error{ErrorCode::InvalidData, "Output shape mismatch"};
            const float* data = out.GetTensorData<float>();
            result.resize(B);
            for (size_t b = 0; b < B; ++b) {
                result[b].assign(data + b * DD, data + (b + 1) * DD);
                if (normalize_) {
                    double norm = 0.0;
                    for (auto v : result[b]) {
                        double dv = static_cast<double>(v);
                        norm += dv * dv;
                    }
                    norm = std::sqrt(norm);
                    if (norm > 1e-8) {
                        for (auto& v : result[b])
                            v = static_cast<float>(v / norm);
                    } else {
                        std::fill(result[b].begin(), result[b].end(), 0.0f);
                    }
                }
            }
            return result;
        }
        return Error{ErrorCode::InvalidData, "Unexpected output tensor rank (batch)"};
    }

    std::string modelPath_;
    std::string modelName_;
    vector::EmbeddingConfig config_;
    yams::vector::TextPreprocessor preprocessor_;

    Ort::Env* env_ = nullptr; // shared global env
    std::unique_ptr<Ort::SessionOptions> sessionOptions_;
    std::unique_ptr<Ort::Session> session_;

    std::vector<std::string> inputNames_;
    std::vector<std::string> outputNames_;

    size_t embeddingDim_ = 0;
    size_t maxSequenceLength_ = 512;
    bool isLoaded_ = false;
    bool test_mode_ = false;

    enum class Pooling { MEAN, CLS, MAX };
    Pooling pooling_mode_ = Pooling::MEAN;
    bool normalize_ = true;

    void parseModelConfigHints() {
        try {
            namespace fs = std::filesystem;
            fs::path mp(modelPath_);
            std::vector<fs::path> candidates;
            if (mp.has_parent_path()) {
                candidates.push_back(mp.parent_path() / "sentence_bert_config.json");
                candidates.push_back(mp.parent_path() / "config.json");
                if (mp.parent_path().has_parent_path()) {
                    auto up = mp.parent_path().parent_path();
                    candidates.push_back(up / "sentence_bert_config.json");
                    candidates.push_back(up / "config.json");
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
                if (j.contains("max_seq_length") && j["max_seq_length"].is_number_integer()) {
                    maxSequenceLength_ = static_cast<size_t>(j["max_seq_length"].get<int>());
                } else if (j.contains("model_max_length") &&
                           j["model_max_length"].is_number_integer()) {
                    maxSequenceLength_ = static_cast<size_t>(j["model_max_length"].get<int>());
                }
                // Common HF key for context length
                if (j.contains("max_position_embeddings") &&
                    j["max_position_embeddings"].is_number_integer()) {
                    maxSequenceLength_ =
                        static_cast<size_t>(j["max_position_embeddings"].get<int>());
                }
                if (j.contains("pooling_mode_cls_token") &&
                    j["pooling_mode_cls_token"].is_boolean() &&
                    j["pooling_mode_cls_token"].get<bool>()) {
                    pooling_mode_ = Pooling::CLS;
                }
                if (j.contains("pooling_mode_max_tokens") &&
                    j["pooling_mode_max_tokens"].is_boolean() &&
                    j["pooling_mode_max_tokens"].get<bool>()) {
                    pooling_mode_ = Pooling::MAX;
                }
                if (j.contains("pooling_mode_mean_tokens") &&
                    j["pooling_mode_mean_tokens"].is_boolean()) {
                    if (j["pooling_mode_mean_tokens"].get<bool>())
                        pooling_mode_ = Pooling::MEAN;
                }
                if (j.contains("normalize_embeddings") && j["normalize_embeddings"].is_boolean()) {
                    normalize_ = j["normalize_embeddings"].get<bool>();
                }
                if (j.contains("embedding_dimension") &&
                    j["embedding_dimension"].is_number_integer()) {
                    embeddingDim_ = static_cast<size_t>(j["embedding_dimension"].get<int>());
                }
                // Hidden size often present in HF config
                if (j.contains("hidden_size") && j["hidden_size"].is_number_integer()) {
                    embeddingDim_ = static_cast<size_t>(j["hidden_size"].get<int>());
                }
                // Nomic-specific hints via architectures
                try {
                    if (j.contains("architectures") && j["architectures"].is_array()) {
                        for (const auto& a : j["architectures"]) {
                            if (a.is_string()) {
                                auto s = a.get<std::string>();
                                if (s.find("Nomic") != std::string::npos) {
                                    pooling_mode_ = Pooling::MEAN;
                                    normalize_ = true;
                                    // If very long context supported and not set, prefer 8192
                                    if (maxSequenceLength_ == 0 || maxSequenceLength_ < 2048) {
                                        maxSequenceLength_ = 8192;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                } catch (...) {
                }
                break; // stop after first readable config
            }
        } catch (...) {
        }
    }
};

// ============================================================================
// OnnxModelSession Public Interface
// ============================================================================

OnnxModelSession::OnnxModelSession(const std::string& modelPath, const std::string& modelName,
                                   const vector::EmbeddingConfig& config)
    : pImpl(std::make_unique<Impl>(modelPath, modelName, config)) {
    info_.name = modelName;
    info_.path = modelPath;
    info_.loadTime = std::chrono::system_clock::now();

    // Get file size for memory estimation
    if (fs::exists(modelPath)) {
        info_.memoryUsageBytes = fs::file_size(modelPath);
    }

    // Eagerly load the model to surface errors immediately during startup/acquisition
    spdlog::debug("[ONNX] Created session for '{}' - eager loading...", modelName);
    if (auto res = pImpl->loadModel(); !res) {
        spdlog::error("[ONNX] Eager load failed for '{}': {}", modelName, res.error().message);
        // We can't return an error from constructor, but pImpl state remains unloaded/invalid
    }
}

OnnxModelSession::~OnnxModelSession() = default;

Result<std::vector<float>> OnnxModelSession::generateEmbedding(const std::string& text) {
    info_.requestCount++;

    auto result = pImpl->generateEmbedding(text);
    if (!result) {
        info_.errorCount++;
    }

    return result;
}

Result<std::vector<std::vector<float>>>
OnnxModelSession::generateBatchEmbeddings(const std::vector<std::string>& texts) {
    return pImpl->generateBatchEmbeddings(texts);
}

bool OnnxModelSession::isValid() const {
    return pImpl->isValid();
}

// ============================================================================
// OnnxModelPool Implementation
// ============================================================================

OnnxModelPool::OnnxModelPool(const ModelPoolConfig& config) : config_(config) {
    // In mock mode, avoid initializing any ONNX Runtime state to prevent
    // platform-specific crashes on hosts without compatible runtimes.
    if (std::getenv("YAMS_USE_MOCK_PROVIDER") || std::getenv("YAMS_SKIP_MODEL_LOADING") ||
        std::getenv("YAMS_TEST_MODE")) {
        spdlog::warn("[ONNX] Mock provider mode enabled; skipping global ONNX environment init");
        return;
    }

    // NOTE: We do NOT create a separate Ort::Env here.
    // Instead, OnnxModelSession::Impl uses the global get_global_ort_env() function.
    // Creating multiple Ort::Env instances causes thread pool conflicts that lead to
    // "resource deadlock would occur" errors (EDEADLK, code 36).
    // The ortEnv_ and sessionOptions_ members are kept for API compatibility but not used.

    try {
        // Log that we're using the shared global environment
        // NOTE: We do NOT create a separate Ort::Env here - OnnxModelSession::Impl
        // uses the global get_global_ort_env() function to avoid thread pool conflicts.
        spdlog::info(
            "[ONNX] Pool: Using shared global Ort::Env (sessions use single-threaded execution)");

        if (config.enableGPU) {
            // TODO: Add GPU provider configuration
            spdlog::info("GPU support requested but not yet implemented");
        }
    } catch (const std::exception& e) {
        // Do not crash the daemon if runtime init fails; log and continue.
        spdlog::warn("[ONNX] Runtime environment init failed: {} — continuing (mock may be used)",
                     e.what());
    } catch (...) {
        spdlog::warn("[ONNX] Runtime environment init failed with unknown error — continuing");
    }
}

OnnxModelPool::~OnnxModelPool() {
    try {
        shutdown();
    } catch (const std::exception& e) {
        try {
            spdlog::warn("[OnnxModelPool] destructor exception: {}", e.what());
        } catch (...) {
        }
    } catch (...) {
        try {
            spdlog::warn("[OnnxModelPool] destructor unknown exception");
        } catch (...) {
        }
    }
}

Result<void> OnnxModelPool::initialize() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    if (initialized_) {
        return Result<void>();
    }

    spdlog::info("Initializing ONNX model pool with max {} models", config_.maxLoadedModels);

    // Eagerly initialize the global Ort::Env NOW, before any concurrent access.
    // This prevents "resource deadlock would occur" errors that can happen when
    // multiple threads try to initialize ONNX Runtime simultaneously.
    try {
        spdlog::info("[ONNX Pool] Eagerly initializing global Ort::Env...");
        (void)get_global_ort_env();
        spdlog::info("[ONNX Pool] Global Ort::Env ready");
    } catch (const std::exception& e) {
        spdlog::error("[ONNX Pool] Failed to initialize Ort::Env: {}", e.what());
        return Error{ErrorCode::InternalError, std::string("ONNX Env init failed: ") + e.what()};
    }

    // Start background preloading if configured (non-blocking)
    if (!config_.lazyLoading && !config_.preloadModels.empty()) {
        // Launch preloading in background thread to avoid blocking
        // Store the thread so we can join it during shutdown
        preloadThread_ = std::thread([this]() {
            spdlog::info("Starting background model preloading");
            // Check shutdown flag periodically during preload
            for (const auto& modelName : config_.preloadModels) {
                if (shutdown_.load(std::memory_order_acquire)) {
                    spdlog::info("Background preloading interrupted by shutdown");
                    return;
                }
                auto result = loadModel(modelName);
                if (!result) {
                    spdlog::warn("Failed to preload model {}: {}", modelName,
                                 result.error().message);
                }
            }
            spdlog::info("Background model preloading completed");
        });
    }

    initialized_ = true;
    return Result<void>();
}

void OnnxModelPool::shutdown() {
    if (shutdown_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }

    if (preloadThread_.joinable()) {
        try {
            spdlog::info("Waiting for background preload thread to finish...");
        } catch (...) {
        }
        try {
            preloadThread_.join();
            try {
                spdlog::info("Background preload thread joined");
            } catch (...) {
            }
        } catch (const std::exception& e) {
            try {
                spdlog::warn("Failed to join preload thread: {}", e.what());
            } catch (...) {
            }
        } catch (...) {
        }
    }

    try {
        std::lock_guard<std::recursive_mutex> lock(mutex_);

        try {
            spdlog::info("Shutting down ONNX model pool");
        } catch (...) {
        }

        for (auto& [name, entry] : models_) {
            if (entry.pool) {
                try {
                    entry.pool->shutdown();
                } catch (const std::exception& e) {
                    try {
                        spdlog::warn("Failed to shutdown model pool '{}': {}", name, e.what());
                    } catch (...) {
                    }
                } catch (...) {
                }
            }
        }

        models_.clear();
        initialized_ = false;
    } catch (const std::exception& e) {
        try {
            spdlog::warn("ONNX model pool shutdown exception: {}", e.what());
        } catch (...) {
        }
    } catch (...) {
        try {
            spdlog::warn("ONNX model pool shutdown unknown exception");
        } catch (...) {
        }
    }
}

Result<OnnxModelPool::ModelHandle> OnnxModelPool::acquireModel(const std::string& modelName,
                                                               std::chrono::milliseconds timeout) {
    if (!initialized_) {
        if (auto result = initialize(); !result) {
            return result.error();
        }
    }

    totalRequests_++;

    // Check if model is already loaded
    // IMPORTANT: Get pool pointer under lock, then release lock before calling acquire()
    // to avoid deadlock with ResourcePool's internal locking (PBI-1c1 fix)
    std::shared_ptr<ResourcePool<OnnxModelSession>> poolPtr;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);

        auto it = models_.find(modelName);
        if (it != models_.end() && it->second.pool) {
            updateAccessStats(modelName);
            cacheHits_++;
            poolPtr = it->second.pool;
        }
    }

    // Acquire outside of lock to avoid deadlock
    if (poolPtr) {
        return poolPtr->acquire(timeout);
    }

    cacheMisses_++;

    // Model not loaded, need to load it
    if (auto result = loadModel(modelName); !result) {
        return result.error();
    }

    // Try again after loading - get pool pointer while holding lock
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);
        auto it = models_.find(modelName);
        if (it != models_.end() && it->second.pool) {
            updateAccessStats(modelName);
            poolPtr = it->second.pool;
        }
    }

    // Acquire outside of lock
    if (poolPtr) {
        return poolPtr->acquire(timeout);
    }

    return Error{ErrorCode::NotFound, "Failed to load model: " + modelName};
}

bool OnnxModelPool::isModelLoaded(const std::string& modelName) const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    auto it = models_.find(modelName);
    return it != models_.end() && it->second.pool != nullptr;
}

std::vector<std::string> OnnxModelPool::getLoadedModels() const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    std::vector<std::string> loaded;
    for (const auto& [name, entry] : models_) {
        if (entry.pool) {
            loaded.push_back(name);
        }
    }

    return loaded;
}

Result<void> OnnxModelPool::loadModel(const std::string& modelName) {
    spdlog::info("[ONNX Plugin] loadModel() called for: {}", modelName);

    // In test environments, avoid heavy model loading entirely
    if (yams::test::shouldSkipModelLoading()) {
        return Error{ErrorCode::NotFound, "Model loading skipped in test mode"};
    }

    // Single-flight pattern: only one thread loads a given model at a time.
    // Other threads wait for the loading thread to finish.
    {
        std::unique_lock<std::recursive_mutex> lock(mutex_);

        // Check if already loaded
        auto it = models_.find(modelName);
        if (it != models_.end() && it->second.pool) {
            spdlog::info("[ONNX Plugin] Model already loaded: {}", modelName);
            return Result<void>();
        }

        // Check if another thread is currently loading this model
        if (loadingModels_.count(modelName) > 0) {
            spdlog::info("[ONNX Plugin] Model '{}' is being loaded by another thread, waiting...",
                         modelName);
            // Wait for the other thread to finish loading
            loadingCv_.wait(lock, [this, &modelName]() {
                // Wake up when either: model is loaded, or no longer in loading set
                auto it = models_.find(modelName);
                bool loaded = (it != models_.end() && it->second.pool);
                bool notLoading = (loadingModels_.count(modelName) == 0);
                return loaded || notLoading;
            });

            // Check if model was loaded successfully by the other thread
            it = models_.find(modelName);
            if (it != models_.end() && it->second.pool) {
                spdlog::info("[ONNX Plugin] Model '{}' loaded by another thread", modelName);
                return Result<void>();
            }
            // If not loaded and not loading, we'll try to load it ourselves
            spdlog::warn("[ONNX Plugin] Model '{}' load failed by other thread, retrying",
                         modelName);
        }

        // Mark this model as being loaded by us
        loadingModels_.insert(modelName);
    }

    // RAII guard to remove from loadingModels_ and notify waiters
    struct LoadingGuard {
        OnnxModelPool* pool;
        std::string name;
        ~LoadingGuard() {
            std::lock_guard<std::recursive_mutex> lock(pool->mutex_);
            pool->loadingModels_.erase(name);
            pool->loadingCv_.notify_all();
        }
    } guard{this, modelName};

    // Resolve model path WITHOUT holding the lock
    std::string modelPath = resolveModelPath(modelName);
    spdlog::info("[ONNX Plugin] Resolved path: {}", modelPath);

    // Direct filesystem check
    spdlog::info("[ONNX Plugin] Checking file existence: {}", modelPath);
    bool fileExists = false;
    try {
        fileExists = fs::exists(modelPath) && fs::is_regular_file(modelPath);
    } catch (const std::exception& e) {
        spdlog::warn("Error checking model file existence: {}", e.what());
    }

    if (!fileExists) {
        spdlog::info("Model file not found: {} (searched for: {})", modelName, modelPath);
        const bool offline = []() {
            if (const char* s = std::getenv("YAMS_ONNX_OFFLINE")) {
                std::string v(s);
                std::transform(v.begin(), v.end(), v.begin(), ::tolower);
                return v == "1" || v == "true" || v == "yes" || v == "on";
            }
            return false;
        }();
        if (offline) {
            return Error{ErrorCode::NotFound,
                         "Model file not found in offline mode. Pre-place artifacts in cache or "
                         "~/.yams/models, or unset YAMS_ONNX_OFFLINE"};
        }
        spdlog::info("Download the model with: yams model --download {}", modelName);
        return Error{ErrorCode::NotFound,
                     "Model file not found: " + modelPath +
                         ". Download with: yams model --download " + modelName +
                         " or set YAMS_ALLOW_MODEL_DOWNLOAD=1 for automatic downloads"};
    }

    // Check file size
    size_t modelSize = 0;
    try {
        modelSize = fs::file_size(modelPath);
    } catch (const std::exception& e) {
        spdlog::warn("Error getting model file size: {}", e.what());
        modelSize = 100 * 1024 * 1024; // Assume 100MB if can't determine
    }

    // Configure pool for this model (no lock needed - local variables)
    spdlog::info("[ONNX Plugin] Configuring resource pool");
    PoolConfig<OnnxModelSession> poolConfig;
    poolConfig.minSize = 1;
    poolConfig.maxSize = 3; // Allow up to 3 concurrent users per model
    poolConfig.maxIdle = 2;
    poolConfig.idleTimeout = config_.modelIdleTimeout;
    // CRITICAL: Disable preCreateResources to avoid creating ONNX session during
    // ResourcePool construction. Sessions will be created lazily on first acquire().
    // This prevents EDEADLK errors from ONNX Runtime's internal thread pool.
    poolConfig.preCreateResources = false;

    // Create embedding config
    vector::EmbeddingConfig embConfig;
    embConfig.model_path = modelPath;
    embConfig.model_name = modelName;
    embConfig.enable_gpu = config_.enableGPU;
    embConfig.num_threads = config_.numThreads;

    // Create ResourcePool WITHOUT holding mutex_ - the factory lambda will be called
    // later during acquire(), also without holding mutex_.
    spdlog::info("[ONNX Plugin] Creating ResourcePool for '{}'", modelName);
    auto t0 = std::chrono::steady_clock::now();

    std::shared_ptr<ResourcePool<OnnxModelSession>> pool;
    try {
        spdlog::info("[ONNX Plugin] About to construct ResourcePool...");
        pool = std::make_shared<ResourcePool<OnnxModelSession>>(
            poolConfig,
            [modelPath, modelName, embConfig](const std::string&) -> Result<ModelSessionPtr> {
                try {
                    spdlog::info("[ONNX Plugin] Factory: creating OnnxModelSession for '{}'",
                                 modelName);
                    return std::make_shared<OnnxModelSession>(modelPath, modelName, embConfig);
                } catch (const std::exception& e) {
                    spdlog::error("[ONNX Plugin] Factory: failed to create session: {}", e.what());
                    return Error{ErrorCode::InternalError,
                                 std::string("Failed to create model session: ") + e.what()};
                } catch (...) {
                    spdlog::error("[ONNX Plugin] Factory: unknown error creating session");
                    return Error{ErrorCode::InternalError, "Unknown error creating ONNX session"};
                }
            },
            [](const OnnxModelSession& session) { return session.isValid(); });
        spdlog::info("[ONNX Plugin] ResourcePool constructed successfully");
    } catch (const std::system_error& e) {
        spdlog::error("[ONNX Plugin] ResourcePool construction threw system_error: {} (code={})",
                      e.what(), e.code().value());
        return Error{ErrorCode::InternalError,
                     std::string("ResourcePool construction failed: ") + e.what()};
    } catch (const std::exception& e) {
        spdlog::error("[ONNX Plugin] ResourcePool construction threw exception: {}", e.what());
        return Error{ErrorCode::InternalError,
                     std::string("ResourcePool construction failed: ") + e.what()};
    }

    // Now acquire lock briefly to update shared state
    spdlog::info("[ONNX Plugin] Acquiring mutex for model registration...");
    {
        std::lock_guard<std::recursive_mutex> lock(mutex_);

        // Check if we need to evict models
        size_t loadedCount = 0;
        for (const auto& [name, entry] : models_) {
            if (entry.pool)
                loadedCount++;
        }

        if (loadedCount >= config_.maxLoadedModels) {
            evictLRU(1);
        }

        // Check memory constraints
        if (!canLoadModel(modelSize)) {
            return Error{ErrorCode::ResourceExhausted, "Insufficient memory to load model"};
        }

        // Create/update model entry
        spdlog::info("[ONNX Plugin] Registering model entry for: {}", modelName);
        ModelEntry& entry = models_[modelName];
        entry.name = modelName;
        entry.path = modelPath;
        entry.lastAccess = std::chrono::steady_clock::now();
        entry.pool = pool;

        // Update cached model count for non-blocking status queries
        loadedModelCount_.fetch_add(1, std::memory_order_relaxed);
    }

    auto t1 = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    spdlog::info("Registered ONNX model: {} ({}MB) load_ms={} path={}", modelName,
                 modelSize / (1024 * 1024), ms, modelPath);

    spdlog::info("[ONNX Plugin] Model '{}' registered successfully (session created on first use)",
                 modelName);
    return Result<void>();
}

Result<void> OnnxModelPool::unloadModel(const std::string& modelName) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    auto it = models_.find(modelName);
    if (it == models_.end()) {
        return Error{ErrorCode::NotFound, "Model not found: " + modelName};
    }

    if (it->second.isHot) {
        return Error{ErrorCode::InvalidOperation, "Cannot unload hot model: " + modelName};
    }

    if (it->second.pool) {
        it->second.pool->shutdown();
        it->second.pool.reset();

        // Update cached model count for non-blocking status queries
        loadedModelCount_.fetch_sub(1, std::memory_order_relaxed);
    }

    spdlog::info("Unloaded ONNX model: {}", modelName);

    return Result<void>();
}

Result<void> OnnxModelPool::preloadModels() {
    for (const auto& modelName : config_.preloadModels) {
        spdlog::info("Preloading model: {}", modelName);

        // Load model directly - no async wrapper needed
        auto result = loadModel(modelName);
        if (!result) {
            spdlog::warn("Failed to preload model {}: {}", modelName, result.error().message);
        } else {
            // Mark as hot model
            std::lock_guard<std::recursive_mutex> lock(mutex_);
            auto it = models_.find(modelName);
            if (it != models_.end()) {
                it->second.isHot = true;
            }
        }
    }

    return Result<void>();
}

void OnnxModelPool::evictLRU(size_t numToEvict) {
    // Find least recently used models that are not hot
    std::vector<std::pair<std::string, std::chrono::steady_clock::time_point>> candidates;

    for (const auto& [name, entry] : models_) {
        if (entry.pool && !entry.isHot) {
            candidates.emplace_back(name, entry.lastAccess);
        }
    }

    // Sort by last access time
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });

    // Evict the oldest models
    for (size_t i = 0; i < std::min(numToEvict, candidates.size()); ++i) {
        const auto& modelName = candidates[i].first;
        auto it = models_.find(modelName);
        if (it != models_.end() && it->second.pool) {
            spdlog::info("Evicting model due to LRU: {}", modelName);
            it->second.pool->shutdown();
            it->second.pool.reset();
        }
    }
}

OnnxModelPool::PoolStats OnnxModelPool::getStats() const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    PoolStats stats;
    stats.totalRequests = totalRequests_.load();
    stats.cacheHits = cacheHits_.load();
    stats.cacheMisses = cacheMisses_.load();
    stats.hitRate =
        stats.totalRequests > 0 ? static_cast<double>(stats.cacheHits) / stats.totalRequests : 0.0;

    stats.loadedModels = 0;
    stats.totalMemoryBytes = 0;

    for (const auto& [name, entry] : models_) {
        if (entry.pool) {
            stats.loadedModels++;

            // Get model info (approximate from first session in pool)
            // Note: This is simplified - in production, track actual memory usage
            if (fs::exists(entry.path)) {
                size_t modelSize = fs::file_size(entry.path);
                stats.totalMemoryBytes += modelSize;
            }

            // Populate per-model stats for status reporting (copyable ModelInfo)
            ModelInfo mi;
            mi.name = name;
            mi.path = entry.path;
            mi.embeddingDim = 384;      // default when unknown
            mi.maxSequenceLength = 512; // default when unknown
            try {
                if (fs::exists(entry.path)) {
                    mi.memoryUsageBytes = fs::file_size(entry.path);
                }
            } catch (...) {
                mi.memoryUsageBytes = 0;
            }
            mi.loadTime = std::chrono::system_clock::now();
            mi.requestCount = 0;
            mi.errorCount = 0;
            stats.modelStats.emplace(name, std::move(mi));
        }
    }

    return stats;
}

size_t OnnxModelPool::getMemoryUsage() const {
    return getStats().totalMemoryBytes;
}

void OnnxModelPool::performMaintenance() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    auto now = std::chrono::steady_clock::now();

    for (auto& [name, entry] : models_) {
        if (entry.pool && !entry.isHot) {
            // Check if model has been idle too long
            auto idleTime = now - entry.lastAccess;
            if (idleTime > config_.modelIdleTimeout) {
                spdlog::info("Unloading idle model: {}", name);
                entry.pool->shutdown();
                entry.pool.reset();
            } else if (entry.pool) {
                // Clean up expired resources in the pool
                entry.pool->evictExpired();
            }
        }
    }
}

std::string OnnxModelPool::resolveModelPath(const std::string& modelName) const {
    auto is_hf_like = [](const std::string& s) {
        if (s.rfind("hf://", 0) == 0)
            return true;
        if (s.rfind("https://huggingface.co/", 0) == 0)
            return true;
        return (s.find('/') != std::string::npos) && (s.find('/') == s.rfind('/')) &&
               s.find(".onnx") == std::string::npos && s.find('/') > 0;
    };
    auto to_repo_id = [](const std::string& s) {
        if (s.rfind("hf://", 0) == 0)
            return s.substr(5);
        const std::string prefix = "https://huggingface.co/";
        if (s.rfind(prefix, 0) == 0) {
            auto rest = s.substr(prefix.size());
            auto slash = rest.find('/');
            if (slash != std::string::npos) {
                auto slash2 = rest.find('/', slash + 1);
                if (slash2 != std::string::npos)
                    return rest.substr(0, slash2);
            }
            return rest;
        }
        return s;
    };
    // Get home directory (cross-platform)
    std::string homeDir;
    if (const char* home = std::getenv("HOME")) {
        homeDir = home;
    } else if (const char* userprofile = std::getenv("USERPROFILE")) {
        // Windows: USERPROFILE is the user's home directory
        homeDir = userprofile;
    }

    // Windows-specific paths
    std::string localAppData;
    std::string appData;
#ifdef _WIN32
    if (const char* lad = std::getenv("LOCALAPPDATA")) {
        localAppData = lad;
    }
    if (const char* ad = std::getenv("APPDATA")) {
        appData = ad;
    }
#endif

    // Build search paths with priority:
    // 1) Treat modelName as a full path
    // 2) Configured modelsRoot (if set), with common filename layouts
    // 3) User models (~/.yams/models), local models/, system share models
    std::vector<std::string> searchPaths;
    searchPaths.push_back(modelName);

    if (is_hf_like(modelName)) {
        std::string repo = to_repo_id(modelName);
        // Check for revision hint
        std::string preferredRev;
        {
            std::lock_guard<std::recursive_mutex> lk(mutex_);
            auto it = modelHints_.find(modelName);
            if (it != modelHints_.end())
                preferredRev = it->second.hfRevision;
        }
        auto cache_roots = [&]() {
            std::vector<std::string> roots;
            if (const char* env = std::getenv("YAMS_ONNX_HF_CACHE"))
                roots.push_back(env);
            if (const char* hf = std::getenv("HF_HOME")) {
                roots.push_back(std::string(hf));
                roots.push_back(std::string(hf) + "/hub");
            }
            if (!homeDir.empty()) {
                roots.push_back(homeDir + "/.cache/huggingface");
                roots.push_back(homeDir + "/.cache/huggingface/hub");
            }
            return roots;
        }();
        std::string repo_dir = "models--";
        for (auto c : repo)
            repo_dir += (c == '/' ? std::string("--") : std::string(1, c));
        for (const auto& root : cache_roots) {
            try {
                fs::path base = fs::path(root) / repo_dir;
                if (!fs::exists(base))
                    continue;
                fs::path found;
                if (!preferredRev.empty()) {
                    fs::path snapdir;
                    fs::path ref = base / "refs" / preferredRev;
                    if (fs::exists(ref)) {
                        try {
                            std::ifstream in(ref);
                            std::string s;
                            std::getline(in, s);
                            if (!s.empty())
                                snapdir = base / "snapshots" / s;
                        } catch (...) {
                        }
                    }
                    if (snapdir.empty())
                        snapdir = base / "snapshots" / preferredRev;
                    if (fs::exists(snapdir)) {
                        for (auto& p : fs::recursive_directory_iterator(snapdir)) {
                            if (!p.is_regular_file())
                                continue;
                            if (p.path().filename() == "model.onnx") {
                                found = p.path();
                                break;
                            }
                        }
                    }
                }
                if (found.empty()) {
                    for (auto& p : fs::recursive_directory_iterator(base)) {
                        if (!p.is_regular_file())
                            continue;
                        if (p.path().filename() == "model.onnx") {
                            found = p.path();
                            break;
                        }
                    }
                }
                if (!found.empty()) {
                    spdlog::debug("[ONNX] Resolved HF {} to {}", repo, found.string());
                    return found.string();
                }
            } catch (...) {
            }
        }
        spdlog::debug("[ONNX] HF repo '{}' not found in cache; falling back to filesystem search",
                      repo);
    }

    // Expand configured modelsRoot if present (supports '~')
    if (!config_.modelsRoot.empty()) {
        std::string root = config_.modelsRoot;
        if (!root.empty() && root[0] == '~') {
            if (!homeDir.empty()) {
                root = homeDir + root.substr(1);
            }
        }
        // Common layouts under configured root
        searchPaths.push_back(root + "/" + modelName + "/model.onnx");
        searchPaths.push_back(root + "/" + modelName + "/" + modelName + ".onnx");
        searchPaths.push_back(root + "/" + modelName + ".onnx");
    }

    // Default locations (XDG standard first, then fallbacks)
    // Priority: XDG_DATA_HOME > ~/.local/share/yams > ~/.yams > relative > system
    std::string xdg_data_home;
    if (const char* xdg = std::getenv("XDG_DATA_HOME")) {
        xdg_data_home = xdg;
    } else if (!homeDir.empty()) {
        xdg_data_home = homeDir + "/.local/share";
    }

    // XDG paths (highest priority)
    if (!xdg_data_home.empty()) {
        searchPaths.push_back(xdg_data_home + "/yams/models/" + modelName + "/model.onnx");
        searchPaths.push_back(xdg_data_home + "/yams/models/" + modelName + "/" + modelName +
                              ".onnx");
        searchPaths.push_back(xdg_data_home + "/yams/models/" + modelName + ".onnx");
    }

    // Legacy ~/.yams/models (for backward compatibility)
    if (!homeDir.empty()) {
        searchPaths.push_back(homeDir + "/.yams/models/" + modelName + "/model.onnx");
        searchPaths.push_back(homeDir + "/.yams/models/" + modelName + "/" + modelName + ".onnx");
        searchPaths.push_back(homeDir + "/.yams/models/" + modelName + ".onnx");
    }

#ifdef _WIN32
    // Windows: Check LOCALAPPDATA/yams/models (primary Windows location)
    if (!localAppData.empty()) {
        searchPaths.push_back(localAppData + "\\yams\\models\\" + modelName + "\\model.onnx");
        searchPaths.push_back(localAppData + "\\yams\\models\\" + modelName + "\\" + modelName +
                              ".onnx");
        searchPaths.push_back(localAppData + "\\yams\\models\\" + modelName + ".onnx");
    }
    // Windows: Check APPDATA/yams/models (roaming profile)
    if (!appData.empty()) {
        searchPaths.push_back(appData + "\\yams\\models\\" + modelName + "\\model.onnx");
        searchPaths.push_back(appData + "\\yams\\models\\" + modelName + "\\" + modelName +
                              ".onnx");
        searchPaths.push_back(appData + "\\yams\\models\\" + modelName + ".onnx");
    }
#endif

    // Relative paths
    searchPaths.push_back("models/" + modelName + "/model.onnx");
    searchPaths.push_back("models/" + modelName + "/" + modelName + ".onnx");
    searchPaths.push_back("models/" + modelName + ".onnx");

#ifdef _WIN32
    // Windows: System paths
    searchPaths.push_back("C:\\ProgramData\\yams\\models\\" + modelName + "\\model.onnx");
    searchPaths.push_back("C:\\ProgramData\\yams\\models\\" + modelName + "\\" + modelName +
                          ".onnx");
    searchPaths.push_back("C:\\ProgramData\\yams\\models\\" + modelName + ".onnx");
#else
    // Unix: System paths
    searchPaths.push_back("/usr/local/share/yams/models/" + modelName + "/model.onnx");
    searchPaths.push_back("/usr/local/share/yams/models/" + modelName + "/" + modelName + ".onnx");
    searchPaths.push_back("/usr/local/share/yams/models/" + modelName + ".onnx");
#endif

    // Log search paths on first attempt for debugging
    spdlog::info("[ONNX Plugin] Resolving model path for '{}', searching {} locations", modelName,
                 searchPaths.size());
#ifdef _WIN32
    spdlog::debug("[ONNX Plugin] Windows paths: LOCALAPPDATA={}, APPDATA={}, HOME={}", localAppData,
                  appData, homeDir);
#endif

    // Direct filesystem checks - no async, no timeouts needed for local files
    for (const auto& path : searchPaths) {
        if (path.empty())
            continue;

        try {
            if (fs::exists(path)) {
                spdlog::info("[ONNX Plugin] Found model at: {}", path);
                return path;
            }
        } catch (const std::exception& e) {
            spdlog::debug("[ONNX Plugin] Error checking path {}: {}", path, e.what());
        }
    }

    spdlog::warn("[ONNX Plugin] Model '{}' not found in any search path. Searched:", modelName);
    for (const auto& path : searchPaths) {
        if (!path.empty()) {
            spdlog::warn("[ONNX Plugin]   - {}", path);
        }
    }
    // Default to models directory
    return "models/" + modelName + ".onnx";
}

Result<OnnxModelPool::ModelSessionPtr>
OnnxModelPool::createModelSession(const std::string& modelName) {
    std::string modelPath = resolveModelPath(modelName);

    vector::EmbeddingConfig config;
    config.model_path = modelPath;
    config.model_name = modelName;
    config.enable_gpu = config_.enableGPU;
    config.num_threads = config_.numThreads;

    try {
        auto session = std::make_shared<OnnxModelSession>(modelPath, modelName, config);
        return session;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Failed to create model session: ") + e.what()};
    }
}

void OnnxModelPool::updateAccessStats(const std::string& modelName) {
    auto it = models_.find(modelName);
    if (it != models_.end()) {
        it->second.lastAccess = std::chrono::steady_clock::now();
        it->second.accessCount++;
    }
}

bool OnnxModelPool::canLoadModel(size_t estimatedSize) const {
    size_t currentUsage = getMemoryUsage();
    size_t maxBytes = config_.maxMemoryGB * 1024 * 1024 * 1024;

    return (currentUsage + estimatedSize) <= maxBytes;
}

void OnnxModelPool::setResolutionHints(const std::string& modelName, const ResolutionHints& hints) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    modelHints_[modelName] = hints;
}

} // namespace yams::daemon
