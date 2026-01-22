#include <yams/daemon/resource/onnx_colbert_session.h>
#include <yams/vector/embedding_generator.h>

#include <nlohmann/json.hpp>
#include <onnxruntime_cxx_api.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <sstream>
#include <string_view>
#include <unordered_map>

namespace fs = std::filesystem;

static std::recursive_mutex* g_colbert_onnx_mutex = new std::recursive_mutex();
static Ort::Env* g_colbert_onnx_env = nullptr;
static bool g_colbert_onnx_env_initialized = false;

static Ort::Env& get_colbert_ort_env() {
    std::lock_guard<std::recursive_mutex> lock(*g_colbert_onnx_mutex);
    if (!g_colbert_onnx_env_initialized) {
        OrtThreadingOptions* threading_options = nullptr;
        Ort::GetApi().CreateThreadingOptions(&threading_options);
        if (threading_options) {
            Ort::GetApi().SetGlobalIntraOpNumThreads(threading_options, 1);
            Ort::GetApi().SetGlobalInterOpNumThreads(threading_options, 1);
            Ort::GetApi().SetGlobalSpinControl(threading_options, 0);
            g_colbert_onnx_env =
                new Ort::Env(threading_options, ORT_LOGGING_LEVEL_WARNING, "YamsColbert");
            Ort::GetApi().ReleaseThreadingOptions(threading_options);
        } else {
            g_colbert_onnx_env = new Ort::Env(ORT_LOGGING_LEVEL_WARNING, "YamsColbert");
        }
        g_colbert_onnx_env_initialized = true;
    }
    return *g_colbert_onnx_env;
}

namespace yams::daemon {

class OnnxColbertSession::Impl {
public:
    Impl(const std::string& modelPath, const std::string& modelName, const ColbertConfig& config)
        : modelPath_(modelPath), modelName_(modelName), config_(config),
          preprocessor_(vector::EmbeddingConfig{}) {
        if (std::getenv("YAMS_USE_MOCK_PROVIDER") || std::getenv("YAMS_SKIP_MODEL_LOADING") ||
            std::getenv("YAMS_TEST_MODE")) {
            testMode_ = true;
            return;
        }

        env_ = &get_colbert_ort_env();
        sessionOptions_ = std::make_unique<Ort::SessionOptions>();

        int intra = config_.num_threads > 0 ? config_.num_threads : 4;
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
        sessionOptions_->AddConfigEntry("session.intra_op.allow_spinning", "0");
        sessionOptions_->AddConfigEntry("session.inter_op.allow_spinning", "0");
        sessionOptions_->SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_BASIC);
        sessionOptions_->EnableMemPattern();
        sessionOptions_->EnableCpuMemArena();
    }

    Result<void> loadModel() {
        if (testMode_) {
            maxSequenceLength_ = config_.max_sequence_length;
            tokenDim_ = config_.token_dim;
            isLoaded_ = true;
            return Result<void>();
        }

        try {
            auto options = sessionOptions_->Clone();
            options.SetIntraOpNumThreads(1);
            options.SetInterOpNumThreads(1);
            session_ = std::make_unique<Ort::Session>(*env_, fs::path(modelPath_).c_str(), options);

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

            parseModelConfig();
            isLoaded_ = true;
            return Result<void>();
        } catch (const Ort::Exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Failed to load ColBERT model: ") + e.what()};
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Failed to load ColBERT model: ") + e.what()};
        }
    }

    Result<ColbertEmbeddings> encode(const std::string& text, bool isQuery) {
        if (testMode_) {
            return mockEmbeddings(text, isQuery);
        }
        if (!isLoaded_) {
            if (auto r = loadModel(); !r)
                return r.error();
        }

        auto tokenIds = tokenizeWithMarkers(text, isQuery);
        auto trimmedTokens = preprocessor_.truncateTokens(tokenIds, maxSequenceLength_);
        auto paddedTokens = preprocessor_.padTokens(trimmedTokens, maxSequenceLength_);
        auto attentionMask = preprocessor_.generateAttentionMask(paddedTokens);

        std::vector<int64_t> inputIds(paddedTokens.begin(), paddedTokens.end());
        std::vector<int64_t> attention(attentionMask.begin(), attentionMask.end());
        std::vector<int64_t> shape = {1, static_cast<int64_t>(maxSequenceLength_)};

        auto memoryInfo = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
        std::vector<Ort::Value> inputs;
        inputs.push_back(Ort::Value::CreateTensor<int64_t>(memoryInfo, inputIds.data(),
                                                           inputIds.size(), shape.data(), 2));
        inputs.push_back(Ort::Value::CreateTensor<int64_t>(memoryInfo, attention.data(),
                                                           attention.size(), shape.data(), 2));

        std::vector<const char*> inNames;
        for (auto& n : inputNames_)
            inNames.push_back(n.c_str());
        std::vector<const char*> outNames;
        for (auto& n : outputNames_)
            outNames.push_back(n.c_str());

        auto outputs = session_->Run(Ort::RunOptions{nullptr}, inNames.data(), inputs.data(),
                                     inputs.size(), outNames.data(), outNames.size());
        if (outputs.empty()) {
            return Error{ErrorCode::InternalError, "ColBERT returned no outputs"};
        }

        auto tensorInfo = outputs[0].GetTensorTypeAndShapeInfo();
        auto shapeOut = tensorInfo.GetShape();
        if (shapeOut.size() < 3) {
            return Error{ErrorCode::InternalError, "ColBERT output has unexpected rank"};
        }
        size_t seqLen = static_cast<size_t>(shapeOut[1]);
        size_t dim = static_cast<size_t>(shapeOut[2]);

        const float* data = outputs[0].GetTensorData<float>();
        ColbertEmbeddings result;
        result.token_ids = trimmedTokens;
        result.token_embeddings.resize(seqLen);
        for (size_t i = 0; i < seqLen; ++i) {
            result.token_embeddings[i].assign(data + i * dim, data + (i + 1) * dim);
        }

        if (!config_.skiplist.empty()) {
            applySkiplist(result.token_embeddings, result.token_ids, config_.skiplist);
        }

        return result;
    }

    Result<std::vector<ColbertEmbeddings>> encodeBatch(const std::vector<std::string>& documents) {
        std::vector<ColbertEmbeddings> out;
        out.reserve(documents.size());
        for (const auto& doc : documents) {
            auto r = encode(doc, false);
            if (!r)
                return r.error();
            out.push_back(std::move(r.value()));
        }
        return out;
    }

    std::vector<float> maxPoolEmbeddings(const ColbertEmbeddings& embeddings, size_t dim) const {
        std::vector<float> pooled(dim, -std::numeric_limits<float>::infinity());
        if (embeddings.token_embeddings.empty()) {
            std::fill(pooled.begin(), pooled.end(), 0.0f);
            return pooled;
        }
        for (const auto& token : embeddings.token_embeddings) {
            for (size_t i = 0; i < dim && i < token.size(); ++i) {
                pooled[i] = std::max(pooled[i], token[i]);
            }
        }
        for (auto& v : pooled) {
            if (!std::isfinite(v))
                v = 0.0f;
        }
        return pooled;
    }

    void normalizeEmbedding(std::vector<float>& vec) const {
        double norm = 0.0;
        for (float v : vec) {
            norm += static_cast<double>(v) * static_cast<double>(v);
        }
        norm = std::sqrt(norm);
        if (norm > 1e-8) {
            for (auto& v : vec) {
                v = static_cast<float>(v / norm);
            }
        } else {
            std::fill(vec.begin(), vec.end(), 0.0f);
        }
    }

    float computeMaxSim(const ColbertEmbeddings& query, const ColbertEmbeddings& document) const {
        float score = 0.0f;
        for (const auto& q : query.token_embeddings) {
            float maxSim = -std::numeric_limits<float>::infinity();
            for (const auto& d : document.token_embeddings) {
                float sim = dot(q, d);
                if (sim > maxSim)
                    maxSim = sim;
            }
            score += maxSim;
        }
        return score;
    }

    bool isValid() const { return testMode_ ? isLoaded_ : (isLoaded_ && session_ != nullptr); }

    size_t maxSequenceLength() const { return maxSequenceLength_; }
    size_t tokenDim() const { return tokenDim_; }

private:
    std::vector<int32_t> tokenizeWithMarkers(const std::string& text, bool isQuery) {
        std::vector<int32_t> tokens;
        tokens.push_back(isQuery ? config_.query_marker_id : config_.doc_marker_id);
        auto inner = tokenizer_.encode(text);
        tokens.insert(tokens.end(), inner.begin(), inner.end());
        return tokens;
    }

    void applySkiplist(std::vector<std::vector<float>>& embeddings,
                       const std::vector<int32_t>& tokenIds, const std::vector<int32_t>& skiplist) {
        if (embeddings.empty() || tokenIds.empty())
            return;
        for (size_t i = 0; i < tokenIds.size() && i < embeddings.size(); ++i) {
            if (std::find(skiplist.begin(), skiplist.end(), tokenIds[i]) != skiplist.end()) {
                std::fill(embeddings[i].begin(), embeddings[i].end(), 0.0f);
            }
        }
    }

    float dot(const std::vector<float>& a, const std::vector<float>& b) const {
        float sum = 0.0f;
        size_t n = std::min(a.size(), b.size());
        for (size_t i = 0; i < n; ++i) {
            sum += a[i] * b[i];
        }
        return sum;
    }

    ColbertEmbeddings mockEmbeddings(const std::string& text, bool isQuery) {
        ColbertEmbeddings mock;
        mock.token_ids.push_back(isQuery ? config_.query_marker_id : config_.doc_marker_id);
        mock.token_ids.push_back(static_cast<int32_t>(text.size() % 32000));
        mock.token_embeddings.assign(mock.token_ids.size(), std::vector<float>(tokenDim_, 0.0f));
        if (!mock.token_embeddings.empty()) {
            mock.token_embeddings[0][0] = 1.0f;
        }
        return mock;
    }

    void parseModelConfig() {
        maxSequenceLength_ = config_.max_sequence_length;
        tokenDim_ = config_.token_dim;

        try {
            fs::path mp(modelPath_);
            fs::path cfg = mp.parent_path() / "config.json";
            if (!fs::exists(cfg)) {
                cfg = mp.parent_path().parent_path() / "config.json";
            }
            if (fs::exists(cfg)) {
                std::ifstream in(cfg);
                if (in.good()) {
                    nlohmann::json j;
                    in >> j;
                    if (j.contains("max_position_embeddings") &&
                        j["max_position_embeddings"].is_number_integer()) {
                        maxSequenceLength_ =
                            static_cast<size_t>(j["max_position_embeddings"].get<int>());
                    }
                }
            }

            fs::path tok = mp.parent_path() / "tokenizer.json";
            if (!fs::exists(tok)) {
                tok = mp.parent_path().parent_path() / "tokenizer.json";
            }
            if (!tokenizer_.load(tok.string())) {
                spdlog::warn("[ColBERT] tokenizer.json not loaded from {}",
                             tok.empty() ? "<empty>" : tok.string());
            }
        } catch (...) {
        }
    }

    std::string modelPath_;
    std::string modelName_;
    ColbertConfig config_;
    vector::TextPreprocessor preprocessor_;
    class SimpleTokenizer {
    public:
        bool load(const std::string& path) {
            if (path.empty()) {
                return false;
            }
            try {
                std::ifstream file(path);
                if (!file)
                    return false;
                auto json = nlohmann::json::parse(file, nullptr, false);
                if (json.is_discarded())
                    return false;

                vocab_.clear();
                id_to_token_.clear();
                is_unigram_ = false;

                if (json.contains("model") && json["model"].contains("vocab")) {
                    const auto& vocab = json["model"]["vocab"];
                    if (vocab.is_array()) {
                        is_unigram_ = true;
                        id_to_token_.reserve(vocab.size());
                        for (size_t id = 0; id < vocab.size(); ++id) {
                            const auto& entry = vocab[id];
                            if (entry.is_array() && !entry.empty() && entry[0].is_string()) {
                                std::string token = entry[0].get<std::string>();
                                vocab_[token] = static_cast<int>(id);
                                id_to_token_.push_back(token);
                            } else {
                                id_to_token_.push_back("");
                            }
                        }
                    } else if (vocab.is_object()) {
                        is_unigram_ = false;
                        for (auto& [token, id_val] : vocab.items()) {
                            if (!id_val.is_number())
                                continue;
                            int id = id_val.get<int>();
                            vocab_[token] = id;
                            if (id >= static_cast<int>(id_to_token_.size())) {
                                id_to_token_.resize(id + 1);
                            }
                            id_to_token_[id] = token;
                        }
                    }
                }

                if (json.contains("added_tokens")) {
                    for (const auto& token : json["added_tokens"]) {
                        if (token.contains("content") && token.contains("id")) {
                            std::string content = token["content"].get<std::string>();
                            int id = token["id"].get<int>();
                            vocab_[content] = id;
                            if (id >= static_cast<int>(id_to_token_.size())) {
                                id_to_token_.resize(id + 1);
                            }
                            id_to_token_[id] = content;
                            bool is_special = token.value("special", false);
                            if (is_special ||
                                (!content.empty() && (content[0] == '[' || content[0] == '<'))) {
                                if (content == "[UNK]" || content == "<unk>")
                                    unk_token_id_ = id;
                            }
                        }
                    }
                }

                if (unk_token_id_ < 0) {
                    auto it = vocab_.find("[UNK]");
                    if (it != vocab_.end())
                        unk_token_id_ = it->second;
                }

                loaded_ = !vocab_.empty();
                return loaded_;
            } catch (const std::exception& e) {
                spdlog::warn("[ColBERT] Failed to load tokenizer: {}", e.what());
                return false;
            }
        }

        std::vector<int32_t> encode(const std::string& text) const {
            if (!loaded_) {
                return {};
            }
            if (is_unigram_) {
                return encode_unigram(text);
            }
            return encode_wordpiece(text);
        }

    private:
        static constexpr const char* kSpieceUnderline = "\xe2\x96\x81";

        std::vector<int32_t> encode_unigram(const std::string& text) const {
            std::vector<int32_t> ids;
            std::string normalized = text;
            std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

            bool first_word = true;
            size_t pos = 0;
            while (pos < normalized.size()) {
                while (pos < normalized.size() &&
                       std::isspace(static_cast<unsigned char>(normalized[pos]))) {
                    ++pos;
                }
                if (pos >= normalized.size())
                    break;
                size_t start = pos;
                while (pos < normalized.size() &&
                       !std::isspace(static_cast<unsigned char>(normalized[pos]))) {
                    ++pos;
                }
                std::string word = normalized.substr(start, pos - start);
                if (word.empty())
                    continue;
                if (first_word) {
                    word = std::string(kSpieceUnderline) + word;
                    first_word = false;
                } else {
                    word = std::string(kSpieceUnderline) + word;
                }
                encode_unigram_word(word, ids);
            }
            return ids;
        }

        void encode_unigram_word(const std::string& word, std::vector<int32_t>& ids) const {
            auto it = vocab_.find(word);
            if (it != vocab_.end()) {
                ids.push_back(it->second);
                return;
            }

            size_t pos = 0;
            while (pos < word.size()) {
                int best_id = unk_token_id_;
                size_t best_len = 1;
                for (size_t len = word.size() - pos; len >= 1; --len) {
                    std::string candidate = word.substr(pos, len);
                    auto found = vocab_.find(candidate);
                    if (found != vocab_.end()) {
                        best_id = found->second;
                        best_len = len;
                        break;
                    }
                }

                if (best_id == unk_token_id_ && pos < word.size()) {
                    unsigned char c = static_cast<unsigned char>(word[pos]);
                    if ((c & 0x80) == 0) {
                        best_len = 1;
                    } else if ((c & 0xE0) == 0xC0) {
                        best_len = 2;
                    } else if ((c & 0xF0) == 0xE0) {
                        best_len = 3;
                    } else if ((c & 0xF8) == 0xF0) {
                        best_len = 4;
                    }
                    best_len = std::min(best_len, word.size() - pos);
                    std::string utf8_char = word.substr(pos, best_len);
                    auto char_it = vocab_.find(utf8_char);
                    if (char_it != vocab_.end()) {
                        best_id = char_it->second;
                    }
                }

                ids.push_back(best_id);
                pos += best_len;
            }
        }

        std::vector<int32_t> encode_wordpiece(const std::string& text) const {
            std::vector<int32_t> ids;
            std::string normalized = text;
            std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            std::istringstream iss(normalized);
            std::string word;
            while (iss >> word) {
                auto word_ids = encode_wordpiece_word(word);
                ids.insert(ids.end(), word_ids.begin(), word_ids.end());
            }
            return ids;
        }

        std::vector<int32_t> encode_wordpiece_word(const std::string& word) const {
            std::vector<int32_t> ids;
            auto it = vocab_.find(word);
            if (it != vocab_.end()) {
                ids.push_back(it->second);
                return ids;
            }

            size_t pos = 0;
            bool first_piece = true;
            while (pos < word.size()) {
                size_t best_len = 0;
                int best_id = unk_token_id_;
                for (size_t len = word.size() - pos; len >= 1; --len) {
                    std::string piece = word.substr(pos, len);
                    if (!first_piece) {
                        piece = "##" + piece;
                    }
                    auto piece_it = vocab_.find(piece);
                    if (piece_it != vocab_.end()) {
                        best_id = piece_it->second;
                        best_len = len;
                        break;
                    }
                }
                if (best_len == 0) {
                    ids.push_back(unk_token_id_);
                    pos += 1;
                } else {
                    ids.push_back(best_id);
                    pos += best_len;
                }
                first_piece = false;
            }
            return ids;
        }

        std::unordered_map<std::string, int> vocab_;
        std::vector<std::string> id_to_token_;
        int unk_token_id_ = 0;
        bool is_unigram_ = false;
        bool loaded_ = false;
    };
    SimpleTokenizer tokenizer_{};

    Ort::Env* env_ = nullptr;
    std::unique_ptr<Ort::SessionOptions> sessionOptions_;
    std::unique_ptr<Ort::Session> session_;
    std::vector<std::string> inputNames_;
    std::vector<std::string> outputNames_;

    size_t maxSequenceLength_ = 512;
    size_t tokenDim_ = 48;
    bool isLoaded_ = false;
    bool testMode_ = false;
};

OnnxColbertSession::OnnxColbertSession(const std::string& modelPath, const std::string& modelName,
                                       const ColbertConfig& config)
    : modelPath_(modelPath), modelName_(modelName), config_(config),
      pImpl_(std::make_unique<Impl>(modelPath, modelName, config)) {
    if (auto r = pImpl_->loadModel(); !r) {
        spdlog::error("[ColBERT] Eager load failed for '{}': {}", modelName, r.error().message);
    } else {
        maxSequenceLength_ = pImpl_->maxSequenceLength();
        tokenDim_ = pImpl_->tokenDim();
    }
}

OnnxColbertSession::~OnnxColbertSession() = default;

Result<ColbertEmbeddings> OnnxColbertSession::encodeQuery(const std::string& query) {
    return pImpl_->encode(query, true);
}

Result<ColbertEmbeddings> OnnxColbertSession::encodeDocument(const std::string& document) {
    return pImpl_->encode(document, false);
}

Result<std::vector<ColbertEmbeddings>>
OnnxColbertSession::encodeDocuments(const std::vector<std::string>& documents) {
    return pImpl_->encodeBatch(documents);
}

Result<std::vector<float>>
OnnxColbertSession::encodeDocumentEmbedding(const std::string& document) {
    auto encoded = pImpl_->encode(document, false);
    if (!encoded)
        return encoded.error();
    auto pooled = pImpl_->maxPoolEmbeddings(encoded.value(), tokenDim_);
    pImpl_->normalizeEmbedding(pooled);
    return pooled;
}

Result<std::vector<std::vector<float>>>
OnnxColbertSession::encodeDocumentEmbeddings(const std::vector<std::string>& documents) {
    auto batch = pImpl_->encodeBatch(documents);
    if (!batch)
        return batch.error();
    std::vector<std::vector<float>> out;
    out.reserve(batch.value().size());
    for (const auto& doc : batch.value()) {
        auto pooled = pImpl_->maxPoolEmbeddings(doc, tokenDim_);
        pImpl_->normalizeEmbedding(pooled);
        out.push_back(std::move(pooled));
    }
    return out;
}

float OnnxColbertSession::computeMaxSim(const ColbertEmbeddings& query,
                                        const ColbertEmbeddings& document) const {
    return pImpl_->computeMaxSim(query, document);
}

bool OnnxColbertSession::isValid() const {
    return pImpl_->isValid();
}

} // namespace yams::daemon
