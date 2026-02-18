#include "gliner_session.h"

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <yams/config/config_helpers.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <unordered_map>

// Conditionally include ONNX Runtime
#ifdef YAMS_USE_ONNX_RUNTIME
#include <onnxruntime_cxx_api.h>
#endif

namespace yams::glint {

namespace fs = std::filesystem;

// ============================================================================
// Word Splitting Implementation (matches GLiNER's WhitespaceTokenSplitter)
// Pattern: r"\w+(?:[-_]\w+)*|\S"
// - \w+ matches word characters (alphanumeric + underscore)
// - (?:[-_]\w+)* allows hyphenated/underscored words
// - \S matches single non-whitespace (punctuation)
// ============================================================================

namespace {

// Check if character is a word character (alphanumeric or underscore)
inline bool is_word_char(char c) {
    unsigned char uc = static_cast<unsigned char>(c);
    return std::isalnum(uc) || c == '_';
}

// Check if character is a word continuation (hyphen or underscore followed by word chars)
inline bool is_word_continuation(std::string_view text, size_t pos) {
    if (pos >= text.size())
        return false;
    char c = text[pos];
    if (c != '-' && c != '_')
        return false;
    // Check if followed by word character
    return (pos + 1 < text.size()) && is_word_char(text[pos + 1]);
}

} // namespace

std::vector<WordInfo> split_into_words(std::string_view text) {
    std::vector<WordInfo> words;
    size_t i = 0;
    const size_t len = text.size();

    while (i < len) {
        // Skip whitespace
        while (i < len && std::isspace(static_cast<unsigned char>(text[i]))) {
            ++i;
        }
        if (i >= len)
            break;

        size_t start = i;

        if (is_word_char(text[i])) {
            // Match \w+(?:[-_]\w+)* - word characters with optional hyphen/underscore continuations
            while (i < len && is_word_char(text[i])) {
                ++i;
            }
            // Check for hyphen/underscore continuations
            while (i < len && is_word_continuation(text, i)) {
                ++i; // skip hyphen/underscore
                while (i < len && is_word_char(text[i])) {
                    ++i;
                }
            }
        } else if (!std::isspace(static_cast<unsigned char>(text[i]))) {
            // Match \S - single non-whitespace character (punctuation)
            ++i;
        }

        if (i > start) {
            words.push_back({
                .text = std::string(text.substr(start, i - start)),
                .start_char = start,
                .end_char = i,
            });
        }
    }

    return words;
}

// ============================================================================
// Greedy Search Implementation
// ============================================================================

std::vector<EntitySpan> greedy_search(std::vector<EntitySpan> spans, bool flat_ner) {
    if (spans.empty())
        return {};

    // Sort by score descending
    std::sort(spans.begin(), spans.end(),
              [](const EntitySpan& a, const EntitySpan& b) { return a.score > b.score; });

    std::vector<EntitySpan> result;
    std::vector<bool> used(spans.size(), false);

    for (size_t i = 0; i < spans.size(); ++i) {
        if (used[i])
            continue;

        const auto& span = spans[i];
        bool overlaps = false;

        if (flat_ner) {
            // Check for any overlap with already selected spans
            for (const auto& selected : result) {
                // Overlap if ranges intersect
                if (span.start_word <= selected.end_word && span.end_word >= selected.start_word) {
                    overlaps = true;
                    break;
                }
            }
        }

        if (!overlaps) {
            result.push_back(span);
            used[i] = true;
        }
    }

    // Sort result by position
    std::sort(result.begin(), result.end(),
              [](const EntitySpan& a, const EntitySpan& b) { return a.start_char < b.start_char; });

    return result;
}

// ============================================================================
// SentencePiece/Unigram Tokenizer (HuggingFace tokenizer.json format)
// ============================================================================

class SimpleTokenizer {
public:
    // SentencePiece word-start marker (U+2581 LOWER ONE EIGHTH BLOCK)
    static constexpr const char* SPIECE_UNDERLINE = "\xe2\x96\x81"; // "▁" in UTF-8

    SimpleTokenizer() = default;

    bool load(const std::string& path) {
        try {
            std::ifstream file(path);
            if (!file)
                return false;

            auto json = nlohmann::json::parse(file, nullptr, false);
            if (json.is_discarded())
                return false;

            // Detect model type
            std::string model_type;
            if (json.contains("model") && json["model"].contains("type")) {
                model_type = json["model"]["type"].get<std::string>();
            }

            // Parse vocab from model.vocab
            if (json.contains("model") && json["model"].contains("vocab")) {
                const auto& vocab = json["model"]["vocab"];

                if (vocab.is_array()) {
                    // Unigram/SentencePiece format: vocab is array of [token, score] pairs
                    // Index in array is the token ID
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
                    spdlog::info("[Glint] Loaded Unigram tokenizer with {} tokens", vocab_.size());
                } else if (vocab.is_object()) {
                    // WordPiece format: {"token": id}
                    is_unigram_ = false;
                    for (auto& [token, id_val] : vocab.items()) {
                        if (id_val.is_number()) {
                            int id = id_val.get<int>();
                            vocab_[token] = id;
                            if (id >= static_cast<int>(id_to_token_.size())) {
                                id_to_token_.resize(id + 1);
                            }
                            id_to_token_[id] = token;
                        }
                    }
                    spdlog::info("[Glint] Loaded WordPiece tokenizer with {} tokens",
                                 vocab_.size());
                }
            }

            // Parse added tokens (special tokens like <<ENT>>, <<SEP>>)
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

                        // Track special tokens
                        bool is_special = token.value("special", false);
                        if (is_special || content[0] == '[' || content[0] == '<') {
                            if (content == "[CLS]" || content == "<s>")
                                cls_token_id_ = id;
                            else if (content == "[SEP]" || content == "</s>")
                                sep_token_id_ = id;
                            else if (content == "[PAD]" || content == "<pad>")
                                pad_token_id_ = id;
                            else if (content == "[UNK]" || content == "<unk>")
                                unk_token_id_ = id;
                        }
                        // GLiNER-specific tokens
                        if (content == "<<ENT>>")
                            ent_token_id_ = id;
                        else if (content == "<<SEP>>")
                            ent_sep_token_id_ = id;
                    }
                }
            }

            // Set special token IDs from vocab if not found in added_tokens
            if (cls_token_id_ < 0) {
                auto it = vocab_.find("[CLS]");
                if (it != vocab_.end())
                    cls_token_id_ = it->second;
            }
            if (sep_token_id_ < 0) {
                auto it = vocab_.find("[SEP]");
                if (it != vocab_.end())
                    sep_token_id_ = it->second;
            }
            if (unk_token_id_ < 0) {
                auto it = vocab_.find("[UNK]");
                if (it != vocab_.end())
                    unk_token_id_ = it->second;
            }

            loaded_ = !vocab_.empty();
            spdlog::info("[Glint] Tokenizer loaded: CLS={}, SEP={}, UNK={}, ENT={}, ENT_SEP={}",
                         cls_token_id_, sep_token_id_, unk_token_id_, ent_token_id_,
                         ent_sep_token_id_);
            return loaded_;
        } catch (const std::exception& e) {
            spdlog::warn("[Glint] Failed to load tokenizer: {}", e.what());
            return false;
        }
    }

    bool is_loaded() const { return loaded_; }
    bool is_unigram() const { return is_unigram_; }

    int cls_token_id() const { return cls_token_id_; }
    int sep_token_id() const { return sep_token_id_; }
    int pad_token_id() const { return pad_token_id_; }
    int unk_token_id() const { return unk_token_id_; }
    int ent_token_id() const { return ent_token_id_; }
    int ent_sep_token_id() const { return ent_sep_token_id_; }

    // Encode a single word (adds ▁ prefix for SentencePiece)
    std::vector<int> encode(const std::string& text, bool add_prefix = true) const {
        std::vector<int> ids;

        if (is_unigram_) {
            // SentencePiece/Unigram tokenization
            // Add ▁ prefix for word-initial position
            std::string to_encode = add_prefix ? (std::string(SPIECE_UNDERLINE) + text) : text;
            encode_unigram(to_encode, ids);
        } else {
            // WordPiece tokenization
            encode_wordpiece(text, ids);
        }

        return ids;
    }

    std::string decode(int id) const {
        if (id >= 0 && id < static_cast<int>(id_to_token_.size())) {
            return id_to_token_[id];
        }
        return "[UNK]";
    }

private:
    // Unigram tokenization: greedy longest-match
    void encode_unigram(const std::string& text, std::vector<int>& ids) const {
        // First try exact match (common case for known words like "▁Sundar")
        auto it = vocab_.find(text);
        if (it != vocab_.end()) {
            ids.push_back(it->second);
            return;
        }

        // Greedy longest-match tokenization
        size_t pos = 0;
        while (pos < text.size()) {
            // Find longest matching token starting at pos
            int best_id = unk_token_id_;
            size_t best_len = 1; // At minimum, consume 1 byte

            // Try decreasing lengths to find longest match
            // Start with remaining text length, go down to 1
            for (size_t len = text.size() - pos; len >= 1; --len) {
                std::string candidate = text.substr(pos, len);
                auto it = vocab_.find(candidate);
                if (it != vocab_.end()) {
                    best_id = it->second;
                    best_len = len;
                    break; // Found longest match
                }
            }

            // Handle multi-byte UTF-8 characters
            if (best_id == unk_token_id_ && pos < text.size()) {
                unsigned char c = static_cast<unsigned char>(text[pos]);
                if ((c & 0x80) == 0) {
                    best_len = 1; // ASCII
                } else if ((c & 0xE0) == 0xC0) {
                    best_len = 2; // 2-byte UTF-8
                } else if ((c & 0xF0) == 0xE0) {
                    best_len = 3; // 3-byte UTF-8
                } else if ((c & 0xF8) == 0xF0) {
                    best_len = 4; // 4-byte UTF-8
                }
                // Clamp to remaining text
                best_len = std::min(best_len, text.size() - pos);

                // Try the single UTF-8 character
                std::string utf8_char = text.substr(pos, best_len);
                auto char_it = vocab_.find(utf8_char);
                if (char_it != vocab_.end()) {
                    best_id = char_it->second;
                }
            }

            ids.push_back(best_id);
            pos += best_len;
        }
    }

    // WordPiece tokenization (for BERT-style models)
    void encode_wordpiece(const std::string& word, std::vector<int>& ids) const {
        // Try exact match first
        auto it = vocab_.find(word);
        if (it != vocab_.end()) {
            ids.push_back(it->second);
            return;
        }

        // Try lowercase
        std::string lower = word;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        it = vocab_.find(lower);
        if (it != vocab_.end()) {
            ids.push_back(it->second);
            return;
        }

        // Greedy WordPiece: longest match, with ## prefix for continuations
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
                // No match found, emit UNK and advance by 1
                ids.push_back(unk_token_id_);
                pos += 1;
            } else {
                ids.push_back(best_id);
                pos += best_len;
            }
            first_piece = false;
        }
    }

    std::unordered_map<std::string, int> vocab_;
    std::vector<std::string> id_to_token_;
    int cls_token_id_ = 1; // Default for DeBERTa
    int sep_token_id_ = 2; // Default for DeBERTa
    int pad_token_id_ = 0;
    int unk_token_id_ = 3; // Default for DeBERTa
    int ent_token_id_ = -1;
    int ent_sep_token_id_ = -1;
    bool is_unigram_ = false;
    bool loaded_ = false;
};

// ============================================================================
// GlinerSession::Impl
// ============================================================================

class GlinerSession::Impl {
public:
    explicit Impl(const GlinerConfig& config) : config_(config) {}

    ~Impl() = default;

    /// Auto-discover GLiNER model in standard locations.
    /// Search order: YAMS_GLINT_MODEL_PATH env, YAMS_DATA_DIR, YAMS_STORAGE,
    /// ~/.local/share/yams, /Volumes/picaso/yams (dev), /opt/homebrew/share/yams
    static std::string discover_model_path() {
        if (const char* env_path = std::getenv("YAMS_GLINT_MODEL_PATH")) {
            if (fs::exists(env_path)) {
                spdlog::info("[Glint] Using model from YAMS_GLINT_MODEL_PATH: {}", env_path);
                return env_path;
            }
        }

        const std::vector<std::string> model_names = {
            "gliner_small-v2.1-quantized",
            "gliner_small-v2.1",
            "gliner_medium-v2.1-quantized",
            "gliner_medium-v2.1",
        };

        std::vector<fs::path> search_dirs;

        // Use config helper for platform-aware data directory resolution
        // This handles YAMS_DATA_DIR, YAMS_STORAGE env vars and XDG/Windows paths
        search_dirs.emplace_back(yams::config::get_data_dir() / "models" / "gliner");

        for (const auto& base_dir : search_dirs) {
            if (!fs::exists(base_dir))
                continue;

            for (const auto& model_name : model_names) {
                fs::path model_path = base_dir / model_name / "model.onnx";
                if (fs::exists(model_path)) {
                    spdlog::info("[Glint] Auto-discovered model at: {}", model_path.string());
                    return model_path.string();
                }
            }

            std::error_code ec;
            for (const auto& entry : fs::directory_iterator(base_dir, ec)) {
                if (!entry.is_directory())
                    continue;
                fs::path model_path = entry.path() / "model.onnx";
                if (fs::exists(model_path)) {
                    spdlog::info("[Glint] Auto-discovered model at: {}", model_path.string());
                    return model_path.string();
                }
            }
        }

        spdlog::debug("[Glint] No GLiNER model found in standard locations");
        return "";
    }

    bool initialize() {
        if (config_.model_path.empty()) {
            config_.model_path = discover_model_path();
        }

        if (!config_.model_path.empty() && !fs::exists(config_.model_path)) {
            last_error_ = "Model path does not exist: " + config_.model_path;
            spdlog::error("[Glint] {}", last_error_);
            return false;
        }

        // Try to load tokenizer
        std::string tokenizer_path = config_.tokenizer_path;
        if (tokenizer_path.empty() && !config_.model_path.empty()) {
            fs::path model_dir = fs::path(config_.model_path).parent_path();
            fs::path tok_path = model_dir / "tokenizer.json";
            if (fs::exists(tok_path)) {
                tokenizer_path = tok_path.string();
            }
        }

        if (!tokenizer_path.empty()) {
            if (!tokenizer_.load(tokenizer_path)) {
                spdlog::warn("[Glint] Could not load tokenizer from {}", tokenizer_path);
            }
        }

#ifdef YAMS_USE_ONNX_RUNTIME
        if (!config_.model_path.empty() && fs::exists(config_.model_path)) {
            try {
                Ort::SessionOptions session_options;
                session_options.SetIntraOpNumThreads(config_.num_threads);
                session_options.SetInterOpNumThreads(1);
                session_options.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_BASIC);

                env_ = std::make_unique<Ort::Env>(ORT_LOGGING_LEVEL_WARNING, "Glint");
                session_ = std::make_unique<Ort::Session>(
                    *env_, fs::path(config_.model_path).c_str(), session_options);

                Ort::AllocatorWithDefaultOptions allocator;
                size_t num_inputs = session_->GetInputCount();
                input_names_.reserve(num_inputs);
                for (size_t i = 0; i < num_inputs; ++i) {
                    auto name = session_->GetInputNameAllocated(i, allocator);
                    input_names_.emplace_back(name.get());
                    spdlog::debug("[Glint] Input {}: '{}'", i, input_names_.back());
                }
                input_names_cstr_.reserve(num_inputs);
                for (const auto& name : input_names_) {
                    input_names_cstr_.push_back(name.c_str());
                }

                size_t num_outputs = session_->GetOutputCount();
                output_names_.reserve(num_outputs);
                for (size_t i = 0; i < num_outputs; ++i) {
                    auto name = session_->GetOutputNameAllocated(i, allocator);
                    output_names_.emplace_back(name.get());
                    spdlog::debug("[Glint] Output {}: '{}'", i, output_names_.back());
                }
                output_names_cstr_.reserve(num_outputs);
                for (const auto& name : output_names_) {
                    output_names_cstr_.push_back(name.c_str());
                }

                spdlog::info("[Glint] Loaded ONNX model with {} inputs, {} outputs", num_inputs,
                             num_outputs);
                ready_ = true;
                return true;
            } catch (const Ort::Exception& e) {
                last_error_ = std::string("ONNX error: ") + e.what();
                spdlog::error("[Glint] {}", last_error_);
            }
        }
#endif

        spdlog::info("[Glint] Running in mock mode (no ONNX model loaded)");
        mock_mode_ = true;
        ready_ = true;
        return true;
    }

    bool is_ready() const { return ready_; }

    std::vector<EntitySpan> extract(std::string_view text, std::span<const std::string> labels) {
        if (!ready_) {
            return {};
        }

        auto words = split_into_words(text);
        if (words.empty()) {
            return {};
        }

        if (mock_mode_) {
            return extract_mock(text, words, labels);
        }

#ifdef YAMS_USE_ONNX_RUNTIME
        return extract_onnx(text, words, labels);
#else
        return extract_mock(text, words, labels);
#endif
    }

    const std::vector<std::string>& entity_labels() const { return config_.entity_labels; }
    float threshold() const { return config_.threshold; }
    void set_threshold(float t) { config_.threshold = t; }
    std::string_view last_error() const { return last_error_; }

private:
    // Mock extraction - returns empty for now, can be extended for testing
    std::vector<EntitySpan> extract_mock(std::string_view /*text*/,
                                         const std::vector<WordInfo>& /*words*/,
                                         std::span<const std::string> /*labels*/) {
        // In mock mode, return empty result
        // This allows the plugin to function for testing without a real model
        return {};
    }

#ifdef YAMS_USE_ONNX_RUNTIME
    std::vector<EntitySpan> extract_onnx([[maybe_unused]] std::string_view text,
                                         const std::vector<WordInfo>& words,
                                         std::span<const std::string> labels) {
        if (!session_) {
            return {};
        }

        const size_t num_words = words.size();
        const size_t num_labels = labels.size();
        const size_t max_width = config_.max_width;

        // Build input prompt: [CLS] <<ENT>> label1 <<ENT>> label2 ... <<SEP>> word1 word2 ... [SEP]
        // GLiNER expects:
        // - [CLS] at the start (id=1)
        // - <<ENT>> before each entity type label (id=128002)
        // - <<SEP>> to separate entity types from text (id=128003)
        // - [SEP] at the end (id=2)
        std::vector<int64_t> input_ids;
        std::vector<int64_t> attention_mask;
        std::vector<int64_t> words_mask;

        // Get token IDs
        int cls_id = tokenizer_.is_loaded() ? tokenizer_.cls_token_id() : 1;      // [CLS]
        int ent_id = tokenizer_.is_loaded() ? tokenizer_.ent_token_id() : 128002; // <<ENT>>
        int ent_sep_id =
            tokenizer_.is_loaded() ? tokenizer_.ent_sep_token_id() : 128003; // <<SEP>> (entity sep)
        int sep_id = tokenizer_.is_loaded() ? tokenizer_.sep_token_id() : 2; // [SEP]

        // Fallback if ent_token_id not found in tokenizer
        if (ent_id < 0)
            ent_id = 128002;
        if (ent_sep_id < 0)
            ent_sep_id = 128003;

        spdlog::debug("[Glint] Token IDs: CLS={}, ENT={}, ENT_SEP={}, SEP={}", cls_id, ent_id,
                      ent_sep_id, sep_id);

        // Add [CLS] token at the start
        // words_mask uses 0 for special/prompt tokens (matching Python GLiNER convention)
        input_ids.push_back(cls_id);
        attention_mask.push_back(1);
        words_mask.push_back(0); // Special token

        // Add entity type prompts: <<ENT>> label_tokens for each entity type
        size_t word_idx = 0;
        for (const auto& label : labels) {
            // Add <<ENT>> marker before each entity type
            input_ids.push_back(ent_id);
            attention_mask.push_back(1);
            words_mask.push_back(0); // Prompt token, not a text word

            // Tokenize label
            if (tokenizer_.is_loaded()) {
                auto label_ids = tokenizer_.encode(label);
                for (int id : label_ids) {
                    input_ids.push_back(id);
                    attention_mask.push_back(1);
                    words_mask.push_back(0); // Prompt token
                }
            } else {
                // Simple: just use a placeholder
                input_ids.push_back(100 + word_idx++);
                attention_mask.push_back(1);
                words_mask.push_back(0);
            }
        }

        // Add <<SEP>> to separate entity types from text (NOT [SEP])
        input_ids.push_back(ent_sep_id);
        attention_mask.push_back(1);
        words_mask.push_back(0); // Special token

        // Add text words - GLiNER uses 1-based word indexing
        // IMPORTANT: Only the FIRST subword token of each word gets the word index;
        // continuation subwords get 0 (matching Python's token_level=False default)
        for (size_t i = 0; i < num_words; ++i) {
            const int64_t text_word_idx = static_cast<int64_t>(i + 1); // 1-based indexing
            if (tokenizer_.is_loaded()) {
                auto word_ids = tokenizer_.encode(words[i].text);
                for (size_t j = 0; j < word_ids.size(); ++j) {
                    input_ids.push_back(word_ids[j]);
                    attention_mask.push_back(1);
                    // Only first subword gets the word index; rest get 0
                    words_mask.push_back(j == 0 ? text_word_idx : 0);
                }
            } else {
                input_ids.push_back(1000 + static_cast<int64_t>(i));
                attention_mask.push_back(1);
                words_mask.push_back(text_word_idx);
            }
        }

        // Add [SEP] token at the end
        input_ids.push_back(sep_id);
        attention_mask.push_back(1);
        words_mask.push_back(0); // Special token

        spdlog::debug("[Glint] Input sequence length: {}, num_words: {}", input_ids.size(),
                      num_words);

        // Prepare text_lengths tensor
        std::vector<int64_t> text_lengths = {static_cast<int64_t>(num_words)};

        // Prepare span_idx tensor - fixed grid of [num_words, max_width] spans
        // Shape: [1, num_words * max_width, 2]
        // Invalid spans (where start + width >= num_words) are masked in span_mask
        const size_t span_count = num_words * max_width;
        std::vector<int64_t> span_idx;
        span_idx.reserve(span_count * 2);
        // Note: ONNX expects bool for span_mask, use unique_ptr<bool[]> since vector<bool> lacks
        // .data()
        std::unique_ptr<bool[]> span_mask(new bool[span_count]);

        for (size_t start = 0; start < num_words; ++start) {
            for (size_t width = 0; width < max_width; ++width) {
                size_t end = start + width;
                span_idx.push_back(static_cast<int64_t>(start));
                span_idx.push_back(static_cast<int64_t>(end));
                // Mask is true only for valid spans (end < num_words)
                size_t span_i = start * max_width + width;
                span_mask[span_i] = (end < num_words);
            }
        }
        const size_t num_spans = span_count;

        // Create ONNX tensors
        Ort::MemoryInfo mem_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
        const int64_t batch = 1;
        const int64_t seq_len = static_cast<int64_t>(input_ids.size());

        std::array<int64_t, 2> ids_shape = {batch, seq_len};
        std::array<int64_t, 2> text_len_shape = {batch, 1};
        std::array<int64_t, 3> span_idx_shape = {batch, static_cast<int64_t>(num_spans), 2};
        std::array<int64_t, 2> span_mask_shape = {batch, static_cast<int64_t>(num_spans)};

        std::vector<Ort::Value> input_tensors;
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(
            mem_info, input_ids.data(), input_ids.size(), ids_shape.data(), ids_shape.size()));
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(
            mem_info, attention_mask.data(), attention_mask.size(), ids_shape.data(),
            ids_shape.size()));
        input_tensors.push_back(Ort::Value::CreateTensor<int64_t>(
            mem_info, words_mask.data(), words_mask.size(), ids_shape.data(), ids_shape.size()));
        input_tensors.push_back(
            Ort::Value::CreateTensor<int64_t>(mem_info, text_lengths.data(), text_lengths.size(),
                                              text_len_shape.data(), text_len_shape.size()));
        input_tensors.push_back(
            Ort::Value::CreateTensor<int64_t>(mem_info, span_idx.data(), span_idx.size(),
                                              span_idx_shape.data(), span_idx_shape.size()));
        // span_mask requires bool type
        input_tensors.push_back(Ort::Value::CreateTensor<bool>(
            mem_info, span_mask.get(), num_spans, span_mask_shape.data(), span_mask_shape.size()));

        // Run inference
        try {
            spdlog::debug("[Glint] Running ONNX inference");
            auto outputs = session_->Run(Ort::RunOptions{nullptr}, input_names_cstr_.data(),
                                         input_tensors.data(), input_tensors.size(),
                                         output_names_cstr_.data(), output_names_.size());

            if (outputs.empty()) {
                spdlog::warn("[Glint] No outputs returned from ONNX model");
                return {};
            }

            // Parse output logits
            // Expected shape: [batch, num_words, max_width, num_classes]
            auto& logits_tensor = outputs[0];
            auto type_info = logits_tensor.GetTensorTypeAndShapeInfo();
            auto shape = type_info.GetShape();

            if (shape.size() != 4) {
                spdlog::warn("[Glint] Unexpected output shape: {} dims (expected 4)", shape.size());
                return {};
            }

            spdlog::debug("[Glint] Output shape: [{}, {}, {}, {}]", shape[0], shape[1], shape[2],
                          shape[3]);

            const float* logits = logits_tensor.GetTensorData<float>();
            const size_t out_words = static_cast<size_t>(shape[1]);
            const size_t out_width = static_cast<size_t>(shape[2]);
            const size_t out_classes = static_cast<size_t>(shape[3]);

            spdlog::debug("[Glint] Processing output: words={}, width={}, classes={}, threshold={}",
                          out_words, out_width, out_classes, config_.threshold);

            // Decode spans
            std::vector<EntitySpan> spans;
            for (size_t w = 0; w < out_words && w < num_words; ++w) {
                for (size_t k = 0; k < out_width && (w + k) < num_words; ++k) {
                    for (size_t c = 0; c < out_classes && c < num_labels; ++c) {
                        size_t idx = w * out_width * out_classes + k * out_classes + c;
                        float logit = logits[idx];
                        float prob = 1.0f / (1.0f + std::exp(-logit)); // sigmoid

                        if (prob >= config_.threshold) {
                            size_t start = w;
                            size_t end = w + k;

                            // Build entity text
                            std::string entity_text;
                            for (size_t i = start; i <= end; ++i) {
                                if (!entity_text.empty())
                                    entity_text += " ";
                                entity_text += words[i].text;
                            }

                            spdlog::debug(
                                "[Glint] Detected: w={}, k={}, start={}, end={}, label={}, "
                                "prob={:.4f}, text=\"{}\"",
                                w, k, start, end, labels[c], prob, entity_text);

                            spans.push_back({
                                .start_word = start,
                                .end_word = end,
                                .start_char = words[start].start_char,
                                .end_char = words[end].end_char,
                                .text = std::move(entity_text),
                                .label = std::string(labels[c]),
                                .score = prob,
                            });
                        }
                    }
                }
            }

            return greedy_search(std::move(spans), config_.flat_ner);

        } catch (const Ort::Exception& e) {
            last_error_ = std::string("Inference error: ") + e.what();
            spdlog::error("[Glint] {}", last_error_);
            return {};
        }
    }
#endif

    GlinerConfig config_;
    SimpleTokenizer tokenizer_;
    std::string last_error_;
    bool ready_ = false;
    bool mock_mode_ = false;

#ifdef YAMS_USE_ONNX_RUNTIME
    std::unique_ptr<Ort::Env> env_;
    std::unique_ptr<Ort::Session> session_;
    std::vector<std::string> input_names_;
    std::vector<std::string> output_names_;
    std::vector<const char*> input_names_cstr_;
    std::vector<const char*> output_names_cstr_;
#endif
};

// ============================================================================
// GlinerSession Public Interface
// ============================================================================

GlinerSession::GlinerSession(const GlinerConfig& config) : impl_(std::make_unique<Impl>(config)) {}

GlinerSession::~GlinerSession() = default;

GlinerSession::GlinerSession(GlinerSession&&) noexcept = default;
GlinerSession& GlinerSession::operator=(GlinerSession&&) noexcept = default;

bool GlinerSession::initialize() {
    return impl_->initialize();
}

bool GlinerSession::is_ready() const noexcept {
    return impl_->is_ready();
}

std::vector<EntitySpan> GlinerSession::extract(std::string_view text) {
    return impl_->extract(text, impl_->entity_labels());
}

std::vector<EntitySpan> GlinerSession::extract(std::string_view text,
                                               std::span<const std::string> labels) {
    return impl_->extract(text, labels);
}

const std::vector<std::string>& GlinerSession::entity_labels() const noexcept {
    return impl_->entity_labels();
}

float GlinerSession::threshold() const noexcept {
    return impl_->threshold();
}

void GlinerSession::set_threshold(float threshold) noexcept {
    impl_->set_threshold(threshold);
}

std::string_view GlinerSession::last_error() const noexcept {
    return impl_->last_error();
}

} // namespace yams::glint
