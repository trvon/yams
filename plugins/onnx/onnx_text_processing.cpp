#include <yams/daemon/resource/onnx_text_processing.h>

#include <algorithm>
#include <regex>
#include <sstream>

namespace yams::daemon {

OnnxTextPreprocessor::OnnxTextPreprocessor(const OnnxTextConfig& config) : config_(config) {
    initializeBasicVocab();
}

std::string OnnxTextPreprocessor::normalizeText(const std::string& text) {
    std::string normalized = text;
    if (config_.model_settings.do_lower_case) {
        std::transform(normalized.begin(), normalized.end(), normalized.begin(), ::tolower);
    }

    std::regex multiple_spaces(R"(\s+)");
    normalized = std::regex_replace(normalized, multiple_spaces, " ");
    normalized.erase(0, normalized.find_first_not_of(" \t\n\r"));
    normalized.erase(normalized.find_last_not_of(" \t\n\r") + 1);
    return normalized;
}

std::vector<int32_t> OnnxTextPreprocessor::tokenize(const std::string& text) {
    std::vector<int32_t> tokens;
    std::string normalized = normalizeText(text);
    tokens.push_back(getSpecialTokenId(config_.model_settings.cls_token));

    std::istringstream iss(normalized);
    std::string word;
    while (iss >> word) {
        std::regex punct_regex(R"([^\w\s])");
        word = std::regex_replace(word, punct_regex, "");
        if (!word.empty()) {
            tokens.push_back(getTokenId(word));
        }
    }

    tokens.push_back(getSpecialTokenId(config_.model_settings.sep_token));
    return tokens;
}

std::vector<std::vector<int32_t>>
OnnxTextPreprocessor::tokenizeBatch(const std::vector<std::string>& texts) {
    std::vector<std::vector<int32_t>> batch_tokens;
    batch_tokens.reserve(texts.size());
    for (const auto& text : texts) {
        batch_tokens.push_back(tokenize(text));
    }
    return batch_tokens;
}

std::vector<int32_t> OnnxTextPreprocessor::truncateTokens(const std::vector<int32_t>& tokens,
                                                          size_t max_length) {
    if (tokens.size() <= max_length) {
        return tokens;
    }

    std::vector<int32_t> truncated;
    truncated.reserve(max_length);
    if (!tokens.empty()) {
        truncated.push_back(tokens[0]);
    }

    size_t content_length = max_length - 2;
    if (tokens.size() > 2) {
        auto start_it = tokens.begin() + 1;
        auto end_it = tokens.begin() + std::min(content_length + 1, tokens.size() - 1);
        truncated.insert(truncated.end(), start_it, end_it);
    }

    if (tokens.size() > 1) {
        truncated.push_back(getSpecialTokenId(config_.model_settings.sep_token));
    }
    return truncated;
}

std::vector<int32_t> OnnxTextPreprocessor::padTokens(const std::vector<int32_t>& tokens,
                                                     size_t target_length) {
    std::vector<int32_t> padded = tokens;
    while (padded.size() < target_length) {
        padded.push_back(static_cast<int32_t>(config_.padding_token_id));
    }
    return padded;
}

std::vector<int32_t>
OnnxTextPreprocessor::generateAttentionMask(const std::vector<int32_t>& tokens) {
    std::vector<int32_t> attention_mask;
    attention_mask.reserve(tokens.size());
    int32_t pad_token_id = static_cast<int32_t>(config_.padding_token_id);
    for (int32_t token : tokens) {
        attention_mask.push_back(token == pad_token_id ? 0 : 1);
    }
    return attention_mask;
}

size_t OnnxTextPreprocessor::getVocabSize() const {
    return vocab_to_id_.size();
}

bool OnnxTextPreprocessor::isValidToken(int32_t token_id) const {
    return token_id >= 0 && token_id < static_cast<int32_t>(vocab_to_id_.size());
}

std::string OnnxTextPreprocessor::decodeToken(int32_t token_id) const {
    for (const auto& [token, id] : vocab_to_id_) {
        if (id == token_id) {
            return token;
        }
    }
    return config_.model_settings.unk_token;
}

void OnnxTextPreprocessor::initializeBasicVocab() {
    vocab_to_id_[config_.model_settings.cls_token] = 0;
    vocab_to_id_[config_.model_settings.sep_token] = 1;
    vocab_to_id_[config_.model_settings.unk_token] = 2;
    vocab_to_id_[config_.model_settings.pad_token] = 3;
    vocab_to_id_[config_.model_settings.mask_token] = 4;
}

int32_t OnnxTextPreprocessor::getTokenId(const std::string& token) {
    auto it = vocab_to_id_.find(token);
    if (it != vocab_to_id_.end()) {
        return it->second;
    }

    const size_t max_vocab_size = 30527;
    if (vocab_to_id_.size() < max_vocab_size) {
        vocab_to_id_[token] = static_cast<int32_t>(vocab_to_id_.size());
        return vocab_to_id_[token];
    }
    return static_cast<int32_t>(config_.unk_token_id);
}

int32_t OnnxTextPreprocessor::getSpecialTokenId(const std::string& token) {
    auto it = vocab_to_id_.find(token);
    if (it != vocab_to_id_.end()) {
        return it->second;
    }
    return static_cast<int32_t>(config_.unk_token_id);
}

} // namespace yams::daemon
