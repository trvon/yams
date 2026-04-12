#include <yams/vector/tokenizer.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>

namespace yams::vector {

// ---------------------------------------------------------------------------
// Post-processor parsing helpers (file-scoped)
// ---------------------------------------------------------------------------

namespace {

/// Extract the first SpecialToken id string from a TemplateProcessing template array.
/// Walks `single` or `pair` arrays looking for {"SpecialToken": {"id": "..."}}.
std::string extractSpecialTokenId(const nlohmann::json& templateArray, bool first) {
    if (!templateArray.is_array())
        return "";
    std::string lastFound;
    for (const auto& entry : templateArray) {
        if (entry.contains("SpecialToken") && entry["SpecialToken"].contains("id")) {
            auto id = entry["SpecialToken"]["id"].get<std::string>();
            if (first)
                return id;             // Return the very first SpecialToken
            lastFound = std::move(id); // Track the last SpecialToken seen
        }
    }
    return lastFound; // Returns last found, or "" if none
}

/// Resolve a special_tokens map entry to a numeric token id.
/// In HuggingFace tokenizer.json, post_processor.special_tokens looks like:
///   { "[CLS]": { "ids": [101], "tokens": ["[CLS]"] }, ... }
int32_t resolveSpecialTokenId(const nlohmann::json& specialTokens, const std::string& tokenStr,
                              const std::unordered_map<std::string, int>& vocab) {
    if (specialTokens.is_object() && specialTokens.contains(tokenStr)) {
        const auto& entry = specialTokens[tokenStr];
        if (entry.contains("ids") && entry["ids"].is_array() && !entry["ids"].empty()) {
            return entry["ids"][0].get<int32_t>();
        }
    }
    // Fall back to vocab lookup
    auto it = vocab.find(tokenStr);
    if (it != vocab.end())
        return static_cast<int32_t>(it->second);
    return -1;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// HuggingFaceTokenizer
// ---------------------------------------------------------------------------

bool HuggingFaceTokenizer::load(const std::string& path) {
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
        idToToken_.clear();
        isUnigram_ = false;

        // --- vocabulary -------------------------------------------------
        if (json.contains("model") && json["model"].contains("vocab")) {
            const auto& vocab = json["model"]["vocab"];
            if (vocab.is_array()) {
                // Unigram / SentencePiece format: array of [token, score]
                isUnigram_ = true;
                idToToken_.reserve(vocab.size());
                for (size_t id = 0; id < vocab.size(); ++id) {
                    const auto& entry = vocab[id];
                    if (entry.is_array() && !entry.empty() && entry[0].is_string()) {
                        std::string token = entry[0].get<std::string>();
                        vocab_[token] = static_cast<int>(id);
                        idToToken_.push_back(token);
                    } else {
                        idToToken_.push_back("");
                    }
                }
            } else if (vocab.is_object()) {
                // WordPiece / BERT format: object { token: id }
                isUnigram_ = false;
                for (auto& [token, idVal] : vocab.items()) {
                    if (!idVal.is_number())
                        continue;
                    int id = idVal.get<int>();
                    vocab_[token] = id;
                    if (id >= static_cast<int>(idToToken_.size())) {
                        idToToken_.resize(id + 1);
                    }
                    idToToken_[id] = token;
                }
            }
        }

        // --- added tokens -----------------------------------------------
        if (json.contains("added_tokens")) {
            for (const auto& token : json["added_tokens"]) {
                if (token.contains("content") && token.contains("id")) {
                    std::string content = token["content"].get<std::string>();
                    int id = token["id"].get<int>();
                    vocab_[content] = id;
                    if (id >= static_cast<int>(idToToken_.size())) {
                        idToToken_.resize(id + 1);
                    }
                    idToToken_[id] = content;
                    bool isSpecial = token.value("special", false);
                    if (isSpecial ||
                        (!content.empty() && (content[0] == '[' || content[0] == '<'))) {
                        if (content == "[UNK]" || content == "<unk>")
                            unkTokenId_ = id;
                    }
                }
            }
        }

        // Fallback: look up [UNK] in the main vocab if not found in added_tokens
        if (unkTokenId_ < 0) {
            auto it = vocab_.find("[UNK]");
            if (it != vocab_.end())
                unkTokenId_ = it->second;
        }

        loaded_ = !vocab_.empty();
        if (!loaded_)
            return false;

        // --- post_processor (special token injection) -------------------
        hasPostProcessor_ = false;
        bosTokenId_ = -1;
        eosTokenId_ = -1;
        if (json.contains("post_processor") && json["post_processor"].is_object()) {
            const auto& pp = json["post_processor"];
            auto type = pp.value("type", std::string{});

            // Handle both direct TemplateProcessing and Sequence wrapper
            const nlohmann::json* tmpl = nullptr;
            if (type == "TemplateProcessing") {
                tmpl = &pp;
            } else if (type == "Sequence" && pp.contains("processors") &&
                       pp["processors"].is_array()) {
                // Sequence wraps multiple processors; use the first
                // TemplateProcessing we find for BOS, last for EOS.
                for (const auto& proc : pp["processors"]) {
                    if (proc.value("type", std::string{}) == "TemplateProcessing") {
                        tmpl = &proc;
                        break;
                    }
                }
            }

            if (tmpl && tmpl->contains("single") && tmpl->contains("special_tokens")) {
                const auto& st = (*tmpl)["special_tokens"];
                const auto& single = (*tmpl)["single"];

                // BOS = first SpecialToken in the single template
                auto bosStr = extractSpecialTokenId(single, /*first=*/true);
                // EOS = last SpecialToken in the single template
                auto eosStr = extractSpecialTokenId(single, /*first=*/false);

                // For Sequence type, EOS may be in a later processor
                if (type == "Sequence" && pp.contains("processors") &&
                    pp["processors"].is_array()) {
                    const auto& procs = pp["processors"];
                    // Collect special_tokens from all processors
                    nlohmann::json mergedSt = nlohmann::json::object();
                    for (const auto& [k, v] : st.items()) {
                        mergedSt[k] = v;
                    }
                    for (const auto& proc : procs) {
                        if (proc.contains("special_tokens") && proc["special_tokens"].is_object()) {
                            for (auto& [k, v] : proc["special_tokens"].items()) {
                                mergedSt[k] = v;
                            }
                        }
                        // Pick EOS from the last processor's single template
                        if (proc.contains("single")) {
                            auto lastEos = extractSpecialTokenId(proc["single"], /*first=*/false);
                            if (!lastEos.empty())
                                eosStr = lastEos;
                        }
                    }
                    bosTokenId_ = resolveSpecialTokenId(mergedSt, bosStr, vocab_);
                    eosTokenId_ = resolveSpecialTokenId(mergedSt, eosStr, vocab_);
                } else {
                    bosTokenId_ = resolveSpecialTokenId(st, bosStr, vocab_);
                    eosTokenId_ = resolveSpecialTokenId(st, eosStr, vocab_);
                }

                hasPostProcessor_ = (bosTokenId_ >= 0 || eosTokenId_ >= 0);
            }
        }

        return true;
    } catch (const std::exception& e) {
        spdlog::warn("[Tokenizer] Failed to load tokenizer from '{}': {}", path, e.what());
        return false;
    }
}

std::vector<int32_t> HuggingFaceTokenizer::encode(const std::string& text) const {
    if (!loaded_) {
        return {};
    }
    if (isUnigram_) {
        return encodeUnigram(text);
    }
    return encodeWordPiece(text);
}

std::vector<int32_t> HuggingFaceTokenizer::encodeWithSpecialTokens(const std::string& text) const {
    if (!loaded_)
        return {};
    auto ids = encode(text);
    if (!hasPostProcessor_)
        return ids;

    std::vector<int32_t> result;
    result.reserve(ids.size() + 2);
    if (bosTokenId_ >= 0)
        result.push_back(bosTokenId_);
    result.insert(result.end(), ids.begin(), ids.end());
    if (eosTokenId_ >= 0)
        result.push_back(eosTokenId_);
    return result;
}

std::vector<int32_t> HuggingFaceTokenizer::encodeWithSpecialTokens(const std::string& textA,
                                                                   const std::string& textB) const {
    if (!loaded_)
        return {};
    auto idsA = encode(textA);
    auto idsB = encode(textB);
    if (!hasPostProcessor_) {
        // No template — just concatenate both segments
        idsA.insert(idsA.end(), idsB.begin(), idsB.end());
        return idsA;
    }

    // Pair format: [BOS] A [EOS] B [EOS]
    // Covers both BERT ([CLS] A [SEP] B [SEP]) and
    // SentencePiece (<s> A </s> B </s>).
    std::vector<int32_t> result;
    result.reserve(idsA.size() + idsB.size() + 3);
    if (bosTokenId_ >= 0)
        result.push_back(bosTokenId_);
    result.insert(result.end(), idsA.begin(), idsA.end());
    if (eosTokenId_ >= 0)
        result.push_back(eosTokenId_);
    result.insert(result.end(), idsB.begin(), idsB.end());
    if (eosTokenId_ >= 0)
        result.push_back(eosTokenId_);
    return result;
}

int32_t HuggingFaceTokenizer::unkTokenId() const {
    return unkTokenId_;
}

int32_t HuggingFaceTokenizer::tokenToId(const std::string& token) const {
    auto it = vocab_.find(token);
    if (it != vocab_.end()) {
        return it->second;
    }
    return unkTokenId_;
}

std::string HuggingFaceTokenizer::idToToken(int32_t id) const {
    if (id >= 0 && static_cast<size_t>(id) < idToToken_.size()) {
        return idToToken_[id];
    }
    return "";
}

bool HuggingFaceTokenizer::isLoaded() const {
    return loaded_;
}

size_t HuggingFaceTokenizer::vocabSize() const {
    return vocab_.size();
}

// ---------------------------------------------------------------------------
// Unigram (SentencePiece) encoding
// ---------------------------------------------------------------------------

std::vector<int32_t> HuggingFaceTokenizer::encodeUnigram(const std::string& text) const {
    std::vector<int32_t> ids;
    // NOTE: Do NOT lowercase — XLM-RoBERTa (bge-reranker-base) is case-sensitive.
    // The Unigram/SentencePiece vocab contains cased entries.
    const std::string& normalized = text;

    size_t pos = 0;
    while (pos < normalized.size()) {
        // Skip whitespace
        while (pos < normalized.size() &&
               std::isspace(static_cast<unsigned char>(normalized[pos]))) {
            ++pos;
        }
        if (pos >= normalized.size())
            break;

        // Extract next word
        size_t start = pos;
        while (pos < normalized.size() &&
               !std::isspace(static_cast<unsigned char>(normalized[pos]))) {
            ++pos;
        }
        std::string word = normalized.substr(start, pos - start);
        if (word.empty())
            continue;

        // All words get the SentencePiece prefix (including the first)
        word = std::string(kSpieceUnderline) + word;
        encodeUnigramWord(word, ids);
    }
    return ids;
}

void HuggingFaceTokenizer::encodeUnigramWord(const std::string& word,
                                             std::vector<int32_t>& ids) const {
    // Fast path: whole word is in vocab
    auto it = vocab_.find(word);
    if (it != vocab_.end()) {
        ids.push_back(it->second);
        return;
    }

    // Greedy longest-match subword tokenization
    size_t pos = 0;
    while (pos < word.size()) {
        int bestId = unkTokenId_;
        size_t bestLen = 1;
        for (size_t len = word.size() - pos; len >= 1; --len) {
            std::string candidate = word.substr(pos, len);
            auto found = vocab_.find(candidate);
            if (found != vocab_.end()) {
                bestId = found->second;
                bestLen = len;
                break;
            }
        }

        // If nothing matched, try matching the full UTF-8 character
        if (bestId == unkTokenId_ && pos < word.size()) {
            unsigned char c = static_cast<unsigned char>(word[pos]);
            if ((c & 0x80) == 0) {
                bestLen = 1;
            } else if ((c & 0xE0) == 0xC0) {
                bestLen = 2;
            } else if ((c & 0xF0) == 0xE0) {
                bestLen = 3;
            } else if ((c & 0xF8) == 0xF0) {
                bestLen = 4;
            }
            bestLen = std::min(bestLen, word.size() - pos);
            std::string utf8Char = word.substr(pos, bestLen);
            auto charIt = vocab_.find(utf8Char);
            if (charIt != vocab_.end()) {
                bestId = charIt->second;
            }
        }

        ids.push_back(bestId);
        pos += bestLen;
    }
}

// ---------------------------------------------------------------------------
// WordPiece (BERT) encoding
// ---------------------------------------------------------------------------

std::vector<int32_t> HuggingFaceTokenizer::encodeWordPiece(const std::string& text) const {
    std::vector<int32_t> ids;
    std::string normalized = text;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    std::istringstream iss(normalized);
    std::string word;
    while (iss >> word) {
        auto wordIds = encodeWordPieceWord(word);
        ids.insert(ids.end(), wordIds.begin(), wordIds.end());
    }
    return ids;
}

std::vector<int32_t> HuggingFaceTokenizer::encodeWordPieceWord(const std::string& word) const {
    std::vector<int32_t> ids;

    // Fast path: whole word is in vocab
    auto it = vocab_.find(word);
    if (it != vocab_.end()) {
        ids.push_back(it->second);
        return ids;
    }

    // Greedy longest-match with ## continuation prefix
    size_t pos = 0;
    bool firstPiece = true;
    while (pos < word.size()) {
        size_t bestLen = 0;
        int bestId = unkTokenId_;
        for (size_t len = word.size() - pos; len >= 1; --len) {
            std::string piece = word.substr(pos, len);
            if (!firstPiece) {
                piece = "##" + piece;
            }
            auto pieceIt = vocab_.find(piece);
            if (pieceIt != vocab_.end()) {
                bestId = pieceIt->second;
                bestLen = len;
                break;
            }
        }
        if (bestLen == 0) {
            ids.push_back(unkTokenId_);
            pos += 1;
        } else {
            ids.push_back(bestId);
            pos += bestLen;
        }
        firstPiece = false;
    }
    return ids;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

std::unique_ptr<ITokenizer> createTokenizer(const std::string& tokenizerJsonPath) {
    auto tokenizer = std::make_unique<HuggingFaceTokenizer>();
    if (tokenizer->load(tokenizerJsonPath)) {
        return tokenizer;
    }
    return nullptr;
}

} // namespace yams::vector
