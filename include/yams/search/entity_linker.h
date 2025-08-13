#pragma once

#include <yams/core/types.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::search {

// EntityLinker configuration
struct EntityLinkerConfig {
    // Minimum confidence threshold for emitting a linked entity
    float min_confidence = 0.50f;

    // When true, normalize alias/text by lowercasing and trimming punctuation
    bool case_insensitive = true;

    // Consider multi-token phrases up to this n-gram size
    std::size_t max_ngram = 3;

    // When true, use alias dictionary lookups
    bool enable_alias_lookup = true;

    // When true, attempt simple plural/singular normalization ("cats" -> "cat")
    bool enable_plural_normalization = true;

    // Optional stopwords to ignore during tokenization
    std::unordered_set<std::string> stopwords;

    // Validate config
    bool is_valid() const {
        return min_confidence >= 0.0f && min_confidence <= 1.0f &&
               max_ngram >= 1;
    }
};

// A surface mention in text
struct EntityMention {
    std::string text;   // Mention text as it appears
    std::size_t start;  // Byte start offset in the original text
    std::size_t end;    // Byte end offset (exclusive) in the original text
};

// Result of linking a mention to a KG node
struct LinkedEntity {
    EntityMention mention;
    // Canonical node key (e.g., kg_nodes.node_key). Nullopt if no confident link.
    std::optional<std::string> node_key;
    // Confidence in [0, 1]
    float confidence = 0.0f;
    // Optional: source alias used for linking
    std::optional<std::string> matched_alias;
};

// Alias entry used to build the dictionary
struct AliasEntry {
    std::string alias;       // surface form, e.g., "NYC"
    std::string node_key;    // canonical KG node key, e.g., "geo:Q60"
    float prior = 1.0f;      // prior confidence for this mapping in [0,1]
};

// Interface for entity linking
class EntityLinker {
public:
    virtual ~EntityLinker() = default;

    // Configuration
    virtual void setConfig(const EntityLinkerConfig& cfg) = 0;
    virtual const EntityLinkerConfig& getConfig() const = 0;

    // Manage alias dictionary (optional for implementations that support it)
    virtual Result<void> addAlias(const AliasEntry& entry) = 0;
    virtual Result<void> addAliases(const std::vector<AliasEntry>& entries) = 0;
    virtual Result<void> clearAliases() = 0;

    // Link entities in raw text
    virtual Result<std::vector<LinkedEntity>> linkText(const std::string& text) = 0;

    // Optional: link pre-tokenized input (token, start, end)
    struct TokenSpan { std::string token; std::size_t start; std::size_t end; };
    virtual Result<std::vector<LinkedEntity>> linkTokens(const std::vector<TokenSpan>& tokens) = 0;
};

// A simple, local-first heuristic entity linker.
// Features:
// - Lowercase + punctuation-trim normalization
// - Alias dictionary with priors
// - n-gram phrase matching (up to max_ngram)
// - Optional plural/singular normalization
// - Confidence = alias_prior +/- small heuristics; filtered by min_confidence
class SimpleHeuristicEntityLinker final : public EntityLinker {
public:
    SimpleHeuristicEntityLinker() = default;
    explicit SimpleHeuristicEntityLinker(EntityLinkerConfig cfg) : _config(std::move(cfg)) {}

    void setConfig(const EntityLinkerConfig& cfg) override { _config = cfg; }
    const EntityLinkerConfig& getConfig() const override { return _config; }

    Result<void> addAlias(const AliasEntry& entry) override {
        if (entry.alias.empty() || entry.node_key.empty()) {
            return Error{ErrorCode::InvalidArgument, "alias and node_key must be non-empty"};
        }
        if (entry.prior < 0.0f || entry.prior > 1.0f) {
            return Error{ErrorCode::InvalidArgument, "prior must be within [0,1]"};
        }
        const auto norm = normalize(entry.alias);
        std::lock_guard<std::mutex> lock(_mutex);
        _alias_map[norm].push_back({entry.node_key, clamp01(entry.prior)});
        return {};
    }

    Result<void> addAliases(const std::vector<AliasEntry>& entries) override {
        std::lock_guard<std::mutex> lock(_mutex);
        for (const auto& e : entries) {
            if (e.alias.empty() || e.node_key.empty()) {
                return Error{ErrorCode::InvalidArgument, "alias and node_key must be non-empty"};
            }
            if (e.prior < 0.0f || e.prior > 1.0f) {
                return Error{ErrorCode::InvalidArgument, "prior must be within [0,1]"};
            }
            _alias_map[normalize(e.alias)].push_back({e.node_key, clamp01(e.prior)});
        }
        return {};
    }

    Result<void> clearAliases() override {
        std::lock_guard<std::mutex> lock(_mutex);
        _alias_map.clear();
        return {};
    }

    Result<std::vector<LinkedEntity>> linkText(const std::string& text) override {
        auto tokens = tokenize(text);
        return linkTokens(tokens);
    }

    Result<std::vector<LinkedEntity>> linkTokens(const std::vector<TokenSpan>& tokens) override {
        if (!_config.is_valid()) {
            return Error{ErrorCode::InvalidArgument, "Invalid EntityLinkerConfig"};
        }
        std::vector<LinkedEntity> out;
        if (tokens.empty()) return out;

        // Longest-first matching to avoid overlapping shorter phrases
        const std::size_t max_n = std::max<std::size_t>(1, _config.max_ngram);
        std::vector<bool> used(tokens.size(), false);

        for (std::size_t n = max_n; n >= 1; --n) {
            if (n > tokens.size()) {
                if (n == 1) break; // guard size_t underflow
                continue;
            }
            for (std::size_t i = 0; i + n <= tokens.size(); ++i) {
                if (anyUsed(used, i, i + n)) continue;

                // Build phrase
                std::string phrase;
                phrase.reserve(n * 8);
                for (std::size_t j = 0; j < n; ++j) {
                    if (j) phrase.push_back(' ');
                    phrase.append(tokens[i + j].token);
                }

                // Normalize phrase (including plural handling)
                auto norm = normalize(phrase);
                if (_config.enable_plural_normalization) {
                    norm = singularizeHeuristic(norm);
                }

                auto match = lookupBest(norm);
                if (!match.has_value()) {
                    continue;
                }

                // Compute confidence with small heuristic bonuses
                const auto& best = match.value();
                float conf = best.second;

                // Heuristic: favor longer phrases a little
                conf = clamp01(conf + 0.03f * static_cast<float>(n - 1));

                // Heuristic: tiny boost for longer surface mentions
                const std::size_t surface_len =
                    tokens[i].start < tokens[i + n - 1].end
                        ? (tokens[i + n - 1].end - tokens[i].start)
                        : 0;
                conf = clamp01(conf + 0.0015f * static_cast<float>(std::min<std::size_t>(surface_len, 200)));

                if (conf < _config.min_confidence) {
                    continue;
                }

                LinkedEntity le;
                le.mention.text = reconstructSurface(tokens, i, i + n);
                le.mention.start = tokens[i].start;
                le.mention.end = tokens[i + n - 1].end;
                le.node_key = best.first;
                le.confidence = conf;
                le.matched_alias = norm;
                out.push_back(std::move(le));

                // Mark used
                for (std::size_t j = i; j < i + n; ++j) {
                    used[j] = true;
                }
            }

            if (n == 1) break; // guard size_t underflow
        }

        return out;
    }

private:
    // alias_map: normalized_alias -> vector of (node_key, prior)
    using NodePrior = std::pair<std::string, float>;

    EntityLinkerConfig _config{};
    std::unordered_map<std::string, std::vector<NodePrior>> _alias_map;
    mutable std::mutex _mutex;

    static bool isWordChar(unsigned char c) {
        return std::isalnum(c) != 0 || c == '_' || c == '-';
    }

    static std::string toLower(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return s;
    }

    static std::string_view trimPunctView(std::string_view sv) {
        auto l = sv.begin();
        auto r = sv.end();
        while (l < r && !std::isalnum(static_cast<unsigned char>(*l))) ++l;
        while (r > l && !std::isalnum(static_cast<unsigned char>(*(r - 1)))) --r;
        return std::string_view(l, static_cast<std::size_t>(r - l));
    }

    std::string normalize(std::string s) const {
        std::string out = s;
        if (_config.case_insensitive) out = toLower(std::move(out));
        auto view = trimPunctView(out);
        return std::string(view);
    }

    static std::string singularizeHeuristic(const std::string& s) {
        // simplistic heuristic: trailing 's' -> drop; "ies" -> "y"
        if (s.size() >= 3) {
            if (s.size() > 3 && s.compare(s.size() - 3, 3, "ies") == 0) {
                std::string t = s;
                t.erase(t.end() - 3, t.end());
                t.push_back('y');
                return t;
            }
        }
        if (!s.empty() && s.back() == 's') {
            std::string t = s;
            t.pop_back();
            return t;
        }
        return s;
    }

    static float clamp01(float v) {
        if (v < 0.0f) return 0.0f;
        if (v > 1.0f) return 1.0f;
        return v;
    }

    // Tokenize into TokenSpan with byte offsets
    std::vector<TokenSpan> tokenize(const std::string& text) const {
        std::vector<TokenSpan> tokens;
        tokens.reserve(text.size() / 5 + 1); // heuristic
        std::size_t i = 0;
        while (i < text.size()) {
            // skip non-word
            while (i < text.size() && !isWordChar(static_cast<unsigned char>(text[i]))) {
                ++i;
            }
            if (i >= text.size()) break;
            const std::size_t start = i;
            std::string tok;
            while (i < text.size() && isWordChar(static_cast<unsigned char>(text[i]))) {
                tok.push_back(_config.case_insensitive
                                  ? static_cast<char>(std::tolower(static_cast<unsigned char>(text[i++])))
                                  : text[i++]);
            }
            const std::size_t end = i;
            if (!_config.stopwords.empty() &&
                _config.stopwords.count(tok) > 0) {
                continue;
            }
            tokens.push_back({std::move(tok), start, end});
        }
        return tokens;
    }

    static bool anyUsed(const std::vector<bool>& used, std::size_t b, std::size_t e) {
        for (std::size_t i = b; i < e; ++i) {
            if (used[i]) return true;
        }
        return false;
    }

    static std::string reconstructSurface(const std::vector<TokenSpan>& tokens,
                                          std::size_t b, std::size_t e) {
        // Concatenate original tokens with spaces (best-effort)
        std::string s;
        for (std::size_t i = b; i < e; ++i) {
            if (i != b) s.push_back(' ');
            s.append(tokens[i].token);
        }
        return s;
    }

    // Return the best (node_key, prior) for a normalized alias, if any
    std::optional<NodePrior> lookupBest(const std::string& norm_alias) const {
        if (!_config.enable_alias_lookup) return std::nullopt;
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _alias_map.find(norm_alias);
        if (it == _alias_map.end() || it->second.empty()) return std::nullopt;

        const auto& vec = it->second;
        const auto best_it = std::max_element(vec.begin(), vec.end(),
                                              [](const NodePrior& a, const NodePrior& b) {
                                                  return a.second < b.second;
                                              });
        return *best_it;
    }
};

} // namespace yams::search