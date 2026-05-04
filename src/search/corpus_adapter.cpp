#include <yams/search/corpus_adapter.h>

#include <yams/metadata/metadata_repository.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace yams::search {
namespace {

struct QuerySeed {
    std::string text;
    std::string kind;
    float weight{1.0f};
};

std::string lowerCopy(std::string_view in) {
    std::string out;
    out.reserve(in.size());
    for (unsigned char ch : in) {
        out.push_back(static_cast<char>(std::tolower(ch)));
    }
    return out;
}

std::string trimCopy(std::string_view in) {
    while (!in.empty() && std::isspace(static_cast<unsigned char>(in.front()))) {
        in.remove_prefix(1);
    }
    while (!in.empty() && std::isspace(static_cast<unsigned char>(in.back()))) {
        in.remove_suffix(1);
    }
    return std::string(in);
}

bool looksStructured(std::string_view query) {
    return query.find('=') != std::string_view::npos || query.find('/') != std::string_view::npos ||
           query.find('\\') != std::string_view::npos ||
           query.find('.') != std::string_view::npos || query.find('#') != std::string_view::npos;
}

bool isWordChar(unsigned char ch) noexcept {
    return std::isalnum(ch) || ch == '_' || ch == '-' || ch == '.' || ch == '/' || ch == '\\' ||
           ch == '#';
}

const std::unordered_set<std::string_view>& englishStopwords() {
    static const std::unordered_set<std::string_view> kStopwords = {
        "a",    "an",   "and", "are",  "as",    "at",    "be",     "by",    "can", "could", "do",
        "does", "did",  "for", "from", "has",   "have",  "how",    "i",     "in",  "into",  "is",
        "it",   "lets", "of",  "on",   "or",    "our",   "should", "that",  "the", "their", "this",
        "to",   "us",   "we",  "what", "where", "which", "with",   "would", "you", "your"};
    return kStopwords;
}

bool isStopword(std::string_view token) {
    return englishStopwords().contains(token);
}

std::vector<std::string> tokenizeQueryTerms(std::string_view query) {
    std::vector<std::string> terms;
    std::size_t pos = 0;
    while (pos < query.size()) {
        while (pos < query.size() && !isWordChar(static_cast<unsigned char>(query[pos]))) {
            ++pos;
        }
        const std::size_t start = pos;
        while (pos < query.size() && isWordChar(static_cast<unsigned char>(query[pos]))) {
            ++pos;
        }
        if (pos > start) {
            std::string term = lowerCopy(query.substr(start, pos - start));
            while (!term.empty() &&
                   (term.front() == '.' || term.front() == '-' || term.front() == '#')) {
                term.erase(term.begin());
            }
            while (!term.empty() &&
                   (term.back() == '.' || term.back() == '-' || term.back() == '#')) {
                term.pop_back();
            }
            if (term.size() > 1) {
                terms.push_back(std::move(term));
            }
        }
    }
    return terms;
}

std::vector<std::pair<std::string, std::string>> parseMetadataFilters(std::string_view query) {
    std::vector<std::pair<std::string, std::string>> filters;
    std::size_t pos = 0;
    while (pos < query.size()) {
        while (pos < query.size() && std::isspace(static_cast<unsigned char>(query[pos]))) {
            ++pos;
        }
        const std::size_t keyStart = pos;
        while (pos < query.size() && query[pos] != '=' &&
               !std::isspace(static_cast<unsigned char>(query[pos]))) {
            ++pos;
        }
        if (pos >= query.size() || query[pos] != '=' || pos == keyStart) {
            while (pos < query.size() && !std::isspace(static_cast<unsigned char>(query[pos]))) {
                ++pos;
            }
            continue;
        }
        std::string key(query.substr(keyStart, pos - keyStart));
        ++pos; // '='
        const std::size_t valueStart = pos;
        while (pos < query.size() && !std::isspace(static_cast<unsigned char>(query[pos]))) {
            ++pos;
        }
        if (pos > valueStart) {
            filters.emplace_back(std::move(key),
                                 std::string(query.substr(valueStart, pos - valueStart)));
        }
    }
    return filters;
}

void addUniqueSeed(std::vector<QuerySeed>& seeds, std::unordered_set<std::string>& seen,
                   std::string text, std::string kind, float weight) {
    text = trimCopy(text);
    if (text.size() < 2) {
        return;
    }
    std::string key = lowerCopy(text);
    if (!seen.insert(key).second) {
        return;
    }
    seeds.push_back(QuerySeed{std::move(text), std::move(kind), weight});
}

std::vector<QuerySeed> buildPathSeeds(std::string_view query, std::size_t maxSeeds = 8) {
    std::vector<QuerySeed> seeds;
    std::unordered_set<std::string> seen;
    seeds.reserve(maxSeeds);

    const std::string whole = trimCopy(query);
    if ((looksStructured(query) || query.size() <= 64) && !whole.empty()) {
        addUniqueSeed(seeds, seen, whole, "whole_query", looksStructured(query) ? 1.0f : 0.82f);
    }

    for (const auto& term : tokenizeQueryTerms(query)) {
        if (seeds.size() >= maxSeeds) {
            break;
        }
        if (term.find('=') != std::string::npos) {
            continue;
        }
        const bool pathish =
            term.find('/') != std::string::npos || term.find('\\') != std::string::npos ||
            term.find('.') != std::string::npos || term.find('_') != std::string::npos ||
            term.find('-') != std::string::npos || term.find('#') != std::string::npos;
        if (pathish && term.size() >= 3) {
            addUniqueSeed(seeds, seen, term, "structured_token", 0.95f);
        }
    }

    std::vector<std::string> contentTerms;
    for (auto& term : tokenizeQueryTerms(query)) {
        if (term.find('=') != std::string::npos || term.size() < 3 || isStopword(term)) {
            continue;
        }
        // Split path-like tokens into useful leaf seeds: docs/research/foo.md -> foo, research.
        std::size_t start = 0;
        for (std::size_t i = 0; i <= term.size(); ++i) {
            if (i == term.size() || term[i] == '/' || term[i] == '\\' || term[i] == '.' ||
                term[i] == '_' || term[i] == '-') {
                if (i > start + 2) {
                    contentTerms.push_back(term.substr(start, i - start));
                }
                start = i + 1;
            }
        }
        contentTerms.push_back(std::move(term));
    }

    for (const auto& term : contentTerms) {
        if (seeds.size() >= maxSeeds) {
            break;
        }
        addUniqueSeed(seeds, seen, term, "content_term", 0.70f);
    }

    // Language-seeded phrase windows.  This is intentionally English-first for
    // YAMS agent memory: break natural-language requests into compact content
    // spans that can match directory/file names without needing embeddings.
    for (std::size_t i = 0; i < contentTerms.size() && seeds.size() < maxSeeds; ++i) {
        std::string phrase;
        for (std::size_t j = i; j < std::min(contentTerms.size(), i + 3) && seeds.size() < maxSeeds;
             ++j) {
            if (!phrase.empty()) {
                phrase.push_back(' ');
            }
            phrase += contentTerms[j];
            if (j > i && phrase.size() <= 48) {
                addUniqueSeed(seeds, seen, phrase, "content_phrase", 0.62f);
            }
        }
    }

    return seeds;
}

void upsertByHash(std::unordered_map<std::string, ComponentResult>& byKey, ComponentResult item) {
    const std::string key = detail::makeFusionDedupKey(item, true);
    auto it = byKey.find(key);
    if (it == byKey.end() || item.score > it->second.score) {
        byKey[std::move(key)] = std::move(item);
    }
}

} // namespace

Result<std::vector<ComponentResult>>
YamsNativeCorpusAdapter::execute(const CorpusAdapterContext& context) const {
    std::vector<ComponentResult> results;
    if (!context.metadataRepo || context.query.empty() || context.limit == 0) {
        return results;
    }

    const auto start = std::chrono::steady_clock::now();
    std::unordered_map<std::string, ComponentResult> byKey;
    byKey.reserve(context.limit * 2);

    const auto pathSeeds = buildPathSeeds(context.query);
    const auto metadataFilters = parseMetadataFilters(context.query);
    std::size_t pathQueries = 0;

    auto collectPathMatches = [&]() -> void {
        for (const auto& seed : pathSeeds) {
            if (byKey.size() >= context.limit * 2) {
                break;
            }
            yams::metadata::DocumentQueryOptions options;
            options.containsFragment = seed.text;
            options.containsUsesFts = true;
            options.limit = static_cast<int>(std::max<std::size_t>(8, context.limit / 2));
            auto docs = context.metadataRepo->queryDocuments(options);
            ++pathQueries;
            if (!docs) {
                continue;
            }
            const std::string lowerSeed = lowerCopy(seed.text);
            for (std::size_t rank = 0; rank < docs.value().size(); ++rank) {
                const auto& doc = docs.value()[rank];
                if (doc.sha256Hash.empty() && doc.filePath.empty()) {
                    continue;
                }
                const std::string lowerPath = lowerCopy(doc.filePath);
                const auto pos = lowerPath.find(lowerSeed);
                float score = 0.48f * seed.weight;
                if (pos != std::string::npos && !lowerPath.empty()) {
                    const float position =
                        1.0f - (static_cast<float>(pos) / static_cast<float>(lowerPath.size()));
                    const float coverage =
                        static_cast<float>(std::min(lowerSeed.size(), lowerPath.size())) /
                        static_cast<float>(std::max<std::size_t>(1, lowerPath.size()));
                    score = std::clamp(seed.weight * (0.56f + position * 0.18f + coverage * 0.26f),
                                       0.0f, 1.0f);
                }
                ComponentResult item;
                item.documentHash = doc.sha256Hash;
                item.filePath = doc.filePath;
                item.score = score;
                item.source = ComponentResult::Source::CorpusAdapter;
                item.rank = rank;
                item.snippet = doc.filePath;
                item.debugInfo["corpus_adapter"] = name();
                item.debugInfo["adapter_signal"] = "path_seed";
                item.debugInfo["seed"] = seed.text;
                item.debugInfo["seed_kind"] = seed.kind;
                item.debugInfo["seed_count"] = std::to_string(pathSeeds.size());
                upsertByHash(byKey, std::move(item));
            }
        }
    };

    auto collectMetadataMatches = [&]() -> void {
        if (metadataFilters.empty()) {
            return;
        }
        yams::metadata::DocumentQueryOptions options;
        options.metadataFilters = metadataFilters;
        options.limit = static_cast<int>(context.limit);
        auto docs = context.metadataRepo->queryDocuments(options);
        if (!docs) {
            return;
        }
        for (std::size_t rank = 0; rank < docs.value().size(); ++rank) {
            const auto& doc = docs.value()[rank];
            ComponentResult item;
            item.documentHash = doc.sha256Hash;
            item.filePath = doc.filePath;
            item.score = std::clamp(0.88f + 0.03f * static_cast<float>(std::min<std::size_t>(
                                                        metadataFilters.size(), 4)),
                                    0.0f, 1.0f);
            item.source = ComponentResult::Source::CorpusAdapter;
            item.rank = rank;
            item.snippet = doc.filePath;
            item.debugInfo["corpus_adapter"] = name();
            item.debugInfo["adapter_signal"] = "metadata_filter";
            item.debugInfo["metadata_filter_count"] = std::to_string(metadataFilters.size());
            item.debugInfo["seed_count"] = std::to_string(pathSeeds.size());
            upsertByHash(byKey, std::move(item));
        }
    };

    collectPathMatches();
    collectMetadataMatches();

    results.reserve(std::min(context.limit, byKey.size()));
    for (auto& [_, item] : byKey) {
        results.push_back(std::move(item));
    }
    std::sort(results.begin(), results.end(),
              [](const ComponentResult& a, const ComponentResult& b) {
                  if (a.score != b.score) {
                      return a.score > b.score;
                  }
                  return a.rank < b.rank;
              });
    if (results.size() > context.limit) {
        results.resize(context.limit);
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();
    for (std::size_t i = 0; i < results.size(); ++i) {
        results[i].rank = i;
        results[i].debugInfo["adapter_us"] = std::to_string(elapsed);
        results[i].debugInfo["path_seed_queries"] = std::to_string(pathQueries);
    }
    return results;
}

} // namespace yams::search
