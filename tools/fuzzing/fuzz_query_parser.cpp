// SPDX-License-Identifier: GPL-3.0-or-later
// QueryParser Fuzzer - Targets user-facing search query parsing and translation

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>

#include <yams/search/query_parser.h>

using namespace yams::search;

namespace {
constexpr size_t kMaxQueryBytes = 4096;

static std::string bytesToQueryString(const uint8_t* data, size_t size) {
    if (!data || size == 0)
        return {};

    size_t n = std::min(size, kMaxQueryBytes);
    return std::string(reinterpret_cast<const char*>(data), n);
}

static QueryParserConfig configFromBytes(const uint8_t* data, size_t size) {
    QueryParserConfig cfg;
    if (!data || size == 0)
        return cfg;

    auto b = [&](size_t i, uint8_t def = 0) -> uint8_t { return (i < size) ? data[i] : def; };

    cfg.defaultOperator = (b(0) & 1) ? QueryParserConfig::DefaultOperator::Or
                                    : QueryParserConfig::DefaultOperator::And;
    cfg.allowLeadingWildcard = (b(1) & 1) != 0;
    cfg.enablePositionIncrements = (b(2) & 1) != 0;
    cfg.autoGeneratePhraseQueries = (b(3) & 1) != 0;

    cfg.phraseSlop = static_cast<int>(b(4) % 16); // keep small to avoid huge expansions
    cfg.fuzzyMaxExpansions = 1 + static_cast<int>(b(5) % 64);

    // 0.0 .. 1.0
    cfg.fuzzyMinSimilarity = static_cast<float>(b(6)) / 255.0f;

    // Keep defaultField/searchableFields stable (avoid unbounded memory growth).
    return cfg;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (!data || size == 0)
        return 0;

    QueryParser parser(configFromBytes(data, size));

    QueryParser::ParseOptions options;
    options.literalText = (data[0] & 2) != 0;

    std::string query = bytesToQueryString(data, size);
    if (query.empty())
        return 0;

    // Exercise: parse + validate + translate to FTS5 + optimize/analyze.
    {
        auto parsed = parser.parse(query, options);
        auto validation = parser.validate(query);
        (void)validation;

        if (parsed) {
            auto ast = std::move(parsed.value());

            // Convert to FTS5 (uses parser's currentOptions_).
            auto fts5 = parser.toFTS5Query(ast.get());
            (void)fts5;

            // Exercise optimizer/analyzer on the AST.
            QueryOptimizer optimizer;
            auto optimized = optimizer.optimize(std::move(ast));

            QueryAnalyzer analyzer;
            auto stats = analyzer.analyze(optimized.get());
            (void)stats;

            // Translate optimized form too.
            auto fts5_opt = parser.toFTS5Query(optimized.get());
            (void)fts5_opt;
        }
    }

    // Exercise: alternate parse mode on the same bytes (drives different escaping paths).
    {
        QueryParser::ParseOptions literal;
        literal.literalText = !options.literalText;
        auto parsed = parser.parse(query, literal);
        if (parsed) {
            auto fts5 = parser.toFTS5Query(parsed.value().get());
            (void)fts5;
        }
    }

    return 0;
}
