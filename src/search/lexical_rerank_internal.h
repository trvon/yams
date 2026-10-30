// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace simeon {
class StaticEmbedding;
}

namespace yams::search::detail {

// Scores each document (by sha256 hash) against the query terms (higher is better, e.g. -bm25).
// Hashes absent from the returned map have no lexical match.
using LexicalRerankScorerFn = std::function<Result<std::unordered_map<std::string, double>>(
    const std::vector<std::string>& terms, const std::vector<std::string>& documentHashes)>;

// Scores each document (by sha256 hash) for semantic similarity to `query` (e.g. static-embedding
// cosine). Hashes absent from the returned map get a neutral score.
using SemanticScorerFn = std::function<Result<std::unordered_map<std::string, double>>(
    const std::string& query, const std::vector<std::string>& documentHashes)>;

struct LexicalRerankOutcome {
    enum class Status { Skipped, Failed, Applied };
    Status status = Status::Skipped;
    std::string skipReason;
    std::string errorMessage;
    std::size_t depth = 0;   // pool size scored
    std::size_t matched = 0; // pool documents with a lexical score
    bool semanticApplied = false;
    std::string semanticError;
};

// Lowercased [A-Za-z0-9]+ tokens longer than one character, minus English stopwords, sorted and
// deduplicated.
std::vector<std::string> lexicalRerankTerms(const std::string& query);

// Reranks the fused top results by a z-score blend. Lexical only (no semantic scorer, or it
// fails, or staticRerankDepth is 0): top-`lexicalRerankDepth`,
// (1 - w) * z(fused) + w * z(lexical) with w = lexicalRerankWeight. With semantic scores:
// top-`staticRerankDepth`, (1 - wl - ws) * z(fused) + wl * z(lexical) + ws * z(semantic) with
// wl = staticRerankLexicalWeight and ws = staticRerankWeight. The pool is rescored onto its
// original score range, kept above every tail score; the tail is untouched.
// Cosine between the static embedding of `query` and of each document's first `maxChars`
// characters of content (one preview query). Empty when the query has no known token.
Result<std::unordered_map<std::string, double>>
staticEmbeddingScores(const simeon::StaticEmbedding& table, metadata::MetadataRepository& repo,
                      const std::string& query, const std::vector<std::string>& documentHashes,
                      int maxChars = 1500);

LexicalRerankOutcome applyLexicalDeepRerank(std::vector<SearchResult>& results,
                                            const std::string& query,
                                            const SearchEngineConfig& config,
                                            const LexicalRerankScorerFn& scorer,
                                            const SemanticScorerFn& semanticScorer = {});

} // namespace yams::search::detail
