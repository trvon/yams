// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::search::detail {

// Scores each document (by sha256 hash) against the query terms (higher is better, e.g. -bm25).
// Hashes absent from the returned map have no lexical match.
using LexicalRerankScorerFn = std::function<Result<std::unordered_map<std::string, double>>(
    const std::vector<std::string>& terms, const std::vector<std::string>& documentHashes)>;

struct LexicalRerankOutcome {
    enum class Status { Skipped, Failed, Applied };
    Status status = Status::Skipped;
    std::string skipReason;
    std::string errorMessage;
    std::size_t depth = 0;   // pool size scored
    std::size_t matched = 0; // pool documents with a lexical score
};

// Lowercased [A-Za-z0-9]+ tokens longer than one character, minus English stopwords, sorted and
// deduplicated.
std::vector<std::string> lexicalRerankTerms(const std::string& query);

// Reranks the fused top-`lexicalRerankDepth` by a z-score blend of the fused score and a
// full-document lexical score: (1 - w) * z(fused) + w * z(lexical). The pool is rescored onto its
// original score range, kept above every tail score; the tail is untouched.
LexicalRerankOutcome applyLexicalDeepRerank(std::vector<SearchResult>& results,
                                            const std::string& query,
                                            const SearchEngineConfig& config,
                                            const LexicalRerankScorerFn& scorer);

} // namespace yams::search::detail
