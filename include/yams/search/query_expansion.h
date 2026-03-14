// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include <yams/search/query_concept_extractor.h>
#include <yams/search/query_text_utils.h>

namespace yams::search {

struct FtsFallbackClause {
    std::string query;
    float penalty = 1.0f;
};

float tokenFallbackSalience(const QueryToken& token);

std::vector<std::string>
generateAnchoredSubPhrases(const std::string& query, size_t maxPhrases,
                           const std::unordered_map<std::string, float>* idfByToken);

std::vector<QueryConcept>
generateFallbackQueryConcepts(const std::string& query,
                              const std::unordered_map<std::string, float>& idfByToken,
                              size_t maxConcepts);

std::vector<FtsFallbackClause>
generateAggressiveFtsFallbackClauses(const std::string& query, size_t maxClauses,
                                     const std::unordered_map<std::string, float>& idfByToken);

} // namespace yams::search
