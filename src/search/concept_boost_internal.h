// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/search/query_concept_extractor.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>

#include <vector>

namespace yams::search::detail {

// Post-fusion concept boost: reweights results whose snippet/filename contains query-concept terms,
// bounded by config.conceptMaxBoost, then re-sorts. No-op when disabled or inputs empty.
void applyConceptBoost(std::vector<SearchResult>& results,
                       const std::vector<QueryConcept>& concepts, const SearchEngineConfig& config);

} // namespace yams::search::detail
