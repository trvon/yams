// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include "../../include/yams/search/classic_search_pipeline.h"

namespace yams::search {

ClassicSearchPipeline::ClassicSearchPipeline(ExecuteFn executeClassic)
    : executeClassic_(executeClassic) {}

std::string_view ClassicSearchPipeline::name() const noexcept {
    return "classic";
}

Result<SearchResponse> ClassicSearchPipeline::execute(std::string_view query,
                                                      const SearchParams& params,
                                                      const SearchEngineConfig& config,
                                                      SearchPipelineContext& context) {
    (void)config;
    (void)context;
    return executeClassic_(std::string(query), params);
}

} // namespace yams::search
