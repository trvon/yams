// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <functional>
#include <string>
#include <string_view>

#include "search_pipeline.h"

namespace yams::search {

class ClassicSearchPipeline final : public SearchPipeline {
public:
    using ExecuteFn =
        std::function<Result<SearchResponse>(const std::string& query, const SearchParams& params)>;

    explicit ClassicSearchPipeline(ExecuteFn executeClassic);

    [[nodiscard]] std::string_view name() const noexcept override;

    Result<SearchResponse> execute(std::string_view query, const SearchParams& params,
                                   const SearchEngineConfig& config,
                                   SearchPipelineContext& context) override;

private:
    ExecuteFn executeClassic_;
};

} // namespace yams::search
