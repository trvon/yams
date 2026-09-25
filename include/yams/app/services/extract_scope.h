// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/app/services/services.hpp>
#include <yams/core/types.h>
#include <yams/extraction/format_handlers/format_handler.hpp>
#include <yams/extraction/format_handlers/text_basic_handler.hpp>
#include <yams/search/query_qualifiers.hpp>

#include <cstddef>
#include <functional>
#include <string>
#include <vector>

namespace yams::app::services {

/// What applying a query's extraction scope did to a result list.
struct ExtractScopeOutcome {
    std::size_t shaped{0};   // results whose snippet now holds the requested scope
    std::size_t unshaped{0}; // results left as they were (no handler, too large, unreadable)
    std::string note;        // one line for queryInfo / searchStats; empty when nothing to say
};

/// Reads a document's stored bytes by content hash.
using ExtractScopeReader = std::function<Result<std::vector<std::byte>>(const std::string& hash)>;

/// Apply the `lines:` / `pages:` / `section:` / `selector:` qualifier parsed from a search query.
///
/// `lines:<range>` replaces each result's snippet with those lines (1-based, `"1-3,10"`) of its
/// stored content, through the same text handler `get` uses. A result without a line-range
/// handler (binary formats), whose content exceeds `maxBytes`, or that cannot be read keeps its
/// snippet and is counted as unshaped. `pages:`, `section:` and `selector:` have no search-time
/// handler: they are reported in the note rather than silently ignored.
inline ExtractScopeOutcome applyExtractScope(std::vector<SearchItem>& results,
                                             const search::ExtractScope& scope,
                                             const ExtractScopeReader& read,
                                             std::size_t maxBytes = std::size_t{1} << 20) {
    using search::ExtractScopeType;
    ExtractScopeOutcome outcome;
    switch (scope.type) {
        case ExtractScopeType::All:
            return outcome;
        case ExtractScopeType::Pages:
            outcome.note = "pages:" + scope.range + " is not applied to search results";
            return outcome;
        case ExtractScopeType::Section:
            outcome.note = "section:" + scope.section + " is not applied to search results";
            return outcome;
        case ExtractScopeType::Selector:
            outcome.note = "selector:" + scope.selector + " is not applied to search results";
            return outcome;
        case ExtractScopeType::Lines:
            break;
    }
    if (scope.range.empty()) {
        outcome.unshaped = results.size();
        outcome.note = "lines: needs a range such as lines:1-20; not applied";
        return outcome;
    }

    extraction::format::HandlerRegistry registry;
    extraction::format::registerTextBasicHandler(registry);
    extraction::format::ExtractionQuery query;
    query.scope = extraction::format::Scope::Range;
    query.range = scope.range;

    std::string invalidRange;
    for (auto& item : results) {
        if (item.hash.empty() || !read) {
            ++outcome.unshaped;
            continue;
        }
        const auto dot = item.path.find_last_of('.');
        const std::string ext = dot == std::string::npos ? std::string{} : item.path.substr(dot);
        if (!registry.selectBest(item.mimeType, ext)) {
            ++outcome.unshaped;
            continue;
        }
        auto bytes = read(item.hash);
        if (!bytes || bytes.value().size() > maxBytes) {
            ++outcome.unshaped;
            continue;
        }
        auto extracted = registry.extract(item.mimeType, ext, bytes.value(), query);
        if (!extracted) {
            if (extracted.error().code == ErrorCode::InvalidArgument) {
                invalidRange = extracted.error().message;
            }
            ++outcome.unshaped;
            continue;
        }
        if (!extracted.value().text) {
            ++outcome.unshaped;
            continue;
        }
        item.snippet = *extracted.value().text;
        ++outcome.shaped;
    }

    if (!invalidRange.empty()) {
        outcome.note = "lines:" + scope.range + " is an invalid range; not applied";
        return outcome;
    }
    outcome.note = "lines:" + scope.range + " applied to " + std::to_string(outcome.shaped) +
                   " of " + std::to_string(results.size()) + " results";
    if (outcome.unshaped > 0) {
        outcome.note += " (" + std::to_string(outcome.unshaped) +
                        " without line-range text, too large, or unreadable)";
    }
    return outcome;
}

} // namespace yams::app::services
