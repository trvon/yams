// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <atomic>
#include <cstdint>

namespace yams::search {

struct SearchEngineStatistics {
    std::atomic<uint64_t> totalQueries{0};
    std::atomic<uint64_t> successfulQueries{0};
    std::atomic<uint64_t> failedQueries{0};
    std::atomic<uint64_t> timedOutQueries{0};
    std::atomic<uint64_t> textQueries{0};
    std::atomic<uint64_t> pathTreeQueries{0};
    std::atomic<uint64_t> kgQueries{0};
    std::atomic<uint64_t> vectorQueries{0};
    std::atomic<uint64_t> entityVectorQueries{0};
    std::atomic<uint64_t> tagQueries{0};
    std::atomic<uint64_t> metadataQueries{0};
    std::atomic<uint64_t> totalQueryTimeMicros{0};
    std::atomic<uint64_t> avgQueryTimeMicros{0};
    std::atomic<uint64_t> avgTextTimeMicros{0};
    std::atomic<uint64_t> avgPathTreeTimeMicros{0};
    std::atomic<uint64_t> avgKgTimeMicros{0};
    std::atomic<uint64_t> avgVectorTimeMicros{0};
    std::atomic<uint64_t> avgEntityVectorTimeMicros{0};
    std::atomic<uint64_t> avgTagTimeMicros{0};
    std::atomic<uint64_t> avgMetadataTimeMicros{0};
    std::atomic<uint64_t> avgResultsPerQuery{0};
    std::atomic<uint64_t> avgComponentsPerResult{0};
    std::atomic<uint64_t> simeonLexicalCacheHits{0};
    std::atomic<uint64_t> simeonLexicalScoreMicros{0};
    std::atomic<uint64_t> simeonLexicalScoreCalls{0};
};

} // namespace yams::search
