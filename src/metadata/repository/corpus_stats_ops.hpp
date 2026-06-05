// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

#include <yams/storage/corpus_stats.h>

namespace yams::metadata::repository {

enum class ExtensionBucket { Other, Code, Prose, Binary };

inline ExtensionBucket classifyExtensionBucket(std::string_view ext) {
    const std::string normalized{ext};
    if (storage::detail::kCodeExtensions.contains(normalized)) {
        return ExtensionBucket::Code;
    }
    if (storage::detail::kProseExtensions.contains(normalized)) {
        return ExtensionBucket::Prose;
    }
    if (storage::detail::kBinaryExtensions.contains(normalized)) {
        return ExtensionBucket::Binary;
    }
    return ExtensionBucket::Other;
}

struct ExtensionBucketCounts {
    uint64_t code{0};
    uint64_t prose{0};
    uint64_t binary{0};
};

inline ExtensionBucketCounts
calculateExtensionBucketCounts(const std::unordered_map<std::string, int64_t>& extensionCounts) {
    ExtensionBucketCounts counts;
    for (const auto& [ext, count] : extensionCounts) {
        const auto nonNegativeCount = static_cast<uint64_t>(std::max<int64_t>(count, 0));
        switch (classifyExtensionBucket(ext)) {
            case ExtensionBucket::Code:
                counts.code += nonNegativeCount;
                break;
            case ExtensionBucket::Prose:
                counts.prose += nonNegativeCount;
                break;
            case ExtensionBucket::Binary:
                counts.binary += nonNegativeCount;
                break;
            case ExtensionBucket::Other:
                break;
        }
    }
    return counts;
}

struct CorpusStatsOverlayInput {
    int64_t docCount{0};
    int64_t embeddedCount{0};
    int64_t totalSizeBytes{0};
    int64_t docsWithTags{0};
    int64_t tagCount{0};
    int64_t codeDocCount{0};
    int64_t proseDocCount{0};
    int64_t binaryDocCount{0};
    int64_t pathDepthSum{0};
    int64_t pathDepthMax{0};
    int64_t contentExtractedCount{0};
    int64_t ftsIndexedCount{0};
    int64_t symbolCount{0};
    int64_t nativeSymbolCount{0};
    int64_t nerEntityCount{0};
    int64_t kgEdgeCount{0};
    int64_t kgAliasCount{0};
    std::unordered_map<std::string, int64_t> extensionCounts;
};

inline int64_t currentEpochMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

inline void resetCorpusStatsForNoDocuments(storage::CorpusStats& stats) {
    stats.docCount = 0;
    stats.totalSizeBytes = 0;
    stats.embeddingCount = 0;
    stats.embeddingCoverage = 0.0;
    stats.docsWithTags = 0;
    stats.tagCount = 0;
    stats.symbolCount = 0;
    stats.nativeSymbolCount = 0;
    stats.nerEntityCount = 0;
    stats.codeRatio = 0.0;
    stats.proseRatio = 0.0;
    stats.binaryRatio = 0.0;
    stats.tagCoverage = 0.0;
    stats.symbolDensity = 0.0;
    stats.nativeSymbolDensity = 0.0;
    stats.nerEntityDensity = 0.0;
    stats.contentExtractedCount = 0;
    stats.contentExtractedCoverage = 0.0;
    stats.ftsIndexedCount = 0;
    stats.ftsIndexedCoverage = 0.0;
    stats.titleCount = 0;
    stats.titleCoverage = 0.0;
    stats.docsWithLanguage = 0;
    stats.languageCoverage = 0.0;
    stats.kgEdgeCount = 0;
    stats.kgEdgeDensity = 0.0;
    stats.kgAliasCount = 0;
    stats.kgAliasDensity = 0.0;
    stats.avgDocLengthBytes = 0.0;
    stats.pathDepthAvg = 0.0;
    stats.pathDepthMax = 0.0;
    stats.extensionCounts.clear();
}

inline storage::CorpusStats applyCorpusStatsOnlineOverlay(storage::CorpusStats stats,
                                                          const CorpusStatsOverlayInput& live) {
    const auto reconciledAtMs = stats.computedAtMs;
    const double reconciledMinDepth = stats.pathDepthAvg - stats.pathRelativeDepthAvg;

    if (live.docCount > 0) {
        stats.docCount = live.docCount;
        stats.totalSizeBytes = std::max<int64_t>(live.totalSizeBytes, 0);
        stats.embeddingCount = std::clamp<int64_t>(live.embeddedCount, 0, live.docCount);
        stats.embeddingCoverage =
            static_cast<double>(stats.embeddingCount) / static_cast<double>(stats.docCount);
        stats.docsWithTags = std::clamp<int64_t>(live.docsWithTags, 0, live.docCount);
        stats.tagCount = std::max<int64_t>(live.tagCount, 0);
        stats.symbolCount = std::max<int64_t>(live.symbolCount, 0);
        stats.nativeSymbolCount = std::max<int64_t>(live.nativeSymbolCount, 0);
        stats.nerEntityCount = std::max<int64_t>(live.nerEntityCount, 0);
        stats.tagCoverage =
            static_cast<double>(stats.docsWithTags) / static_cast<double>(stats.docCount);
        stats.codeRatio = static_cast<double>(std::max<int64_t>(live.codeDocCount, 0)) /
                          static_cast<double>(stats.docCount);
        stats.proseRatio = static_cast<double>(std::max<int64_t>(live.proseDocCount, 0)) /
                           static_cast<double>(stats.docCount);
        stats.binaryRatio = static_cast<double>(std::max<int64_t>(live.binaryDocCount, 0)) /
                            static_cast<double>(stats.docCount);
        stats.symbolDensity =
            static_cast<double>(stats.symbolCount) / static_cast<double>(stats.docCount);
        stats.nativeSymbolDensity =
            static_cast<double>(stats.nativeSymbolCount) / static_cast<double>(stats.docCount);
        stats.nerEntityDensity =
            static_cast<double>(stats.nerEntityCount) / static_cast<double>(stats.docCount);

        stats.kgEdgeCount = std::max<int64_t>(live.kgEdgeCount, 0);
        stats.kgEdgeDensity =
            static_cast<double>(stats.kgEdgeCount) / static_cast<double>(stats.docCount);
        stats.kgAliasCount = std::max<int64_t>(live.kgAliasCount, 0);
        stats.kgAliasDensity =
            static_cast<double>(stats.kgAliasCount) / static_cast<double>(stats.docCount);

        stats.contentExtractedCount = live.contentExtractedCount;
        stats.contentExtractedCoverage =
            static_cast<double>(stats.contentExtractedCount) / static_cast<double>(stats.docCount);
        stats.ftsIndexedCount = live.ftsIndexedCount;
        stats.ftsIndexedCoverage =
            static_cast<double>(stats.ftsIndexedCount) / static_cast<double>(stats.docCount);

        stats.pathDepthAvg = static_cast<double>(std::max<int64_t>(live.pathDepthSum, 0)) /
                             static_cast<double>(stats.docCount);
        stats.pathDepthMax = static_cast<double>(std::max<int64_t>(live.pathDepthMax, 0));
        // Carry the reconciled MIN(path_depth) forward. We can't track MIN as a single
        // atomic (delete of the current-min doc would need a scan), but any insert of a
        // path shallower than the reconciled min only lowers the true min, so clamping
        // the relative average at zero keeps it within a valid range.
        const double relativeDepth = stats.pathDepthAvg - reconciledMinDepth;
        stats.pathRelativeDepthAvg = relativeDepth > 0.0 ? relativeDepth : 0.0;
        if (stats.totalSizeBytes > 0) {
            stats.avgDocLengthBytes =
                static_cast<double>(stats.totalSizeBytes) / static_cast<double>(stats.docCount);
        }
        stats.extensionCounts = live.extensionCounts;
    } else {
        resetCorpusStatsForNoDocuments(stats);
    }

    stats.computedAtMs = currentEpochMillis();
    stats.usedOnlineOverlay = true;
    stats.reconciledComputedAtMs = reconciledAtMs;
    stats.pathDepthMaxApproximate = true;
    return stats;
}

} // namespace yams::metadata::repository
