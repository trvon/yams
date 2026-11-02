#pragma once

#include <algorithm>
#include <iterator>
#include <unordered_set>
#include <utility>
#include <yams/daemon/components/InternalEventBus.h>

#include "embedding_memory_accounting.h"

namespace yams::daemon::embed {

// Consume one dispatch prefix, leaving undispatched work in job. Monitor jobs remain
// indivisible: processEmbedJob owns their completion notification.
inline InternalEventBus::EmbedJob takeJobPrefix(InternalEventBus::EmbedJob& job,
                                                std::size_t docCap) {
    if (job.monitor) {
        return std::exchange(job, {});
    }
    const auto take = std::min(std::max<std::size_t>(1, docCap), job.hashes.size());
    InternalEventBus::EmbedJob split;
    split.modelName = job.modelName;
    split.skipExisting = job.skipExisting;
    split.updateSemanticGraph = job.updateSemanticGraph;
    split.hashes.assign(std::make_move_iterator(job.hashes.begin()),
                        std::make_move_iterator(job.hashes.begin() + take));
    job.hashes.erase(job.hashes.begin(), job.hashes.begin() + take);
    split.batchSize = static_cast<uint32_t>(split.hashes.size());
    job.batchSize = static_cast<uint32_t>(job.hashes.size());
    if (!job.preparedDocs.empty()) {
        const std::unordered_set<std::string> selected(split.hashes.begin(), split.hashes.end());
        auto remaining =
            std::stable_partition(job.preparedDocs.begin(), job.preparedDocs.end(),
                                  [&](const auto& doc) { return selected.count(doc.hash) == 0; });
        split.preparedDocs.assign(std::make_move_iterator(remaining),
                                  std::make_move_iterator(job.preparedDocs.end()));
        job.preparedDocs.erase(remaining, job.preparedDocs.end());
    }
    return split;
}

// Consume a prefix bounded by both a document count and the prepared payload bytes it will
// retain. At least one document is always taken so an oversized payload still makes progress
// instead of stalling the queue. Prepared documents are assumed to follow hash order, which the
// dispatch grouping in EmbeddingService maintains.
inline InternalEventBus::EmbedJob takeJobPrefix(InternalEventBus::EmbedJob& job, std::size_t docCap,
                                                std::size_t byteBudget) {
    if (job.monitor) {
        return std::exchange(job, {});
    }
    const std::size_t countCap = std::min(std::max<std::size_t>(1, docCap), job.hashes.size());
    std::size_t take = 0;
    std::size_t bytes = 0;
    std::size_t preparedIndex = 0;
    for (; take < countCap; ++take) {
        std::size_t docBytes = job.hashes[take].size();
        if (preparedIndex < job.preparedDocs.size() &&
            job.preparedDocs[preparedIndex].hash == job.hashes[take]) {
            docBytes = preparedPayloadBytes(std::span<const InternalEventBus::EmbedPreparedDoc>(
                &job.preparedDocs[preparedIndex], 1));
            ++preparedIndex;
        }
        if (take > 0 && byteBudget > 0 && bytes + docBytes > byteBudget) {
            break;
        }
        bytes += docBytes;
    }
    return takeJobPrefix(job, take);
}

} // namespace yams::daemon::embed
