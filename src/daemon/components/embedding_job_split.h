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

// Prepared payload bytes per document hash, built once per dispatch bucket. Order and
// uniqueness of preparedDocs are not required: a hash without a payload counts as its own
// length, and duplicate payloads are summed because takeJobPrefix moves every matching entry.
using PreparedPayloadIndex = std::unordered_map<std::string, std::size_t>;

inline PreparedPayloadIndex preparedPayloadIndex(const InternalEventBus::EmbedJob& job) {
    PreparedPayloadIndex index;
    index.reserve(job.preparedDocs.size());
    for (const auto& doc : job.preparedDocs) {
        const auto bytes =
            preparedPayloadBytes(std::span<const InternalEventBus::EmbedPreparedDoc>(&doc, 1));
        auto [it, inserted] = index.emplace(doc.hash, bytes);
        if (!inserted) {
            it->second = saturatingAdd(it->second, bytes);
        }
    }
    return index;
}

// Consume a prefix bounded by both a document count and the prepared payload bytes it will
// retain. At least one document is always taken so an oversized payload still makes progress
// instead of stalling the queue.
inline InternalEventBus::EmbedJob takeJobPrefix(InternalEventBus::EmbedJob& job, std::size_t docCap,
                                                std::size_t byteBudget,
                                                const PreparedPayloadIndex& index) {
    if (job.monitor) {
        return std::exchange(job, {});
    }
    const std::size_t countCap = std::min(std::max<std::size_t>(1, docCap), job.hashes.size());
    std::size_t take = 0;
    std::size_t bytes = 0;
    for (; take < countCap; ++take) {
        const auto& hash = job.hashes[take];
        auto it = index.find(hash);
        const std::size_t docBytes = it == index.end() ? hash.size() : it->second;
        if (take > 0 && byteBudget > 0 && bytes + docBytes > byteBudget) {
            break;
        }
        bytes += docBytes;
    }
    return takeJobPrefix(job, take);
}

inline InternalEventBus::EmbedJob takeJobPrefix(InternalEventBus::EmbedJob& job, std::size_t docCap,
                                                std::size_t byteBudget) {
    return takeJobPrefix(job, docCap, byteBudget, preparedPayloadIndex(job));
}

} // namespace yams::daemon::embed
