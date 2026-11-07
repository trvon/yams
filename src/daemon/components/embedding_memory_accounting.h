// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <span>
#include <string>
#include <vector>
#include <yams/daemon/components/InternalEventBus.h>

namespace yams::daemon::embed {

// Host-side retained-byte accounting for one embedding job. These are the buffers this process
// owns, not the provider's internal workspace: provider allocations happen behind the model ABI
// and cannot be measured from here. Counting them separately keeps a missing bound honest.

inline std::size_t saturatingAdd(std::size_t total, std::size_t add) noexcept {
    const auto max = std::numeric_limits<std::size_t>::max();
    return add > max - total ? max : total + add;
}

inline std::size_t saturatingMul(std::size_t a, std::size_t b) noexcept {
    if (a == 0 || b == 0) {
        return 0;
    }
    const auto max = std::numeric_limits<std::size_t>::max();
    return a > max / b ? max : a * b;
}

/// Selected chunk text and ids handed over by post-ingest preparation, which is the payload that
/// sits in the channel and pending job list while the consumer is busy.
inline std::size_t
preparedPayloadBytes(std::span<const InternalEventBus::EmbedPreparedDoc> preparedDocs) noexcept {
    std::size_t total = 0;
    for (const auto& doc : preparedDocs) {
        total = saturatingAdd(total, doc.hash.size());
        total = saturatingAdd(total, doc.sourceTextHash.size());
        total = saturatingAdd(total, doc.preparationRecipe.size());
        for (const auto& chunk : doc.chunks) {
            total = saturatingAdd(total, chunk.chunkId.size());
            total = saturatingAdd(total, chunk.content.size());
        }
    }
    return total;
}

/// Chunk text and ids built inside one job before inference. Works for prepared chunks and for
/// the job-local chunk struct, which share the chunkId/content shape.
template <typename ChunkRange>
inline std::size_t chunkPayloadBytes(const ChunkRange& chunks) noexcept {
    std::size_t total = 0;
    for (const auto& chunk : chunks) {
        total = saturatingAdd(total, chunk.chunkId.size());
        total = saturatingAdd(total, chunk.content.size());
    }
    return total;
}

/// Provider output copy the host materialises per inference sub-batch: the returned float matrix
/// plus the host vector-of-vectors it is copied into.
inline std::size_t inferenceOutputBytes(std::size_t batchItems, std::size_t dimension) noexcept {
    return saturatingMul(saturatingMul(batchItems, dimension), 2 * sizeof(float));
}

} // namespace yams::daemon::embed
