// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Helpers shared by more than one repair operation.
#pragma once

#include <yams/api/content_store.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/metadata/document_metadata.h>

#include <spdlog/spdlog.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace yams::daemon::repair {

constexpr size_t kMaxTextToPersistInMetadataBytes =
    16ULL * 1024ULL * 1024ULL; // 16 MiB (best-effort)

template <typename Meta>
inline void submitRepairStatusUpdate(const RepairServiceContext& ctx, Meta& metaRepo,
                                     std::vector<std::string> hashes,
                                     yams::metadata::RepairStatus status, std::string_view source) {
    if (hashes.empty())
        return;
    if (auto* coord = ctx.getWriteCoordinator ? ctx.getWriteCoordinator() : nullptr) {
        auto wb = std::make_unique<WriteBatch>();
        wb->source.assign(source.data(), source.size());
        wb->ops.emplace_back(UpdateRepairStatusOp{hashes, status});
        coord->enqueue(std::unique_ptr<WriteBatch>(wb.release()));
        return;
    }
    if (metaRepo) {
        (void)metaRepo->batchUpdateDocumentRepairStatuses(hashes, status);
    }
}

// Template helper: queue job with exponential backoff when full
template <typename JobT>
boost::asio::awaitable<bool>
queueWithBackoff(std::shared_ptr<SpscQueue<JobT>> queue, JobT&& job,
                 boost::asio::steady_timer& timer, std::atomic<bool>& running,
                 const std::string& jobTypeName, size_t jobSize, int initialDelayMs = 50,
                 int maxDelayMs = 1000, std::function<bool()> shouldStop = {},
                 std::function<void()> onQueueFull = {}) {
    const auto queueStart = std::chrono::steady_clock::now();
    uint64_t totalWaitMs = 0;
    int retries = 0;
    while (!queue->try_push(std::forward<JobT>(job))) {
        if (!running.load(std::memory_order_relaxed) || (shouldStop && shouldStop()))
            co_return false;
        if (retries == 0 && onQueueFull)
            onQueueFull();
        const int cappedRetries = std::min(retries, 5);
        const int delayMs = std::min(initialDelayMs * (1 << cappedRetries), maxDelayMs);
        totalWaitMs += static_cast<uint64_t>(delayMs);
        const auto queued = queue ? queue->size_approx() : 0;
        const auto capacity = queue ? queue->capacity() : 0;
        spdlog::warn("RepairService: {} queue full, retry={} sleep_ms={} queued={} capacity={} "
                     "batch_size={}",
                     jobTypeName, retries + 1, delayMs, queued, capacity, jobSize);
        timer.expires_after(std::chrono::milliseconds(delayMs));
        co_await timer.async_wait(boost::asio::use_awaitable);
        retries++;
    }
    if (retries > 0) {
        const auto pushLatencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::steady_clock::now() - queueStart)
                                       .count();
        spdlog::info("RepairService: queued {} job after {} retries (batch_size={} wait_ms={} "
                     "push_latency_ms={})",
                     jobTypeName, retries, jobSize, totalWaitMs, pushLatencyMs);
    }
    co_return true;
}

/// Lower-cased, dot-prefixed extension from the document's extension or file name.
std::string normalizedRepairExtension(const metadata::DocumentInfo& doc);

/// True when the stored MIME type is missing, generic, or disagrees with the extension.
bool shouldRedetectMime(const metadata::DocumentInfo& doc);

/// Sniff the document's MIME type from its bytes (magic numbers), falling back to the
/// extension hint and finally application/octet-stream.
std::string bestEffortMimeForDocument(const metadata::DocumentInfo& doc,
                                      const std::shared_ptr<api::IContentStore>& store);

} // namespace yams::daemon::repair
