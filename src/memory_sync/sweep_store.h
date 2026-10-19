// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <yams/core/types.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace yams::memory_sync::detail {

/// Disposable, private staging for one complete memory-sync sweep.
///
/// One loop/service owns an instance and serializes all calls. Statements are
/// prepared per call so a streaming visitor may perform nested read lookups on
/// the same SQLite connection without sharing mutable statement state.
class SweepStore final {
public:
    using Fingerprint = std::pair<std::string, std::string>;
    using Check = std::function<Result<void>()>;
    using Visitor = std::function<Result<void>(
        std::string_view indexKey, std::string_view logicalKey, std::string_view recordHash,
        std::span<const std::byte> encoded)>;

    static Result<std::shared_ptr<SweepStore>> create(std::size_t inputBudget, Check check);

    Result<void> observeOperation(std::string_view operationId, std::string_view recordHash,
                                  std::string_view logicalKey);
    Result<void> observeVector(std::string_view identity, std::string_view recordHash,
                               std::string_view operationId);
    Result<void> markFork(std::string_view operationId);
    Result<bool> forked(std::string_view operationId);
    Result<std::optional<Fingerprint>> operation(std::string_view operationId);
    Result<void> addCandidate(std::string_view indexKey, std::string_view logicalKey,
                              std::string_view recordHash, std::string_view writer,
                              std::uint64_t counter, std::span<const std::byte> encoded);
    Result<void> forEach(bool historyOrder, const Visitor& visitor);

    ~SweepStore();

    SweepStore(const SweepStore&) = delete;
    SweepStore& operator=(const SweepStore&) = delete;
    SweepStore(SweepStore&&) = delete;
    SweepStore& operator=(SweepStore&&) = delete;

private:
    struct Impl;
    explicit SweepStore(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;
};

} // namespace yams::memory_sync::detail
