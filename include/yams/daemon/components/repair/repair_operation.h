// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/daemon/components/RepairService.h>

#include <boost/asio/awaitable.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace yams::daemon::repair {

/// Everything an operation may touch. Operations are stateless; all mutable service state
/// they need is reached through this bundle, which RepairService builds per call.
struct OperationEnv {
    const RepairServiceContext& ctx;
    const RepairService::Config& cfg;
    StateComponent* state;
    std::atomic<bool>& running;
};

/// One on-demand repair operation. `name()` is the canonical identifier published in
/// RepairEvent/RepairOperationResult and equals metrics::repairOperationNameForCode(code()).
class IRepairOperation {
public:
    virtual ~IRepairOperation() = default;

    virtual std::string_view name() const noexcept = 0;
    virtual std::uint64_t code() const noexcept = 0;

    virtual RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                                      const RepairService::ProgressFn& progress,
                                      std::atomic<bool>* cancelRequested) = 0;

    /// Background variant used for non-foreground requests. Operations with long blocking
    /// phases override it to hop onto the repair thread pool; the default just calls run().
    virtual boost::asio::awaitable<RepairOperationResult>
    runAsync(OperationEnv& env, const RepairRequest& req, const RepairService::ProgressFn& progress,
             std::atomic<bool>* cancelRequested);
};

/// All operations, in wire-code order (1..13). Built once; safe to share across threads.
const std::vector<std::unique_ptr<IRepairOperation>>& repairOperations();

/// nullptr for an unknown code.
IRepairOperation* repairOperationForCode(std::uint64_t code) noexcept;

} // namespace yams::daemon::repair
