// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>

namespace yams::daemon {

class RepairService;
struct RepairServiceContext;
struct StateComponent;

/**
 * @brief Owns the RepairService lifecycle (start/stop + shared_ptr holder).
 *
 * Extracted from ServiceManager (PBI-088 Phase 6c). The Host is a thin
 * lifecycle container — ServiceManager composes one and forwards the
 * public API through it. This removes `repairService_` and
 * `repairServiceMutex_` from ServiceManager's member set.
 *
 * ## Thread Safety
 * - start()/stop() are idempotent and safe under concurrent invocation
 * - get() is a lock-free atomic load
 */
class RepairServiceHost {
public:
    struct Config {
        bool enable{true};
        std::filesystem::path dataDir;
        std::uint32_t maxBatch{0};
        bool autoRebuildOnDimMismatch{false};
    };

    RepairServiceHost();
    ~RepairServiceHost();

    RepairServiceHost(const RepairServiceHost&) = delete;
    RepairServiceHost& operator=(const RepairServiceHost&) = delete;

    /**
     * @brief Construct and start the RepairService (idempotent).
     *
     * @param cfg        Service configuration
     * @param state      State component for readiness tracking
     * @param activeConn Callback returning the current active connection count
     * @param ctx        Dependency accessors for the service
     */
    void start(Config cfg, StateComponent* state, std::function<std::size_t()> activeConn,
               RepairServiceContext ctx);

    /**
     * @brief Stop and release the RepairService (idempotent).
     */
    void stop();

    /**
     * @brief Access the current RepairService shared_ptr (may be null).
     */
    std::shared_ptr<RepairService> get() const {
        return std::atomic_load_explicit(&service_, std::memory_order_acquire);
    }

private:
    std::shared_ptr<RepairService> service_;
    mutable std::mutex mutex_;
};

} // namespace yams::daemon
