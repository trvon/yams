// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/daemon/components/RepairServiceHost.h>

#include <yams/daemon/components/RepairService.h>

#include <spdlog/spdlog.h>

#include <utility>

namespace yams::daemon {

RepairServiceHost::RepairServiceHost() = default;

RepairServiceHost::~RepairServiceHost() {
    stop();
}

void RepairServiceHost::start(Config cfg, StateComponent* state,
                              std::function<std::size_t()> activeConn, RepairServiceContext ctx) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (std::atomic_load_explicit(&service_, std::memory_order_acquire)) {
        spdlog::debug("[RepairServiceHost] already started");
        return;
    }
    RepairService::Config rcfg;
    rcfg.enable = cfg.enable;
    rcfg.dataDir = std::move(cfg.dataDir);
    rcfg.maxBatch = cfg.maxBatch;
    rcfg.autoRebuildOnDimMismatch = cfg.autoRebuildOnDimMismatch;
    rcfg.maxPendingRepairs = cfg.maxPendingRepairs;
    auto rs = std::make_shared<RepairService>(std::move(ctx), state, std::move(activeConn), rcfg);
    std::atomic_store_explicit(&service_, rs, std::memory_order_release);
    rs->start();
    spdlog::info("[RepairServiceHost] RepairService started");
}

void RepairServiceHost::stop() {
    std::shared_ptr<RepairService> rs;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        rs = std::atomic_load_explicit(&service_, std::memory_order_acquire);
        if (!rs) {
            return;
        }
        std::atomic_store_explicit(&service_, std::shared_ptr<RepairService>{},
                                   std::memory_order_release);
    }
    rs->stop();
    spdlog::info("[RepairServiceHost] RepairService stopped");
}

} // namespace yams::daemon
