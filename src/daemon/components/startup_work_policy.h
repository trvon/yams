// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <yams/daemon/components/ServiceManagerFsm.h>

namespace yams::daemon::embed {

// Shared by the real poller and startup replay/fuzz tests. Pressure admission is a
// separate, subsequent decision: elapsed deferral must not mint missing readiness.
constexpr bool startupMaintenanceEligible(ServiceManagerState state, bool stopping,
                                          bool idle) noexcept {
    return state == ServiceManagerState::Ready && !stopping && idle;
}

} // namespace yams::daemon::embed
