// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Minimal unit covering degraded transition when no provider can be adopted.
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

using namespace yams::daemon;

TEST_CASE("ServiceManager: degrades when no provider available", "[daemon]") {
    DaemonConfig cfg;                // defaults: autoLoadPlugins=false; no pluginDir
    StateComponent state{};          // empty readiness snapshot
    DaemonLifecycleFsm lifecycleFsm; // lifecycle FSM for degradation tracking

    ServiceManager sm(cfg, state, lifecycleFsm);

    auto res = sm.adoptModelProviderFromHosts();
    REQUIRE(res.has_value());
    CHECK_FALSE(res.value());

    auto snap = sm.getEmbeddingProviderFsmSnapshot();
    // Expect degraded since we explicitly dispatch when adoption fails.
    CHECK(snap.state == EmbeddingProviderState::Degraded);
}
