// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

// Validates degraded->ready recovery via ServiceManager's FSM helper.
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/ServiceManager.h>

using namespace yams::daemon;

TEST_CASE("EmbeddingProviderRecovery: degraded then recovered to ready",
          "[daemon][.linux_only]") {
#ifdef _WIN32
    // Skip on Windows: EmbeddingProviderFsm state machine behavior differs on Windows due to
    // ServiceManager initialization sequences not being fully exercised without unix domain sockets
    SKIP("EmbeddingProvider FSM tests skipped on Windows - see daemon IPC plan");
#endif
    DaemonConfig cfg; // defaults are fine; no plugins needed
    StateComponent state{};
    DaemonLifecycleFsm lifecycleFsm;

    ServiceManager sm(cfg, state, lifecycleFsm);

    // Simulate degraded provider
    sm.__test_setModelProviderDegraded(true, "unit-test degraded");
    auto s1 = sm.getEmbeddingProviderFsmSnapshot();
    CHECK(s1.state == EmbeddingProviderState::Degraded);
    CHECK(s1.lastError.find("degraded") != std::string::npos);

    // Simulate recovery (helper dispatches ModelLoadedEvent with dimension 0)
    sm.__test_setModelProviderDegraded(false, "");
    auto s2 = sm.getEmbeddingProviderFsmSnapshot();
    CHECK(s2.state == EmbeddingProviderState::ModelReady);
    CHECK(s2.embeddingDimension == 0u);
}
