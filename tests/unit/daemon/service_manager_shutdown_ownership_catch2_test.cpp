// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/ServiceManager.h>

using namespace yams::daemon;

TEST_CASE("ServiceManager: AbiHost released on shutdown and FSM stopped", "[daemon]") {
    DaemonConfig cfg;
    StateComponent state{};
    DaemonLifecycleFsm lifecycleFsm;

    ServiceManager sm(cfg, state, lifecycleFsm);

    // Abi host is created during construction when possible
    auto* hostBefore = sm.__test_getAbiHost();
    REQUIRE(hostBefore != nullptr);

    // Shutdown should release plugin hosts/loaders and mark FSM stopped
    sm.shutdown();

    auto* hostAfter = sm.__test_getAbiHost();
    CHECK(hostAfter == nullptr);

    auto fsnap = sm.getServiceManagerFsmSnapshot();
    CHECK((fsnap.state == ServiceManagerState::ShuttingDown ||
           fsnap.state == ServiceManagerState::Stopped));
}
