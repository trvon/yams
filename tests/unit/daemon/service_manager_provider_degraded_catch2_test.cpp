// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Minimal unit covering degraded transition when no provider can be adopted.
#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <optional>
#include <string>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/EmbeddingProviderFsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

using namespace yams::daemon;

namespace {

struct EnvGuard {
    std::string name;
    std::optional<std::string> originalValue;

    EnvGuard(const std::string& envName, const std::string& newValue) : name(envName) {
        if (const char* orig = std::getenv(name.c_str())) {
            originalValue = orig;
        }
#ifdef _WIN32
        _putenv_s(name.c_str(), newValue.c_str());
#else
        setenv(name.c_str(), newValue.c_str(), 1);
#endif
    }

    ~EnvGuard() {
        if (originalValue) {
#ifdef _WIN32
            _putenv_s(name.c_str(), originalValue->c_str());
#else
            setenv(name.c_str(), originalValue->c_str(), 1);
#endif
        } else {
#ifdef _WIN32
            _putenv_s(name.c_str(), "");
#else
            unsetenv(name.c_str());
#endif
        }
    }
};

} // namespace

TEST_CASE("ServiceManager: degrades when no provider available", "[daemon]") {
    EnvGuard backendGuard("YAMS_EMBED_BACKEND", "daemon");
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

TEST_CASE("Daemon lifecycle: Initializing -> Degraded when provider missing and not required",
          "[daemon][lifecycle][unit]") {
    EnvGuard backendGuard("YAMS_EMBED_BACKEND", "daemon");
    DaemonConfig cfg;
    cfg.enableModelProvider = true;
    cfg.modelProviderRequired = false;

    StateComponent state{};
    state.readiness.modelProviderReady.store(false, std::memory_order_release);

    DaemonLifecycleFsm lifecycleFsm;
    lifecycleFsm.dispatch(BootstrappedEvent{});
    REQUIRE(lifecycleFsm.snapshot().state == LifecycleState::Initializing);

    ServiceManager sm(cfg, state, lifecycleFsm);
    auto adopt = sm.adoptModelProviderFromHosts();
    REQUIRE(adopt.has_value());
    REQUIRE_FALSE(adopt.value());

    const bool providerExpected = cfg.enableModelProvider;
    const bool providerReady = state.readiness.modelProviderReady.load(std::memory_order_acquire);
    if (providerExpected && !providerReady) {
        lifecycleFsm.setSubsystemDegraded(
            "model_provider", true,
            "Model provider unavailable; embeddings disabled until provider recovery");
        lifecycleFsm.dispatch(DegradedEvent{});
    } else {
        lifecycleFsm.dispatch(HealthyEvent{});
    }

    auto snap = lifecycleFsm.snapshot();
    REQUIRE(snap.state == LifecycleState::Degraded);
    REQUIRE(lifecycleFsm.isSubsystemDegraded("model_provider"));
}
