// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/config/config_helpers.h>
#include <yams/daemon/components/TuneAdvisor.h>

#include <future>
#include <memory>
#include <optional>
#include <string>

namespace {

class EnvironmentRestore {
public:
    explicit EnvironmentRestore(const char* name)
        : name_(name), before_(yams::config::getenv_optional(name)) {}

    ~EnvironmentRestore() {
        (void)yams::config::set_environment(name_, before_ ? before_->c_str() : nullptr);
    }

private:
    const char* name_;
    std::optional<std::string> before_;
};

TEST_CASE("TuneAdvisor lifecycle initialization blocks followers and transfers on failure",
          "[daemon][tuning][lifecycle][ownership]") {
    using Lease = yams::daemon::TuneAdvisor::ConfiguredOverrideLifecycleLease;
    using namespace std::chrono_literals;

    auto primary = std::make_unique<Lease>();
    REQUIRE(primary->ownsInitialization());
    auto follower = std::async(std::launch::async, [] {
        Lease lease;
        const bool owns = lease.ownsInitialization();
        lease.commitInitialization();
        return owns;
    });
    CHECK((follower.wait_for(25ms) == std::future_status::timeout));
    primary->commitInitialization();
    CHECK_FALSE(follower.get());
    primary.reset();

    auto failedPrimary = std::make_unique<Lease>();
    REQUIRE(failedPrimary->ownsInitialization());
    auto successor = std::async(std::launch::async, [] {
        Lease lease;
        const bool owns = lease.ownsInitialization();
        lease.commitInitialization();
        return owns;
    });
    CHECK((successor.wait_for(25ms) == std::future_status::timeout));
    failedPrimary.reset();
    CHECK(successor.get());
}

TEST_CASE("TuneAdvisor refreshes compatibility parsing between production lifecycles",
          "[daemon][tuning][environment][snapshot][lifecycle]") {
    EnvironmentRestore cpuRestore{"YAMS_CPU_HIGH_PCT"};
    REQUIRE(yams::config::set_environment("YAMS_CPU_HIGH_PCT", "61"));
    yams::daemon::TuneAdvisor::refreshCompatibilityEnvironmentSnapshot();
    CHECK((yams::daemon::TuneAdvisor::cpuHighThresholdPercent() == 61.0));

    REQUIRE(yams::config::set_environment("YAMS_CPU_HIGH_PCT", "73"));
    yams::daemon::TuneAdvisor::refreshCompatibilityEnvironmentSnapshot();
    CHECK((yams::daemon::TuneAdvisor::cpuHighThresholdPercent() == 73.0));
}

TEST_CASE("TuneAdvisor snapshots compatibility environment in production",
          "[daemon][tuning][environment][snapshot]") {
    EnvironmentRestore timeoutRestore{"YAMS_IPC_TIMEOUT_MS"};
    EnvironmentRestore streamRestore{"YAMS_STREAM_CHUNK_TIMEOUT_MS"};
    REQUIRE(yams::config::set_environment("YAMS_IPC_TIMEOUT_MS", "4321"));
    REQUIRE(yams::config::set_environment("YAMS_STREAM_CHUNK_TIMEOUT_MS", "5432"));
    yams::daemon::TuneAdvisor::refreshCompatibilityEnvironmentSnapshot();

    CHECK((yams::daemon::TuneAdvisor::ipcTimeoutMs() == 4321u));
    REQUIRE(yams::config::set_environment("YAMS_IPC_TIMEOUT_MS", "9876"));
    REQUIRE(yams::config::set_environment("YAMS_STREAM_CHUNK_TIMEOUT_MS", "8765"));
    CHECK((yams::daemon::TuneAdvisor::ipcTimeoutMs() == 4321u));
    CHECK((yams::daemon::TuneAdvisor::streamChunkTimeoutMs() == 5432u));
}

} // namespace
