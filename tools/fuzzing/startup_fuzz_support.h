// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <span>

#include <yams/daemon/components/HostPressure.h>
#include <yams/daemon/components/ServiceManagerFsm.h>

#include "../../src/daemon/components/semantic_graph_candidate_policy.h"
#include "../../src/daemon/components/startup_work_policy.h"

namespace yams::fuzz {

// Stable byte vocabulary: a saved input is replayable by Catch2 and libFuzzer/AFL.
// Only contract-valid FSM dispatches are generated; this is not a thread scheduler
// or a model of arbitrary late callbacks. One replay owns the process-global tinyfsm.
enum class StartupAction : std::uint8_t {
    Advance,
    Fail,
    Shutdown,
    FinishShutdown,
    Idle,
    Busy,
    Pressure,
    Calm,
    Tick,
    Deadline,
    Probe,
    Cancel,
    Reset,
    Count
};

struct StartupReplayResult {
    const char* error{nullptr};
    std::size_t offset{0};
    std::uint32_t statesVisited{0};
    std::size_t admissions{0};
    std::size_t forcedAdmissions{0};
    [[nodiscard]] bool ok() const noexcept { return error == nullptr; }
};

inline StartupReplayResult replayStartup(std::span<const std::uint8_t> input) {
    using namespace daemon;
    ServiceManagerFsm fsm;
    DeferralGate gate;
    auto now = DeferralGate::Clock::time_point{};
    constexpr auto maxDeferral = std::chrono::milliseconds{300000};
    bool idle = true;
    bool pressure = false;
    bool cancelled = false;
    bool sawShutdown = false;
    bool sawFailure = false;
    StartupReplayResult result;
    result.statesVisited = 1U << static_cast<unsigned>(ServiceManagerState::Uninitialized);

    // Bounded even for a huge fuzz input; no real clocks, IO, queues or sleeps.
    for (std::size_t i = 0; i < std::min(input.size(), std::size_t{512}); ++i) {
        result.offset = i;
        const auto before = fsm.snapshot().state;
        const auto action = static_cast<StartupAction>(
            (input[i] & 0x7fU) % static_cast<std::uint8_t>(StartupAction::Count));
        switch (action) {
            case StartupAction::Advance:
                switch (before) {
                    case ServiceManagerState::Uninitialized:
                        (void)fsm.tryStartOpeningDatabase();
                        break;
                    case ServiceManagerState::OpeningDatabase:
                        fsm.dispatch(DatabaseOpenedEvent{});
                        break;
                    case ServiceManagerState::DatabaseReady:
                        fsm.dispatch(MigrationStartedEvent{});
                        break;
                    case ServiceManagerState::MigratingSchema:
                        fsm.dispatch(MigrationCompletedEvent{});
                        break;
                    case ServiceManagerState::SchemaReady:
                        if ((input[i] & 0x80U) != 0) {
                            fsm.dispatch(VectorsInitializedEvent{64});
                        } else {
                            fsm.dispatch(SearchEngineBuildStartedEvent{});
                        }
                        break;
                    case ServiceManagerState::VectorsReady:
                        fsm.dispatch(SearchEngineBuildStartedEvent{});
                        break;
                    case ServiceManagerState::BuildingSearchEngine:
                        fsm.dispatch(SearchEngineBuiltEvent{});
                        break;
                    default:
                        break;
                }
                break;
            case StartupAction::Fail:
                if (before != ServiceManagerState::Stopped &&
                    before != ServiceManagerState::Failed) {
                    fsm.dispatch(InitializationFailedEvent{"injected startup failure"});
                }
                break;
            case StartupAction::Shutdown:
                fsm.dispatch(ShutdownEvent{});
                sawShutdown = true;
                break;
            case StartupAction::FinishShutdown:
                if (before == ServiceManagerState::ShuttingDown) {
                    (void)fsm.completeShutdown();
                }
                break;
            case StartupAction::Idle:
                idle = true;
                break;
            case StartupAction::Busy:
                idle = false;
                break;
            case StartupAction::Pressure:
                pressure = true;
                break;
            case StartupAction::Calm:
                pressure = false;
                break;
            case StartupAction::Tick:
                now += std::chrono::milliseconds{100};
                break;
            case StartupAction::Deadline:
                now += maxDeferral + std::chrono::milliseconds{1};
                break;
            case StartupAction::Cancel:
                cancelled = true;
                break;
            case StartupAction::Reset:
                if (before == ServiceManagerState::Stopped ||
                    before == ServiceManagerState::Failed) {
                    fsm.reset();
                    gate = DeferralGate{};
                    cancelled = sawShutdown = sawFailure = false;
                }
                break;
            case StartupAction::Probe:
            case StartupAction::Count:
                break;
        }
        const auto phase = fsm.snapshot().state;
        result.statesVisited |= 1U << static_cast<unsigned>(phase);
        sawFailure = sawFailure || phase == ServiceManagerState::Failed;
        if (sawShutdown && phase != ServiceManagerState::ShuttingDown &&
            phase != ServiceManagerState::Stopped) {
            result.error = "shutdown resurrected without an explicit terminal reset";
            return result;
        }
        if (sawFailure && phase == ServiceManagerState::Ready) {
            result.error = "failed startup became ready without reset";
            return result;
        }
        const bool stopping = cancelled || sawShutdown;
        const bool eligible = embed::startupMaintenanceEligible(phase, stopping, idle);
        // Oracle is independent of the policy implementation, using the real FSM API.
        if (eligible != (fsm.isReady() && !stopping && idle)) {
            result.error = "maintenance eligibility disagrees with actual startup lifecycle";
            return result;
        }
        const auto deferralsBefore = gate.deferrals();
        const auto forcedBefore = gate.forcedRuns();
        const auto since = gate.deferredSince();
        bool admitted = false;
        if (eligible) {
            admitted = gate.admit(pressure, now, maxDeferral);
            const bool expected = !pressure || (since && now - *since >= maxDeferral);
            if (admitted != expected) {
                result.error = "pressure/starvation admission violated the virtual deadline";
                return result;
            }
        }
        if (!eligible && (admitted || gate.deferrals() != deferralsBefore ||
                          gate.forcedRuns() != forcedBefore)) {
            result.error = "ineligible startup work touched the pressure admission gate";
            return result;
        }
        result.admissions += admitted ? 1 : 0;
        result.forcedAdmissions += static_cast<std::size_t>(gate.forcedRuns() - forcedBefore);
    }
    return result;
}

// Fuzz raw IEEE-754 values as well as lifecycle order. The production helpers reject
// unusable vectors/scores and normalize legal scores BEFORE adaptive thresholding.
inline bool semanticScoresBounded(std::span<const std::uint8_t> input) {
    std::array<float, 32> left{};
    std::array<float, 32> right{};
    const auto count = std::min(input.size() / 8, left.size());
    auto decode = [&](std::size_t offset) {
        std::uint32_t bits = 0;
        for (unsigned byte = 0; byte < 4; ++byte) {
            bits |= static_cast<std::uint32_t>(input[offset + byte]) << (byte * 8);
        }
        return std::bit_cast<float>(bits);
    };
    for (std::size_t i = 0; i < count; ++i) {
        left[i] = decode(i * 8);
        right[i] = decode(i * 8 + 4);
    }
    const auto a = std::span<const float>{left.data(), count};
    const auto b = std::span<const float>{right.data(), count};
    const auto ia = daemon::embed::semanticInverseNorm(a);
    const auto ib = daemon::embed::semanticInverseNorm(b);
    const auto score = daemon::embed::semanticCosineSimilarity(a, ia, b, ib);
    const auto self = daemon::embed::semanticCosineSimilarity(a, ia, a, ia);
    return std::isfinite(ia) && ia >= 0 && std::isfinite(ib) && ib >= 0 && std::isfinite(score) &&
           score >= -1 && score <= 1 && std::isfinite(self) && self >= 0 && self <= 1;
}

} // namespace yams::fuzz
