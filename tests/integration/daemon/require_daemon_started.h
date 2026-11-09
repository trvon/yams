// Catch2 helper: start a DaemonHarness or FAIL with its startup diagnostics.
//
// Kept out of test_daemon_harness.h because benchmarks include the harness without Catch2.
#pragma once

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstddef>

#include "test_daemon_harness.h"

namespace yams::test {

// A daemon that cannot start is a test failure, not a skip: skipping let a broken startup path
// report green. Every attempt's rejection reason and the daemon log tail go into the message.
inline void requireStarted(DaemonHarness& harness, std::size_t attempts = 3,
                           std::chrono::milliseconds timeout = std::chrono::seconds(30),
                           std::chrono::milliseconds retryDelay = std::chrono::milliseconds(250)) {
    if (!harness.startWithRetry(timeout, attempts, nullptr, retryDelay)) {
        FAIL("Daemon failed to start after " << attempts << " attempt(s) of " << timeout.count()
                                             << "ms:\n"
                                             << harness.startupDiagnostics());
    }
}

} // namespace yams::test
