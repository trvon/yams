// Minimal daemon preflight placeholder for integration harnesses.
#pragma once
namespace yams::tests {
struct DaemonPreflight {
    static void ensure_environment() {
        // No-op stub: real harness would set sockets/env; smoke tests don't require it.
    }
};
} // namespace yams::tests
