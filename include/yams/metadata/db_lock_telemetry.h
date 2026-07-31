#pragma once

#include <cstdint>

namespace yams::metadata {

/// Records a terminal SQLite lock error for adaptive concurrency control.
void reportDbLockError() noexcept;

/// Returns the current terminal SQLite lock-error count without resetting it.
[[nodiscard]] std::uint64_t dbLockErrorCount() noexcept;

/// Returns and clears the terminal SQLite lock-error count for the next tuning window.
[[nodiscard]] std::uint64_t getAndResetDbLockErrors() noexcept;

} // namespace yams::metadata
