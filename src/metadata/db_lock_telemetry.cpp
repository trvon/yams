#include <yams/metadata/db_lock_telemetry.h>

#include <atomic>

namespace yams::metadata {
namespace {
std::atomic<std::uint64_t> dbLockErrorCount_{0};
}

void reportDbLockError() noexcept {
    dbLockErrorCount_.fetch_add(1, std::memory_order_relaxed);
}

std::uint64_t dbLockErrorCount() noexcept {
    return dbLockErrorCount_.load(std::memory_order_relaxed);
}

std::uint64_t getAndResetDbLockErrors() noexcept {
    return dbLockErrorCount_.exchange(0, std::memory_order_relaxed);
}

} // namespace yams::metadata
