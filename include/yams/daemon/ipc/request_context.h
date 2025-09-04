#pragma once

#include <atomic>
#include <chrono>

namespace yams {
namespace daemon {

struct RequestContext {
    std::chrono::steady_clock::time_point start{};
    std::atomic<bool> completed{false};
    std::atomic<bool> canceled{false};
    std::atomic<size_t> frames_enqueued{0};
    std::atomic<size_t> bytes_enqueued{0};
};

} // namespace daemon
} // namespace yams
