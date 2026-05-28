#pragma once

#include <chrono>
#include <cstddef>

namespace yams::content {

[[nodiscard]] inline double successRate(std::size_t total, std::size_t successful) noexcept {
    return total > 0 ? static_cast<double>(successful) / static_cast<double>(total) : 0.0;
}

[[nodiscard]] inline double averageProcessingTimeMs(std::chrono::milliseconds totalTime,
                                                    std::size_t total) noexcept {
    return total > 0 ? static_cast<double>(totalTime.count()) / static_cast<double>(total) : 0.0;
}

[[nodiscard]] inline double averageCount(std::size_t totalCount, std::size_t total) noexcept {
    return total > 0 ? static_cast<double>(totalCount) / static_cast<double>(total) : 0.0;
}

} // namespace yams::content
