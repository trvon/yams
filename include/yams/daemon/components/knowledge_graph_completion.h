// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <atomic>
#include <cstdint>

namespace yams::daemon {

enum class KnowledgeGraphCompletionStage : std::uint8_t {
    Graph = 1,
    TitleNl = 2,
};

class KnowledgeGraphCompletion {
public:
    KnowledgeGraphCompletion(bool expectGraph, bool expectTitleNl) noexcept
        : expectedMask_(static_cast<std::uint8_t>(
              (expectGraph ? static_cast<std::uint8_t>(KnowledgeGraphCompletionStage::Graph) : 0U) |
              (expectTitleNl ? static_cast<std::uint8_t>(KnowledgeGraphCompletionStage::TitleNl)
                             : 0U))) {}

    [[nodiscard]] bool markCommitted(KnowledgeGraphCompletionStage stage) noexcept {
        const auto stageBit = static_cast<std::uint8_t>(stage);
        const auto graphBit = static_cast<std::uint8_t>(KnowledgeGraphCompletionStage::Graph);
        const auto titleNlBit = static_cast<std::uint8_t>(KnowledgeGraphCompletionStage::TitleNl);
        if ((stageBit != graphBit && stageBit != titleNlBit) || expectedMask_ == 0U ||
            (expectedMask_ & stageBit) == 0U) {
            return false;
        }

        const auto committed = static_cast<std::uint8_t>(
            committedMask_.fetch_or(stageBit, std::memory_order_acq_rel) | stageBit);
        return (committed & expectedMask_) == expectedMask_;
    }

private:
    const std::uint8_t expectedMask_;
    std::atomic<std::uint8_t> committedMask_{0U};
};

} // namespace yams::daemon
