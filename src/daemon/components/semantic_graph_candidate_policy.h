// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <cstddef>

namespace yams::daemon::embed {

// Keep corpus cardinality, scored pairs, and retained output cardinality distinct.
struct SemanticGraphCandidateCounts {
    std::size_t sourceDocuments;
    std::size_t candidateDocuments;
    std::size_t candidateNeighbors;
    std::size_t retainedNeighbors;

    [[nodiscard]] bool hasGraphWork() const noexcept { return retainedNeighbors != 0; }
    [[nodiscard]] std::size_t nodeCapacity() const noexcept {
        return sourceDocuments + retainedNeighbors;
    }
};

} // namespace yams::daemon::embed
