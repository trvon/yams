// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <cstddef>
#include <string>

namespace yams::daemon {

// Typed startup policy for embedding-worker safeguards. Callers that leave a field unset retain
// compatibility resolution; explicit values are immutable for the service lifecycle.
struct EmbeddingServiceConfig {
    // Zero means unspecified; effective policy resolves a safe default or compatibility value.
    std::size_t coremlUnifiedConcurrency{0};
    std::string coremlUnifiedConcurrencySource;
    // Zero means unspecified: prepared jobs are split so their selected payload stays near the
    // resolved default. Bounds the bytes held by the channel and pending list, not provider
    // workspace.
    std::size_t maxPreparedPayloadBytes{0};
};

struct EffectiveEmbeddingServiceConfig {
    std::size_t coremlUnifiedConcurrency{1};
    std::string coremlUnifiedConcurrencySource{"default"};
};

} // namespace yams::daemon
