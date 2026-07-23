#pragma once

#include <yams/vector/embedding_generator.h>

#include <memory>
#include <string>

namespace yams::vector {

std::unique_ptr<IEmbeddingBackend> makeSimeonBackend(const EmbeddingConfig& config);

// Versioned coordinate-space identity resolved from the exact encoder config.
// The frozen theorem experiment profile is named `simeon-v1-384`.
std::string simeonEmbeddingSpaceIdentity(const EmbeddingConfig& config);

// Compact identity of the active Simeon recipe. Emitted in per-search
// telemetry so retrieval-quality runs can be joined to their coordinate space.
std::string simeonRecipeLabel();

} // namespace yams::vector
