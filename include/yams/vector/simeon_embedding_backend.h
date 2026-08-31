#pragma once

// Breaks the public-header cycle with embedding_generator.h: this header only
// needs EmbeddingConfig by reference for its declarations. Consumers that use
// EmbeddingConfig values must include <yams/vector/embedding_generator.h>.
#include <memory>
#include <string>

namespace yams::vector {

class IEmbeddingBackend;
struct EmbeddingConfig;

std::unique_ptr<IEmbeddingBackend> makeSimeonBackend(const EmbeddingConfig& config);

// Versioned coordinate-space identity resolved from the exact encoder config.
// The frozen theorem experiment profile is named `simeon-v1-384`.
std::string simeonEmbeddingSpaceIdentity(const EmbeddingConfig& config);

// Compact identity of the active Simeon recipe. Emitted in per-search
// telemetry so retrieval-quality runs can be joined to their coordinate space.
std::string simeonRecipeLabel();

} // namespace yams::vector
