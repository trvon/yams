#pragma once

#include <yams/vector/embedding_generator.h>

#include <memory>
#include <string>

namespace yams::vector {

std::unique_ptr<IEmbeddingBackend> makeSimeonBackend(const EmbeddingConfig& config);

// Compact identity of the active simeon recipe assembled from the
// YAMS_SIMEON_* environment knobs (projection, sketch dim, output dim, PQ
// bytes). Emitted in per-search telemetry so retrieval-quality runs can be
// joined back to the encoder configuration that produced them.
std::string simeonRecipeLabel();

} // namespace yams::vector
