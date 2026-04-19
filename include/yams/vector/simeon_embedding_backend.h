#pragma once

#include <yams/vector/embedding_generator.h>

#include <memory>

namespace yams::vector {

std::unique_ptr<IEmbeddingBackend> makeSimeonBackend(const EmbeddingConfig& config);

} // namespace yams::vector
