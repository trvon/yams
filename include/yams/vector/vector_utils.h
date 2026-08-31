#pragma once

#include <vector>

namespace yams::vector::vector_utils {

/// Normalize a vector to unit length. Returns the input unchanged if its norm is zero.
std::vector<float> normalize(const std::vector<float>& vector);

/// Cosine similarity between two float vectors. Returns 0.0 when the sizes
/// differ or either vector has zero norm. Extracted from the static
/// VectorDatabase::computeCosineSimilarity so vector backends do not depend
/// on the storage-class header for pure math helpers.
double computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b);

} // namespace yams::vector::vector_utils
