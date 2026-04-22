#pragma once

#include <vector>

namespace yams::vector::vector_utils {

/// Normalize a vector to unit length. Returns the input unchanged if its norm is zero.
std::vector<float> normalize(const std::vector<float>& vector);

} // namespace yams::vector::vector_utils
