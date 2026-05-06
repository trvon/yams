#include <yams/vector/vector_utils.h>

#include <cmath>

namespace yams::vector::vector_utils {

std::vector<float> normalize(const std::vector<float>& vector) {
    float norm = 0.0f;
    for (float v : vector)
        norm += v * v;
    norm = std::sqrt(norm);
    if (norm == 0.0f)
        return vector;

    std::vector<float> normalized;
    normalized.reserve(vector.size());
    for (float v : vector)
        normalized.push_back(v / norm);
    return normalized;
}

} // namespace yams::vector::vector_utils
