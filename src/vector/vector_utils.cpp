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

double computeCosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
    if (a.size() != b.size()) {
        return 0.0;
    }

    double dot_product = 0.0;
    double norm_a = 0.0;
    double norm_b = 0.0;

    for (size_t i = 0; i < a.size(); ++i) {
        dot_product += static_cast<double>(a[i]) * static_cast<double>(b[i]);
        norm_a += static_cast<double>(a[i]) * static_cast<double>(a[i]);
        norm_b += static_cast<double>(b[i]) * static_cast<double>(b[i]);
    }

    norm_a = std::sqrt(norm_a);
    norm_b = std::sqrt(norm_b);

    if (norm_a == 0.0 || norm_b == 0.0) {
        return 0.0;
    }

    return dot_product / (norm_a * norm_b);
}

} // namespace yams::vector::vector_utils
