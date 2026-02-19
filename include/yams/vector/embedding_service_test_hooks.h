#pragma once

#include <functional>
#include <string>
#include <vector>

namespace yams::vector::testing {

std::vector<std::vector<float>> generateEmbeddingsAdaptiveSplit(
    const std::vector<std::string>& texts,
    const std::function<std::vector<std::vector<float>>(const std::vector<std::string>&)>&
        generator);

} // namespace yams::vector::testing
