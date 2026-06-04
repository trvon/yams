#pragma once

#include <cstdint>
#include <vector>
#include <yams/core/types.h>

namespace yams::vector {

/**
 * @brief Narrow interface for quantizer persistence (TurboQuant metadata).
 *
 * Quantizer config persistence is a self-contained concern — orthogonal to
 * vector CRUD, search-index lifecycle, and entity storage.
 */
class IQuantizerStore {
public:
    virtual ~IQuantizerStore() = default;

    /// Persist per-coordinate scales for a specific (dim,bits,seed) configuration.
    virtual Result<void> persistTurboQuantPerCoordScales(size_t dim, uint8_t bits, uint64_t seed,
                                                         const std::vector<float>& scales) = 0;

    /// Persist the full fitted model (scales + per-coord centroids).
    virtual Result<void> persistTurboQuantFittedModel(size_t dim, uint8_t bits, uint64_t seed,
                                                      const std::vector<float>& scales,
                                                      const std::vector<float>& centroids) = 0;
};

} // namespace yams::vector
