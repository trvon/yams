#pragma once

#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

namespace yams::vector::detail {

constexpr std::uint32_t kSimeonPqBlobVersion = 1;

template <typename T> inline void appendSimeonPod(std::vector<std::uint8_t>& blob, const T& value) {
    const auto* ptr = reinterpret_cast<const std::uint8_t*>(&value);
    blob.insert(blob.end(), ptr, ptr + sizeof(T));
}

template <typename T>
inline bool readSimeonPod(const std::vector<std::uint8_t>& blob, std::size_t& offset, T& value) {
    if (offset + sizeof(T) > blob.size()) {
        return false;
    }
    std::memcpy(&value, blob.data() + offset, sizeof(T));
    offset += sizeof(T);
    return true;
}

inline std::vector<std::uint8_t> serializeSimeonPqCodebooks(std::uint32_t dim, std::uint32_t m,
                                                            std::uint32_t k, bool trained,
                                                            std::span<const float> codebooks) {
    std::vector<std::uint8_t> blob;
    blob.reserve(sizeof(std::uint32_t) * 5 + sizeof(std::uint64_t) + codebooks.size_bytes());
    appendSimeonPod(blob, kSimeonPqBlobVersion);
    appendSimeonPod(blob, dim);
    appendSimeonPod(blob, m);
    appendSimeonPod(blob, k);
    appendSimeonPod(blob, static_cast<std::uint32_t>(trained ? 1u : 0u));
    appendSimeonPod(blob, static_cast<std::uint64_t>(codebooks.size()));
    if (!codebooks.empty()) {
        const auto* ptr = reinterpret_cast<const std::uint8_t*>(codebooks.data());
        blob.insert(blob.end(), ptr, ptr + codebooks.size_bytes());
    }
    return blob;
}

inline bool deserializeSimeonPqCodebooks(const std::vector<std::uint8_t>& blob, std::uint32_t dim,
                                         std::uint32_t m, std::uint32_t k, bool& trained,
                                         std::vector<float>& codebooks) {
    std::size_t offset = 0;
    std::uint32_t version = 0;
    std::uint32_t storedDim = 0;
    std::uint32_t storedM = 0;
    std::uint32_t storedK = 0;
    std::uint32_t storedTrained = 0;
    std::uint64_t codebookCount = 0;
    if (!readSimeonPod(blob, offset, version) || !readSimeonPod(blob, offset, storedDim) ||
        !readSimeonPod(blob, offset, storedM) || !readSimeonPod(blob, offset, storedK) ||
        !readSimeonPod(blob, offset, storedTrained) ||
        !readSimeonPod(blob, offset, codebookCount)) {
        return false;
    }
    const std::size_t expected = static_cast<std::size_t>(m) * k * (dim / m);
    if (version != kSimeonPqBlobVersion || storedDim != dim || storedM != m || storedK != k ||
        codebookCount != expected || offset + expected * sizeof(float) != blob.size()) {
        return false;
    }

    codebooks.resize(expected);
    if (expected > 0) {
        std::memcpy(codebooks.data(), blob.data() + offset, expected * sizeof(float));
    }
    trained = storedTrained != 0;
    return true;
}

} // namespace yams::vector::detail
