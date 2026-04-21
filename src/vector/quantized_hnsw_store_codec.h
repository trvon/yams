#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <vector>

#include <sqlite-vec-cpp/index/hnsw_quantized.hpp>

namespace yams::vector::detail {

constexpr uint32_t kQuantizedHnswStoreBlobVersion = 1;

template <typename T> void appendPod(std::vector<uint8_t>& blob, const T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    const auto* bytes = reinterpret_cast<const uint8_t*>(&value);
    blob.insert(blob.end(), bytes, bytes + sizeof(T));
}

template <typename T> bool readPod(const std::vector<uint8_t>& blob, size_t& offset, T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    if (offset + sizeof(T) > blob.size()) {
        return false;
    }
    std::memcpy(&value, blob.data() + offset, sizeof(T));
    offset += sizeof(T);
    return true;
}

inline void appendBytes(std::vector<uint8_t>& blob, const void* data, size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    blob.insert(blob.end(), bytes, bytes + size);
}

inline bool readBytes(const std::vector<uint8_t>& blob, size_t& offset, void* out, size_t size) {
    if (offset + size > blob.size()) {
        return false;
    }
    std::memcpy(out, blob.data() + offset, size);
    offset += size;
    return true;
}

template <typename StoreT>
std::vector<uint8_t> serializeQuantizedStoreBlob(sqlite_vec_cpp::index::QuantizationType type,
                                                 const StoreT& store);

template <>
inline std::vector<uint8_t> serializeQuantizedStoreBlob<sqlite_vec_cpp::quantization::LVQ8Store>(
    sqlite_vec_cpp::index::QuantizationType type,
    const sqlite_vec_cpp::quantization::LVQ8Store& store) {
    std::vector<uint8_t> blob;
    const uint64_t codesSize = static_cast<uint64_t>(store.codes.size());
    const uint64_t scalesSize = static_cast<uint64_t>(store.scales.size());
    const uint64_t offsetsSize = static_cast<uint64_t>(store.offsets.size());
    blob.reserve(sizeof(uint32_t) * 2 + sizeof(uint64_t) * 5 + codesSize +
                 scalesSize * sizeof(float) + offsetsSize * sizeof(float));
    appendPod(blob, kQuantizedHnswStoreBlobVersion);
    appendPod(blob, static_cast<uint32_t>(type));
    appendPod(blob, static_cast<uint64_t>(store.dim));
    appendPod(blob, static_cast<uint64_t>(store.count));
    appendPod(blob, codesSize);
    appendPod(blob, scalesSize);
    appendPod(blob, offsetsSize);
    if (!store.codes.empty()) {
        appendBytes(blob, store.codes.data(), store.codes.size());
    }
    if (!store.scales.empty()) {
        appendBytes(blob, store.scales.data(), store.scales.size() * sizeof(float));
    }
    if (!store.offsets.empty()) {
        appendBytes(blob, store.offsets.data(), store.offsets.size() * sizeof(float));
    }
    return blob;
}

template <>
inline std::vector<uint8_t> serializeQuantizedStoreBlob<sqlite_vec_cpp::quantization::LVQ4Store>(
    sqlite_vec_cpp::index::QuantizationType type,
    const sqlite_vec_cpp::quantization::LVQ4Store& store) {
    std::vector<uint8_t> blob;
    const uint64_t codesSize = static_cast<uint64_t>(store.codes.size());
    const uint64_t scalesSize = static_cast<uint64_t>(store.scales.size());
    const uint64_t offsetsSize = static_cast<uint64_t>(store.offsets.size());
    blob.reserve(sizeof(uint32_t) * 2 + sizeof(uint64_t) * 6 + codesSize +
                 scalesSize * sizeof(float) + offsetsSize * sizeof(float));
    appendPod(blob, kQuantizedHnswStoreBlobVersion);
    appendPod(blob, static_cast<uint32_t>(type));
    appendPod(blob, static_cast<uint64_t>(store.dim));
    appendPod(blob, static_cast<uint64_t>(store.count));
    appendPod(blob, static_cast<uint64_t>(store.bytes_per_vec));
    appendPod(blob, codesSize);
    appendPod(blob, scalesSize);
    appendPod(blob, offsetsSize);
    if (!store.codes.empty()) {
        appendBytes(blob, store.codes.data(), store.codes.size());
    }
    if (!store.scales.empty()) {
        appendBytes(blob, store.scales.data(), store.scales.size() * sizeof(float));
    }
    if (!store.offsets.empty()) {
        appendBytes(blob, store.offsets.data(), store.offsets.size() * sizeof(float));
    }
    return blob;
}

template <>
inline std::vector<uint8_t> serializeQuantizedStoreBlob<sqlite_vec_cpp::quantization::RaBitQStore>(
    sqlite_vec_cpp::index::QuantizationType type,
    const sqlite_vec_cpp::quantization::RaBitQStore& store) {
    std::vector<uint8_t> blob;
    const uint64_t bitsSize = static_cast<uint64_t>(store.bits.size());
    const uint64_t normsSize = static_cast<uint64_t>(store.norms.size());
    const uint64_t centroidSize = static_cast<uint64_t>(store.centroid.size());
    blob.reserve(sizeof(uint32_t) * 2 + sizeof(uint64_t) * 6 + bitsSize +
                 (normsSize + centroidSize) * sizeof(float));
    appendPod(blob, kQuantizedHnswStoreBlobVersion);
    appendPod(blob, static_cast<uint32_t>(type));
    appendPod(blob, static_cast<uint64_t>(store.dim));
    appendPod(blob, static_cast<uint64_t>(store.count));
    appendPod(blob, static_cast<uint64_t>(store.bytes_per_vec));
    appendPod(blob, bitsSize);
    appendPod(blob, normsSize);
    appendPod(blob, centroidSize);
    if (!store.bits.empty()) {
        appendBytes(blob, store.bits.data(), store.bits.size());
    }
    if (!store.norms.empty()) {
        appendBytes(blob, store.norms.data(), store.norms.size() * sizeof(float));
    }
    if (!store.centroid.empty()) {
        appendBytes(blob, store.centroid.data(), store.centroid.size() * sizeof(float));
    }
    return blob;
}

inline bool deserializeLvq8StoreBlob(const std::vector<uint8_t>& blob,
                                     sqlite_vec_cpp::quantization::LVQ8Store& store) {
    size_t offset = 0;
    uint32_t version = 0;
    uint32_t type = 0;
    uint64_t dim = 0;
    uint64_t count = 0;
    uint64_t codesSize = 0;
    uint64_t scalesSize = 0;
    uint64_t offsetsSize = 0;
    if (!readPod(blob, offset, version) || !readPod(blob, offset, type) ||
        !readPod(blob, offset, dim) || !readPod(blob, offset, count) ||
        !readPod(blob, offset, codesSize) || !readPod(blob, offset, scalesSize) ||
        !readPod(blob, offset, offsetsSize)) {
        return false;
    }
    if (version != kQuantizedHnswStoreBlobVersion ||
        type != static_cast<uint32_t>(sqlite_vec_cpp::index::QuantizationType::LVQ8) ||
        codesSize != count * dim || scalesSize != count || offsetsSize != count) {
        return false;
    }
    store = {};
    store.dim = static_cast<size_t>(dim);
    store.count = static_cast<size_t>(count);
    store.codes.resize(static_cast<size_t>(codesSize));
    store.scales.resize(static_cast<size_t>(scalesSize));
    store.offsets.resize(static_cast<size_t>(offsetsSize));
    return readBytes(blob, offset, store.codes.data(), store.codes.size()) &&
           readBytes(blob, offset, store.scales.data(), store.scales.size() * sizeof(float)) &&
           readBytes(blob, offset, store.offsets.data(), store.offsets.size() * sizeof(float)) &&
           offset == blob.size();
}

inline bool deserializeLvq4StoreBlob(const std::vector<uint8_t>& blob,
                                     sqlite_vec_cpp::quantization::LVQ4Store& store) {
    size_t offset = 0;
    uint32_t version = 0;
    uint32_t type = 0;
    uint64_t dim = 0;
    uint64_t count = 0;
    uint64_t bytesPerVec = 0;
    uint64_t codesSize = 0;
    uint64_t scalesSize = 0;
    uint64_t offsetsSize = 0;
    if (!readPod(blob, offset, version) || !readPod(blob, offset, type) ||
        !readPod(blob, offset, dim) || !readPod(blob, offset, count) ||
        !readPod(blob, offset, bytesPerVec) || !readPod(blob, offset, codesSize) ||
        !readPod(blob, offset, scalesSize) || !readPod(blob, offset, offsetsSize)) {
        return false;
    }
    if (version != kQuantizedHnswStoreBlobVersion ||
        type != static_cast<uint32_t>(sqlite_vec_cpp::index::QuantizationType::LVQ4) ||
        scalesSize != count || offsetsSize != count || bytesPerVec != ((dim + 1) / 2) ||
        codesSize != count * bytesPerVec) {
        return false;
    }
    store = {};
    store.dim = static_cast<size_t>(dim);
    store.count = static_cast<size_t>(count);
    store.bytes_per_vec = static_cast<size_t>(bytesPerVec);
    store.codes.resize(static_cast<size_t>(codesSize));
    store.scales.resize(static_cast<size_t>(scalesSize));
    store.offsets.resize(static_cast<size_t>(offsetsSize));
    return readBytes(blob, offset, store.codes.data(), store.codes.size()) &&
           readBytes(blob, offset, store.scales.data(), store.scales.size() * sizeof(float)) &&
           readBytes(blob, offset, store.offsets.data(), store.offsets.size() * sizeof(float)) &&
           offset == blob.size();
}

inline bool deserializeRaBitQStoreBlob(const std::vector<uint8_t>& blob,
                                       sqlite_vec_cpp::quantization::RaBitQStore& store) {
    size_t offset = 0;
    uint32_t version = 0;
    uint32_t type = 0;
    uint64_t dim = 0;
    uint64_t count = 0;
    uint64_t bytesPerVec = 0;
    uint64_t bitsSize = 0;
    uint64_t normsSize = 0;
    uint64_t centroidSize = 0;
    if (!readPod(blob, offset, version) || !readPod(blob, offset, type) ||
        !readPod(blob, offset, dim) || !readPod(blob, offset, count) ||
        !readPod(blob, offset, bytesPerVec) || !readPod(blob, offset, bitsSize) ||
        !readPod(blob, offset, normsSize) || !readPod(blob, offset, centroidSize)) {
        return false;
    }
    if (version != kQuantizedHnswStoreBlobVersion ||
        type != static_cast<uint32_t>(sqlite_vec_cpp::index::QuantizationType::RaBitQ) ||
        bytesPerVec != ((dim + 7) / 8) || bitsSize != count * bytesPerVec || normsSize != count ||
        centroidSize != dim) {
        return false;
    }
    store = {};
    store.dim = static_cast<size_t>(dim);
    store.count = static_cast<size_t>(count);
    store.bytes_per_vec = static_cast<size_t>(bytesPerVec);
    store.bits.resize(static_cast<size_t>(bitsSize));
    store.norms.resize(static_cast<size_t>(normsSize));
    store.centroid.resize(static_cast<size_t>(centroidSize));
    return readBytes(blob, offset, store.bits.data(), store.bits.size()) &&
           readBytes(blob, offset, store.norms.data(), store.norms.size() * sizeof(float)) &&
           readBytes(blob, offset, store.centroid.data(), store.centroid.size() * sizeof(float)) &&
           offset == blob.size();
}

} // namespace yams::vector::detail
