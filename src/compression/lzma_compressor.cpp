#include "lzma_compressor.h"
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <array>
#include <chrono>
#include <cstring>
#include <yams/compression/compression_monitor.h>

// LZMA SDK headers
extern "C" {
#include <Alloc.h>
#include <Lzma2Dec.h>
#include <Lzma2Enc.h>
#include <LzmaDec.h>
#include <LzmaEnc.h>
}

namespace yams::compression {

namespace {
constexpr uint8_t LZMA_DEFAULT_COMPRESSION_LEVEL = 5;
constexpr uint8_t KRONOS_LZMA_PROPS_SIZE = 5;
constexpr uint8_t LZMA2_PROPS_SIZE = 1;

// Dictionary sizes for each compression level
constexpr std::array<uint32_t, 10> DICT_SIZES = {
    1 << 16, // Level 0: 64KB
    1 << 18, // Level 1: 256KB
    1 << 20, // Level 2: 1MB
    1 << 21, // Level 3: 2MB
    1 << 22, // Level 4: 4MB
    1 << 23, // Level 5: 8MB (default)
    1 << 24, // Level 6: 16MB
    1 << 25, // Level 7: 32MB
    1 << 26, // Level 8: 64MB
    1 << 27  // Level 9: 128MB
};

// LZMA SDK allocation functions
static void* LzmaAlloc(ISzAllocPtr, size_t size) {
    return malloc(size);
}

static void LzmaFree(ISzAllocPtr, void* address) {
    free(address);
}

static ISzAlloc g_Alloc = {LzmaAlloc, LzmaFree};

/**
 * @brief Convert LZMA SDK error to Result error
 */
[[nodiscard]] Error makeLzmaError(const char* operation, SRes code) {
    const char* errorMsg = "Unknown error";
    switch (code) {
        case SZ_OK:
            return Error{ErrorCode::Success, "Success"};
        case SZ_ERROR_DATA:
            errorMsg = "Data error";
            break;
        case SZ_ERROR_MEM:
            errorMsg = "Memory allocation error";
            break;
        case SZ_ERROR_CRC:
            errorMsg = "CRC error";
            break;
        case SZ_ERROR_UNSUPPORTED:
            errorMsg = "Unsupported properties";
            break;
        case SZ_ERROR_PARAM:
            errorMsg = "Invalid parameter";
            break;
        case SZ_ERROR_INPUT_EOF:
            errorMsg = "Unexpected end of input";
            break;
        case SZ_ERROR_OUTPUT_EOF:
            errorMsg = "Output buffer overflow";
            break;
        case SZ_ERROR_READ:
            errorMsg = "Read error";
            break;
        case SZ_ERROR_WRITE:
            errorMsg = "Write error";
            break;
        case SZ_ERROR_PROGRESS:
            errorMsg = "Progress callback error";
            break;
        case SZ_ERROR_FAIL:
            errorMsg = "Generic failure";
            break;
        case SZ_ERROR_THREAD:
            errorMsg = "Threading error";
            break;
        default:
            break;
}
    return Error{ErrorCode::CompressionError, fmt::format("{} failed: {}", operation, errorMsg)};
}
} // namespace

//-----------------------------------------------------------------------------
// LZMACompressor::Impl
//-----------------------------------------------------------------------------

class LZMACompressor::Impl {
public:
    explicit Impl(Variant v) : variant_(v) {}

    [[nodiscard]] Result<CompressionResult> compress(std::span<const std::byte> data,
                                                     uint8_t level) {
        // Use default level if 0
        if (level == 0) {
            level = LZMA_DEFAULT_COMPRESSION_LEVEL;
        }

        // Validate level
        if (level > 9) {
            return Error{ErrorCode::InvalidArgument,
                         fmt::format("Invalid compression level: {}", level)};
        }

        switch (variant_) {
            case Variant::LZMA:
                return compressLZMA(data, level);
            case Variant::LZMA2:
                return compressLZMA2(data, level);
            default:
                return Error{ErrorCode::InvalidState, "Invalid LZMA variant"};
        }
    }

    [[nodiscard]] Result<std::vector<std::byte>> decompress(std::span<const std::byte> data,
                                                            size_t expectedSize) {
        if (data.empty()) {
            return std::vector<std::byte>{};
        }

        switch (variant_) {
            case Variant::LZMA:
                return decompressLZMA(data, expectedSize);
            case Variant::LZMA2:
                return decompressLZMA2(data, expectedSize);
            default:
                return Error{ErrorCode::InvalidState, "Invalid LZMA variant"};
        }
    }

    void setVariant(Variant v) { variant_ = v; }
    Variant variant() const { return variant_; }

    [[nodiscard]] size_t maxCompressedSize(size_t inputSize) const {
        // LZMA can expand data in worst case
        // Add space for properties and potential expansion
        if (variant_ == Variant::LZMA) {
            return inputSize + inputSize / 3 + 128 + KRONOS_LZMA_PROPS_SIZE;
        } else {
            return inputSize + inputSize / 3 + 128 + LZMA2_PROPS_SIZE;
        }
    }

private:
    Variant variant_;

    [[nodiscard]] Result<CompressionResult> compressLZMA(std::span<const std::byte> data,
                                                         uint8_t level) {
        auto start = std::chrono::steady_clock::now();

        // Prepare encoder properties
        CLzmaEncProps props;
        LzmaEncProps_Init(&props);
        props.level = level;
        props.dictSize = DICT_SIZES[level];
        props.lc = 3;         // literal context bits
        props.lp = 0;         // literal pos bits
        props.pb = 2;         // pos bits
        props.fb = 32;        // fast bytes
        props.numThreads = 1; // Single-threaded

        // Allocate output buffer
        size_t destLen = maxCompressedSize(data.size());
        std::vector<std::byte> compressed(destLen);

        // Properties buffer
        std::array<Byte, KRONOS_LZMA_PROPS_SIZE> propsEncoded;
        SizeT propsSize = KRONOS_LZMA_PROPS_SIZE;

        // Perform compression
        SRes res = LzmaEncode(reinterpret_cast<Byte*>(compressed.data() + KRONOS_LZMA_PROPS_SIZE),
                              &destLen, reinterpret_cast<const Byte*>(data.data()), data.size(),
                              &props, propsEncoded.data(), &propsSize,
                              0,       // writeEndMark
                              nullptr, // progress
                              &g_Alloc, &g_Alloc);

        if (res != SZ_OK) {
            return makeLzmaError("LzmaEncode", res);
        }

        // Copy properties to output
        std::memcpy(compressed.data(), propsEncoded.data(), KRONOS_LZMA_PROPS_SIZE);

        // Resize to actual size
        compressed.resize(KRONOS_LZMA_PROPS_SIZE + destLen);

        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        CompressionResult result;
        result.data = std::move(compressed);
        result.algorithm = CompressionAlgorithm::LZMA;
        result.level = level;
        result.originalSize = data.size();
        result.compressedSize = result.data.size();
        result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(duration);

        spdlog::debug("LZMA compressed {} bytes to {} bytes (ratio: {:.2f}x) in {}μs", data.size(),
                      result.compressedSize,
                      static_cast<double>(data.size()) / static_cast<double>(result.compressedSize), duration.count());

        return result;
    }

    [[nodiscard]] Result<CompressionResult> compressLZMA2(std::span<const std::byte> data,
                                                          uint8_t level) {
        auto start = std::chrono::steady_clock::now();

        // Create LZMA2 encoder
        CLzma2EncHandle enc = Lzma2Enc_Create(&g_Alloc, &g_Alloc);
        if (!enc) {
            return Error{ErrorCode::InternalError, "Failed to create LZMA2 encoder"};
        }

        // RAII guard to ensure encoder is freed
        struct EncodeGuard {
            CLzma2EncHandle enc;
            ~EncodeGuard() {
                if (enc)
                    Lzma2Enc_Destroy(enc);
            }
        } guard{enc};

        // Set properties
        CLzma2EncProps props;
        Lzma2EncProps_Init(&props);
        props.lzmaProps.level = level;
        props.lzmaProps.dictSize = DICT_SIZES[level];
        props.blockSize = 0; // Auto
        props.numTotalThreads = 1;

        SRes res = Lzma2Enc_SetProps(enc, &props);
        if (res != SZ_OK) {
            return makeLzmaError("Lzma2Enc_SetProps", res);
        }

        // Get encoded properties
        Byte propsByte = Lzma2Enc_WriteProperties(enc);

        // Allocate output buffer
        size_t destLen = maxCompressedSize(data.size());
        std::vector<std::byte> compressed(destLen);

        // Write properties byte
        compressed[0] = std::byte{propsByte};

        // Compress
        size_t outSize = destLen - 1; // Account for properties byte
        res = Lzma2Enc_Encode2(enc,
                               nullptr, // outStream
                               reinterpret_cast<Byte*>(compressed.data() + 1), &outSize,
                               nullptr, // inStream
                               reinterpret_cast<const Byte*>(data.data()), data.size(),
                               nullptr // progress
        );
        destLen = outSize + 1; // Include properties byte

        if (res != SZ_OK) {
            return makeLzmaError("Lzma2Enc_Encode2", res);
        }

        // Resize to actual size
        compressed.resize(1 + destLen);

        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        CompressionResult result;
        result.data = std::move(compressed);
        result.algorithm = CompressionAlgorithm::LZMA;
        result.level = level;
        result.originalSize = data.size();
        result.compressedSize = result.data.size();
        result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(duration);

        spdlog::debug("LZMA2 compressed {} bytes to {} bytes (ratio: {:.2f}x) in {}μs", data.size(),
                      result.compressedSize,
                      static_cast<double>(data.size()) / static_cast<double>(result.compressedSize), duration.count());

        return result;
    }

    [[nodiscard]] Result<std::vector<std::byte>> decompressLZMA(std::span<const std::byte> data,
                                                                size_t expectedSize) {
        if (data.size() < KRONOS_LZMA_PROPS_SIZE) {
            return Error{ErrorCode::InvalidData, "Compressed data too small for LZMA"};
        }

        auto start = std::chrono::steady_clock::now();

        // Extract properties
        const Byte* props = reinterpret_cast<const Byte*>(data.data());
        const Byte* src = props + KRONOS_LZMA_PROPS_SIZE;
        size_t srcLen = data.size() - KRONOS_LZMA_PROPS_SIZE;

        // Decode properties
        CLzmaDec state;
        LzmaDec_Construct(&state);

        SRes res = LzmaDec_Allocate(&state, props, KRONOS_LZMA_PROPS_SIZE, &g_Alloc);
        if (res != SZ_OK) {
            return makeLzmaError("LzmaDec_Allocate", res);
        }

        // RAII guard to ensure decoder is freed on all paths
        struct DecodeGuard {
            CLzmaDec* state;
            ~DecodeGuard() {
                if (state)
                    LzmaDec_Free(state, &g_Alloc);
            }
        } guard{&state};

        // Initialize decoder
        LzmaDec_Init(&state);

        // Allocate output buffer lazily; grow as needed during decode
        std::vector<std::byte> decompressed;

        // Decompress in chunks
        constexpr size_t CHUNK_SIZE = static_cast<size_t>(64) * static_cast<size_t>(1024);
        size_t inPos = 0;

        while (inPos < srcLen) {
            size_t inSize = std::min(srcLen - inPos, CHUNK_SIZE);
            size_t outSize = CHUNK_SIZE;
            size_t oldSize = decompressed.size();

            decompressed.resize(oldSize + outSize);

            ELzmaStatus status;
            res =
                LzmaDec_DecodeToBuf(&state, reinterpret_cast<Byte*>(decompressed.data() + oldSize),
                                    &outSize, src + inPos, &inSize, LZMA_FINISH_ANY, &status);

            if (res != SZ_OK) {
                return makeLzmaError("LzmaDec_DecodeToBuf", res);
            }

            decompressed.resize(oldSize + outSize);
            inPos += inSize;

            if (status == LZMA_STATUS_FINISHED_WITH_MARK ||
                status == LZMA_STATUS_MAYBE_FINISHED_WITHOUT_MARK) {
                break;
            }
        }

        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        spdlog::debug("LZMA decompressed {} bytes to {} bytes in {}μs", data.size(),
                      decompressed.size(), duration.count());

        return decompressed;
    }

    [[nodiscard]] Result<std::vector<std::byte>> decompressLZMA2(std::span<const std::byte> data,
                                                                 size_t expectedSize) {
        if (data.size() < LZMA2_PROPS_SIZE) {
            return Error{ErrorCode::InvalidData, "Compressed data too small for LZMA2"};
        }

        auto start = std::chrono::steady_clock::now();

        // Extract properties byte
        Byte propsByte = static_cast<Byte>(data[0]);
        const Byte* src = reinterpret_cast<const Byte*>(data.data() + 1);
        size_t srcLen = data.size() - 1;

        // Create decoder
        CLzma2Dec state;
        Lzma2Dec_Construct(&state);

        SRes res = Lzma2Dec_Allocate(&state, propsByte, &g_Alloc);
        if (res != SZ_OK) {
            return makeLzmaError("Lzma2Dec_Allocate", res);
        }

        // RAII guard to ensure decoder is freed on all paths
        struct DecodeGuard {
            CLzma2Dec* state;
            ~DecodeGuard() {
                if (state)
                    Lzma2Dec_Free(state, &g_Alloc);
            }
        } guard{&state};

        // Initialize decoder
        Lzma2Dec_Init(&state);

        // Allocate output buffer lazily; grow as needed during decode
        std::vector<std::byte> decompressed;

        // Decompress
        constexpr size_t CHUNK_SIZE = static_cast<size_t>(64) * static_cast<size_t>(1024);
        size_t inPos = 0;

        while (inPos < srcLen) {
            size_t inSize = std::min(srcLen - inPos, CHUNK_SIZE);
            size_t outSize = CHUNK_SIZE;
            size_t oldSize = decompressed.size();

            decompressed.resize(oldSize + outSize);

            ELzmaStatus status;
            res =
                Lzma2Dec_DecodeToBuf(&state, reinterpret_cast<Byte*>(decompressed.data() + oldSize),
                                     &outSize, src + inPos, &inSize, LZMA_FINISH_ANY, &status);

            if (res != SZ_OK) {
                return makeLzmaError("Lzma2Dec_DecodeToBuf", res);
            }

            decompressed.resize(oldSize + outSize);
            inPos += inSize;

            if (status == LZMA_STATUS_FINISHED_WITH_MARK) {
                break;
            }
        }

        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        spdlog::debug("LZMA2 decompressed {} bytes to {} bytes in {}μs", data.size(),
                      decompressed.size(), duration.count());

        return decompressed;
    }
};

//-----------------------------------------------------------------------------
// LZMACompressor
//-----------------------------------------------------------------------------

LZMACompressor::LZMACompressor() : pImpl(std::make_unique<Impl>(Variant::LZMA2)) {}

LZMACompressor::LZMACompressor(Variant variant) : pImpl(std::make_unique<Impl>(variant)) {}

LZMACompressor::~LZMACompressor() = default;

void LZMACompressor::setVariant(Variant variant) {
    pImpl->setVariant(variant);
}

LZMACompressor::Variant LZMACompressor::variant() const {
    return pImpl->variant();
}

Result<CompressionResult> LZMACompressor::compress(std::span<const std::byte> data, uint8_t level) {
    // Start tracking
    CompressionTracker tracker(CompressionAlgorithm::LZMA, data.size());

    auto result = pImpl->compress(data, level);

    // Record result
    if (result.has_value()) {
        tracker.complete(result.value());
    } else {
        tracker.failed();
    }

    return result;
}

Result<std::vector<std::byte>> LZMACompressor::decompress(std::span<const std::byte> data,
                                                          size_t expectedSize) {
    // Start tracking
    DecompressionTracker tracker(CompressionAlgorithm::LZMA, data.size());

    auto result = pImpl->decompress(data, expectedSize);

    // Record result
    if (result.has_value()) {
        tracker.complete(result.value().size());
    } else {
        tracker.failed();
    }

    return result;
}

size_t LZMACompressor::maxCompressedSize(size_t inputSize) const {
    return pImpl->maxCompressedSize(inputSize);
}

uint32_t LZMACompressor::getDictionarySize(uint8_t level) {
    if (level > 9)
        level = 9;
    return DICT_SIZES[level];
}

} // namespace yams::compression
