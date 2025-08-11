#include "zstandard_compressor.h"
#include <yams/compression/compression_monitor.h>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/fmt.h>
#include <chrono>
#include <cstring>

namespace yams::compression {

namespace {
    constexpr uint8_t DEFAULT_COMPRESSION_LEVEL = 3;
    constexpr size_t MIN_DICT_SIZE = 1024;
    
    /**
     * @brief Convert Zstandard error to Result error
     */
    [[nodiscard]] Error makeZstdError(const char* operation, size_t code) {
        return Error{
            ErrorCode::CompressionError,
            fmt::format("{} failed: {}", operation, ZSTD_getErrorName(code))
        };
    }
}

//-----------------------------------------------------------------------------
// ZstandardCompressor::Impl
//-----------------------------------------------------------------------------

class ZstandardCompressor::Impl {
public:
    Impl() {
        // Create contexts with custom deleters
        cctx.reset(ZSTD_createCCtx());
        if (!cctx) {
            spdlog::error("Failed to create Zstandard compression context");
        }
        
        dctx.reset(ZSTD_createDCtx());
        if (!dctx) {
            spdlog::error("Failed to create Zstandard decompression context");
        }
    }
    
    ~Impl() = default;
    
    [[nodiscard]] Result<CompressionResult> compress(
        std::span<const std::byte> data, 
        uint8_t level) {
        
        if (!cctx) {
            return Error{ErrorCode::InvalidState, "Compression context not initialized"};
        }
        
        // Use default level if 0
        if (level == 0) {
            level = DEFAULT_COMPRESSION_LEVEL;
        }
        
        // Validate level
        if (level < 1 || level > 22) {
            return Error{ErrorCode::InvalidArgument, 
                fmt::format("Invalid compression level: {}", level)};
        }
        
        // Get maximum compressed size
        const size_t maxSize = ZSTD_compressBound(data.size());
        std::vector<std::byte> compressed(maxSize);
        
        auto start = std::chrono::steady_clock::now();
        
        // Perform compression
        const size_t result = ZSTD_compressCCtx(
            cctx.get(),
            compressed.data(), compressed.size(),
            data.data(), data.size(),
            level
        );
        
        if (ZSTD_isError(result)) {
            return makeZstdError("ZSTD_compressCCtx", result);
        }
        
        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        // Resize to actual compressed size
        compressed.resize(result);
        
        CompressionResult compResult;
        compResult.data = std::move(compressed);
        compResult.algorithm = CompressionAlgorithm::Zstandard;
        compResult.level = level;
        compResult.originalSize = data.size();
        compResult.compressedSize = result;
        compResult.duration = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        
        spdlog::debug("Zstandard compressed {} bytes to {} bytes (ratio: {:.2f}x) in {}μs",
            data.size(), result, 
            static_cast<double>(data.size()) / result,
            duration.count());
        
        return compResult;
    }
    
    [[nodiscard]] Result<std::vector<std::byte>> decompress(
        std::span<const std::byte> data,
        size_t expectedSize) {
        
        if (!dctx) {
            return Error{ErrorCode::InvalidState, "Decompression context not initialized"};
        }
        
        // Get the decompressed size
        size_t decompressedSize = expectedSize;
        if (decompressedSize == 0) {
            decompressedSize = ZSTD_getFrameContentSize(data.data(), data.size());
            if (decompressedSize == ZSTD_CONTENTSIZE_ERROR) {
                return Error{ErrorCode::InvalidData, "Not a valid Zstandard frame"};
            }
            if (decompressedSize == ZSTD_CONTENTSIZE_UNKNOWN) {
                // Start with a reasonable guess
                decompressedSize = data.size() * 4;
            }
        }
        
        std::vector<std::byte> decompressed(decompressedSize);
        
        auto start = std::chrono::steady_clock::now();
        
        // Perform decompression
        const size_t result = ZSTD_decompressDCtx(
            dctx.get(),
            decompressed.data(), decompressed.size(),
            data.data(), data.size()
        );
        
        if (ZSTD_isError(result)) {
            return makeZstdError("ZSTD_decompressDCtx", result);
        }
        
        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        // Resize to actual decompressed size
        decompressed.resize(result);
        
        spdlog::debug("Zstandard decompressed {} bytes to {} bytes in {}μs",
            data.size(), result, duration.count());
        
        return decompressed;
    }
    
    // Thread-safe context management
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> cctx{nullptr, &ZSTD_freeCCtx};
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> dctx{nullptr, &ZSTD_freeDCtx};
    
    // Dictionary support (future)
    std::shared_ptr<ZSTD_CDict> cdict;
    std::shared_ptr<ZSTD_DDict> ddict;
};

//-----------------------------------------------------------------------------
// ZstandardCompressor
//-----------------------------------------------------------------------------

ZstandardCompressor::ZstandardCompressor() 
    : pImpl(std::make_unique<Impl>()) {
}

ZstandardCompressor::~ZstandardCompressor() = default;

Result<CompressionResult> ZstandardCompressor::compress(
    std::span<const std::byte> data,
    uint8_t level) {
    
    // Start tracking
    CompressionTracker tracker(CompressionAlgorithm::Zstandard, data.size());
    
    auto result = pImpl->compress(data, level);
    
    // Record result
    if (result.has_value()) {
        tracker.complete(result.value());
    } else {
        tracker.failed();
    }
    
    return result;
}

Result<std::vector<std::byte>> ZstandardCompressor::decompress(
    std::span<const std::byte> data,
    size_t expectedSize) {
    
    // Start tracking
    DecompressionTracker tracker(CompressionAlgorithm::Zstandard, data.size());
    
    auto result = pImpl->decompress(data, expectedSize);
    
    // Record result
    if (result.has_value()) {
        tracker.complete(result.value().size());
    } else {
        tracker.failed();
    }
    
    return result;
}

std::unique_ptr<IStreamingCompressor> ZstandardCompressor::createStreamCompressor(
    uint8_t level) {
    return std::make_unique<ZstandardStreamCompressor>(level);
}

std::unique_ptr<IStreamingDecompressor> ZstandardCompressor::createStreamDecompressor() {
    return std::make_unique<ZstandardStreamDecompressor>();
}

//-----------------------------------------------------------------------------
// ZstandardStreamCompressor::Impl
//-----------------------------------------------------------------------------

class ZstandardStreamCompressor::Impl {
public:
    explicit Impl(uint8_t level) : level_(level) {
        cstream.reset(ZSTD_createCStream());
        if (!cstream) {
            spdlog::error("Failed to create Zstandard compression stream");
            return;
        }
        
        // Initialize stream
        const size_t initResult = ZSTD_initCStream(cstream.get(), 
            level_ == 0 ? DEFAULT_COMPRESSION_LEVEL : level_);
        if (ZSTD_isError(initResult)) {
            spdlog::error("Failed to initialize compression stream: {}", 
                ZSTD_getErrorName(initResult));
            cstream.reset();
        }
    }
    
    [[nodiscard]] Result<size_t> compress(
        std::span<const std::byte> input,
        std::span<std::byte> output,
        bool isLastChunk) {
        
        if (!cstream) {
            return Error{ErrorCode::InvalidState, "Compression stream not initialized"};
        }
        
        ZSTD_inBuffer inBuf{input.data(), input.size(), 0};
        ZSTD_outBuffer outBuf{output.data(), output.size(), 0};
        
        const ZSTD_EndDirective mode = isLastChunk ? ZSTD_e_end : ZSTD_e_continue;
        size_t remaining = 0;
        
        do {
            remaining = ZSTD_compressStream2(cstream.get(), &outBuf, &inBuf, mode);
            if (ZSTD_isError(remaining)) {
                return makeZstdError("ZSTD_compressStream2", remaining);
            }
        } while ((mode == ZSTD_e_end && remaining > 0) || 
                 (mode == ZSTD_e_continue && inBuf.pos < inBuf.size));
        
        return outBuf.pos;
    }
    
    [[nodiscard]] Result<void> reset() {
        if (!cstream) {
            return Error{ErrorCode::InvalidState, "Compression stream not initialized"};
        }
        
        const size_t result = ZSTD_CCtx_reset(cstream.get(), ZSTD_reset_session_only);
        if (ZSTD_isError(result)) {
            return makeZstdError("ZSTD_CCtx_reset", result);
        }
        
        return {};
    }
    
    [[nodiscard]] size_t recommendedOutputSize(size_t inputSize) const {
        return ZSTD_CStreamOutSize();
    }
    
private:
    std::unique_ptr<ZSTD_CStream, decltype(&ZSTD_freeCStream)> cstream{nullptr, &ZSTD_freeCStream};
    uint8_t level_;
};

//-----------------------------------------------------------------------------
// ZstandardStreamCompressor
//-----------------------------------------------------------------------------

ZstandardStreamCompressor::ZstandardStreamCompressor(uint8_t level)
    : pImpl(std::make_unique<Impl>(level)) {
}

ZstandardStreamCompressor::~ZstandardStreamCompressor() = default;

Result<void> ZstandardStreamCompressor::init(uint8_t level) {
    // Already initialized in constructor
    return {};
}

Result<size_t> ZstandardStreamCompressor::compress(
    std::span<const std::byte> input,
    std::span<std::byte> output) {
    return pImpl->compress(input, output, false);
}

Result<size_t> ZstandardStreamCompressor::finish(std::span<std::byte> output) {
    std::span<const std::byte> empty;
    return pImpl->compress(empty, output, true);
}

void ZstandardStreamCompressor::reset() {
    auto result = pImpl->reset();
    if (!result) {
        spdlog::error("Failed to reset stream: {}", result.error().message);
    }
}

size_t ZstandardStreamCompressor::recommendedOutputSize(size_t inputSize) const {
    return pImpl->recommendedOutputSize(inputSize);
}

//-----------------------------------------------------------------------------
// ZstandardStreamDecompressor::Impl
//-----------------------------------------------------------------------------

class ZstandardStreamDecompressor::Impl {
public:
    Impl() {
        dstream.reset(ZSTD_createDStream());
        if (!dstream) {
            spdlog::error("Failed to create Zstandard decompression stream");
            return;
        }
        
        const size_t initResult = ZSTD_initDStream(dstream.get());
        if (ZSTD_isError(initResult)) {
            spdlog::error("Failed to initialize decompression stream: {}", 
                ZSTD_getErrorName(initResult));
            dstream.reset();
        }
    }
    
    [[nodiscard]] Result<size_t> decompress(
        std::span<const std::byte> input,
        std::span<std::byte> output) {
        
        if (!dstream) {
            return Error{ErrorCode::InvalidState, "Decompression stream not initialized"};
        }
        
        ZSTD_inBuffer inBuf{input.data(), input.size(), 0};
        ZSTD_outBuffer outBuf{output.data(), output.size(), 0};
        
        const size_t result = ZSTD_decompressStream(dstream.get(), &outBuf, &inBuf);
        if (ZSTD_isError(result)) {
            return makeZstdError("ZSTD_decompressStream", result);
        }
        
        isComplete_ = (result == 0);
        return outBuf.pos;
    }
    
    [[nodiscard]] Result<void> reset() {
        if (!dstream) {
            return Error{ErrorCode::InvalidState, "Decompression stream not initialized"};
        }
        
        const size_t result = ZSTD_DCtx_reset(dstream.get(), ZSTD_reset_session_only);
        if (ZSTD_isError(result)) {
            return makeZstdError("ZSTD_DCtx_reset", result);
        }
        
        isComplete_ = false;
        return {};
    }
    
    [[nodiscard]] bool isComplete() const {
        return isComplete_;
    }
    
    [[nodiscard]] size_t recommendedOutputSize(size_t inputSize) const {
        return ZSTD_DStreamOutSize();
    }
    
private:
    std::unique_ptr<ZSTD_DStream, decltype(&ZSTD_freeDStream)> dstream{nullptr, &ZSTD_freeDStream};
    bool isComplete_{false};
};

//-----------------------------------------------------------------------------
// ZstandardStreamDecompressor
//-----------------------------------------------------------------------------

ZstandardStreamDecompressor::ZstandardStreamDecompressor()
    : pImpl(std::make_unique<Impl>()) {
}

ZstandardStreamDecompressor::~ZstandardStreamDecompressor() = default;

Result<void> ZstandardStreamDecompressor::init() {
    // Already initialized in constructor
    return {};
}

Result<size_t> ZstandardStreamDecompressor::decompress(
    std::span<const std::byte> input,
    std::span<std::byte> output) {
    return pImpl->decompress(input, output);
}

void ZstandardStreamDecompressor::reset() {
    auto result = pImpl->reset();
    if (!result) {
        spdlog::error("Failed to reset stream: {}", result.error().message);
    }
}

bool ZstandardStreamDecompressor::isFinished() const {
    return pImpl->isComplete();
}

size_t ZstandardStreamDecompressor::recommendedOutputSize(size_t inputSize) const {
    return pImpl->recommendedOutputSize(inputSize);
}

} // namespace yams::compression