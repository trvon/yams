#include <yams/compression/compressor_interface.h>
#include "zstandard_compressor.h"
#include "lzma_compressor.h"
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/fmt.h>

namespace yams::compression {

CompressionRegistry& CompressionRegistry::instance() {
    static CompressionRegistry registry;
    return registry;
}

void CompressionRegistry::registerCompressor(
    CompressionAlgorithm algorithm, 
    CompressorFactory factory) {
    
    std::lock_guard lock(mutex_);
    
    if (factories_.count(algorithm) > 0) {
        spdlog::warn("Overwriting existing compressor for algorithm: {}", 
            static_cast<int>(algorithm));
    }
    
    factories_[algorithm] = std::move(factory);
    spdlog::debug("Registered compressor for algorithm: {}", 
        static_cast<int>(algorithm));
}

std::unique_ptr<ICompressor> CompressionRegistry::createCompressor(
    CompressionAlgorithm algorithm) const {
    
    std::lock_guard lock(mutex_);
    
    auto it = factories_.find(algorithm);
    if (it == factories_.end()) {
        spdlog::error("Compressor not found for algorithm: {}", 
            static_cast<int>(algorithm));
        return nullptr;
    }
    
    try {
        auto compressor = it->second();
        if (!compressor) {
            spdlog::error("Factory returned null compressor");
            return nullptr;
        }
        return compressor;
    } catch (const std::exception& e) {
        spdlog::error("Failed to create compressor: {}", e.what());
        return nullptr;
    }
}

bool CompressionRegistry::isAvailable(CompressionAlgorithm algorithm) const {
    std::lock_guard lock(mutex_);
    return factories_.count(algorithm) > 0;
}

std::vector<CompressionAlgorithm> CompressionRegistry::availableAlgorithms() const {
    std::lock_guard lock(mutex_);
    
    std::vector<CompressionAlgorithm> algorithms;
    algorithms.reserve(factories_.size());
    
    for (const auto& [algo, _] : factories_) {
        if (algo != CompressionAlgorithm::None) {
            algorithms.push_back(algo);
        }
    }
    
    return algorithms;
}

//-----------------------------------------------------------------------------
// Automatic Registration
//-----------------------------------------------------------------------------

namespace {
    /**
     * @brief RAII registrar for Zstandard
     */
    struct ZstdRegistrar {
        ZstdRegistrar() {
            CompressionRegistry::instance().registerCompressor(
                CompressionAlgorithm::Zstandard,
                []() { return std::make_unique<ZstandardCompressor>(); }
            );
        }
    } zstdRegistrar;
    
    /**
     * @brief RAII registrar for LZMA
     */
    struct LzmaRegistrar {
        LzmaRegistrar() {
            CompressionRegistry::instance().registerCompressor(
                CompressionAlgorithm::LZMA,
                []() { return std::make_unique<LZMACompressor>(); }
            );
        }
    } lzmaRegistrar;
    
    /**
     * @brief RAII registrar for None (passthrough)
     */
    struct NoneRegistrar {
        NoneRegistrar() {
            CompressionRegistry::instance().registerCompressor(
                CompressionAlgorithm::None,
                []() { 
                    // Create a passthrough compressor for "None"
                    class PassthroughCompressor : public ICompressor {
                    public:
                        Result<CompressionResult> compress(
                            std::span<const std::byte> data,
                            uint8_t) override {
                            
                            CompressionResult result;
                            result.data.assign(data.begin(), data.end());
                            result.algorithm = CompressionAlgorithm::None;
                            result.level = 0;
                            result.originalSize = data.size();
                            result.compressedSize = data.size();
                            result.duration = std::chrono::milliseconds(0);
                            return result;
                        }
                        
                        Result<std::vector<std::byte>> decompress(
                            std::span<const std::byte> data,
                            size_t) override {
                            return std::vector<std::byte>(data.begin(), data.end());
                        }
                        
                        CompressionAlgorithm algorithm() const override {
                            return CompressionAlgorithm::None;
                        }
                        
                        std::pair<uint8_t, uint8_t> supportedLevels() const override {
                            return {0, 0};
                        }
                        
                        size_t maxCompressedSize(size_t inputSize) const override {
                            return inputSize;
                        }
                        
                        bool supportsStreaming() const override { return false; }
                        bool supportsDictionary() const override { return false; }
                    };
                    
                    return std::make_unique<PassthroughCompressor>();
                }
            );
        }
    } noneRegistrar;
}

} // namespace yams::compression