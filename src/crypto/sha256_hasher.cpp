#include <yams/crypto/hasher.h>
#include <openssl/evp.h>
#include <spdlog/spdlog.h>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif
#include <fstream>
#include <array>

namespace yams::crypto {

struct SHA256Hasher::Impl {
    EVP_MD_CTX* ctx = nullptr;
    ProgressCallback progressCallback;
    
    Impl() : ctx(EVP_MD_CTX_new()) {
        if (!ctx) {
            throw std::runtime_error("Failed to create EVP_MD_CTX");
        }
    }
    
    ~Impl() {
        if (ctx) {
            EVP_MD_CTX_free(ctx);
        }
    }
    
    // Delete copy operations
    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;
    
    // Move operations
    Impl(Impl&& other) noexcept : ctx(other.ctx) {
        other.ctx = nullptr;
    }
    
    Impl& operator=(Impl&& other) noexcept {
        if (this != &other) {
            if (ctx) {
                EVP_MD_CTX_free(ctx);
            }
            ctx = other.ctx;
            other.ctx = nullptr;
        }
        return *this;
    }
};

SHA256Hasher::SHA256Hasher() : pImpl(std::make_unique<Impl>()) {
    init();
}

SHA256Hasher::~SHA256Hasher() = default;

SHA256Hasher::SHA256Hasher(SHA256Hasher&&) noexcept = default;
SHA256Hasher& SHA256Hasher::operator=(SHA256Hasher&&) noexcept = default;

void SHA256Hasher::init() {
    if (EVP_DigestInit_ex(pImpl->ctx, EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("Failed to initialize SHA256");
    }
}

void SHA256Hasher::update(std::span<const std::byte> data) {
    if (EVP_DigestUpdate(pImpl->ctx, data.data(), data.size()) != 1) {
        throw std::runtime_error("Failed to update SHA256");
    }
}

std::string SHA256Hasher::finalize() {
    std::array<unsigned char, EVP_MAX_MD_SIZE> hash{};
    unsigned int hashLen = 0;
    
    if (EVP_DigestFinal_ex(pImpl->ctx, hash.data(), &hashLen) != 1) {
        throw std::runtime_error("Failed to finalize SHA256");
    }
    
    // Convert to hex string
    std::string result;
    result.reserve(hashLen * 2);
    
    for (unsigned int i = 0; i < hashLen; ++i) {
        result += yamsfmt::format("{:02x}", hash[i]);
    }
    
    // Reset for potential reuse
    init();
    
    return result;
}

std::string SHA256Hasher::hashFile(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        throw std::runtime_error(yamsfmt::format("Failed to open file: {}", path.string()));
    }
    
    init();
    
    constexpr size_t bufferSize = DEFAULT_BUFFER_SIZE;
    std::vector<std::byte> buffer(bufferSize);
    
    auto fileSize = std::filesystem::file_size(path);
    uint64_t processed = 0;
    
    while (file) {
        file.read(reinterpret_cast<char*>(buffer.data()), bufferSize);
        auto bytesRead = file.gcount();
        
        if (bytesRead > 0) {
            update(std::span{buffer.data(), static_cast<size_t>(bytesRead)});
            processed += static_cast<uint64_t>(bytesRead);
            
            if (pImpl->progressCallback) {
                pImpl->progressCallback(processed, fileSize);
            }
        }
    }
    
    return finalize();
}

std::future<Result<std::string>> SHA256Hasher::hashFileAsync(
    const std::filesystem::path& path) {
    
    return std::async(std::launch::async, [this, path]() -> Result<std::string> {
        try {
            return hashFile(path);
        } catch (const std::exception& e) {
            spdlog::error("Failed to hash file {}: {}", path.string(), e.what());
            return Result<std::string>(ErrorCode::FileNotFound);
        }
    });
}

void SHA256Hasher::setProgressCallback(ProgressCallback callback) {
    pImpl->progressCallback = std::move(callback);
}

std::string SHA256Hasher::hash(std::span<const std::byte> data) {
    SHA256Hasher hasher;
    hasher.update(data);
    return hasher.finalize();
}

std::unique_ptr<IContentHasher> createSHA256Hasher() {
    return std::make_unique<SHA256Hasher>();
}

} // namespace yams::crypto