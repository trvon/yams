#include <spdlog/spdlog.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <yams/crypto/hasher.h>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif
#include <array>
#include <fstream>
#include <mutex>

namespace yams::crypto {

namespace {

std::string bytesToHex(const unsigned char* data, unsigned int size) {
    static constexpr char kHexDigits[] = "0123456789abcdef";
    std::string out;
    out.resize(static_cast<std::string::size_type>(size) * 2U);
    for (unsigned int i = 0; i < size; ++i) {
        const unsigned int value = data[i];
        const size_t pos = static_cast<size_t>(i) * 2U;
        out[pos] = kHexDigits[(value >> 4U) & 0x0FU];
        out[pos + 1U] = kHexDigits[value & 0x0FU];
    }
    return out;
}

} // namespace

struct SHA256Hasher::Impl {
    std::mutex mtx;
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
    Impl(Impl&& other) noexcept : ctx(other.ctx) { other.ctx = nullptr; }

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
    std::lock_guard<std::mutex> lock(pImpl->mtx);
    if (EVP_DigestInit_ex(pImpl->ctx, EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("Failed to initialize SHA256");
    }
}

SHA256Hasher::~SHA256Hasher() = default;

SHA256Hasher::SHA256Hasher(SHA256Hasher&&) noexcept = default;
SHA256Hasher& SHA256Hasher::operator=(SHA256Hasher&&) noexcept = default;

void SHA256Hasher::init() {
    std::lock_guard<std::mutex> lock(pImpl->mtx);
    if (EVP_DigestInit_ex(pImpl->ctx, EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("Failed to initialize SHA256");
    }
}

void SHA256Hasher::update(std::span<const std::byte> data) {
    std::lock_guard<std::mutex> lock(pImpl->mtx);
    if (EVP_DigestUpdate(pImpl->ctx, data.data(), data.size()) != 1) {
        throw std::runtime_error("Failed to update SHA256");
    }
}

std::string SHA256Hasher::finalize() {
    std::lock_guard<std::mutex> lock(pImpl->mtx);
    std::array<unsigned char, EVP_MAX_MD_SIZE> hash{};
    unsigned int hashLen = 0;

    if (EVP_DigestFinal_ex(pImpl->ctx, hash.data(), &hashLen) != 1) {
        throw std::runtime_error("Failed to finalize SHA256");
    }

    std::string result = bytesToHex(hash.data(), hashLen);

    // Reset for potential reuse
    if (EVP_DigestInit_ex(pImpl->ctx, EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("Failed to re-initialize SHA256 after finalize");
    }

    return result;
}

std::string SHA256Hasher::hashFile(const std::filesystem::path& path) {
    std::lock_guard<std::mutex> lock(pImpl->mtx);
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        throw std::runtime_error(yamsfmt::format("Failed to open file: {}", path.string()));
    }

    if (EVP_DigestInit_ex(pImpl->ctx, EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("Failed to initialize SHA256 for hashFile");
    }

    constexpr size_t bufferSize = DEFAULT_BUFFER_SIZE;
    std::vector<std::byte> buffer(bufferSize);

    auto fileSize = std::filesystem::file_size(path);
    uint64_t processed = 0;

    while (file) {
        file.read(reinterpret_cast<char*>(buffer.data()), bufferSize);
        auto bytesRead = file.gcount();

        if (bytesRead > 0) {
            if (EVP_DigestUpdate(pImpl->ctx, buffer.data(), static_cast<size_t>(bytesRead)) != 1) {
                throw std::runtime_error("Failed to update SHA256 for hashFile");
            }
            processed += static_cast<uint64_t>(bytesRead);

            if (pImpl->progressCallback) {
                pImpl->progressCallback(processed, fileSize);
            }
        }
    }

    std::array<unsigned char, EVP_MAX_MD_SIZE> hash{};
    unsigned int hashLen = 0;
    if (EVP_DigestFinal_ex(pImpl->ctx, hash.data(), &hashLen) != 1) {
        throw std::runtime_error("Failed to finalize SHA256 for hashFile");
    }

    return bytesToHex(hash.data(), hashLen);
}

std::future<Result<std::string>> SHA256Hasher::hashFileAsync(const std::filesystem::path& path) {
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
    std::lock_guard<std::mutex> lock(pImpl->mtx);
    pImpl->progressCallback = std::move(callback);
}

std::string SHA256Hasher::hash(std::span<const std::byte> data) {
    SHA256_CTX ctx;
    std::array<unsigned char, SHA256_DIGEST_LENGTH> digest{};
    if (SHA256_Init(&ctx) != 1) {
        throw std::runtime_error("Failed to initialize one-shot SHA256 context");
    }
    if (!data.empty()) {
        const auto* input = reinterpret_cast<const unsigned char*>(data.data());
        if (SHA256_Update(&ctx, input, data.size()) != 1) {
            throw std::runtime_error("Failed to update one-shot SHA256 hash");
        }
    }
    if (SHA256_Final(digest.data(), &ctx) != 1) {
        throw std::runtime_error("Failed to finalize one-shot SHA256 hash");
    }
    return bytesToHex(digest.data(), SHA256_DIGEST_LENGTH);
}

std::unique_ptr<IContentHasher> createSHA256Hasher() {
    return std::make_unique<SHA256Hasher>();
}

} // namespace yams::crypto
