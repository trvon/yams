#pragma once

#include <filesystem>
#include <functional>
#include <future>
#include <string>
#include <yams/core/concepts.h>
#include <yams/core/span.h>
#include <yams/core/types.h>

namespace yams::crypto {

// Interface for content hashers
class IContentHasher {
public:
    virtual ~IContentHasher() = default;

    // Stream-based hashing
    virtual void init() = 0;
    virtual void update(yams::span<const std::byte> data) = 0;
    virtual std::string finalize() = 0;

    // Convenience method for hashing files
    virtual std::string hashFile(const std::filesystem::path& path) = 0;

    // Generic hash method using concepts
    template <HashableData T> std::string hash(const T& data) {
        init();
        if constexpr (std::is_same_v<T, std::filesystem::path>) {
            return hashFile(data);
        } else {
            // Use std::as_bytes directly with explicit span construction for compatibility
            // This works because ByteSpanConvertible ensures data can be converted to byte span
            auto span = std::as_bytes(std::span(data));
            update(span);
            return finalize();
        }
    }

    // Async file hashing
    virtual std::future<Result<std::string>> hashFileAsync(const std::filesystem::path& path) = 0;

    // Progress callback support
    using ProgressCallback = std::function<void(uint64_t, uint64_t)>;
    virtual void setProgressCallback(ProgressCallback callback) = 0;
};

// SHA-256 implementation
class SHA256Hasher : public IContentHasher {
public:
    SHA256Hasher();
    ~SHA256Hasher();

    // Disable copy, enable move
    SHA256Hasher(const SHA256Hasher&) = delete;
    SHA256Hasher& operator=(const SHA256Hasher&) = delete;
    SHA256Hasher(SHA256Hasher&&) noexcept;
    SHA256Hasher& operator=(SHA256Hasher&&) noexcept;

    void init() override;
    void update(std::span<const std::byte> data) override;
    std::string finalize() override;

    std::string hashFile(const std::filesystem::path& path) override;

    std::future<Result<std::string>> hashFileAsync(const std::filesystem::path& path) override;

    void setProgressCallback(ProgressCallback callback) override;

    // Static utility for one-shot hashing
    static std::string hash(std::span<const std::byte> data);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

// Factory function
std::unique_ptr<IContentHasher> createSHA256Hasher();

// Backward compatibility typedef
using IHasher = IContentHasher;

} // namespace yams::crypto