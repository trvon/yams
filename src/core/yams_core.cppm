// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// C++20 Module Interface for YAMS Core
// This is a proof-of-concept demonstrating modules with existing YAMS code.
// Goal: Provide module interface for frequently-used core types to reduce
//       redundant header parsing across translation units.

module;

// Global module fragment - include C++ standard library
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <span>
#include <optional>
#include <expected>
#include <vector>
#include <chrono>
#include <filesystem>

// Include third-party headers that aren't modularized yet
#include <tl/expected.hpp>

export module yams.core;

// Re-export commonly used standard types
export namespace std {
    using std::string;
    using std::string_view;
    using std::span;
    using std::optional;
    using std::vector;
    using std::byte;
    using std::size_t;
    using std::uint8_t;
    using std::uint16_t;
    using std::uint32_t;
    using std::uint64_t;
    using std::filesystem::path;
    
    // C++23 std::expected if available, otherwise tl::expected
    #if __cpp_lib_expected >= 202202L
    using std::expected;
    using std::unexpected;
    #endif
}

// Export tl::expected (used throughout YAMS)
export namespace tl {
    using tl::expected;
    using tl::unexpected;
    using tl::make_unexpected;
}

// Export YAMS core types
export namespace yams::core {

/// SHA-256 hash representation (32 bytes)
struct Hash {
    std::array<std::uint8_t, 32> data;
    
    constexpr Hash() noexcept : data{} {}
    
    explicit Hash(std::span<const std::uint8_t, 32> bytes) noexcept {
        std::copy(bytes.begin(), bytes.end(), data.begin());
    }
    
    /// Convert to hex string
    std::string to_hex() const;
    
    /// Parse from hex string
    static tl::expected<Hash, std::string> from_hex(std::string_view hex);
    
    /// Comparison operators
    constexpr bool operator==(const Hash& other) const noexcept {
        return data == other.data;
    }
    
    constexpr auto operator<=>(const Hash& other) const noexcept {
        return data <=> other.data;
    }
    
    /// Check if hash is zero (null)
    constexpr bool is_null() const noexcept {
        return std::all_of(data.begin(), data.end(), 
                          [](auto b) { return b == 0; });
    }
};

/// Result type alias for common patterns
template<typename T>
using Result = tl::expected<T, std::string>;

/// Byte range type
using ByteSpan = std::span<const std::byte>;
using MutableByteSpan = std::span<std::byte>;

/// Document metadata
struct Metadata {
    std::optional<std::string> name;
    std::optional<std::string> mime_type;
    std::vector<std::string> tags;
    std::optional<std::string> collection;
    std::optional<std::string> snapshot_id;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point modified_at;
    std::optional<std::filesystem::path> source_path;
    std::size_t size_bytes = 0;
    
    Metadata() 
        : created_at(std::chrono::system_clock::now())
        , modified_at(std::chrono::system_clock::now()) 
    {}
};

/// Storage statistics
struct StorageStats {
    std::size_t total_documents = 0;
    std::size_t total_bytes_logical = 0;
    std::size_t total_bytes_stored = 0;
    std::size_t total_chunks = 0;
    std::size_t unique_chunks = 0;
    double deduplication_ratio = 0.0;
    
    /// Calculate space saved by deduplication
    std::size_t bytes_saved() const noexcept {
        return total_bytes_logical > total_bytes_stored 
            ? total_bytes_logical - total_bytes_stored 
            : 0;
    }
};

/// Error types
enum class ErrorCode {
    Success = 0,
    NotFound,
    AlreadyExists,
    InvalidArgument,
    IoError,
    CorruptData,
    OutOfMemory,
    PermissionDenied,
    NetworkError,
    Timeout,
    InternalError
};

/// Convert error code to string
const char* error_code_string(ErrorCode code) noexcept;

/// Create an error Result
template<typename T>
Result<T> make_error(ErrorCode code, std::string_view message = "") {
    std::string msg = std::string(error_code_string(code));
    if (!message.empty()) {
        msg += ": ";
        msg += message;
    }
    return tl::unexpected(std::move(msg));
}

} // namespace yams::core

// Module implementation units can be added later
// For PoC, implementation stays in .cpp files
