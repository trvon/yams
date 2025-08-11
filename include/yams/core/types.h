#pragma once

#include <cstdint>
#include <string>
#include <chrono>
#include <variant>
#include <vector>
#include <span>

namespace yams {

// Type aliases
using Hash = std::string;
using ByteVector = std::vector<std::byte>;
using ByteSpan = std::span<const std::byte>;
using TimePoint = std::chrono::system_clock::time_point;
using Duration = std::chrono::milliseconds;
using DocumentId = int64_t;

// Error types
enum class ErrorCode {
    Success = 0,
    FileNotFound,
    PermissionDenied,
    CorruptedData,
    StorageFull,
    InvalidArgument,
    NetworkError,
    DatabaseError,
    HashMismatch,
    ChunkNotFound,
    ManifestInvalid,
    TransactionFailed,
    OperationCancelled,
    OperationInProgress,
    InvalidOperation,
    InvalidState,
    InvalidData,
    InternalError,
    NotFound,
    NotSupported,
    CompressionError,
    Timeout,
    TransactionAborted,
    ResourceExhausted,
    SystemShutdown,
    ValidationError,
    WriteError,
    NotInitialized,
    NotImplemented,
    Unknown
};

// Convert error code to string
constexpr const char* errorToString(ErrorCode error) {
    switch (error) {
        case ErrorCode::Success: return "Success";
        case ErrorCode::FileNotFound: return "File not found";
        case ErrorCode::PermissionDenied: return "Permission denied";
        case ErrorCode::CorruptedData: return "Corrupted data";
        case ErrorCode::StorageFull: return "Storage full";
        case ErrorCode::InvalidArgument: return "Invalid argument";
        case ErrorCode::NetworkError: return "Network error";
        case ErrorCode::DatabaseError: return "Database error";
        case ErrorCode::HashMismatch: return "Hash mismatch";
        case ErrorCode::ChunkNotFound: return "Chunk not found";
        case ErrorCode::ManifestInvalid: return "Invalid manifest";
        case ErrorCode::TransactionFailed: return "Transaction failed";
        case ErrorCode::OperationCancelled: return "Operation cancelled";
        case ErrorCode::OperationInProgress: return "Operation in progress";
        case ErrorCode::InvalidOperation: return "Invalid operation";
        case ErrorCode::InvalidState: return "Invalid state";
        case ErrorCode::InvalidData: return "Invalid data";
        case ErrorCode::InternalError: return "Internal error";
        case ErrorCode::NotFound: return "Not found";
        case ErrorCode::NotSupported: return "Not supported";
        case ErrorCode::CompressionError: return "Compression error";
        case ErrorCode::Timeout: return "Operation timed out";
        case ErrorCode::TransactionAborted: return "Transaction aborted";
        case ErrorCode::ResourceExhausted: return "Resource exhausted";
        case ErrorCode::SystemShutdown: return "System shutdown";
        case ErrorCode::ValidationError: return "Validation error";
        case ErrorCode::WriteError: return "Write error";
        case ErrorCode::NotInitialized: return "Not initialized";
        case ErrorCode::NotImplemented: return "Not implemented";
        case ErrorCode::Unknown: return "Unknown error";
    }
    return "Unknown error";
}

// Error struct for detailed error information
struct Error {
    ErrorCode code;
    std::string message;
    
    Error() : code(ErrorCode::Success), message("") {}
    Error(ErrorCode c, std::string msg) : code(c), message(std::move(msg)) {}
    Error(ErrorCode c) : code(c), message(errorToString(c)) {}
    
    // Comparison operators for ErrorCode
    bool operator==(ErrorCode c) const {
        return code == c;
    }
    
    bool operator!=(ErrorCode c) const {
        return code != c;
    }
    
    // Friend operators for ErrorCode on the left side
    friend bool operator==(ErrorCode c, const Error& error) {
        return error.code == c;
    }
    
    friend bool operator!=(ErrorCode c, const Error& error) {
        return error.code != c;
    }
};

// Simple Result type for operations that can fail (compatible with pre-C++23)
template<typename T>
class Result {
public:
    Result(T&& value) : data_(std::move(value)) {}
    Result(const T& value) : data_(value) {}
    Result(ErrorCode error) : data_(Error{error}) {}
    Result(Error error) : data_(std::move(error)) {}
    
    bool has_value() const noexcept {
        return std::holds_alternative<T>(data_);
    }
    
    explicit operator bool() const noexcept {
        return has_value();
    }
    
    const T& value() const& {
        if (!has_value()) {
            throw std::runtime_error("Result contains error");
        }
        return std::get<T>(data_);
    }
    
    T&& value() && {
        if (!has_value()) {
            throw std::runtime_error("Result contains error");
        }
        return std::get<T>(std::move(data_));
    }
    
    const Error& error() const {
        if (has_value()) {
            throw std::runtime_error("Result contains value");
        }
        return std::get<Error>(data_);
    }
    
private:
    std::variant<T, Error> data_;
};

// Specialization for void
template<>
class Result<void> {
public:
    Result() : error_() {}
    Result(ErrorCode error) : error_(Error{error}) {}
    Result(Error error) : error_(std::move(error)) {}
    
    bool has_value() const noexcept {
        return error_.code == ErrorCode::Success;
    }
    
    explicit operator bool() const noexcept {
        return has_value();
    }
    
    void value() const {
        if (!has_value()) {
            throw std::runtime_error("Result contains error");
        }
    }
    
    const Error& error() const {
        if (has_value()) {
            throw std::runtime_error("Result contains value");
        }
        return error_;
    }
    
private:
    Error error_{ErrorCode::Success, ""};
};

} // namespace yams

// Format support for ErrorCode
#include <format>
template<>
struct std::formatter<yams::ErrorCode> {
    constexpr auto parse(std::format_parse_context& ctx) {
        return ctx.begin();
    }
    
    auto format(yams::ErrorCode error, std::format_context& ctx) const {
        return std::format_to(ctx.out(), "{}", yams::errorToString(error));
    }
};

// fmt library support for ErrorCode (for spdlog)
#if defined(SPDLOG_FMT_EXTERNAL) || defined(FMT_VERSION)
#include <fmt/format.h>
template<>
struct fmt::formatter<yams::ErrorCode> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.begin();
    }
    
    template<typename FormatContext>
    auto format(yams::ErrorCode error, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", yams::errorToString(error));
    }
};
#endif

namespace yams {

// Common constants
inline constexpr size_t HASH_SIZE = 32;  // SHA-256
inline constexpr size_t HASH_STRING_SIZE = 64;  // Hex encoded
inline constexpr size_t DEFAULT_CHUNK_SIZE = 64 * 1024;  // 64KB
inline constexpr size_t MIN_CHUNK_SIZE = 16 * 1024;  // 16KB
inline constexpr size_t MAX_CHUNK_SIZE = 256 * 1024;  // 256KB
inline constexpr size_t DEFAULT_BUFFER_SIZE = 64 * 1024;  // 64KB

// Storage statistics
struct StorageStats {
    uint64_t totalObjects = 0;
    uint64_t totalBytes = 0;
    uint64_t uniqueBlocks = 0;
    uint64_t deduplicatedBytes = 0;
    double dedupRatio = 0.0;
    uint64_t writeOperations = 0;
    uint64_t readOperations = 0;
    TimePoint lastModified;
};

// File information
struct FileInfo {
    Hash hash;
    uint64_t size;
    std::string mimeType;
    TimePoint createdAt;
    std::string originalName;
};

// Chunk information
struct ChunkInfo {
    Hash hash;
    uint64_t offset;
    size_t size;
    
    // C++20 spaceship operator for comparisons
    auto operator<=>(const ChunkInfo&) const = default;
};

} // namespace yams