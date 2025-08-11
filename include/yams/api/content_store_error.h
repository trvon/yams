#pragma once

#include <yams/core/types.h>

#include <exception>
#include <string>
#include <optional>

namespace yams::api {

// Content store specific error codes
enum class ContentStoreError {
    None = 0,
    FileNotFound,
    PermissionDenied,
    StorageFull,
    CorruptedContent,
    HashMismatch,
    InvalidHash,
    InvalidMetadata,
    NetworkError,
    OperationCancelled,
    OperationInProgress,
    TooManyOperations,
    CompressionError,
    DecompressionError,
    SerializationError,
    DeserializationError,
    IntegrityCheckFailed,
    UnsupportedOperation,
    InvalidConfiguration,
    Unknown
};

// Convert content store error to string
inline const char* contentStoreErrorToString(ContentStoreError error) {
    switch (error) {
        case ContentStoreError::None: return "No error";
        case ContentStoreError::FileNotFound: return "File not found";
        case ContentStoreError::PermissionDenied: return "Permission denied";
        case ContentStoreError::StorageFull: return "Storage full";
        case ContentStoreError::CorruptedContent: return "Corrupted content";
        case ContentStoreError::HashMismatch: return "Hash mismatch";
        case ContentStoreError::InvalidHash: return "Invalid hash";
        case ContentStoreError::InvalidMetadata: return "Invalid metadata";
        case ContentStoreError::NetworkError: return "Network error";
        case ContentStoreError::OperationCancelled: return "Operation cancelled";
        case ContentStoreError::OperationInProgress: return "Operation in progress";
        case ContentStoreError::TooManyOperations: return "Too many concurrent operations";
        case ContentStoreError::CompressionError: return "Compression error";
        case ContentStoreError::DecompressionError: return "Decompression error";
        case ContentStoreError::SerializationError: return "Serialization error";
        case ContentStoreError::DeserializationError: return "Deserialization error";
        case ContentStoreError::IntegrityCheckFailed: return "Integrity check failed";
        case ContentStoreError::UnsupportedOperation: return "Unsupported operation";
        case ContentStoreError::InvalidConfiguration: return "Invalid configuration";
        case ContentStoreError::Unknown: return "Unknown error";
    }
    return "Unknown error";
}

// Content store exception
class ContentStoreException : public std::runtime_error {
public:
    ContentStoreException(ContentStoreError error, const std::string& message)
        : std::runtime_error(message)
        , error_(error) {}
    
    ContentStoreException(ContentStoreError error, const std::string& message, 
                         const std::string& hash)
        : std::runtime_error(message)
        , error_(error)
        , contentHash_(hash) {}
    
    [[nodiscard]] ContentStoreError getError() const noexcept { return error_; }
    
    [[nodiscard]] const std::optional<std::string>& getContentHash() const noexcept { 
        return contentHash_; 
    }
    
    [[nodiscard]] const char* errorString() const noexcept {
        return contentStoreErrorToString(error_);
    }
    
private:
    ContentStoreError error_;
    std::optional<std::string> contentHash_;
};

// Operation cancelled exception
class OperationCancelledException : public ContentStoreException {
public:
    explicit OperationCancelledException(const std::string& message)
        : ContentStoreException(ContentStoreError::OperationCancelled, message) {}
};

// Storage full exception
class StorageFullException : public ContentStoreException {
public:
    StorageFullException(const std::string& message, uint64_t availableBytes, 
                        uint64_t requiredBytes)
        : ContentStoreException(ContentStoreError::StorageFull, message)
        , availableBytes_(availableBytes)
        , requiredBytes_(requiredBytes) {}
    
    [[nodiscard]] uint64_t getAvailableBytes() const noexcept { return availableBytes_; }
    [[nodiscard]] uint64_t getRequiredBytes() const noexcept { return requiredBytes_; }
    
private:
    uint64_t availableBytes_;
    uint64_t requiredBytes_;
};

// Hash mismatch exception
class HashMismatchException : public ContentStoreException {
public:
    HashMismatchException(const std::string& message, const std::string& expectedHash, 
                         const std::string& actualHash)
        : ContentStoreException(ContentStoreError::HashMismatch, message, expectedHash)
        , expectedHash_(expectedHash)
        , actualHash_(actualHash) {}
    
    [[nodiscard]] const std::string& getExpectedHash() const noexcept { return expectedHash_; }
    [[nodiscard]] const std::string& getActualHash() const noexcept { return actualHash_; }
    
private:
    std::string expectedHash_;
    std::string actualHash_;
};

// Convert generic error code to content store error
inline ContentStoreError toContentStoreError(ErrorCode error) {
    switch (error) {
        case ErrorCode::Success: return ContentStoreError::None;
        case ErrorCode::FileNotFound: return ContentStoreError::FileNotFound;
        case ErrorCode::PermissionDenied: return ContentStoreError::PermissionDenied;
        case ErrorCode::CorruptedData: return ContentStoreError::CorruptedContent;
        case ErrorCode::StorageFull: return ContentStoreError::StorageFull;
        case ErrorCode::NetworkError: return ContentStoreError::NetworkError;
        case ErrorCode::HashMismatch: return ContentStoreError::HashMismatch;
        case ErrorCode::OperationCancelled: return ContentStoreError::OperationCancelled;
        case ErrorCode::OperationInProgress: return ContentStoreError::OperationInProgress;
        default: return ContentStoreError::Unknown;
    }
}

} // namespace yams::api