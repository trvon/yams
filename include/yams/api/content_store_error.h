#pragma once

#include <yams/core/types.h>

#include <exception>
#include <optional>
#include <string>

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

// Content store exception
class ContentStoreException : public std::runtime_error {
public:
    ContentStoreException(ContentStoreError error, const std::string& message)
        : std::runtime_error(message), error_(error) {}

    ContentStoreException(ContentStoreError error, const std::string& message,
                          const std::string& hash)
        : std::runtime_error(message), error_(error), contentHash_(hash) {}

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

} // namespace yams::api