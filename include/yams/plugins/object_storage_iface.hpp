#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace yams::plugins {

enum class ChecksumAlgo { None, CRC32C, SHA256 };
enum class SseType { None, SSE_KMS, SSE_C };

struct PutOptions {
    std::optional<std::string> kmsKeyId;
    SseType sseType{SseType::None};
    std::optional<std::string> storageClass; // e.g., STANDARD, STANDARD_IA
    std::optional<std::string> contentType;
    std::unordered_map<std::string, std::string> metadata; // user metadata
    std::size_t partSize{0};
    std::size_t concurrency{0};
    ChecksumAlgo checksum{ChecksumAlgo::CRC32C};
    std::optional<std::string> idempotencyKey;
};

struct GetOptions {
    bool verifyChecksumAtEnd{false};
    std::optional<std::string> ifMatch;
    std::optional<std::string> ifNoneMatch;
};

struct Range {
    // inclusive byte range [start, end]; if end == npos, read to end
    static constexpr std::uint64_t npos = static_cast<std::uint64_t>(-1);
    std::uint64_t start{0};
    std::uint64_t end{npos};
};

struct ObjectMetadata {
    std::uint64_t size{0};
    std::optional<std::string> etag;
    std::optional<std::string> checksum;     // hex/base64 per provider
    std::optional<std::string> checksumAlgo; // "crc32c" | "sha256"
    std::optional<std::string> storageClass;
    std::optional<std::string> versionId;
    std::optional<std::string> lastModified; // RFC 3339
    std::unordered_map<std::string, std::string> userMetadata;
};

struct PutResult {
    std::optional<std::string> versionId;
    std::optional<std::string> etag;
    std::optional<std::string> checksum;
    std::optional<std::string> storageClass;
};

struct ObjectSummary {
    std::string key;
    std::uint64_t size{0};
    std::optional<std::string> etag;
    std::optional<std::string> lastModified;
};

template <typename T> struct Page {
    std::vector<T> items;
    std::optional<std::string> nextPageToken;
};

struct ErrorInfo {
    enum class Category { Retryable, Terminal, Auth, NotFound, Quota };
    Category category{Category::Terminal};
    int http{0};
    std::optional<std::string> providerCode;
    int sysErrno{0};
    std::string message;
    std::optional<int> retryAfterMs;
};

struct VerifyResult {
    bool ok{false};
    std::optional<std::string> expectedChecksum;
    std::optional<std::string> actualChecksum;
    std::optional<std::string> algo;
};

struct HealthStatus {
    bool ready{false};
    std::optional<std::string> lastError;
    std::optional<double> throughputBps1m;
    std::optional<double> errorRate1m;
};

// Capability names align with docs: multipart,resume,crc32c,sha256,sse-kms,sse-c,storage-class,
// compose,versioning,presign-upload,presign-download,dr-presence,dr-replication-lag
struct Capabilities {
    std::vector<std::string> features; // set of feature ids
    std::vector<std::string> targets;  // e.g., {"s3","r2","minio"}
};

class IObjectStorageBackend {
public:
    virtual ~IObjectStorageBackend() = default;

    virtual Capabilities capabilities() const = 0;

    // Basic ops
    virtual std::variant<PutResult, ErrorInfo> put(std::string_view key, const void* data,
                                                   std::size_t len, const PutOptions& opts) = 0;
    virtual std::variant<ObjectMetadata, ErrorInfo> head(std::string_view key,
                                                         const GetOptions& opts) = 0;
    virtual std::variant<std::vector<std::uint8_t>, ErrorInfo>
    get(std::string_view key, std::optional<Range> range, const GetOptions& opts) = 0;
    virtual std::optional<ErrorInfo> remove(std::string_view key) = 0;
    virtual std::variant<Page<ObjectSummary>, ErrorInfo> list(std::string_view prefix,
                                                              std::optional<std::string> delimiter,
                                                              std::optional<std::string> pageToken,
                                                              std::optional<int> maxKeys) = 0;

    // Optional ops
    virtual std::variant<PutResult, ErrorInfo> compose(std::string_view target,
                                                       const std::vector<std::string>& sources,
                                                       const PutOptions& opts) {
        return ErrorInfo{ErrorInfo::Category::Terminal, 501};
    }

    // Hooks
    virtual VerifyResult verifyObject(std::string_view key, std::optional<ChecksumAlgo> algo) = 0;
    virtual HealthStatus health() const = 0;

    // DR hooks (optional)
    virtual std::variant<bool, std::monostate> drObjectPresent(std::string_view /*target*/,
                                                               std::string_view /*key*/) {
        return std::monostate{};
    }
    virtual std::variant<std::uint64_t, std::monostate> replicationLagSeconds() {
        return std::monostate{};
    }
};

} // namespace yams::plugins
