#pragma once

/*
 * YAMS Downloader - Public Types and Manager Interfaces (C++20)
 *
 * This header defines the public data types and abstract interfaces for the
 * downloader subsystem. It intentionally contains no implementation details.
 *
 * Design principles:
 * - Store-only by default: artifacts are finalized into YAMS CAS (content-addressed storage)
 * - Repo-local staging on the same filesystem as CAS for atomic rename
 * - Robust progress reporting and cancellability
 * - Clear separation of concerns (HTTP adapter, integrity verification, disk writer, resume, rate
 * limit)
 *
 * Copyright (c) YAMS Contributors
 */

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace yams::downloader {

// ================================
// Fundamental enums and constants
// ================================

/**
 * Hash algorithms supported for integrity verification.
 */
enum class HashAlgo {
    Sha256,
    Sha512,
    Md5 // optional; discouraged for security-critical verification
};

/**
 * Export overwrite behavior (applies only when exporting from CAS to user paths).
 */
enum class OverwritePolicy { Never, IfDifferentEtag, Always };

/**
 * Progress stages during a single download lifecycle.
 */
enum class ProgressStage { Resolving, Connecting, Downloading, Verifying, Finalizing };

/**
 * Canonical error codes for downloader operations.
 * Note: Not a std::error_code category to keep this header implementation-free.
 */
enum class ErrorCode {
    None = 0,
    InvalidArgument,
    NetworkError,
    Timeout,
    TlsVerificationFailed,
    ServerError,
    IoError,
    ChecksumMismatch,
    ResumeNotSupported,
    PolicyViolation,
    ExdevCrossDeviceRename,
    Unknown
};

// ===================
// Small data objects
// ===================

/**
 * HTTP header key/value pair.
 */
struct Header {
    std::string name;
    std::string value;
};

/**
 * Checksum descriptor (algorithm + hex digest).
 */
struct Checksum {
    HashAlgo algo{HashAlgo::Sha256};
    std::string hex; // lower-case hex
};

/**
 * Retry/backoff policy.
 */
struct RetryPolicy {
    int maxAttempts{5};
    std::chrono::milliseconds initialBackoff{500};
    double multiplier{2.0};
    std::chrono::milliseconds maxBackoff{15000};
};

/**
 * Rate limit configuration (0 = unlimited).
 */
struct RateLimit {
    std::uint64_t globalBps{0};
    std::uint64_t perConnectionBps{0};
};

/**
 * TLS configuration.
 */
struct TlsConfig {
    bool insecure{false};
    std::string caPath; // empty = system default
};

/**
 * Storage configuration for CAS and staging directories.
 */
struct StorageConfig {
    std::filesystem::path objectsDir{"data/objects"};
    std::filesystem::path stagingDir{"data/staging"};
};

/**
 * Downloader default configuration.
 */
struct DownloaderConfig {
    int defaultConcurrency{4};
    std::size_t defaultChunkSizeBytes{8ull * 1024ull * 1024ull}; // 8 MiB
    std::chrono::milliseconds defaultTimeout{60000};
    bool followRedirects{true};
    bool resume{true};
    RateLimit rateLimit{};
    HashAlgo defaultChecksumAlgo{HashAlgo::Sha256};
    bool storeOnly{true};
    std::uint64_t maxFileBytes{0}; // 0 = unlimited
};

/**
 * A single download request (store-only by default).
 * Optionally specify export_path/dir to export from CAS after storing.
 */
struct DownloadRequest {
    std::string url;

    // Optional export targets (only used if explicitly requested)
    std::optional<std::filesystem::path> exportPath;
    std::optional<std::filesystem::path> exportDir;

    std::vector<Header> headers;
    std::optional<Checksum> checksum;

    int concurrency{4};
    std::size_t chunkSizeBytes{8ull * 1024ull * 1024ull};
    std::chrono::milliseconds timeout{60000};

    RetryPolicy retry{};
    RateLimit rateLimit{};

    bool resume{true};
    std::optional<std::string> proxy;
    TlsConfig tls{};
    bool followRedirects{true};

    OverwritePolicy overwrite{OverwritePolicy::Never}; // applies to export only
    bool storeOnly{true}; // if true, no user-path writes unless export is explicitly asked
};

/**
 * Streaming progress event for a single URL.
 */
struct ProgressEvent {
    std::string url;
    std::uint64_t downloadedBytes{0};
    std::optional<std::uint64_t> totalBytes{};
    std::optional<float> percentage{}; // 0.0 - 100.0 (approx)
    std::optional<std::uint64_t> speedBps{};
    std::optional<std::uint32_t> etaSeconds{};
    ProgressStage stage{ProgressStage::Downloading};
    std::chrono::steady_clock::time_point timestamp{std::chrono::steady_clock::now()};
};

/**
 * Canonical error object.
 */
struct Error {
    ErrorCode code{ErrorCode::None};
    std::string message;
};

/**
 * Final result for a single URL after store (and optional export).
 */
struct FinalResult {
    std::string url;

    // CAS info
    std::string hash;                 // e.g., "sha256:<hex>"
    std::filesystem::path storedPath; // objects/sha256/aa/bb/<full_hash>
    std::uint64_t sizeBytes{0};

    bool success{false};
    std::optional<int> httpStatus{};
    std::optional<std::string> etag{};
    std::optional<std::string> lastModified{};
    std::optional<bool> checksumOk{};

    std::optional<Error> error{};

    std::chrono::milliseconds elapsed{0};
};

// =========================
// Lightweight Expected<T>
// =========================

/**
 * Minimal Expected<T> for interfaces (header-only, no exceptions required).
 * - If ok() is true, value() is valid; otherwise error() is set.
 */
template <typename T> class Expected {
public:
    Expected() = default;
    Expected(const T& v) : _ok(true), _value(v) {}
    Expected(T&& v) noexcept : _ok(true), _value(std::move(v)) {}
    Expected(const Error& e) : _ok(false), _error(e) {}
    Expected(Error&& e) noexcept : _ok(false), _error(std::move(e)) {}

    [[nodiscard]] bool ok() const noexcept { return _ok; }
    [[nodiscard]] const T& value() const& { return _value; }
    [[nodiscard]] T& value() & { return _value; }
    [[nodiscard]] T&& value() && { return std::move(_value); }
    [[nodiscard]] const Error& error() const& { return _error; }

private:
    bool _ok{false};
    T _value{};
    Error _error{};
};

// Specialization for Expected<void>
template <> class Expected<void> {
public:
    Expected() : _ok(true) {}
    Expected(const Error& e) : _ok(false), _error(e) {}
    Expected(Error&& e) noexcept : _ok(false), _error(std::move(e)) {}

    [[nodiscard]] bool ok() const noexcept { return _ok; }
    [[nodiscard]] const Error& error() const& { return _error; }

private:
    bool _ok{true};
    Error _error{};
};

// ===================
// Callback signatures
// ===================

using ProgressCallback = std::function<void(const ProgressEvent&)>;
using ShouldCancel = std::function<bool()>;                // return true to cancel ASAP
using LogCallback = std::function<void(std::string_view)>; // optional structured/plain logs

// ==========================
// Service interface classes
// ==========================

/**
 * HTTP adapter abstraction (libcurl-based implementation will satisfy this).
 * Implementations should support Range requests and robust TLS.
 */
class IHttpAdapter {
public:
    virtual ~IHttpAdapter() = default;

    /**
     * Probe server metadata (HEAD preferred) for resume capability and content length.
     */
    virtual Expected<void>
    probe(std::string_view url, const std::vector<Header>& headers,
          /* out */ bool& resumeSupported,
          /* out */ std::optional<std::uint64_t>& contentLength,
          /* out */ std::optional<std::string>& etag,
          /* out */ std::optional<std::string>& lastModified, const TlsConfig& tls,
          const std::optional<std::string>& proxy,
          std::chrono::milliseconds timeout) = 0; // TODO(parallel): consider exposing server part
                                                  // size/alignment for multi-range planning

    /**
     * Fetch a range [offset, offset+size) and stream data to the provided sink callback.
     * The sink may be called multiple times on the same thread.
     */
    virtual Expected<void>
    fetchRange(std::string_view url, const std::vector<Header>& headers, std::uint64_t offset,
               std::uint64_t size, // if unknown, impl may stream until EOF
               const TlsConfig& tls, const std::optional<std::string>& proxy,
               std::chrono::milliseconds timeout,
               const std::function<Expected<void>(std::span<const std::byte>)>& sink,
               const ShouldCancel& shouldCancel, const ProgressCallback& onProgress) = 0;
};

/**
 * Integrity verifier interface (streaming hash calculator).
 */
class IIntegrityVerifier {
public:
    virtual ~IIntegrityVerifier() = default;
    virtual void reset(HashAlgo algo) = 0;
    virtual void update(std::span<const std::byte> data) = 0;
    virtual Checksum finalize() = 0;
};

/**
 * Disk writer for staging and finalize into CAS.
 * Implementations must ensure atomic rename when staging and CAS share the same filesystem.
 */
class IDiskWriter {
public:
    virtual ~IDiskWriter() = default;

    /**
     * Create a new staging file for the session. Must be created with restrictive permissions.
     */
    virtual Expected<std::filesystem::path> createStagingFile(const StorageConfig& storage,
                                                              std::string_view sessionId,
                                                              std::string_view tempExtension) = 0;

    // Resume-aware helper:
    // Attempts to create or open an existing staging file for resume flows.
    // - expectedSize: optional total size (when known from probe)
    // - currentSize (out): current bytes present in staging (0 for fresh file)
    // Implementations should:
    //   1) Create parent dirs with restrictive perms
    //   2) Create file (0600) if missing; otherwise open existing
    //   3) Optionally preallocate up to expectedSize for sparse-friendly FS (best-effort)
    //   4) Return the path and currentSize for next-range calculation
    virtual Expected<std::filesystem::path> createOrOpenStagingFile(
        const StorageConfig& storage, std::string_view sessionId, std::string_view tempExtension,
        std::optional<std::uint64_t> expectedSize,
        /* out */ std::uint64_t& currentSize) = 0; // TODO(resume): implement sparse preallocation
                                                   // where supported

    /**
     * Write a contiguous block at a specific offset (impl may choose pwrite or positioning).
     */
    virtual Expected<void> writeAt(const std::filesystem::path& stagingFile, std::uint64_t offset,
                                   std::span<const std::byte> data) = 0;

    /**
     * Ensure data durability (fsync file and staging dir).
     */
    virtual Expected<void> sync(const std::filesystem::path& stagingFile,
                                const StorageConfig& storage) = 0;

    /**
     * Finalize by moving the staging file into CAS at the canonical path for hashHex.
     * Must attempt atomic rename; on EXDEV, implementations may fall back to copy+fsync+rename.
     */
    virtual Expected<std::filesystem::path> finalizeToCas(const std::filesystem::path& stagingFile,
                                                          std::string_view hashHex,
                                                          const StorageConfig& storage) = 0;

    /**
     * Best-effort cleanup of staging file(s).
     */
    virtual void cleanup(const std::filesystem::path& stagingFile) noexcept = 0;
};

/**
 * Resume persistence for partial downloads (ETag/Last-Modified + completed ranges).
 */
class IResumeStore {
public:
    struct State {
        std::optional<std::string> etag;
        std::optional<std::string> lastModified;
        std::vector<std::pair<std::uint64_t, std::uint64_t>> completedRanges; // [offset, length]
        std::uint64_t totalBytes{0};                                          // 0 if unknown
    };

    virtual ~IResumeStore() = default;

    virtual Expected<std::optional<State>> load(std::string_view url) = 0;
    virtual Expected<void> save(std::string_view url, const State& state) = 0;
    virtual void remove(std::string_view url) noexcept = 0;
};

/**
 * Token-bucket style limiter interface.
 */
class IRateLimiter {
public:
    virtual ~IRateLimiter() = default;

    /**
     * Blocks/yields until 'bytes' tokens are available based on configured limits.
     * Implementations may sleep or yield cooperatively.
     */
    virtual void acquire(std::uint64_t bytes, const ShouldCancel& shouldCancel) = 0;

    /**
     * Set runtime limits (0 = unlimited).
     */
    virtual void setLimits(const RateLimit& limit) = 0;
};

/**
 * Download manager abstraction (orchestrates adapter/writer/integrity/resume/rate-limit).
 */
class IDownloadManager {
public:
    virtual ~IDownloadManager() = default;

    /**
     * Execute a single request. Returns FinalResult with CAS hash/storedPath when successful.
     * Progress and cancellation are optional.
     */
    virtual Expected<FinalResult> download(const DownloadRequest& request,
                                           const ProgressCallback& onProgress = {},
                                           const ShouldCancel& shouldCancel = {},
                                           const LogCallback& onLog = {}) = 0;

    /**
     * Execute multiple requests. Order of results matches the order of requests.
     */
    virtual std::vector<Expected<FinalResult>>
    downloadMany(const std::vector<DownloadRequest>& requests,
                 const ProgressCallback& onProgress = {}, const ShouldCancel& shouldCancel = {},
                 const LogCallback& onLog = {}) = 0;

    /**
     * Access effective storage and downloader configuration.
     */
    [[nodiscard]] virtual StorageConfig storageConfig() const = 0;
    [[nodiscard]] virtual DownloaderConfig config() const = 0;
};

// ======================
// Utility path builders
// ======================

/**
 * Build CAS path for a given sha256 hex digest:
 * objectsDir/sha256/aa/bb/<full_hex>
 * Precondition: hexDigest.size() == 64 and hexDigest is lowercase hex.
 */
[[nodiscard]] inline std::filesystem::path casPathForSha256(const std::filesystem::path& objectsDir,
                                                            std::string_view hexDigest) {
    if (hexDigest.size() < 64) {
        // Defensive fallback; caller should enforce size 64 for SHA-256
        return objectsDir / "sha256" / "_x" / "_y" / std::string(hexDigest);
    }
    auto shard1 = std::string{hexDigest.substr(0, 2)};
    auto shard2 = std::string{hexDigest.substr(2, 2)};
    return objectsDir / "sha256" / shard1 / shard2 / std::string(hexDigest);
}

/**
 * Build canonical "sha256:<hex>" string for a digest.
 */
[[nodiscard]] inline std::string makeSha256Id(std::string_view hexDigest) {
    std::string id("sha256:");
    id.append(hexDigest);
    return id;
}

/**
 * Factory function to create a default DownloadManager instance.
 */
std::unique_ptr<IDownloadManager> makeDownloadManager(const StorageConfig& storage,
                                                      const DownloaderConfig& cfg);

} // namespace yams::downloader