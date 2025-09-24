/*
 * yams/src/downloader/download_manager.cpp
 *
 * DownloadManager MVP (single-stream):
 * - Probe server for basic capabilities and size (HEAD or GET bytes=0-0)
 * - Stream the object once (no parallel ranges) using IHttpAdapter::fetchRange
 * - Compute SHA-256 during streaming
 * - Finalize into CAS (content-addressed storage) via IDiskWriter (atomic rename)
 * - Enforce store-only default; export is out of scope for MVP
 * - Optional checksum verification against a provided Checksum
 * - Optional rate limiting and cooperative cancellation
 *
 * NOTE:
 * - Resume is not implemented yet in this MVP beyond a probe; we always download from offset 0.
 * - ETag/Last-Modified are not captured by the current Http adapter MVP; fields will remain empty.
 */

#include <yams/downloader/downloader.hpp>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <fstream>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace yams::downloader {
std::unique_ptr<IIntegrityVerifier> makeIntegrityVerifierSha256Only();
}

namespace {

using Range = std::pair<std::uint64_t, std::uint64_t>;

const char* kExportMetaSuffix = ".yams.meta.json";

void normalizeRanges(std::vector<Range>& ranges) {
    if (ranges.empty())
        return;
    std::sort(ranges.begin(), ranges.end(),
              [](const Range& a, const Range& b) { return a.first < b.first; });
    std::vector<Range> merged;
    merged.reserve(ranges.size());
    for (const auto& [offset, length] : ranges) {
        if (length == 0)
            continue;
        if (merged.empty()) {
            merged.emplace_back(offset, length);
            continue;
        }
        auto& back = merged.back();
        const auto backEnd = back.first + back.second;
        const auto currentEnd = offset + length;
        if (offset <= backEnd) {
            if (currentEnd > backEnd) {
                back.second = currentEnd - back.first;
            }
        } else {
            merged.emplace_back(offset, length);
        }
    }
    ranges.swap(merged);
}

void appendRange(std::vector<Range>& ranges, std::uint64_t offset, std::uint64_t length) {
    if (length == 0)
        return;
    if (!ranges.empty()) {
        auto& back = ranges.back();
        const auto backEnd = back.first + back.second;
        const auto currentEnd = offset + length;
        if (offset <= backEnd) {
            if (currentEnd > backEnd) {
                back.second = currentEnd - back.first;
            }
            return;
        }
    }
    ranges.emplace_back(offset, length);
}

std::uint64_t contiguousPrefixLength(const std::vector<Range>& ranges) {
    std::uint64_t cursor = 0;
    for (const auto& [offset, length] : ranges) {
        if (offset > cursor)
            break;
        const auto end = offset + length;
        if (end > cursor) {
            cursor = end;
        }
    }
    return cursor;
}

std::filesystem::path exportMetaPath(const std::filesystem::path& exportPath) {
    auto meta = exportPath;
    meta += kExportMetaSuffix;
    return meta;
}

std::optional<nlohmann::json> loadExportMetadata(const std::filesystem::path& exportPath) {
    auto metaPath = exportMetaPath(exportPath);
    if (!std::filesystem::exists(metaPath))
        return std::nullopt;
    std::ifstream in(metaPath);
    if (!in)
        return std::nullopt;
    try {
        nlohmann::json j;
        in >> j;
        if (!j.is_object())
            return std::nullopt;
        return j;
    } catch (...) {
        return std::nullopt;
    }
}

void writeExportMetadata(const std::filesystem::path& exportPath, const std::string& hash,
                         const std::optional<std::string>& etag,
                         const std::optional<std::string>& lastModified) {
    nlohmann::json meta = nlohmann::json::object();
    meta["hash"] = hash;
    if (etag && !etag->empty())
        meta["etag"] = *etag;
    if (lastModified && !lastModified->empty())
        meta["last_modified"] = *lastModified;
    meta["updated_at"] = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();

    auto metaPath = exportMetaPath(exportPath);
    std::error_code ec;
    std::filesystem::create_directories(metaPath.parent_path(), ec);
    std::ofstream out(metaPath, std::ios::binary | std::ios::trunc);
    if (!out)
        return;
    out << meta.dump(2);
}

std::optional<std::string> computeFileSha256(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path))
        return std::nullopt;
    auto verifier = yams::downloader::makeIntegrityVerifierSha256Only();
    verifier->reset(yams::downloader::HashAlgo::Sha256);
    std::ifstream in(path, std::ios::binary);
    if (!in)
        return std::nullopt;
    std::array<char, 1 << 16> buffer{};
    while (in) {
        in.read(buffer.data(), buffer.size());
        auto read = in.gcount();
        if (read > 0) {
            verifier->update(std::span<const std::byte>(
                reinterpret_cast<const std::byte*>(buffer.data()), static_cast<std::size_t>(read)));
        }
    }
    auto result = verifier->finalize();
    if (result.hex.empty())
        return std::nullopt;
    return result.hex;
}

} // namespace

namespace yams::downloader {

namespace fs = std::filesystem;
using nlohmann::json;

// Persistent JSON ResumeStore (sidecar under staging)
// File layout (JSON object keyed by URL):
// {
//   "https://example.com/file.bin": {
//     "etag": "abc123",
//     "last_modified": "Tue, 19 Aug 2025 09:00:00 GMT",
//     "total_bytes": 1048576,
//     "completed_ranges": [[0,524288],[524288,524288]]
//   },
//   ...
// }
class JsonResumeStore final : public IResumeStore {
public:
    explicit JsonResumeStore(const StorageConfig& storage)
        : path_(fs::path(storage.stagingDir) / "downloader" / "resume.json") {
        std::error_code ec;
        fs::create_directories(path_.parent_path(), ec);
        (void)ec;
        (void)loadFile(); // best-effort
    }

    Expected<std::optional<State>> load(std::string_view url) override {
        auto jr = loadFile();
        if (!jr.ok())
            return jr.error();
        auto& root = jr.value();
        auto it = root.find(std::string(url));
        if (it == root.end())
            return std::optional<State>{std::nullopt};
        const auto& entry = it.value();
        State st;
        if (entry.contains("etag") && entry["etag"].is_string()) {
            st.etag = entry["etag"].get<std::string>();
        }
        if (entry.contains("last_modified") && entry["last_modified"].is_string()) {
            st.lastModified = entry["last_modified"].get<std::string>();
        }
        if (entry.contains("total_bytes") && entry["total_bytes"].is_number_unsigned()) {
            st.totalBytes = entry["total_bytes"].get<std::uint64_t>();
        }
        if (entry.contains("completed_ranges") && entry["completed_ranges"].is_array()) {
            for (const auto& r : entry["completed_ranges"]) {
                if (r.is_array() && r.size() == 2 && r[0].is_number_unsigned() &&
                    r[1].is_number_unsigned()) {
                    st.completedRanges.emplace_back(r[0].get<std::uint64_t>(),
                                                    r[1].get<std::uint64_t>());
                }
            }
        }
        return std::optional<State>{st};
    }

    Expected<void> save(std::string_view url, const State& state) override {
        auto jr = loadFile();
        if (!jr.ok())
            return jr.error();
        auto& root = jr.value();

        json entry;
        if (state.etag)
            entry["etag"] = *state.etag;
        if (state.lastModified)
            entry["last_modified"] = *state.lastModified;
        entry["total_bytes"] = state.totalBytes;
        entry["completed_ranges"] = json::array();
        for (const auto& [off, len] : state.completedRanges) {
            entry["completed_ranges"].push_back(json::array({off, len}));
        }
        root[std::string(url)] = entry;
        return writeFile(root);
    }

    void remove(std::string_view url) noexcept override {
        auto jr = loadFile();
        if (!jr.ok())
            return;
        auto& root = jr.value();
        root.erase(std::string(url));
        (void)writeFile(root);
    }

private:
    Expected<json> loadFile() const {
        if (!fs::exists(path_)) {
            return json::object();
        }
        std::ifstream in(path_);
        if (!in) {
            return Error{ErrorCode::IoError, "Failed to open resume JSON for read"};
        }
        try {
            json root;
            in >> root;
            if (!root.is_object())
                root = json::object();
            return root;
        } catch (...) {
            // Corrupt/unreadable -> return fresh object
            return json::object();
        }
    }

    Expected<void> writeFile(const json& root) const {
        std::ofstream out(path_, std::ios::trunc);
        if (!out) {
            return Error{ErrorCode::IoError, "Failed to open resume JSON for write"};
        }
        out << root.dump(2);
        return {};
    }

private:
    fs::path path_;
};

// ---- Forward-declared factories from component .cpps (no public headers yet) ----
std::unique_ptr<IHttpAdapter> makeCurlHttpAdapter();
std::unique_ptr<IDiskWriter> makeDiskWriter();
std::unique_ptr<IResumeStore> makeInMemoryResumeStore();
std::unique_ptr<IRateLimiter> makeRateLimiter();

// ---- Utility: simple session id ----
static std::string makeSessionId(std::string_view url) {
    using clock = std::chrono::steady_clock;
    auto now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now().time_since_epoch())
            .count();
    std::string sid{"sess-"};
    sid.append(std::to_string(now_ns));
    sid.push_back('-');

    // Use a simple hash of the URL to avoid invalid filename characters
    // This creates a unique identifier without special characters
    std::hash<std::string_view> hasher;
    auto url_hash = hasher(url);
    sid.append(std::to_string(url_hash));

    return sid;
}

// ---- Utility: bytes to size_t guard ----
[[maybe_unused]] static std::size_t to_size_t_checked(std::uint64_t v) {
    if (v > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        return static_cast<std::size_t>(std::numeric_limits<std::size_t>::max());
    }
    return static_cast<std::size_t>(v);
}

// ---- DownloadManager implementation ----
class DownloadManager final : public IDownloadManager {
public:
    DownloadManager(StorageConfig storage, DownloaderConfig cfg,
                    std::unique_ptr<IHttpAdapter> http = nullptr,
                    std::unique_ptr<IDiskWriter> disk = nullptr,
                    std::unique_ptr<IIntegrityVerifier> integ = nullptr,
                    std::unique_ptr<IResumeStore> resume = nullptr,
                    std::unique_ptr<IRateLimiter> limiter = nullptr)
        : storage_(std::move(storage)), config_(std::move(cfg)), http_(std::move(http)),
          disk_(std::move(disk)), integ_(std::move(integ)), resume_(std::move(resume)),
          limiter_(std::move(limiter)) {
        if (!http_)
            http_ = makeCurlHttpAdapter();
        if (!disk_)
            disk_ = makeDiskWriter();
        if (!integ_)
            integ_ = makeIntegrityVerifierSha256Only();
        if (!resume_)
            resume_ = std::make_unique<JsonResumeStore>(storage_);
        if (!limiter_)
            limiter_ = makeRateLimiter();
        limiter_->setLimits(config_.rateLimit);
    }

    Expected<FinalResult> download(const DownloadRequest& request,
                                   const ProgressCallback& onProgress,
                                   const ShouldCancel& shouldCancel,
                                   [[maybe_unused]] const LogCallback& onLog) override {
        try {
            // Apply per-request rate limit override if provided
            if (request.rateLimit.globalBps != 0 || request.rateLimit.perConnectionBps != 0) {
                limiter_->setLimits(request.rateLimit);
            } else {
                limiter_->setLimits(config_.rateLimit);
            }

            // Pre-checks
            if (request.url.empty()) {
                return Error{ErrorCode::InvalidArgument, "Empty URL"};
            }
            if (!request.storeOnly) {
                // For MVP, we only support store-only; export is out of scope
                spdlog::debug(
                    "Non store-only requested but not implemented; proceeding with store-only.");
            }

            // Probe capabilities and potential size
            bool resumeSupported = false;
            std::optional<std::uint64_t> contentLength{};
            std::optional<std::string> currentEtag{};
            std::optional<std::string> currentLastModified{};
            std::optional<std::string> currentContentType{};
            std::optional<std::string> suggestedFilename{};
            {
                auto pr =
                    http_->probe(request.url, request.headers, resumeSupported, contentLength,
                                 currentEtag, currentLastModified, currentContentType,
                                 suggestedFilename, request.tls, request.proxy, request.timeout);
                if (!pr.ok()) {
                    return pr.error();
                }
            }

            // Load any prior resume state (persistent JSON store)
            std::optional<IResumeStore::State> priorState;
            if (resume_) {
                auto lr = resume_->load(request.url);
                if (lr.ok())
                    priorState = lr.value();
            }
            std::vector<Range> completedRanges;
            if (priorState && !priorState->completedRanges.empty()) {
                completedRanges = priorState->completedRanges;
                normalizeRanges(completedRanges);
            }

            // Enforce max file size policy if configured
            if (config_.maxFileBytes > 0 && contentLength &&
                *contentLength > config_.maxFileBytes) {
                return Error{ErrorCode::PolicyViolation,
                             "Object exceeds configured max_file_bytes (" +
                                 std::to_string(config_.maxFileBytes) + ")"};
            }

            // Create/open staging file (resume-aware)
            const std::string sessionId = makeSessionId(request.url);
            std::filesystem::path stagingFile;
            std::uint64_t resumeOffset = 0;
            IResumeStore::State resumeState;
            if (priorState) {
                resumeState = *priorState;
            }

            const bool canResume = resumeSupported && priorState.has_value() &&
                                   priorState->etag.has_value() && currentEtag.has_value() &&
                                   (*priorState->etag == *currentEtag);

            if (canResume) {
                std::uint64_t currentSize = 0;
                auto stRes = disk_->createOrOpenStagingFile(storage_, sessionId, ".part",
                                                            contentLength, currentSize);
                if (!stRes.ok()) {
                    return stRes.error();
                }
                stagingFile = stRes.value();

                const auto contiguous = contiguousPrefixLength(completedRanges);
                resumeOffset = std::min(contiguous, currentSize);

                if (currentSize > resumeOffset) {
                    std::error_code resizeEc;
                    std::filesystem::resize_file(stagingFile, resumeOffset, resizeEc);
                    if (resizeEc) {
                        spdlog::warn("Failed to shrink staging file to resume offset: {}",
                                     resizeEc.message());
                        resumeOffset = currentSize;
                    }
                }

                if (completedRanges.empty() && resumeOffset > 0) {
                    appendRange(completedRanges, 0, resumeOffset);
                }

                spdlog::info("Resuming download from offset {} (ETag matched).", resumeOffset);
            } else {
                // Fresh start (or ETag mismatch)
                auto stagingRes = disk_->createStagingFile(storage_, sessionId, ".part");
                if (!stagingRes.ok()) {
                    return stagingRes.error();
                }
                stagingFile = stagingRes.value();
                resumeOffset = 0;
                completedRanges.clear();
            }

            // Prepare integrity verifier (SHA-256 MVP)
            integ_->reset(HashAlgo::Sha256);

            // Running counters
            std::uint64_t written = resumeOffset;
            std::optional<std::uint64_t> totalBytes = contentLength;

            resumeState.completedRanges = completedRanges;
            if (totalBytes)
                resumeState.totalBytes = *totalBytes;
            else if (priorState)
                resumeState.totalBytes = priorState->totalBytes;
            else
                resumeState.totalBytes = resumeOffset;
            resumeState.etag = currentEtag;
            resumeState.lastModified = currentLastModified;

            const std::uint64_t persistThreshold = std::max<std::uint64_t>(
                1024ull * 1024ull, request.chunkSizeBytes != 0 ? request.chunkSizeBytes
                                                               : config_.defaultChunkSizeBytes);
            std::uint64_t lastPersistedCursor = resumeOffset;
            const bool allowResumePersistence = resume_ && request.resume && resumeSupported;

            auto persistState = [&](bool force) {
                if (!allowResumePersistence)
                    return;
                resumeState.completedRanges = completedRanges;
                if (totalBytes)
                    resumeState.totalBytes = *totalBytes;
                else
                    resumeState.totalBytes = std::max(resumeState.totalBytes, written);
                resumeState.etag = currentEtag;
                resumeState.lastModified = currentLastModified;

                if (!force && written < lastPersistedCursor + persistThreshold)
                    return;

                auto saveRes = resume_->save(request.url, resumeState);
                if (!saveRes.ok()) {
                    spdlog::warn("Failed to persist resume state for {}: {}", request.url,
                                 saveRes.error().message);
                } else {
                    lastPersistedCursor = written;
                }
            };

            // Optional: initial progress event
            if (onProgress) {
                ProgressEvent ev;
                ev.url = request.url;
                ev.downloadedBytes = 0;
                ev.totalBytes = totalBytes; // may be nullopt
                ev.stage = ProgressStage::Connecting;
                onProgress(ev);
            }

            // Sink writes streaming chunks to staging and updates SHA-256; rate-limited
            auto sink = [&](std::span<const std::byte> data) -> Expected<void> {
                if (!data.empty()) {
                    // Rate limit (global + per-connection)
                    limiter_->acquire(static_cast<std::uint64_t>(data.size()), shouldCancel);
                }

                const std::uint64_t chunkStart = written;
                // Write at current offset
                auto wr = disk_->writeAt(stagingFile, written, data);
                if (!wr.ok()) {
                    return wr.error();
                }

                // Update integrity
                integ_->update(data);

                // Bump offset
                written += static_cast<std::uint64_t>(data.size());

                appendRange(completedRanges, chunkStart, static_cast<std::uint64_t>(data.size()));
                persistState(false);

                // Enforce max size cap during streaming if no Content-Length was known
                if (config_.maxFileBytes > 0 && written > config_.maxFileBytes) {
                    return Error{ErrorCode::PolicyViolation,
                                 "Exceeded configured max_file_bytes during download"};
                }

                return Expected<void>{};
            };

            // Compose progress callback (forwarding MVP)
            ProgressCallback forwardProgress =
                onProgress ? [&](const ProgressEvent& ev) { onProgress(ev); } : ProgressCallback{};

            // TODO(parallel): If concurrency > 1 and contentLength known, split into ranges and
            // spawn per-range fetches Skeleton (single-stream for now):
            const std::uint64_t offset = resumeOffset;
            const std::uint64_t size = totalBytes && *totalBytes > resumeOffset
                                           ? (*totalBytes - resumeOffset)
                                           : 0; // 0 => open-ended
            auto fr = http_->fetchRange(request.url, request.headers, offset, size, request.tls,
                                        request.proxy, request.timeout, sink, shouldCancel,
                                        forwardProgress);
            if (!fr.ok()) {
                disk_->cleanup(stagingFile);
                return fr.error();
            }

            persistState(true);

            // Post-download: sync staging file and directory
            auto sr = disk_->sync(stagingFile, storage_);
            if (!sr.ok()) {
                disk_->cleanup(stagingFile);
                return sr.error();
            }

            // Integrity finalize
            auto digest = integ_->finalize(); // algo: Sha256 MVP
            if (digest.hex.empty()) {
                disk_->cleanup(stagingFile);
                return Error{ErrorCode::IoError, "Failed to finalize checksum"};
            }

            // If a checksum is provided in request, enforce verification
            if (request.checksum.has_value()) {
                const auto& exp = *request.checksum;
                // MVP supports SHA-256; if other algo requested, treat as mismatch for now
                bool algo_ok = (exp.algo == HashAlgo::Sha256);
                bool hex_ok = algo_ok && (to_lower(exp.hex) == to_lower(digest.hex));
                if (!algo_ok || !hex_ok) {
                    disk_->cleanup(stagingFile);
                    return Error{ErrorCode::ChecksumMismatch, "Checksum mismatch (expected " +
                                                                  exp.hex + ", got " + digest.hex +
                                                                  ")"};
                }
            }

            // Finalize into CAS
            auto finalRes = disk_->finalizeToCas(stagingFile, digest.hex, storage_);
            if (!finalRes.ok()) {
                disk_->cleanup(stagingFile);
                return finalRes.error();
            }

            // Persist resume metadata (validators and total size) for future resumes
            if (resume_ && allowResumePersistence) {
                resume_->remove(request.url);
            }

            // Optional export after CAS finalize
            if (request.exportPath) {
                const auto& exportPath = *request.exportPath;
                std::error_code fec;
                const bool exists = std::filesystem::exists(exportPath, fec);
                if (exists && request.overwrite == OverwritePolicy::Never) {
                    disk_->cleanup(stagingFile);
                    return Error{ErrorCode::PolicyViolation,
                                 "Export path exists and overwrite=never"};
                }

                bool skipCopy = false;
                if (exists && request.overwrite == OverwritePolicy::IfDifferentEtag) {
                    auto meta = loadExportMetadata(exportPath);
                    if (meta) {
                        if (currentEtag && meta->contains("etag") &&
                            (*currentEtag == (*meta)["etag"].get<std::string>())) {
                            skipCopy = true;
                        } else if (meta->contains("hash")) {
                            try {
                                if ((*meta)["hash"].get<std::string>() == digest.hex) {
                                    skipCopy = true;
                                }
                            } catch (...) {
                                skipCopy = false;
                            }
                        }
                    }
                    if (!skipCopy) {
                        auto existingHash = computeFileSha256(exportPath);
                        if (existingHash && *existingHash == digest.hex) {
                            skipCopy = true;
                        }
                    }
                    if (skipCopy) {
                        writeExportMetadata(exportPath, digest.hex, currentEtag,
                                            currentLastModified);
                    }
                }

                if (!skipCopy) {
                    auto copyOpts = std::filesystem::copy_options::none;
                    if (exists && request.overwrite != OverwritePolicy::Never) {
                        copyOpts |= std::filesystem::copy_options::overwrite_existing;
                    }
                    std::filesystem::create_directories(exportPath.parent_path(), fec);
                    std::filesystem::copy_file(finalRes.value(), exportPath, copyOpts, fec);
                    if (fec) {
                        disk_->cleanup(stagingFile);
                        return Error{ErrorCode::IoError, "Failed to export file: " + fec.message()};
                    }
                    writeExportMetadata(exportPath, digest.hex, currentEtag, currentLastModified);
                }
            }

            // Build final result
            FinalResult out;
            out.url = request.url;
            out.hash = makeSha256Id(digest.hex);
            out.storedPath = finalRes.value();
            out.sizeBytes = written;
            out.success = true;
            out.checksumOk = true;
            if (currentEtag)
                out.etag = *currentEtag;
            if (currentLastModified)
                out.lastModified = *currentLastModified;
            if (currentContentType)
                out.contentType = *currentContentType;
            if (suggestedFilename)
                out.suggestedName = *suggestedFilename;

            if (onProgress) {
                ProgressEvent ev;
                ev.url = request.url;
                ev.downloadedBytes = written;
                ev.totalBytes = totalBytes;
                ev.percentage =
                    (totalBytes && *totalBytes)
                        ? static_cast<float>((static_cast<long double>(written) * 100.0L) /
                                             static_cast<long double>(*totalBytes))
                        : std::optional<float>{};
                ev.stage = ProgressStage::Finalizing;
                onProgress(ev);
            }

            return out;
        } catch (const std::exception& ex) {
            return Error{ErrorCode::Unknown, std::string("Exception: ") + ex.what()};
        } catch (...) {
            return Error{ErrorCode::Unknown, "Unknown exception during download"};
        }
    }

    std::vector<Expected<FinalResult>> downloadMany(const std::vector<DownloadRequest>& requests,
                                                    const ProgressCallback& onProgress,
                                                    const ShouldCancel& shouldCancel,
                                                    const LogCallback& onLog) override {
        std::vector<Expected<FinalResult>> results;
        results.reserve(requests.size());
        for (const auto& r : requests) {
            results.emplace_back(download(r, onProgress, shouldCancel, onLog));
        }
        return results;
    }

    [[nodiscard]] StorageConfig storageConfig() const override { return storage_; }
    [[nodiscard]] DownloaderConfig config() const override { return config_; }

private:
    static std::string to_lower(const std::string& s) {
        std::string out;
        out.reserve(s.size());
        for (unsigned char c : s) {
            out.push_back(static_cast<char>(std::tolower(c)));
        }
        return out;
    }

private:
    StorageConfig storage_;
    DownloaderConfig config_;

    std::unique_ptr<IHttpAdapter> http_;
    std::unique_ptr<IDiskWriter> disk_;
    std::unique_ptr<IIntegrityVerifier> integ_;
    std::unique_ptr<IResumeStore> resume_;
    std::unique_ptr<IRateLimiter> limiter_;
};

// ---- Optional factory to construct a default DownloadManager ----
std::unique_ptr<IDownloadManager> makeDownloadManagerWithDependencies(
    const StorageConfig& storage, const DownloaderConfig& cfg, std::unique_ptr<IHttpAdapter> http,
    std::unique_ptr<IDiskWriter> disk, std::unique_ptr<IIntegrityVerifier> integ,
    std::unique_ptr<IResumeStore> resume, std::unique_ptr<IRateLimiter> limiter) {
    return std::make_unique<DownloadManager>(storage, cfg, std::move(http), std::move(disk),
                                             std::move(integ), std::move(resume),
                                             std::move(limiter));
}

std::unique_ptr<IDownloadManager> makeDownloadManager(const StorageConfig& storage,
                                                      const DownloaderConfig& cfg) {
    return makeDownloadManagerWithDependencies(storage, cfg, nullptr, nullptr, nullptr, nullptr,
                                               nullptr);
}

} // namespace yams::downloader
