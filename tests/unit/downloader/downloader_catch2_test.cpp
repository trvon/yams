// Catch2 migration of test_downloader_basic.cpp with expanded test coverage
// Epic: yams-3s4 | Migration: downloader tests

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

#include <nlohmann/json.hpp>
#include <yams/downloader/downloader.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace yams::downloader;

namespace yams::downloader {
std::unique_ptr<IDiskWriter> makeDiskWriter();
std::unique_ptr<IIntegrityVerifier> makeIntegrityVerifierSha256Only();
std::unique_ptr<IResumeStore> makeInMemoryResumeStore();
std::unique_ptr<IRateLimiter> makeRateLimiter();
std::unique_ptr<IHttpAdapter> makeCurlHttpAdapter();
std::string resolveCaBundle(const TlsConfig& tls, std::span<const std::string_view> candidates);
} // namespace yams::downloader

namespace {

// Catch2 assertion macros decompose comparisons for diagnostics, which triggers
// clang-tidy chained-comparison false positives. This migrated test also has a
// few harmless integer-size fixture constants.
// NOLINTBEGIN(bugprone-chained-comparison, bugprone-implicit-widening-of-multiplication-result)

// Helper to generate a unique temporary directory for tests
fs::path make_temp_dir(const std::string& prefix = "yams-dl-test-") {
    auto base = fs::temp_directory_path();
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<unsigned long long> dist;
    fs::path dir;
    for (int i = 0; i < 5; ++i) {
        dir = base / (prefix + std::to_string(dist(gen)));
        if (!fs::exists(dir)) {
            fs::create_directories(dir);
            break;
        }
    }
    return dir;
}

// Tracking resume store for test verification
class TrackingResumeStore final : public IResumeStore {
public:
    std::optional<State> initialState;
    std::vector<State> savedStates;
    std::vector<std::string> savedUrls;
    std::vector<std::string> loadUrls;
    std::optional<std::string> removedUrl;

    Expected<std::optional<State>> load(std::string_view url) override {
        loadUrls.emplace_back(url);
        return initialState;
    }

    Expected<void> save(std::string_view url, const State& state) override {
        savedUrls.emplace_back(url);
        savedStates.push_back(state);
        initialState = state;
        return {};
    }

    void remove(std::string_view url) noexcept override {
        removedUrl = std::string(url);
        initialState.reset();
    }
};

// Test HTTP adapter
class TestHttpAdapter final : public IHttpAdapter {
public:
    bool resumeSupported{true};
    std::optional<std::uint64_t> contentLength{};
    std::optional<std::string> etag{};
    std::optional<std::string> lastModified{};
    std::vector<std::uint64_t> requestedOffsets;
    std::vector<std::uint64_t> requestedSizes;
    std::vector<std::byte> payload;
    bool checkCancelBeforeSink{false};
    bool cancelObserved{false};
    bool lastProbeFollowRedirects{true};
    int probeCallCount{0};
    int fetchCallCount{0};

    Expected<void> probe(std::string_view, const std::vector<Header>&, bool& resumeSupportedOut,
                         std::optional<std::uint64_t>& contentLengthOut,
                         std::optional<std::string>& etagOut,
                         std::optional<std::string>& lastModifiedOut,
                         std::optional<std::string>& contentTypeOut,
                         std::optional<std::string>& suggestedFilenameOut, const TlsConfig&,
                         const std::optional<std::string>&, bool followRedirects,
                         std::chrono::milliseconds) override {
        probeCallCount++;
        lastProbeFollowRedirects = followRedirects;
        resumeSupportedOut = resumeSupported;
        contentLengthOut = contentLength;
        etagOut = etag;
        lastModifiedOut = lastModified;
        contentTypeOut.reset();
        suggestedFilenameOut.reset();
        return {};
    }

    Expected<void> fetchRange(std::string_view, const std::vector<Header>&, std::uint64_t offset,
                              std::uint64_t size, const TlsConfig&,
                              const std::optional<std::string>&, std::chrono::milliseconds,
                              const std::function<Expected<void>(std::span<const std::byte>)>& sink,
                              const ShouldCancel& shouldCancel,
                              const ProgressCallback& onProgress) override {
        fetchCallCount++;
        requestedOffsets.push_back(offset);
        requestedSizes.push_back(size);
        if (onProgress) {
            ProgressEvent ev;
            ev.downloadedBytes = 0;
            ev.stage = ProgressStage::Downloading;
            onProgress(ev);
        }
        if (checkCancelBeforeSink && shouldCancel && shouldCancel()) {
            cancelObserved = true;
            return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
        }
        if (!payload.empty()) {
            auto res = sink(std::span<const std::byte>(payload.data(), payload.size()));
            if (!res.ok())
                return res;
        }
        return {};
    }
};

// Fake disk writer for testing
class FakeDiskWriter final : public IDiskWriter {
public:
    explicit FakeDiskWriter(StorageConfig storage) : storage_(std::move(storage)) {}

    void setInitialData(const std::vector<std::byte>& data) { initialData_ = data; }

    const std::vector<std::byte>& stagedData() const { return data_; }

    const fs::path& lastFinalPath() const { return lastFinalPath_; }

    const std::string& lastSessionId() const { return lastSessionId_; }

    Expected<fs::path> createStagingFile(const StorageConfig&, std::string_view sessionId,
                                         std::string_view tempExtension) override {
        lastSessionId_ = std::string(sessionId);
        currentPath_ = stagingPath(sessionId, tempExtension);
        data_.clear();
        writeToFile();
        return currentPath_;
    }

    Expected<fs::path> createOrOpenStagingFile(const StorageConfig&, std::string_view sessionId,
                                               std::string_view tempExtension,
                                               std::optional<std::uint64_t> expectedSize,
                                               std::uint64_t& currentSize) override {
        lastSessionId_ = std::string(sessionId);
        currentPath_ = stagingPath(sessionId, tempExtension);
        data_ = initialData_;
        currentSize = data_.size();
        writeToFile();
        if (expectedSize && currentSize == 0) {
            std::error_code ec;
            std::filesystem::resize_file(currentPath_, static_cast<std::uint64_t>(*expectedSize),
                                         ec);
            if (!ec) {
                data_.resize(static_cast<std::size_t>(*expectedSize));
                writeToFile();
            }
        }
        return currentPath_;
    }

    Expected<void> writeAt(const fs::path&, std::uint64_t offset,
                           std::span<const std::byte> data) override {
        const auto end = offset + static_cast<std::uint64_t>(data.size());
        if (end > data_.size()) {
            data_.resize(static_cast<std::size_t>(end));
        }
        std::memcpy(data_.data() + offset, data.data(), data.size());
        writeToFile();
        return {};
    }

    Expected<void> sync(const fs::path&, const StorageConfig&) override { return {}; }

    Expected<fs::path> finalizeToCas(const fs::path&, std::string_view hashHex,
                                     const StorageConfig&) override {
        lastFinalPath_ = casPathForSha256(storage_.objectsDir, hashHex);
        std::error_code ec;
        std::filesystem::create_directories(lastFinalPath_.parent_path(), ec);
        std::ofstream out(lastFinalPath_, std::ios::binary | std::ios::trunc);
        if (!out) {
            return Error{ErrorCode::IoError,
                         "Failed to write CAS object: " + lastFinalPath_.string()};
        }
        if (!data_.empty()) {
            out.write(reinterpret_cast<const char*>(data_.data()),
                      static_cast<std::streamsize>(data_.size()));
        }
        return lastFinalPath_;
    }

    void cleanup(const fs::path& stagingFile) noexcept override {
        std::error_code ec;
        std::filesystem::remove(stagingFile, ec);
    }

private:
    fs::path stagingPath(std::string_view sessionId, std::string_view ext) const {
        fs::path dir = storage_.stagingDir / "downloader";
        std::filesystem::create_directories(dir);
        std::string fn(sessionId);
        if (!ext.empty()) {
            if (ext.front() == '.')
                fn.append(ext);
            else {
                fn.push_back('.');
                fn.append(ext);
            }
        } else {
            fn.append(".part");
        }
        return dir / fn;
    }

    void writeToFile() {
        if (currentPath_.empty())
            return;
        std::error_code ec;
        std::filesystem::create_directories(currentPath_.parent_path(), ec);
        std::ofstream out(currentPath_, std::ios::binary | std::ios::trunc);
        if (!out)
            return;
        if (!data_.empty()) {
            out.write(reinterpret_cast<const char*>(data_.data()),
                      static_cast<std::streamsize>(data_.size()));
        }
    }

    StorageConfig storage_;
    std::vector<std::byte> initialData_{};
    std::vector<std::byte> data_{};
    fs::path currentPath_{};
    fs::path lastFinalPath_{};
    std::string lastSessionId_{};
};

std::vector<std::byte> to_bytes(std::string_view s) {
    std::vector<std::byte> out(s.size());
    std::memcpy(out.data(), s.data(), s.size());
    return out;
}

constexpr const char kExportMetaSuffix[] = ".yams.meta.json";

fs::path meta_path_for(const fs::path& exportPath) {
    return fs::path(exportPath.string() + kExportMetaSuffix);
}

} // namespace

// =============================================================================
// CAS Helpers Tests
// =============================================================================

TEST_CASE("CAS Helpers: Path and ID formatting", "[downloader][cas]") {
    fs::path objectsDir = "/tmp/yams-objects";
    const std::string digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    SECTION("CAS path includes shard directories") {
        auto path = yams::downloader::casPathForSha256(objectsDir, digest);
        CHECK((path.generic_string().find("sha256/01/23/0123456789abcdef") != std::string::npos));
    }

    SECTION("SHA256 ID format") {
        auto id = yams::downloader::makeSha256Id(digest);
        CHECK((id == "sha256:" + digest));
    }

    SECTION("Empty digest") {
        auto id = yams::downloader::makeSha256Id("");
        CHECK((id == "sha256:"));
    }
}

// =============================================================================
// Download Manager Resume Tests
// =============================================================================

TEST_CASE("DownloadManager: Resume functionality", "[downloader][resume]") {
    auto tempDir = make_temp_dir();
    StorageConfig storage{tempDir / "objects", tempDir / "staging"};
    DownloaderConfig cfg;
    cfg.defaultChunkSizeBytes = 4;

    SECTION("Persists and cleans ranges") {
        auto http = std::make_unique<TestHttpAdapter>();
        auto* httpPtr = http.get();
        httpPtr->resumeSupported = true;
        httpPtr->contentLength = 10;
        httpPtr->etag = "etag-123";
        httpPtr->payload = to_bytes("WORLD");

        auto disk = std::make_unique<FakeDiskWriter>(storage);
        auto* diskPtr = disk.get();
        diskPtr->setInitialData(to_bytes("HELLO"));

        auto resume = std::make_unique<TrackingResumeStore>();
        auto* resumePtr = resume.get();
        IResumeStore::State initial;
        initial.totalBytes = 10;
        initial.completedRanges.emplace_back(0, 5);
        initial.etag = "etag-123";
        resumePtr->initialState = initial;

        auto dm = makeDownloadManagerWithDependencies(
            storage, cfg, std::move(http), std::move(disk), nullptr, std::move(resume), nullptr);

        DownloadRequest req;
        req.url = "https://example.com/file.bin";
        req.resume = true;
        req.chunkSizeBytes = 5;
        req.checksum = Checksum{HashAlgo::Sha256,
                                "0b21b7db59cd154904fac6336fa7d2be1bab38d632794f281549584068cdcb74"};

        auto result = dm->download(req);
        REQUIRE(result.ok());
        auto final = result.value();

        REQUIRE((httpPtr->requestedOffsets.size() == 1));
        CHECK((httpPtr->requestedOffsets.front() == 5));

        REQUIRE_FALSE(resumePtr->savedStates.empty());
        const auto& lastState = resumePtr->savedStates.back();
        REQUIRE((lastState.completedRanges.size() == 1));
        CHECK((lastState.completedRanges.front().first == 0));
        CHECK((lastState.completedRanges.front().second == 10));

        REQUIRE(resumePtr->removedUrl.has_value());
        CHECK((*resumePtr->removedUrl == req.url));

        REQUIRE_FALSE(diskPtr->lastFinalPath().empty());
        std::ifstream finalFile(diskPtr->lastFinalPath(), std::ios::binary);
        REQUIRE(finalFile.good());
        std::string contents((std::istreambuf_iterator<char>(finalFile)),
                             std::istreambuf_iterator<char>());
        CHECK((contents == "HELLOWORLD"));

        CHECK(final.hash ==
              "sha256:0b21b7db59cd154904fac6336fa7d2be1bab38d632794f281549584068cdcb74");
        CHECK((final.sizeBytes == 10));
    }

    SECTION("Uses a stable resume session id for the same URL") {
        auto makeResumingManager = [&](FakeDiskWriter*& diskPtr) {
            auto http = std::make_unique<TestHttpAdapter>();
            auto* httpPtr = http.get();
            httpPtr->resumeSupported = true;
            httpPtr->contentLength = 10;
            httpPtr->etag = "etag-123";
            httpPtr->payload = to_bytes("WORLD");

            auto disk = std::make_unique<FakeDiskWriter>(storage);
            diskPtr = disk.get();
            diskPtr->setInitialData(to_bytes("HELLO"));

            auto resume = std::make_unique<TrackingResumeStore>();
            IResumeStore::State initial;
            initial.totalBytes = 10;
            initial.completedRanges.emplace_back(0, 5);
            initial.etag = "etag-123";
            resume->initialState = initial;

            return makeDownloadManagerWithDependencies(storage, cfg, std::move(http),
                                                       std::move(disk), nullptr, std::move(resume),
                                                       nullptr);
        };

        FakeDiskWriter* firstDisk = nullptr;
        auto firstManager = makeResumingManager(firstDisk);

        DownloadRequest req;
        req.url = "https://example.com/file.bin";
        req.resume = true;

        auto firstResult = firstManager->download(req);
        REQUIRE(firstResult.ok());
        REQUIRE_FALSE(firstDisk->lastSessionId().empty());

        FakeDiskWriter* secondDisk = nullptr;
        auto secondManager = makeResumingManager(secondDisk);

        auto secondResult = secondManager->download(req);
        REQUIRE(secondResult.ok());
        REQUIRE_FALSE(secondDisk->lastSessionId().empty());
        CHECK((secondDisk->lastSessionId() == firstDisk->lastSessionId()));
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

TEST_CASE("DownloadManager: probe forwards follow_redirects policy", "[downloader][redirect]") {
    auto tempDir = make_temp_dir();
    StorageConfig storage{tempDir / "objects", tempDir / "staging"};
    DownloaderConfig cfg;

    auto http = std::make_unique<TestHttpAdapter>();
    auto* httpPtr = http.get();
    httpPtr->resumeSupported = false;
    httpPtr->contentLength = 5;
    httpPtr->payload = to_bytes("ABCDE");

    auto disk = std::make_unique<FakeDiskWriter>(storage);
    auto dm = makeDownloadManagerWithDependencies(storage, cfg, std::move(http), std::move(disk),
                                                  nullptr, nullptr, nullptr);

    DownloadRequest req;
    req.url = "https://example.com/no-redirects.bin";
    req.followRedirects = false;

    auto result = dm->download(req);
    REQUIRE(result.ok());
    CHECK_FALSE(httpPtr->lastProbeFollowRedirects);

    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

TEST_CASE("DownloadManager: verifies configured checksum algorithms",
          "[downloader][checksum][algorithms]") {
    const auto sha256 =
        std::string{"f0393febe8baaa55e32f7be2a7cc180bf34e52137d99e056c817a9c07b8f239a"};
    const auto sha512 =
        std::string{"9989a8fcbc29044b5883a0a36c146fe7415b1439e995b4d806ea0af7da9ca4390eb92a604b3ecf"
                    "a3d75f9911c768fbe2aecc59eff1e48dcaeca1957bdde01dfb"};
    const auto md5 = std::string{"2ecdde3959051d913f61b14579ea136d"};

    auto runCase = [&](HashAlgo algo, const std::string& expectedHex) {
        auto tempDir = make_temp_dir();
        StorageConfig storage{tempDir / "objects", tempDir / "staging"};
        DownloaderConfig cfg;

        auto http = std::make_unique<TestHttpAdapter>();
        http->resumeSupported = false;
        http->contentLength = 5;
        http->payload = to_bytes("ABCDE");

        auto disk = std::make_unique<FakeDiskWriter>(storage);
        auto dm = makeDownloadManagerWithDependencies(storage, cfg, std::move(http),
                                                      std::move(disk), nullptr, nullptr, nullptr);

        DownloadRequest req;
        req.url = "https://example.com/checksum.bin";
        req.checksum = Checksum{algo, expectedHex};

        auto result = dm->download(req);
        REQUIRE(result.ok());
        CHECK(result.value().success);

        std::error_code ec;
        fs::remove_all(tempDir, ec);
    };

    SECTION("sha256") {
        runCase(HashAlgo::Sha256, sha256);
    }
    SECTION("sha512") {
        runCase(HashAlgo::Sha512, sha512);
    }
    SECTION("md5") {
        runCase(HashAlgo::Md5, md5);
    }
}

TEST_CASE("DownloadManager: Export with ETag matching", "[downloader][export]") {
    auto tempDir = make_temp_dir();
    StorageConfig storage{tempDir / "objects", tempDir / "staging"};
    DownloaderConfig cfg;

    SECTION("Skips copy when ETag matches") {
        auto http = std::make_unique<TestHttpAdapter>();
        auto* httpPtr = http.get();
        httpPtr->resumeSupported = false;
        httpPtr->contentLength = 5;
        httpPtr->etag = "tag-777";
        httpPtr->payload = to_bytes("ABCDE");

        auto disk = std::make_unique<FakeDiskWriter>(storage);
        auto* diskPtr = disk.get();

        auto resume = std::make_unique<TrackingResumeStore>();
        auto* resumePtr = resume.get();

        auto dm = makeDownloadManagerWithDependencies(
            storage, cfg, std::move(http), std::move(disk), nullptr, std::move(resume), nullptr);

        fs::path exportDir = tempDir / "exports";
        fs::create_directories(exportDir);
        fs::path exportPath = exportDir / "file.bin";
        {
            std::ofstream out(exportPath, std::ios::binary | std::ios::trunc);
            out << "OLD";
        }
        auto metaPath = meta_path_for(exportPath);
        {
            json meta;
            meta["etag"] = "tag-777";
            meta["hash"] = "placeholder";
            std::ofstream out(metaPath, std::ios::binary | std::ios::trunc);
            out << meta.dump(2);
        }

        DownloadRequest req;
        req.url = "https://example.com/export.bin";
        req.exportPath = exportPath;
        req.overwrite = OverwritePolicy::IfDifferentEtag;

        auto result = dm->download(req);
        REQUIRE(result.ok());
        auto final = result.value();

        std::string exported;
        {
            std::ifstream in(exportPath, std::ios::binary);
            exported.assign((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        }
        CHECK((exported == "OLD"));

        json metaUpdated;
        {
            std::ifstream in(metaPath);
            REQUIRE(in.good());
            in >> metaUpdated;
        }
        REQUIRE(metaUpdated.contains("hash"));
        REQUIRE(metaUpdated.contains("etag"));
        CHECK((metaUpdated["etag"].get<std::string>() == "tag-777"));

        REQUIRE_FALSE(diskPtr->lastFinalPath().empty());
        std::ifstream casFile(diskPtr->lastFinalPath(), std::ios::binary);
        std::string casData((std::istreambuf_iterator<char>(casFile)),
                            std::istreambuf_iterator<char>());
        CHECK((casData == "ABCDE"));

        CHECK(resumePtr->savedStates.empty());
        CHECK((httpPtr->requestedOffsets.size() == 1));
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

TEST_CASE("DownloadManager: exported files are writable and carry no sidecar by default",
          "[downloader][export]") {
    auto tempDir = make_temp_dir();
    StorageConfig storage{tempDir / "objects", tempDir / "staging"};
    DownloaderConfig cfg;
    auto http = std::make_unique<TestHttpAdapter>();
    http->resumeSupported = false;
    http->contentLength = 5;
    http->etag = "tag-1";
    http->payload = to_bytes("ABCDE");
    // Real disk writer: CAS objects are finalized read-only.
    auto dm = makeDownloadManagerWithDependencies(storage, cfg, std::move(http), nullptr, nullptr,
                                                  nullptr, nullptr);

    const fs::path exportPath = tempDir / "exports" / "table.smstat";
    DownloadRequest req;
    req.url = "https://example.com/table.smstat";
    req.exportPath = exportPath;
    req.overwrite = OverwritePolicy::Always;

    auto result = dm->download(req);
    REQUIRE(result.ok());
    REQUIRE(result.value().success);
    REQUIRE(fs::exists(exportPath));
    CHECK_FALSE(fs::exists(meta_path_for(exportPath)));
#if !defined(_WIN32)
    const auto exportPerms = fs::status(exportPath).permissions();
    CHECK((exportPerms & fs::perms::owner_write) != fs::perms::none);
    const auto casPerms = fs::status(result.value().storedPath).permissions();
    CHECK((casPerms & fs::perms::owner_write) == fs::perms::none);
#endif

    // A second export over the existing file succeeds, also over a read-only export left by
    // older versions.
#if !defined(_WIN32)
    fs::permissions(exportPath,
                    fs::perms::owner_read | fs::perms::group_read | fs::perms::others_read,
                    fs::perm_options::replace);
#endif
    auto again = dm->download(req);
    REQUIRE(again.ok());
    CHECK(again.value().success);
#if !defined(_WIN32)
    CHECK((fs::status(exportPath).permissions() & fs::perms::owner_write) != fs::perms::none);
#endif

    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

TEST_CASE("DownloadManager: Cooperative cancellation stops fetch", "[downloader][cancel][catch2]") {
    auto tempDir = make_temp_dir();
    StorageConfig storage{tempDir / "objects", tempDir / "staging"};
    DownloaderConfig cfg;

    auto http = std::make_unique<TestHttpAdapter>();
    auto* httpPtr = http.get();
    httpPtr->resumeSupported = false;
    httpPtr->contentLength = 5;
    httpPtr->payload = to_bytes("ABCDE");
    httpPtr->checkCancelBeforeSink = true;

    auto disk = std::make_unique<FakeDiskWriter>(storage);

    auto dm = makeDownloadManagerWithDependencies(storage, cfg, std::move(http), std::move(disk),
                                                  nullptr, nullptr, nullptr);

    DownloadRequest req;
    req.url = "https://example.com/cancel.bin";

    bool canceled = true;
    auto result = dm->download(req, {}, [&]() { return canceled; });
    REQUIRE_FALSE(result.ok());
    CHECK((result.error().code == ErrorCode::OperationCancelled));
    CHECK(httpPtr->cancelObserved);

    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

TEST_CASE("DownloadManager: reports every progress stage in order",
          "[downloader][progress][catch2]") {
    // ProgressStage::Resolving and ::Verifying were declared, and the init, model and download
    // commands render them, but the manager never emitted either: the probe and the checksum
    // check ran silently. Only the manager's own events are checked; the fake adapter emits its
    // own Downloading event from inside fetchRange.
    auto tempDir = make_temp_dir("yams-dl-stages-");
    StorageConfig storage{tempDir / "objects", tempDir / "staging"};
    DownloaderConfig cfg;

    auto http = std::make_unique<TestHttpAdapter>();
    http->resumeSupported = false;
    http->contentLength = 5;
    http->payload = to_bytes("ABCDE");
    auto disk = std::make_unique<FakeDiskWriter>(storage);
    auto dm = makeDownloadManagerWithDependencies(storage, cfg, std::move(http), std::move(disk),
                                                  nullptr, nullptr, nullptr);

    DownloadRequest req;
    req.url = "https://example.com/stages.bin";
    req.checksum = Checksum{HashAlgo::Sha256,
                            "f0393febe8baaa55e32f7be2a7cc180bf34e52137d99e056c817a9c07b8f239a"};

    std::vector<ProgressStage> stages;
    auto result = dm->download(req, [&](const ProgressEvent& ev) { stages.push_back(ev.stage); });
    REQUIRE(result.ok());

    auto indexOf = [&](ProgressStage stage) {
        return std::find(stages.begin(), stages.end(), stage) - stages.begin();
    };
    const auto end = static_cast<std::ptrdiff_t>(stages.size());
    REQUIRE(indexOf(ProgressStage::Resolving) < end);
    REQUIRE(indexOf(ProgressStage::Connecting) < end);
    REQUIRE(indexOf(ProgressStage::Verifying) < end);
    REQUIRE(indexOf(ProgressStage::Finalizing) < end);
    CHECK(indexOf(ProgressStage::Resolving) < indexOf(ProgressStage::Connecting));
    CHECK(indexOf(ProgressStage::Connecting) < indexOf(ProgressStage::Verifying));
    CHECK(indexOf(ProgressStage::Verifying) < indexOf(ProgressStage::Finalizing));

    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

TEST_CASE("CurlHttpAdapter: a user cancel is reported as OperationCancelled",
          "[downloader][cancel][curl][catch2]") {
    // The fake adapter above returns OperationCancelled on its own, so the manager-level
    // cancellation test never exercised the real adapter, which reported a user cancel as
    // PolicyViolation. DownloadService only recognizes OperationCancelled, so a cancel reached
    // callers as a network error. A file:// transfer drives the real curl write callback
    // without a network.
    auto tempDir = make_temp_dir("yams-dl-curl-cancel-");
    const auto payloadPath = tempDir / "payload.bin";
    {
        std::ofstream out(payloadPath, std::ios::binary);
        const std::string chunk(64 * 1024, 'x');
        out.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
    }

    auto http = makeCurlHttpAdapter();
    REQUIRE(http);
    const std::string url = "file://" + payloadPath.string();
    auto result = http->fetchRange(
        url, {}, 0, 0, TlsConfig{}, std::nullopt, std::chrono::milliseconds{5000},
        [](std::span<const std::byte>) -> Expected<void> { return {}; }, [] { return true; }, {});

    REQUIRE_FALSE(result.ok());
    CHECK((result.error().code == ErrorCode::OperationCancelled));

    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

// =============================================================================
// Concrete downloader component tests
// =============================================================================

TEST_CASE("Concrete downloader components cover storage resume integrity and rate limits",
          "[downloader][concrete]") {
    SECTION("InMemoryResumeStore rejects empty keys and round-trips state") {
        auto store = makeInMemoryResumeStore();
        REQUIRE((store != nullptr));

        auto emptyLoad = store->load("");
        CHECK_FALSE(emptyLoad.ok());
        CHECK((emptyLoad.error().code == ErrorCode::InvalidArgument));

        IResumeStore::State state;
        state.etag = "etag-1";
        state.lastModified = "Mon, 01 Jan 2024 00:00:00 GMT";
        state.totalBytes = 42;
        state.completedRanges = {{0, 10}, {10, 32}};
        REQUIRE(store->save("https://example.test/file", state).ok());

        auto loaded = store->load("https://example.test/file");
        REQUIRE(loaded.ok());
        REQUIRE(loaded.value().has_value());
        CHECK((loaded.value()->etag == state.etag));
        CHECK((loaded.value()->lastModified == state.lastModified));
        CHECK((loaded.value()->totalBytes == 42));
        CHECK((loaded.value()->completedRanges == state.completedRanges));

        store->remove("https://example.test/file");
        auto removed = store->load("https://example.test/file");
        REQUIRE(removed.ok());
        CHECK_FALSE(removed.value().has_value());
    }

    SECTION("OpenSSL integrity verifier hashes bytes and resets after finalize") {
        auto verifier = makeIntegrityVerifierSha256Only();
        REQUIRE((verifier != nullptr));
        verifier->reset(HashAlgo::Sha256);
        auto data = to_bytes("abc");
        verifier->update(data);
        auto sum = verifier->finalize();
        CHECK((sum.algo == HashAlgo::Sha256));
        CHECK((sum.hex == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));

        verifier->update(data);
        auto sum2 = verifier->finalize();
        CHECK((sum2.hex == sum.hex));
    }

    SECTION("RateLimiter supports unlimited and cooperative cancellation fast paths") {
        auto limiter = makeRateLimiter();
        REQUIRE((limiter != nullptr));
        limiter->setLimits(RateLimit{});
        const auto start = std::chrono::steady_clock::now();
        limiter->acquire(1024, [] { return false; });
        CHECK((std::chrono::steady_clock::now() - start < std::chrono::milliseconds(100)));

        RateLimit limited;
        limited.globalBps = 1;
        limiter->setLimits(limited);
        bool cancelled = false;
        limiter->acquire(1024, [&] {
            cancelled = true;
            return true;
        });
        CHECK(cancelled);
    }

    SECTION("DiskWriter stages writes and finalizes into SHA-256 CAS layout") {
        auto tempDir = make_temp_dir("yams-dl-concrete-");
        StorageConfig storage{tempDir / "objects", tempDir / "staging"};
        auto writer = makeDiskWriter();
        REQUIRE((writer != nullptr));

        auto staging = writer->createStagingFile(storage, "session-abc", "part");
        REQUIRE(staging.ok());
        CHECK((staging.value().extension() == ".part"));
        CHECK(fs::exists(staging.value()));

        auto data = to_bytes("abc");
        REQUIRE(writer->writeAt(staging.value(), 0, data).ok());
        REQUIRE(writer->sync(staging.value(), storage).ok());

        const std::string digest =
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        auto finalPath = writer->finalizeToCas(staging.value(), digest, storage);
        REQUIRE(finalPath.ok());
        CHECK((finalPath.value() == casPathForSha256(storage.objectsDir, digest)));
        CHECK(fs::exists(finalPath.value()));
        CHECK_FALSE(fs::exists(staging.value()));

        std::ifstream in(finalPath.value(), std::ios::binary);
        std::string contents((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());
        CHECK((contents == "abc"));

        std::uint64_t currentSize = 0;
        auto resumed =
            writer->createOrOpenStagingFile(storage, "session-resume", ".tmp", 4096, currentSize);
        REQUIRE(resumed.ok());
        CHECK((currentSize == 0));
        REQUIRE(writer->writeAt(resumed.value(), 4, data).ok());
        auto reopened = writer->createOrOpenStagingFile(storage, "session-resume", ".tmp",
                                                        std::nullopt, currentSize);
        REQUIRE(reopened.ok());
        CHECK((reopened.value() == resumed.value()));
        CHECK((currentSize >= 7));
        writer->cleanup(reopened.value());
        CHECK_FALSE(fs::exists(reopened.value()));

        std::error_code ec;
        fs::remove_all(tempDir, ec);
    }
}

// The cross-device fallback copies a finished download into its content-addressed path. That path
// is trusted by name: once something exists there, later downloads of the same hash are skipped.
// A failed copy must therefore leave nothing at the destination, never a truncated object.
TEST_CASE("Durable replace leaves no partial object when the copy fails",
          "[downloader][disk][replace]") {
    namespace fs = std::filesystem;
    const auto dir = fs::temp_directory_path() /
                     ("yams_dl_replace_" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(dir);
    const auto dst = dir / "ab" / "cdef";
    fs::create_directories(dst.parent_path());

    SECTION("an unreadable source leaves no destination") {
        const auto unreadable = dir / "a-directory-not-a-file";
        fs::create_directories(unreadable);
        auto result = yams::downloader::detail::copyFileDurablyReplacing(unreadable, dst);
        CHECK_FALSE(result.ok());
        CHECK_FALSE(fs::exists(dst));
        CHECK(fs::is_empty(dst.parent_path())); // no temporary file left either
    }
    SECTION("repeated copies to one destination leave only the destination") {
        const auto src = dir / "staging";
        std::ofstream(src, std::ios::binary) << "bytes";
        REQUIRE(yams::downloader::detail::copyFileDurablyReplacing(src, dst).ok());
        REQUIRE(yams::downloader::detail::copyFileDurablyReplacing(src, dst).ok());
        std::size_t entries = 0;
        for ([[maybe_unused]] const auto& e : fs::directory_iterator(dst.parent_path()))
            ++entries;
        CHECK(entries == 1);
    }
    SECTION("a successful copy replaces the destination and leaves no temporary file") {
        const auto src = dir / "staging";
        std::ofstream(src, std::ios::binary) << "new bytes";
        std::ofstream(dst, std::ios::binary) << "old";
        auto result = yams::downloader::detail::copyFileDurablyReplacing(src, dst);
        REQUIRE(result.ok());
        std::ifstream in(dst, std::ios::binary);
        std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        CHECK(content == "new bytes");
        std::size_t entries = 0;
        for ([[maybe_unused]] const auto& e : fs::directory_iterator(dst.parent_path()))
            ++entries;
        CHECK(entries == 1);
    }
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("CA bundle: an explicit path wins over the platform candidates", "[downloader][tls]") {
    const auto dir = fs::temp_directory_path() / "yams_ca_bundle_test";
    fs::create_directories(dir);
    const auto bundle = dir / "roots.pem";
    std::ofstream(bundle) << "-----BEGIN CERTIFICATE-----\n";
    const std::string bundleStr = bundle.string();
    const std::array<std::string_view, 2> candidates{"/nonexistent/yams/cert.pem", bundleStr};

    SECTION("configured path is used as given") {
        TlsConfig tls;
        tls.caPath = "/custom/ca.pem";
        CHECK(resolveCaBundle(tls, candidates) == "/custom/ca.pem");
    }
    SECTION("first existing candidate is used when none is configured") {
        CHECK(resolveCaBundle(TlsConfig{}, candidates) == bundleStr);
    }
    SECTION("insecure mode does not name a bundle") {
        TlsConfig tls;
        tls.insecure = true;
        CHECK(resolveCaBundle(tls, candidates).empty());
    }
    SECTION("no existing candidate keeps libcurl's default") {
        const std::array<std::string_view, 1> missing{"/nonexistent/yams/cert.pem"};
        CHECK(resolveCaBundle(TlsConfig{}, missing).empty());
        CHECK(resolveCaBundle(TlsConfig{}, {}).empty());
    }
    SECTION("a directory is not a bundle") {
        const std::string dirStr = dir.string();
        const std::array<std::string_view, 1> dirOnly{dirStr};
        CHECK(resolveCaBundle(TlsConfig{}, dirOnly).empty());
    }

    std::error_code ec;
    fs::remove_all(dir, ec);
}
