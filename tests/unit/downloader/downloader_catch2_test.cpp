// Catch2 migration of test_downloader_basic.cpp with expanded test coverage
// Epic: yams-3s4 | Migration: downloader tests

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

#include <nlohmann/json.hpp>
#include <yams/downloader/downloader.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace yams::downloader;

namespace {

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

// HTTP metadata parser helper
struct ParsedHttpMeta {
    std::optional<std::string> etag;
    std::optional<std::string> lastModified;
};

static inline std::string trim(std::string_view sv) {
    size_t b = 0, e = sv.size();
    while (b < e && std::isspace(static_cast<unsigned char>(sv[b])))
        ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(sv[e - 1])))
        --e;
    return std::string(sv.substr(b, e - b));
}

ParsedHttpMeta parse_http_headers_for_meta(const std::vector<std::string>& headers) {
    ParsedHttpMeta out;
    for (const auto& raw : headers) {
        auto pos = raw.find(':');
        if (pos == std::string::npos)
            continue;
        std::string key = trim(std::string_view(raw.data(), pos));
        std::string val = trim(std::string_view(raw.data() + pos + 1, raw.size() - pos - 1));
        std::transform(key.begin(), key.end(), key.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

        if (key == "etag" && !out.etag.has_value()) {
            if (val.size() >= 2 && ((val.front() == '"' && val.back() == '"') ||
                                    (val.front() == '\'' && val.back() == '\''))) {
                out.etag = val.substr(1, val.size() - 2);
            } else {
                out.etag = val;
            }
        } else if (key == "last-modified" && !out.lastModified.has_value()) {
            out.lastModified = val;
        }
    }
    return out;
}

// Persistent JSON-based ResumeStore (test implementation)
class PersistentResumeStore final : public yams::downloader::IResumeStore {
public:
    explicit PersistentResumeStore(fs::path json_path) : path_(std::move(json_path)) {
        (void)loadFile();
    }

    yams::downloader::Expected<std::optional<State>> load(std::string_view url) override {
        auto j = loadFile();
        if (!j.ok())
            return j.error();
        auto& root = j.value();
        const auto it = root.find(std::string(url));
        if (it == root.end())
            return std::optional<State>{std::nullopt};

        State st;
        const auto& entry = it.value();
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

    yams::downloader::Expected<void> save(std::string_view url, const State& state) override {
        auto j = loadFile();
        if (!j.ok())
            return j.error();
        auto& root = j.value();

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
        auto j = loadFile();
        if (!j.ok())
            return;
        auto& root = j.value();
        root.erase(std::string(url));
        (void)writeFile(root);
    }

private:
    yams::downloader::Expected<json> loadFile() const {
        if (!fs::exists(path_)) {
            return json::object();
        }
        std::ifstream in(path_);
        if (!in) {
            return yams::downloader::Error{yams::downloader::ErrorCode::IoError,
                                           "Failed to open resume JSON for read"};
        }
        try {
            json root;
            in >> root;
            if (!root.is_object())
                root = json::object();
            return root;
        } catch (const std::exception&) {
            return json::object();
        }
    }

    yams::downloader::Expected<void> writeFile(const json& root) const {
        std::ofstream out(path_, std::ios::trunc);
        if (!out) {
            return yams::downloader::Error{yams::downloader::ErrorCode::IoError,
                                           "Failed to open resume JSON for write"};
        }
        out << root.dump(2);
        return {};
    }

    fs::path path_;
};

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
    int probeCallCount{0};
    int fetchCallCount{0};

    Expected<void> probe(std::string_view, const std::vector<Header>&, bool& resumeSupportedOut,
                         std::optional<std::uint64_t>& contentLengthOut,
                         std::optional<std::string>& etagOut,
                         std::optional<std::string>& lastModifiedOut,
                         std::optional<std::string>& contentTypeOut,
                         std::optional<std::string>& suggestedFilenameOut, const TlsConfig&,
                         const std::optional<std::string>&, std::chrono::milliseconds) override {
        probeCallCount++;
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
                              const ShouldCancel&, const ProgressCallback& onProgress) override {
        fetchCallCount++;
        requestedOffsets.push_back(offset);
        requestedSizes.push_back(size);
        if (onProgress) {
            ProgressEvent ev;
            ev.downloadedBytes = 0;
            ev.stage = ProgressStage::Downloading;
            onProgress(ev);
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

    Expected<fs::path> createStagingFile(const StorageConfig&, std::string_view sessionId,
                                         std::string_view tempExtension) override {
        currentPath_ = stagingPath(sessionId, tempExtension);
        data_.clear();
        writeToFile();
        return currentPath_;
    }

    Expected<fs::path> createOrOpenStagingFile(const StorageConfig&, std::string_view sessionId,
                                               std::string_view tempExtension,
                                               std::optional<std::uint64_t> expectedSize,
                                               std::uint64_t& currentSize) override {
        currentPath_ = stagingPath(sessionId, tempExtension);
        data_ = initialData_;
        currentSize = data_.size();
        writeToFile();
        if (expectedSize && currentSize == 0) {
            std::error_code ec;
            std::filesystem::resize_file(currentPath_, static_cast<std::uint64_t>(*expectedSize), ec);
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
            return Error{ErrorCode::IoError, "Failed to write CAS object: " + lastFinalPath_.string()};
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
// HTTP Header Parsing Tests
// =============================================================================

TEST_CASE("HTTP Header Parsing: Basic extraction", "[downloader][http][headers]") {
    SECTION("Extracts ETag and Last-Modified") {
        std::vector<std::string> headers = {
            "Date: Tue, 19 Aug 2025 10:11:12 GMT",
            "Server: test-http",
            "ETag: \"abc123-xyz\"",
            "Last-Modified: Tue, 19 Aug 2025 09:00:00 GMT",
            "Accept-Ranges: bytes",
            "Content-Length: 1048576"
        };

        auto parsed = parse_http_headers_for_meta(headers);
        REQUIRE(parsed.etag.has_value());
        REQUIRE(parsed.lastModified.has_value());
        CHECK(*parsed.etag == "abc123-xyz");
        CHECK(*parsed.lastModified == "Tue, 19 Aug 2025 09:00:00 GMT");
    }

    SECTION("Handles missing values") {
        std::vector<std::string> headers = {
            "date: Tue, 19 Aug 2025 10:11:12 GMT",
            "server: test-http",
            "content-length: 512"
        };

        auto parsed = parse_http_headers_for_meta(headers);
        CHECK_FALSE(parsed.etag.has_value());
        CHECK_FALSE(parsed.lastModified.has_value());
    }

    SECTION("Case insensitive header names") {
        std::vector<std::string> headers = {
            "eTaG: 'W/\"weak-etag-value\"'",
            "LAST-MODIFIED:   Wed, 20 Aug 2025 00:00:00 GMT  "
        };

        auto parsed = parse_http_headers_for_meta(headers);
        REQUIRE(parsed.etag.has_value());
        REQUIRE(parsed.lastModified.has_value());
        CHECK(*parsed.etag == "W/\"weak-etag-value\"");
        CHECK(*parsed.lastModified == "Wed, 20 Aug 2025 00:00:00 GMT");
    }

    SECTION("First occurrence wins") {
        std::vector<std::string> headers = {
            "ETag: first-etag",
            "ETag: second-etag"
        };

        auto parsed = parse_http_headers_for_meta(headers);
        REQUIRE(parsed.etag.has_value());
        CHECK(*parsed.etag == "first-etag");
    }

    SECTION("Strips quotes from ETag") {
        std::vector<std::string> headers = {
            "ETag: \"quoted-etag\""
        };
        auto parsed = parse_http_headers_for_meta(headers);
        REQUIRE(parsed.etag.has_value());
        CHECK(*parsed.etag == "quoted-etag");
    }

    SECTION("Handles unquoted ETag") {
        std::vector<std::string> headers = {
            "ETag: unquoted-etag"
        };
        auto parsed = parse_http_headers_for_meta(headers);
        REQUIRE(parsed.etag.has_value());
        CHECK(*parsed.etag == "unquoted-etag");
    }
}

// =============================================================================
// Persistent Resume Store Tests
// =============================================================================

TEST_CASE("PersistentResumeStore: Round-trip", "[downloader][resume]") {
    auto dir = make_temp_dir();
    auto jsonPath = dir / "resume.json";

    SECTION("Save and load single URL") {
        PersistentResumeStore store(jsonPath);

        yams::downloader::IResumeStore::State state;
        state.etag = std::string("abc123etag");
        state.lastModified = std::string("Tue, 19 Aug 2025 09:00:00 GMT");
        state.totalBytes = 1024 * 1024 * 5;
        state.completedRanges = {{0, 256 * 1024}, {256 * 1024, 512 * 1024}};

        auto saveRes = store.save("https://example.com/file.bin", state);
        REQUIRE(saveRes.ok());

        PersistentResumeStore store2(jsonPath);
        auto loadRes = store2.load("https://example.com/file.bin");
        REQUIRE(loadRes.ok());
        REQUIRE(loadRes.value().has_value());

        const auto& got = *loadRes.value();
        REQUIRE(got.etag.has_value());
        REQUIRE(got.lastModified.has_value());
        CHECK(*got.etag == *state.etag);
        CHECK(*got.lastModified == *state.lastModified);
        CHECK(got.totalBytes == state.totalBytes);
        REQUIRE(got.completedRanges.size() == state.completedRanges.size());
    }

    SECTION("Remove entry") {
        PersistentResumeStore store(jsonPath);

        yams::downloader::IResumeStore::State state;
        state.etag = std::string("to-be-removed");
        state.totalBytes = 42;

        REQUIRE(store.save("http://example.com/a", state).ok());
        REQUIRE(store.save("http://example.com/b", state).ok());

        store.remove("http://example.com/a");

        PersistentResumeStore store2(jsonPath);
        auto la = store2.load("http://example.com/a");
        auto lb = store2.load("http://example.com/b");

        REQUIRE(la.ok());
        REQUIRE(lb.ok());
        CHECK_FALSE(la.value().has_value());
        REQUIRE(lb.value().has_value());
        CHECK(lb.value()->etag.has_value());
        CHECK(*lb.value()->etag == "to-be-removed");
    }

    SECTION("Multiple URLs") {
        PersistentResumeStore store(jsonPath);

        for (int i = 0; i < 5; ++i) {
            yams::downloader::IResumeStore::State state;
            state.etag = "etag-" + std::to_string(i);
            state.totalBytes = 1000 * (i + 1);
            REQUIRE(store.save("http://example.com/file" + std::to_string(i), state).ok());
        }

        PersistentResumeStore store2(jsonPath);
        for (int i = 0; i < 5; ++i) {
            auto result = store2.load("http://example.com/file" + std::to_string(i));
            REQUIRE(result.ok());
            REQUIRE(result.value().has_value());
            CHECK(*result.value()->etag == "etag-" + std::to_string(i));
        }
    }

    SECTION("Load non-existent URL") {
        PersistentResumeStore store(jsonPath);
        auto result = store.load("http://nonexistent.com/file");
        REQUIRE(result.ok());
        CHECK_FALSE(result.value().has_value());
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(dir, ec);
}

// =============================================================================
// CAS Helpers Tests
// =============================================================================

TEST_CASE("CAS Helpers: Path and ID formatting", "[downloader][cas]") {
    fs::path objectsDir = "/tmp/yams-objects";
    const std::string digest =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    SECTION("CAS path includes shard directories") {
        auto path = yams::downloader::casPathForSha256(objectsDir, digest);
        CHECK(path.generic_string().find("sha256/01/23/0123456789abcdef") != std::string::npos);
    }

    SECTION("SHA256 ID format") {
        auto id = yams::downloader::makeSha256Id(digest);
        CHECK(id == "sha256:" + digest);
    }

    SECTION("Empty digest") {
        auto id = yams::downloader::makeSha256Id("");
        CHECK(id == "sha256:");
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

        auto dm = makeDownloadManagerWithDependencies(storage, cfg, std::move(http), std::move(disk),
                                                      nullptr, std::move(resume), nullptr);

        DownloadRequest req;
        req.url = "https://example.com/file.bin";
        req.resume = true;
        req.chunkSizeBytes = 5;

        auto result = dm->download(req);
        REQUIRE(result.ok());
        auto final = result.value();

        REQUIRE(httpPtr->requestedOffsets.size() == 1);
        CHECK(httpPtr->requestedOffsets.front() == 5);

        REQUIRE_FALSE(resumePtr->savedStates.empty());
        const auto& lastState = resumePtr->savedStates.back();
        REQUIRE(lastState.completedRanges.size() == 1);
        CHECK(lastState.completedRanges.front().first == 0);
        CHECK(lastState.completedRanges.front().second == 10);

        REQUIRE(resumePtr->removedUrl.has_value());
        CHECK(*resumePtr->removedUrl == req.url);

        REQUIRE_FALSE(diskPtr->lastFinalPath().empty());
        std::ifstream finalFile(diskPtr->lastFinalPath(), std::ios::binary);
        REQUIRE(finalFile.good());
        std::string contents((std::istreambuf_iterator<char>(finalFile)),
                             std::istreambuf_iterator<char>());
        CHECK(contents == "HELLOWORLD");

        CHECK(final.hash.rfind("sha256:", 0) == 0);
        CHECK(final.sizeBytes == 10);
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(tempDir, ec);
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

        auto dm = makeDownloadManagerWithDependencies(storage, cfg, std::move(http), std::move(disk),
                                                      nullptr, std::move(resume), nullptr);

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
        CHECK(exported == "OLD");

        json metaUpdated;
        {
            std::ifstream in(metaPath);
            REQUIRE(in.good());
            in >> metaUpdated;
        }
        REQUIRE(metaUpdated.contains("hash"));
        REQUIRE(metaUpdated.contains("etag"));
        CHECK(metaUpdated["etag"].get<std::string>() == "tag-777");

        REQUIRE_FALSE(diskPtr->lastFinalPath().empty());
        std::ifstream casFile(diskPtr->lastFinalPath(), std::ios::binary);
        std::string casData((std::istreambuf_iterator<char>(casFile)),
                            std::istreambuf_iterator<char>());
        CHECK(casData == "ABCDE");

        CHECK(resumePtr->savedStates.empty());
        CHECK(httpPtr->requestedOffsets.size() == 1);
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

// =============================================================================
// Test HTTP Adapter Tests
// =============================================================================

TEST_CASE("TestHttpAdapter: Behavior verification", "[downloader][adapter]") {
    SECTION("Tracks probe calls") {
        TestHttpAdapter adapter;
        adapter.contentLength = 1000;

        bool resumeSupported;
        std::optional<std::uint64_t> contentLength;
        std::optional<std::string> etag, lastModified, contentType, filename;

        adapter.probe("http://test.com", {}, resumeSupported, contentLength, etag, lastModified,
                      contentType, filename, TlsConfig{}, std::nullopt, std::chrono::milliseconds{1000});

        CHECK(adapter.probeCallCount == 1);
        CHECK(resumeSupported == true);
        CHECK(contentLength.has_value());
        CHECK(*contentLength == 1000);
    }

    SECTION("Tracks fetch calls") {
        TestHttpAdapter adapter;
        adapter.payload = to_bytes("test data");

        std::vector<std::byte> received;
        auto sink = [&received](std::span<const std::byte> data) -> Expected<void> {
            received.insert(received.end(), data.begin(), data.end());
            return {};
        };

        adapter.fetchRange("http://test.com", {}, 0, 9, TlsConfig{}, std::nullopt,
                           std::chrono::milliseconds{1000}, sink, nullptr, nullptr);

        CHECK(adapter.fetchCallCount == 1);
        CHECK(adapter.requestedOffsets.size() == 1);
        CHECK(adapter.requestedOffsets[0] == 0);
        CHECK(adapter.requestedSizes[0] == 9);
        CHECK(received.size() == 9);
    }
}

// =============================================================================
// Tracking Resume Store Tests
// =============================================================================

TEST_CASE("TrackingResumeStore: Tracking verification", "[downloader][resume][tracking]") {
    TrackingResumeStore store;

    SECTION("Tracks load calls") {
        store.load("http://url1.com");
        store.load("http://url2.com");
        store.load("http://url1.com");

        CHECK(store.loadUrls.size() == 3);
        CHECK(store.loadUrls[0] == "http://url1.com");
        CHECK(store.loadUrls[1] == "http://url2.com");
    }

    SECTION("Tracks save calls") {
        IResumeStore::State state;
        state.totalBytes = 100;

        store.save("http://url1.com", state);
        state.totalBytes = 200;
        store.save("http://url2.com", state);

        CHECK(store.savedUrls.size() == 2);
        CHECK(store.savedStates.size() == 2);
        CHECK(store.savedStates[0].totalBytes == 100);
        CHECK(store.savedStates[1].totalBytes == 200);
    }

    SECTION("Tracks remove calls") {
        store.remove("http://removed.com");
        REQUIRE(store.removedUrl.has_value());
        CHECK(*store.removedUrl == "http://removed.com");
    }

    SECTION("Initial state returned on load") {
        IResumeStore::State initial;
        initial.totalBytes = 500;
        initial.etag = "initial-etag";
        store.initialState = initial;

        auto result = store.load("http://any.com");
        REQUIRE(result.ok());
        REQUIRE(result.value().has_value());
        CHECK(result.value()->totalBytes == 500);
        CHECK(*result.value()->etag == "initial-etag");
    }
}

// =============================================================================
// FakeDiskWriter Tests
// =============================================================================

TEST_CASE("FakeDiskWriter: Operations", "[downloader][disk]") {
    auto tempDir = make_temp_dir();
    StorageConfig storage{tempDir / "objects", tempDir / "staging"};
    FakeDiskWriter writer(storage);

    SECTION("Creates staging file") {
        auto result = writer.createStagingFile(storage, "session-123", ".part");
        REQUIRE(result.ok());
        CHECK(fs::exists(result.value()));
    }

    SECTION("Writes at offset") {
        auto staging = writer.createStagingFile(storage, "session-456", ".part");
        REQUIRE(staging.ok());

        auto data = to_bytes("hello");
        auto writeResult = writer.writeAt(staging.value(), 0, data);
        REQUIRE(writeResult.ok());

        auto data2 = to_bytes("world");
        writeResult = writer.writeAt(staging.value(), 5, data2);
        REQUIRE(writeResult.ok());

        CHECK(writer.stagedData().size() == 10);
    }

    SECTION("Handles initial data") {
        writer.setInitialData(to_bytes("PREFIX"));
        std::uint64_t currentSize;
        auto result = writer.createOrOpenStagingFile(storage, "session-789", ".part", 100, currentSize);
        REQUIRE(result.ok());
        CHECK(currentSize == 6);
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

TEST_CASE("Downloader: Edge cases", "[downloader][edge]") {
    SECTION("Empty URL handling") {
        TrackingResumeStore store;
        auto result = store.load("");
        CHECK(result.ok());
    }

    SECTION("Very long URL") {
        std::string longUrl = "http://example.com/" + std::string(1000, 'x');
        TrackingResumeStore store;
        auto result = store.load(longUrl);
        CHECK(result.ok());
        CHECK(store.loadUrls[0] == longUrl);
    }

    SECTION("Unicode in URL") {
        std::string unicodeUrl = "http://example.com/文件/файл.bin";
        TrackingResumeStore store;
        IResumeStore::State state;
        state.totalBytes = 100;
        auto saveResult = store.save(unicodeUrl, state);
        CHECK(saveResult.ok());
        CHECK(store.savedUrls[0] == unicodeUrl);
    }
}

TEST_CASE("HTTP header parsing edge cases", "[downloader][http][edge]") {
    SECTION("Empty headers list") {
        std::vector<std::string> headers;
        auto parsed = parse_http_headers_for_meta(headers);
        CHECK_FALSE(parsed.etag.has_value());
        CHECK_FALSE(parsed.lastModified.has_value());
    }

    SECTION("Malformed headers (no colon)") {
        std::vector<std::string> headers = {
            "InvalidHeaderNoColon",
            "ETag: valid-etag"
        };
        auto parsed = parse_http_headers_for_meta(headers);
        REQUIRE(parsed.etag.has_value());
        CHECK(*parsed.etag == "valid-etag");
    }

    SECTION("Empty header value") {
        std::vector<std::string> headers = {
            "ETag: ",
            "Last-Modified:    "
        };
        auto parsed = parse_http_headers_for_meta(headers);
        // Empty values should still be captured
        REQUIRE(parsed.etag.has_value());
        CHECK(parsed.etag->empty());
    }

    SECTION("Whitespace only value") {
        std::vector<std::string> headers = {
            "ETag:     "
        };
        auto parsed = parse_http_headers_for_meta(headers);
        REQUIRE(parsed.etag.has_value());
        CHECK(parsed.etag->empty());
    }
}
