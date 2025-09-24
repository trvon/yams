#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
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

// Minimal helper to generate a unique temporary directory for tests
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

// Spec helper to parse ETag and Last-Modified from HTTP response headers.
// This mirrors the behavior we want in the production adapter:
// - Header names are case-insensitive
// - Values are trimmed without surrounding quotes (ETag can be quoted)
// - First occurrence wins
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
        // Lower-case key for case-insensitive compare
        std::transform(key.begin(), key.end(), key.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

        if (key == "etag" && !out.etag.has_value()) {
            // Strip optional quotes around etag value
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

// Persistent JSON-based ResumeStore (test-local implementation)
// This serves as a spec for the production implementation:
// - Stores a JSON sidecar keyed by URL
// - Each entry contains: etag, last_modified, total_bytes, completed_ranges [[offset,length],...]
// - load/save/remove behave as expected
class PersistentResumeStore final : public yams::downloader::IResumeStore {
public:
    explicit PersistentResumeStore(fs::path json_path) : path_(std::move(json_path)) {
        (void)loadFile(); // best-effort
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
        } catch (const std::exception& ex) {
            // Corrupt or unreadable; start fresh
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

private:
    fs::path path_;
};

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

class TestHttpAdapter final : public IHttpAdapter {
public:
    bool resumeSupported{true};
    std::optional<std::uint64_t> contentLength{};
    std::optional<std::string> etag{};
    std::optional<std::string> lastModified{};
    std::vector<std::uint64_t> requestedOffsets;
    std::vector<std::uint64_t> requestedSizes;
    std::vector<std::byte> payload;

    Expected<void> probe(std::string_view, const std::vector<Header>&, bool& resumeSupportedOut,
                         std::optional<std::uint64_t>& contentLengthOut,
                         std::optional<std::string>& etagOut,
                         std::optional<std::string>& lastModifiedOut,
                         std::optional<std::string>& contentTypeOut,
                         std::optional<std::string>& suggestedFilenameOut, const TlsConfig&,
                         const std::optional<std::string>&, std::chrono::milliseconds) override {
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

private:
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

// ----------------------------
// Tests
// ----------------------------

TEST(HttpHeaderParsing, ExtractsEtagAndLastModified_Basic) {
    std::vector<std::string> headers = {"Date: Tue, 19 Aug 2025 10:11:12 GMT",
                                        "Server: test-http",
                                        "ETag: \"abc123-xyz\"",
                                        "Last-Modified: Tue, 19 Aug 2025 09:00:00 GMT",
                                        "Accept-Ranges: bytes",
                                        "Content-Length: 1048576"};

    auto parsed = parse_http_headers_for_meta(headers);
    ASSERT_TRUE(parsed.etag.has_value());
    ASSERT_TRUE(parsed.lastModified.has_value());
    EXPECT_EQ(*parsed.etag, "abc123-xyz");
    EXPECT_EQ(*parsed.lastModified, "Tue, 19 Aug 2025 09:00:00 GMT");
}

TEST(HttpHeaderParsing, HandlesMissingValues_AndCaseInsensitivity) {
    std::vector<std::string> headers = {
        "date: Tue, 19 Aug 2025 10:11:12 GMT", "server: test-http", "content-length: 512"
        // no etag, no last-modified
    };

    auto parsed = parse_http_headers_for_meta(headers);
    EXPECT_FALSE(parsed.etag.has_value());
    EXPECT_FALSE(parsed.lastModified.has_value());

    // With mixed case and quotes
    headers.push_back("eTaG: 'W/\"weak-etag-value\"'");
    headers.push_back("LAST-MODIFIED:   Wed, 20 Aug 2025 00:00:00 GMT  ");

    parsed = parse_http_headers_for_meta(headers);
    ASSERT_TRUE(parsed.etag.has_value());
    ASSERT_TRUE(parsed.lastModified.has_value());
    EXPECT_EQ(*parsed.etag, "W/\"weak-etag-value\"");
    EXPECT_EQ(*parsed.lastModified, "Wed, 20 Aug 2025 00:00:00 GMT");
}

TEST(PersistentResumeStore, WriteReadRoundTrip_SingleUrl) {
    auto dir = make_temp_dir();
    auto jsonPath = dir / "resume.json";

    PersistentResumeStore store(jsonPath);

    yams::downloader::IResumeStore::State state;
    state.etag = std::string("abc123etag");
    state.lastModified = std::string("Tue, 19 Aug 2025 09:00:00 GMT");
    state.totalBytes = 1024 * 1024 * 5; // 5MB
    state.completedRanges = {{0, 256 * 1024}, {256 * 1024, 512 * 1024}};

    auto saveRes = store.save("https://example.com/file.bin", state);
    ASSERT_TRUE(saveRes.ok());

    // Re-open via a new instance to simulate process restart
    PersistentResumeStore store2(jsonPath);
    auto loadRes = store2.load("https://example.com/file.bin");
    ASSERT_TRUE(loadRes.ok());
    ASSERT_TRUE(loadRes.value().has_value());

    const auto& got = *loadRes.value();
    ASSERT_TRUE(got.etag.has_value());
    ASSERT_TRUE(got.lastModified.has_value());
    EXPECT_EQ(*got.etag, *state.etag);
    EXPECT_EQ(*got.lastModified, *state.lastModified);
    EXPECT_EQ(got.totalBytes, state.totalBytes);
    ASSERT_EQ(got.completedRanges.size(), state.completedRanges.size());
    for (size_t i = 0; i < state.completedRanges.size(); ++i) {
        EXPECT_EQ(got.completedRanges[i].first, state.completedRanges[i].first);
        EXPECT_EQ(got.completedRanges[i].second, state.completedRanges[i].second);
    }
}

TEST(PersistentResumeStore, RemoveEntry) {
    auto dir = make_temp_dir();
    auto jsonPath = dir / "resume.json";

    PersistentResumeStore store(jsonPath);

    yams::downloader::IResumeStore::State state;
    state.etag = std::string("to-be-removed");
    state.totalBytes = 42;

    ASSERT_TRUE(store.save("http://example.com/a", state).ok());
    ASSERT_TRUE(store.save("http://example.com/b", state).ok());

    // Remove one entry
    store.remove("http://example.com/a");

    PersistentResumeStore store2(jsonPath);
    auto la = store2.load("http://example.com/a");
    auto lb = store2.load("http://example.com/b");

    ASSERT_TRUE(la.ok());
    ASSERT_TRUE(lb.ok());
    EXPECT_FALSE(la.value().has_value()); // removed
    ASSERT_TRUE(lb.value().has_value());  // still present
    EXPECT_TRUE(lb.value()->etag.has_value());
    EXPECT_EQ(*lb.value()->etag, "to-be-removed");
}

TEST(CASHelpers, CasPathAndIdFormatting) {
    fs::path objectsDir = "/tmp/yams-objects";
    const std::string digest =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"; // 64 hex chars

    auto path = yams::downloader::casPathForSha256(objectsDir, digest);
    // Expected shard "01/23" for first four hex chars
    EXPECT_TRUE(path.string().find("sha256/01/23/0123456789abcdef") != std::string::npos);

    auto id = yams::downloader::makeSha256Id(digest);
    EXPECT_EQ(id, "sha256:" + digest);
}

TEST(DownloadManagerResume, PersistsAndCleansRanges) {
    auto tempDir = make_temp_dir();
    StorageConfig storage{tempDir / "objects", tempDir / "staging"};
    DownloaderConfig cfg;
    cfg.defaultChunkSizeBytes = 4;

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
    ASSERT_TRUE(result.ok()) << result.error().message;
    auto final = result.value();

    ASSERT_EQ(httpPtr->requestedOffsets.size(), 1u);
    EXPECT_EQ(httpPtr->requestedOffsets.front(), 5u);

    ASSERT_FALSE(resumePtr->savedStates.empty());
    const auto& lastState = resumePtr->savedStates.back();
    ASSERT_EQ(lastState.completedRanges.size(), 1u);
    EXPECT_EQ(lastState.completedRanges.front().first, 0u);
    EXPECT_EQ(lastState.completedRanges.front().second, 10u);
    ASSERT_TRUE(resumePtr->removedUrl.has_value());
    EXPECT_EQ(*resumePtr->removedUrl, req.url);

    ASSERT_FALSE(diskPtr->lastFinalPath().empty());
    std::ifstream finalFile(diskPtr->lastFinalPath(), std::ios::binary);
    ASSERT_TRUE(finalFile.good());
    std::string contents((std::istreambuf_iterator<char>(finalFile)),
                         std::istreambuf_iterator<char>());
    EXPECT_EQ(contents, "HELLOWORLD");

    EXPECT_TRUE(final.hash.rfind("sha256:", 0) == 0);
    EXPECT_EQ(final.sizeBytes, 10u);
}

TEST(DownloadManagerExport, SkipsCopyWhenEtagMatches) {
    auto tempDir = make_temp_dir();
    StorageConfig storage{tempDir / "objects", tempDir / "staging"};
    DownloaderConfig cfg;

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
    ASSERT_TRUE(result.ok()) << result.error().message;
    auto final = result.value();

    std::string exported;
    {
        std::ifstream in(exportPath, std::ios::binary);
        exported.assign((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    }
    EXPECT_EQ(exported, "OLD");

    json metaUpdated;
    {
        std::ifstream in(metaPath);
        ASSERT_TRUE(in.good());
        in >> metaUpdated;
    }
    ASSERT_TRUE(metaUpdated.contains("hash"));
    std::string expectedHex;
    if (final.hash.rfind("sha256:", 0) == 0) {
        expectedHex = final.hash.substr(7);
    } else {
        expectedHex = final.hash;
    }
    EXPECT_EQ(metaUpdated["hash"].get<std::string>(), expectedHex);
    ASSERT_TRUE(metaUpdated.contains("etag"));
    EXPECT_EQ(metaUpdated["etag"].get<std::string>(), "tag-777");

    ASSERT_FALSE(diskPtr->lastFinalPath().empty());
    std::ifstream casFile(diskPtr->lastFinalPath(), std::ios::binary);
    std::string casData((std::istreambuf_iterator<char>(casFile)),
                        std::istreambuf_iterator<char>());
    EXPECT_EQ(casData, "ABCDE");

    EXPECT_TRUE(resumePtr->savedStates.empty());
    EXPECT_EQ(httpPtr->requestedOffsets.size(), 1u);
}
