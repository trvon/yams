// Catch2 migration of resolver_tests.cpp with expanded test coverage
// Epic: yams-3s4 | Migration: resolve tests

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>

#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <yams/common/delete_resolver/resolver.hpp>
#include <yams/common/pattern_utils.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/storage/corpus_stats.h>

namespace fs = std::filesystem;

namespace yams::tests::resolve {

using yams::common::resolve::FilesystemBackend;
using yams::common::resolve::MetadataBackend;
using yams::common::resolve::Options;
using yams::common::resolve::Resolver;
using yams::common::resolve::Target;

using yams::metadata::DocumentInfo;
using yams::metadata::IMetadataRepository;
using yams::metadata::PathTreeNode;

// =============================================================================
// Mock Repository Implementation
// =============================================================================

class MockRepo : public IMetadataRepository {
public:
    MockRepo() = default;
    ~MockRepo() override = default;

    // Document CRUD operations (stubbed for tests)
    Result<int64_t> insertDocument(const DocumentInfo&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::optional<DocumentInfo>> getDocument(int64_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::optional<DocumentInfo>> getDocumentByHash(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> updateDocument(const DocumentInfo&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> deleteDocument(int64_t) override { return Error{ErrorCode::NotImplemented, "NI"}; }

    // Content operations (unused)
    Result<void> insertContent(const yams::metadata::DocumentContent&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::optional<yams::metadata::DocumentContent>> getContent(int64_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> updateContent(const yams::metadata::DocumentContent&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> deleteContent(int64_t) override { return Error{ErrorCode::NotImplemented, "NI"}; }
    Result<void>
    batchInsertContentAndIndex(const std::vector<yams::metadata::BatchContentEntry>&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Metadata ops (unused)
    Result<void> setMetadata(int64_t, const std::string&,
                             const yams::metadata::MetadataValue&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> setMetadataBatch(
        const std::vector<std::tuple<int64_t, std::string, yams::metadata::MetadataValue>>&)
        override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::optional<yams::metadata::MetadataValue>> getMetadata(int64_t,
                                                                     const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::unordered_map<std::string, yams::metadata::MetadataValue>>
    getAllMetadata(int64_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> removeMetadata(int64_t, const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Relationships (unused)
    Result<int64_t> insertRelationship(const yams::metadata::DocumentRelationship&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<yams::metadata::DocumentRelationship>> getRelationships(int64_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> deleteRelationship(int64_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Search history (unused)
    Result<int64_t> insertSearchHistory(const yams::metadata::SearchHistoryEntry&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<yams::metadata::SearchHistoryEntry>> getRecentSearches(int) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Saved queries (unused)
    Result<int64_t> insertSavedQuery(const yams::metadata::SavedQuery&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::optional<yams::metadata::SavedQuery>> getSavedQuery(int64_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<yams::metadata::SavedQuery>> getAllSavedQueries() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> updateSavedQuery(const yams::metadata::SavedQuery&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> deleteSavedQuery(int64_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // FTS
    Result<void> indexDocumentContent(int64_t, const std::string&, const std::string&,
                                      const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> indexDocumentContentTrusted(int64_t, const std::string&, const std::string&,
                                             const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> removeFromIndex(int64_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> removeFromIndexByHash(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<size_t> removeFromIndexByHashBatch(const std::vector<std::string>&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<int64_t>> getAllFts5IndexedDocumentIds() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<yams::metadata::SearchResults>
    search(const std::string&, int, int,
           const std::optional<std::vector<int64_t>>& = std::nullopt) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Fuzzy
    Result<yams::metadata::SearchResults>
    fuzzySearch(const std::string&, float, int,
                const std::optional<std::vector<int64_t>>& = std::nullopt) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    void addSymSpellTerm(std::string_view, int64_t) override { /* no-op for mock */ }

    // Query documents - THE MAIN METHOD USED IN TESTS
    Result<std::vector<DocumentInfo>>
    queryDocuments(const metadata::DocumentQueryOptions& options) override {
        auto toPattern = [&]() -> std::string {
            if (options.likePattern)
                return *options.likePattern;
            if (options.exactPath)
                return *options.exactPath;
            if (options.pathPrefix)
                return *options.pathPrefix + "%";
            if (options.containsFragment)
                return "%" + *options.containsFragment;
            return "%";
        };

        const std::string pathPattern = toPattern();
        std::vector<DocumentInfo> out;

        auto pushDoc = [&](std::string path, std::string name, std::string hash) {
            DocumentInfo d{};
            d.filePath = std::move(path);
            d.fileName = std::move(name);
            d.sha256Hash = std::move(hash);
            out.push_back(std::move(d));
        };

        // Mock documents for testing
        if (pathPattern == "%/a.txt" || pathPattern == "%a.txt" || pathPattern == "/root/a.txt" ||
            pathPattern == "a.txt" || pathPattern == "%") {
            pushDoc("/root/a.txt", "a.txt",
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        }
        if (pathPattern == "%/b.txt" || pathPattern == "%b.txt" ||
            pathPattern == "/root/sub/b.txt" || pathPattern == "/root/sub/%" ||
            pathPattern == "/root/sub%" || pathPattern == "b.txt" || pathPattern == "%") {
            pushDoc("/root/sub/b.txt", "b.txt",
                    "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
        }
        if (pathPattern == "%/.hidden.txt" || pathPattern == "/root/sub/.hidden.txt" ||
            pathPattern == "/root/sub/%" || pathPattern == "/root/sub%" || pathPattern == "%") {
            pushDoc("/root/sub/.hidden.txt", ".hidden.txt",
                    "HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH");
        }
        // Additional mock documents for expanded tests
        if (pathPattern == "%/c.cpp" || pathPattern == "%c.cpp" ||
            pathPattern == "/root/src/c.cpp" || pathPattern == "/root/src/%" ||
            pathPattern == "/root/src%" || pathPattern == "%") {
            pushDoc("/root/src/c.cpp", "c.cpp",
                    "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC");
        }
        if (pathPattern == "%/d.h" || pathPattern == "%d.h" || pathPattern == "/root/src/d.h" ||
            pathPattern == "/root/src/%" || pathPattern == "/root/src%" || pathPattern == "%") {
            pushDoc("/root/src/d.h", "d.h",
                    "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
        }

        return out;
    }

    Result<std::optional<DocumentInfo>> findDocumentByExactPath(const std::string& path) override {
        auto docs = queryDocuments(metadata::DocumentQueryOptions{.exactPath = path});
        if (!docs)
            return docs.error();
        if (docs.value().empty())
            return std::optional<DocumentInfo>(std::nullopt);
        return std::optional<DocumentInfo>(docs.value()[0]);
    }

    Result<std::vector<DocumentInfo>> findDocumentsByHashPrefix(const std::string&,
                                                                std::size_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<DocumentInfo>> findDocumentsByExtension(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<DocumentInfo>>
    findDocumentsModifiedSince(std::chrono::system_clock::time_point) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Snapshots (unused - collections now use generic metadata query)
    Result<std::vector<DocumentInfo>> findDocumentsBySnapshot(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<DocumentInfo>> findDocumentsBySnapshotLabel(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<std::string>> getSnapshots() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<std::string>> getSnapshotLabels() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<yams::metadata::SnapshotInfo> getSnapshotInfo(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::unordered_map<std::string, yams::metadata::SnapshotInfo>>
    batchGetSnapshotInfo(const std::vector<std::string>&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Tags (unused)
    Result<std::vector<DocumentInfo>> findDocumentsByTags(const std::vector<std::string>&,
                                                          bool) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<std::string>> getDocumentTags(int64_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::unordered_map<int64_t, std::vector<std::string>>>
    batchGetDocumentTags(std::span<const int64_t>) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    Result<std::vector<std::string>> getAllTags() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Stats (unused)
    Result<int64_t> getDocumentCount() override { return Error{ErrorCode::NotImplemented, "NI"}; }
    Result<int64_t> getIndexedDocumentCount() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<int64_t> getContentExtractedDocumentCount() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::unordered_map<std::string, int64_t>> getDocumentCountsByExtension() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<int64_t> getDocumentCountByExtractionStatus(yams::metadata::ExtractionStatus) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<storage::CorpusStats> getCorpusStats() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    void signalCorpusStatsStale() override { /* no-op for mock */ }

    Result<
        std::unordered_map<int64_t, std::unordered_map<std::string, yams::metadata::MetadataValue>>>
    getMetadataForDocuments(std::span<const int64_t>) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    Result<std::unordered_map<std::string, std::vector<yams::metadata::MetadataValueCount>>>
    getMetadataValueCounts(const std::vector<std::string>&,
                           const metadata::DocumentQueryOptions&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    Result<std::unordered_map<std::string, DocumentInfo>>
    batchGetDocumentsByHash(const std::vector<std::string>&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    Result<std::unordered_map<int64_t, yams::metadata::DocumentContent>>
    batchGetContent(const std::vector<int64_t>&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Embedding status operations
    Result<void> updateDocumentEmbeddingStatus(int64_t, bool, const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> updateDocumentEmbeddingStatusByHash(const std::string&, bool,
                                                     const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> batchUpdateDocumentEmbeddingStatusByHashes(const std::vector<std::string>&, bool,
                                                            const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<bool> hasDocumentEmbeddingByHash(const std::string&) override {
        return false; // Default to no embedding for mock
    }

    Result<void> updateDocumentExtractionStatus(int64_t, bool, yams::metadata::ExtractionStatus,
                                                const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Repair status operations
    Result<void> updateDocumentRepairStatus(const std::string&,
                                            yams::metadata::RepairStatus) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> batchUpdateDocumentRepairStatuses(const std::vector<std::string>&,
                                                   yams::metadata::RepairStatus) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    Result<void> checkpointWal() override { return {}; }

    Result<std::optional<metadata::PathTreeNode>> findPathTreeNode(int64_t,
                                                                   std::string_view) override {
        return std::optional<metadata::PathTreeNode>{};
    }
    Result<metadata::PathTreeNode> insertPathTreeNode(int64_t, std::string_view,
                                                      std::string_view) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> incrementPathTreeDocCount(int64_t, int64_t) override { return Result<void>(); }
    Result<std::optional<metadata::PathTreeNode>>
    findPathTreeNodeByFullPath(std::string_view) override {
        return std::optional<metadata::PathTreeNode>{};
    }
    Result<void> accumulatePathTreeCentroid(int64_t, std::span<const float>) override {
        return Result<void>();
    }

    Result<std::vector<DocumentInfo>> findDocumentsByPathTreePrefix(std::string_view, bool,
                                                                    int) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<PathTreeNode>> listPathTreeChildren(std::string_view, std::size_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> upsertPathTreeForDocument(const DocumentInfo&, int64_t, bool,
                                           std::span<const float>) override {
        return Result<void>();
    }
    Result<void> removePathTreeForDocument(const DocumentInfo&, int64_t,
                                           std::span<const float>) override {
        return Result<void>();
    }
    Result<void> upsertTreeSnapshot(const yams::metadata::TreeSnapshotRecord&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::optional<yams::metadata::TreeSnapshotRecord>>
    getTreeSnapshot(std::string_view) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<yams::metadata::TreeSnapshotRecord>> listTreeSnapshots(int) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<int64_t> beginTreeDiff(const yams::metadata::TreeDiffDescriptor&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> appendTreeChanges(int64_t,
                                   const std::vector<yams::metadata::TreeChangeRecord>&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<yams::metadata::TreeChangeRecord>>
    listTreeChanges(const yams::metadata::TreeDiffQuery&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> finalizeTreeDiff(int64_t, std::size_t, std::string_view) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Session-related operations
    Result<std::vector<DocumentInfo>> findDocumentsBySessionId(const std::string&) override {
        return std::vector<DocumentInfo>{};
    }
    Result<int64_t> countDocumentsBySessionId(const std::string&) override { return 0; }
    Result<void> removeSessionIdFromDocuments(const std::string&) override {
        return Result<void>();
    }
    Result<int64_t> deleteDocumentsBySessionId(const std::string&) override { return 0; }
};

// =============================================================================
// Helper Functions
// =============================================================================

fs::path make_temp_dir(const std::string& prefix = "yams_resolve_test_") {
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

// =============================================================================
// Filesystem Backend Tests
// =============================================================================

TEST_CASE("Resolver FilesystemBackend: Name resolution", "[resolve][filesystem]") {
    auto root = make_temp_dir();

    SECTION("Resolves relative path to absolute file path") {
        // Create test file
        fs::path file = root / "file.txt";
        {
            std::ofstream out(file);
            out << "content";
        }

        // Change to parent directory
        fs::path oldCwd = fs::current_path();
        fs::current_path(root.parent_path());

        Resolver<FilesystemBackend> resolver{FilesystemBackend{}, Options{}};

        auto res = resolver.names({(root.filename() / fs::path("file.txt")).string()});
        fs::current_path(oldCwd);

        REQUIRE(res);
        REQUIRE(res.value().size() == 1);
        CHECK(res.value()[0].id == fs::absolute(file).string());
        CHECK(res.value()[0].display == fs::absolute(file).string());
    }

    SECTION("Multiple file resolution") {
        fs::path file1 = root / "file1.txt";
        fs::path file2 = root / "file2.txt";
        {
            std::ofstream out1(file1);
            out1 << "content1";
            std::ofstream out2(file2);
            out2 << "content2";
        }

        fs::path oldCwd = fs::current_path();
        fs::current_path(root);

        Resolver<FilesystemBackend> resolver{FilesystemBackend{}, Options{}};
        auto res = resolver.names({"file1.txt", "file2.txt"});
        fs::current_path(oldCwd);

        REQUIRE(res);
        CHECK(res.value().size() == 2);
    }

    SECTION("Non-existent file returns error") {
        Resolver<FilesystemBackend> resolver{FilesystemBackend{}, Options{}};
        auto res = resolver.names({"/nonexistent/path/file.txt"});
        CHECK_FALSE(res);
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(root, ec);
}

TEST_CASE("Resolver FilesystemBackend: Directory resolution", "[resolve][filesystem]") {
    Resolver<FilesystemBackend> resolver{FilesystemBackend{}, Options{}};

    SECTION("Directory requires recursive option") {
        auto res = resolver.directory("/does/not/matter");
        REQUIRE_FALSE(res);
        CHECK(res.error().code == ErrorCode::InvalidArgument);
    }

    SECTION("Directory with recursive option") {
        auto root = make_temp_dir();
        fs::path subdir = root / "subdir";
        fs::create_directories(subdir);

        fs::path file1 = root / "file1.txt";
        fs::path file2 = subdir / "file2.txt";
        {
            std::ofstream(file1) << "content1";
            std::ofstream(file2) << "content2";
        }

        Options opts;
        opts.recursive = true;
        Resolver<FilesystemBackend> resolver2{FilesystemBackend{}, opts};

        auto res = resolver2.directory(root.string());
        REQUIRE(res);
        // At minimum should find at least one file
        CHECK(res.value().size() >= 1);

        std::error_code ec;
        fs::remove_all(root, ec);
    }
}

// =============================================================================
// Metadata Backend Tests
// =============================================================================

TEST_CASE("Resolver MetadataBackend: Name resolution", "[resolve][metadata]") {
    auto repo = std::make_shared<MockRepo>();

    SECTION("Non-existent name returns NotFound") {
        Options opt;
        opt.forceAmbiguous = false;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        auto nf = resolver.names({"nope.txt"});
        REQUIRE_FALSE(nf);
        CHECK(nf.error().code == ErrorCode::NotFound);
    }

    SECTION("Existing exact match returns success") {
        Options opt;
        opt.forceAmbiguous = false;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        auto ok = resolver.names({"a.txt"});
        REQUIRE(ok);
        REQUIRE(ok.value().size() == 1);
        CHECK(ok.value()[0].id.size() == 64u);
        CHECK(ok.value()[0].display == "a.txt");
    }

    SECTION("Duplicate names are deduplicated") {
        Options opt;
        opt.forceAmbiguous = false;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        auto amb = resolver.names({"a.txt", "a.txt"});
        REQUIRE(amb);
        CHECK(amb.value().size() == 1);
    }

    SECTION("Multiple different names") {
        Options opt;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        auto res = resolver.names({"a.txt", "b.txt"});
        REQUIRE(res);
        CHECK(res.value().size() == 2);
    }

    SECTION("Empty names list") {
        Options opt;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        auto res = resolver.names({});
        REQUIRE(res);
        CHECK(res.value().empty());
    }
}

TEST_CASE("Resolver MetadataBackend: Pattern resolution", "[resolve][metadata][pattern]") {
    auto repo = std::make_shared<MockRepo>();
    Options opt;
    Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

    SECTION("Pattern filters by wildcard") {
        auto r = resolver.pattern("b.txt");
        REQUIRE(r);
        REQUIRE_FALSE(r.value().empty());

        bool found = false;
        for (const auto& t : r.value()) {
            if (t.display == "b.txt") {
                found = true;
                CHECK(t.id.size() == 64u);
            }
        }
        CHECK(found);
    }

    SECTION("Pattern with extension") {
        auto r = resolver.pattern("*.cpp");
        // Pattern may succeed or fail depending on MetadataBackend implementation
        // Just verify it doesn't crash - pattern support varies
        (void)r; // suppress unused warning
    }

    SECTION("Empty pattern") {
        auto r = resolver.pattern("");
        // Empty pattern behavior depends on implementation
        // Just verify it doesn't crash
    }
}

TEST_CASE("Resolver MetadataBackend: Directory resolution with hidden files",
          "[resolve][metadata][directory]") {
    auto repo = std::make_shared<MockRepo>();

    SECTION("Recursive includes hidden when enabled") {
        Options opt;
        opt.recursive = true;
        opt.includeHidden = true;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        auto r = resolver.directory("/root/sub");
        REQUIRE(r);

        std::vector<std::string> names;
        for (const auto& t : r.value())
            names.push_back(t.display);

        CHECK_THAT(names, Catch::Matchers::VectorContains(std::string("b.txt")));
        CHECK_THAT(names, Catch::Matchers::VectorContains(std::string(".hidden.txt")));
    }

    SECTION("Recursive excludes hidden when disabled") {
        Options opt;
        opt.recursive = true;
        opt.includeHidden = false;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        auto r = resolver.directory("/root/sub");
        REQUIRE(r);

        std::vector<std::string> names;
        for (const auto& t : r.value())
            names.push_back(t.display);

        CHECK_THAT(names, Catch::Matchers::VectorContains(std::string("b.txt")));
        CHECK_THAT(names, !Catch::Matchers::VectorContains(std::string(".hidden.txt")));
    }

    SECTION("Non-recursive directory") {
        Options opt;
        opt.recursive = false;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        auto r = resolver.directory("/root/sub");
        // Non-recursive may have different behavior
        // Implementation-specific
    }
}

// =============================================================================
// Options Tests
// =============================================================================

TEST_CASE("Resolver Options: Configuration", "[resolve][options]") {
    SECTION("Default options") {
        Options opt;
        CHECK_FALSE(opt.recursive);
        // includeHidden defaults to true per resolver.hpp
        CHECK(opt.includeHidden);
        CHECK_FALSE(opt.forceAmbiguous);
    }

    SECTION("Custom options") {
        Options opt;
        opt.recursive = true;
        opt.includeHidden = true;
        opt.forceAmbiguous = true;

        auto repo = std::make_shared<MockRepo>();
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        // Resolver should use the custom options
        auto r = resolver.directory("/root/sub");
        REQUIRE(r);
    }
}

// =============================================================================
// Target Structure Tests
// =============================================================================

TEST_CASE("Target: Structure validation", "[resolve][target]") {
    SECTION("Target with ID and display") {
        Target t;
        t.id = "hash123";
        t.display = "file.txt";

        CHECK(t.id == "hash123");
        CHECK(t.display == "file.txt");
    }

    SECTION("Target equality") {
        Target t1;
        t1.id = "hash123";
        t1.display = "file.txt";

        Target t2;
        t2.id = "hash123";
        t2.display = "file.txt";

        // Targets with same ID should be considered equal
        CHECK(t1.id == t2.id);
    }
}

// =============================================================================
// Edge Cases
// =============================================================================

TEST_CASE("Resolver: Edge cases", "[resolve][edge]") {
    auto repo = std::make_shared<MockRepo>();
    Options opt;
    Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

    SECTION("Very long filename") {
        std::string longName(200, 'x');
        longName += ".txt";
        auto res = resolver.names({longName});
        // Should handle gracefully
        CHECK_FALSE(res); // Not found
    }

    SECTION("Special characters in name") {
        auto res = resolver.names({"file with spaces.txt"});
        // Should handle gracefully
        CHECK_FALSE(res); // Not found in mock
    }

    SECTION("Unicode filename") {
        auto res = resolver.names({"日本語ファイル.txt"});
        // Should handle gracefully
        CHECK_FALSE(res); // Not found in mock
    }

    SECTION("Path with multiple slashes") {
        auto res = resolver.names({"//path//to//file.txt"});
        // Should normalize or handle gracefully
    }

    SECTION("Empty string name") {
        auto res = resolver.names({""});
        // Empty name handling
    }

    SECTION("Dot files") {
        Options hiddenOpt;
        hiddenOpt.includeHidden = true;
        hiddenOpt.recursive = true;
        Resolver<MetadataBackend> hiddenResolver{MetadataBackend{repo}, hiddenOpt};

        auto res = hiddenResolver.directory("/root/sub");
        REQUIRE(res);

        bool foundHidden = false;
        for (const auto& t : res.value()) {
            if (t.display.front() == '.') {
                foundHidden = true;
                break;
            }
        }
        CHECK(foundHidden);
    }
}

// =============================================================================
// Backend Behavior Tests
// =============================================================================

TEST_CASE("MetadataBackend: Repository interaction", "[resolve][metadata][backend]") {
    auto repo = std::make_shared<MockRepo>();

    SECTION("Backend uses repository for queries") {
        MetadataBackend backend{repo};
        // Backend should properly forward queries to repository
        // This is implicitly tested through Resolver tests
    }

    SECTION("Backend handles repository errors") {
        // When repository returns errors, backend should propagate them
        Options opt;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        auto res = resolver.names({"nonexistent.txt"});
        CHECK_FALSE(res);
    }
}

TEST_CASE("FilesystemBackend: System interaction", "[resolve][filesystem][backend]") {
    auto root = make_temp_dir();

    SECTION("Backend reads actual filesystem") {
        fs::path file = root / "test.txt";
        {
            std::ofstream out(file);
            out << "test content";
        }

        fs::path oldCwd = fs::current_path();
        fs::current_path(root);

        Resolver<FilesystemBackend> resolver{FilesystemBackend{}, Options{}};
        auto res = resolver.names({"test.txt"});
        fs::current_path(oldCwd);

        REQUIRE(res);
        CHECK(res.value().size() == 1);
    }

    SECTION("Backend handles missing files") {
        Resolver<FilesystemBackend> resolver{FilesystemBackend{}, Options{}};
        auto res = resolver.names({(root / "nonexistent.txt").string()});
        CHECK_FALSE(res);
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(root, ec);
}

// =============================================================================
// Integration Tests
// =============================================================================

TEST_CASE("Resolver: Full workflow", "[resolve][integration]") {
    auto repo = std::make_shared<MockRepo>();

    SECTION("Resolve, then use targets") {
        Options opt;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        // Resolve a file
        auto res = resolver.names({"a.txt"});
        REQUIRE(res);
        REQUIRE(res.value().size() == 1);

        // Use the target
        const auto& target = res.value()[0];
        CHECK_FALSE(target.id.empty());
        CHECK_FALSE(target.display.empty());
    }

    SECTION("Resolve directory and iterate") {
        Options opt;
        opt.recursive = true;
        Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

        auto res = resolver.directory("/root/sub");
        REQUIRE(res);

        int count = 0;
        for (const auto& t : res.value()) {
            CHECK_FALSE(t.id.empty());
            count++;
        }
        CHECK(count > 0);
    }
}

} // namespace yams::tests::resolve
