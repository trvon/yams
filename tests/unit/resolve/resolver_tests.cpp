#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <yams/common/delete_resolver/resolver.hpp>
#include <yams/common/pattern_utils.h>
#include <yams/metadata/metadata_repository.h>

using ::testing::_;
using ::testing::ByMove;
using ::testing::Invoke;
using ::testing::Return;

namespace fs = std::filesystem;

namespace yams::tests::resolve {

using yams::common::resolve::FilesystemBackend;
using yams::common::resolve::MetadataBackend;
using yams::common::resolve::Options;
using yams::common::resolve::Resolver;
using yams::common::resolve::Target;

using yams::metadata::DocumentInfo;
using yams::metadata::IMetadataRepository;

// Minimal mock for IMetadataRepository (override only what we call)
class MockRepo : public IMetadataRepository {
public:
    // Document CRUD operations (unused in these tests; stubbed)
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

    // Metadata ops (unused)
    Result<void> setMetadata(int64_t, const std::string&,
                             const yams::metadata::MetadataValue&) override {
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
    Result<void> removeFromIndex(int64_t) override {
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
    Result<void> buildFuzzyIndex() override { return Error{ErrorCode::NotImplemented, "NI"}; }
    Result<void> updateFuzzyIndex(int64_t) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    // Bulk ops (some are used)
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

        if (pathPattern == "%/a.txt" || pathPattern == "/root/a.txt" || pathPattern == "%") {
            pushDoc("/root/a.txt", "a.txt",
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        }
        if (pathPattern == "%/b.txt" || pathPattern == "/root/sub/b.txt" ||
            pathPattern == "/root/sub/%" || pathPattern == "%") {
            pushDoc("/root/sub/b.txt", "b.txt",
                    "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
        }
        if (pathPattern == "%/.hidden.txt" || pathPattern == "/root/sub/.hidden.txt" ||
            pathPattern == "/root/sub/%" || pathPattern == "%") {
            pushDoc("/root/sub/.hidden.txt", ".hidden.txt",
                    "HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH");
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

    // Collections and snapshots (unused)
    Result<std::vector<DocumentInfo>> findDocumentsByCollection(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<DocumentInfo>> findDocumentsBySnapshot(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<DocumentInfo>> findDocumentsBySnapshotLabel(const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<std::string>> getCollections() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<std::string>> getSnapshots() override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<std::vector<std::string>> getSnapshotLabels() override {
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

    // Embedding status operations
    Result<void> updateDocumentEmbeddingStatus(int64_t, bool, const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }
    Result<void> updateDocumentEmbeddingStatusByHash(const std::string&, bool,
                                                     const std::string&) override {
        return Error{ErrorCode::NotImplemented, "NI"};
    }

    Result<void> checkpointWal() override { return {}; }

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
};

TEST(ResolverFilesystemBackend, ResolveNameToAbsoluteFilePath) {
    // Setup temp directory and file
    fs::path root = fs::temp_directory_path() / ("yams_resolve_fs_" + std::to_string(::getpid()));
    fs::create_directories(root);
    fs::path file = root / "file.txt";
    {
        std::ofstream out(file);
        out << "content";
    }

    // Relative path from root's parent
    fs::path oldCwd = fs::current_path();
    fs::current_path(root.parent_path());

    yams::common::resolve::Resolver<FilesystemBackend> resolver{FilesystemBackend{}, Options{}};

    // Name resolution should return absolute path
    auto res = resolver.names({(root.filename() / fs::path("file.txt")).string()});
    fs::current_path(oldCwd); // restore cwd

    ASSERT_TRUE(res);
    ASSERT_EQ(res.value().size(), 1);
    EXPECT_EQ(res.value()[0].id, fs::absolute(file).string());
    EXPECT_EQ(res.value()[0].display, fs::absolute(file).string());

    // Cleanup
    std::error_code ec;
    fs::remove_all(root, ec);
}

TEST(ResolverFilesystemBackend, ResolveDirectoryRequiresRecursive) {
    yams::common::resolve::Resolver<FilesystemBackend> resolver{FilesystemBackend{}, Options{}};
    auto res = resolver.directory("/does/not/matter");
    ASSERT_FALSE(res);
    EXPECT_EQ(res.error().code, ErrorCode::InvalidArgument);
}

TEST(ResolverMetadataBackend, ResolveNameSingleAndAmbiguous) {
    auto repo = std::make_shared<MockRepo>();
    Options opt;
    opt.forceAmbiguous = false;
    yams::common::resolve::Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

    // Non-existent name -> NotFound
    auto nf = resolver.names({"nope.txt"});
    ASSERT_FALSE(nf);
    EXPECT_EQ(nf.error().code, ErrorCode::NotFound);

    // Existing exact match -> ok
    auto ok = resolver.names({"a.txt"});
    ASSERT_TRUE(ok);
    ASSERT_EQ(ok.value().size(), 1);
    EXPECT_EQ(ok.value()[0].id.size(), 64u);
    EXPECT_EQ(ok.value()[0].display, "a.txt");

    // Ambiguous case: use a name that our mock would return multiple docs for if configured.
    // Our stub returns unique for "a.txt" and "b.txt". We'll simulate ambiguity by calling names({
    // "a.txt", "a.txt" })
    auto amb = resolver.names({"a.txt", "a.txt"});
    ASSERT_TRUE(amb); // dedup happens in resolver
    ASSERT_EQ(amb.value().size(), 1);
}

TEST(ResolverMetadataBackend, ResolvePatternFiltersByWildcard) {
    auto repo = std::make_shared<MockRepo>();
    Options opt;
    yams::common::resolve::Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

    // Our mock returns possible items when pattern prefilter matches.
    // Simulate a pattern that would map to "%/b.txt" (suffix match)
    auto r = resolver.pattern("b.txt"); // glob_to_sql_like -> "b.txt", prefilter tries "%/b.txt"
    ASSERT_TRUE(r);
    ASSERT_FALSE(r.value().empty());
    bool found = false;
    for (const auto& t : r.value()) {
        if (t.display == "b.txt") {
            found = true;
            EXPECT_EQ(t.id.size(), 64u);
        }
    }
    EXPECT_TRUE(found);
}

TEST(ResolverMetadataBackend, ResolveDirectoryRecursiveIncludesHiddenWhenEnabled) {
    auto repo = std::make_shared<MockRepo>();
    Options opt;
    opt.recursive = true;
    opt.includeHidden = true;
    yams::common::resolve::Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

    auto r = resolver.directory("/root/sub");
    ASSERT_TRUE(r);
    // As per mock, /root/sub/ contains b.txt and .hidden.txt
    std::vector<std::string> names;
    for (const auto& t : r.value())
        names.push_back(t.display);
    EXPECT_THAT(names, ::testing::Contains("b.txt"));
    EXPECT_THAT(names, ::testing::Contains(".hidden.txt"));
}

TEST(ResolverMetadataBackend, ResolveDirectoryRecursiveExcludesHiddenWhenDisabled) {
    auto repo = std::make_shared<MockRepo>();
    Options opt;
    opt.recursive = true;
    opt.includeHidden = false;
    yams::common::resolve::Resolver<MetadataBackend> resolver{MetadataBackend{repo}, opt};

    auto r = resolver.directory("/root/sub");
    ASSERT_TRUE(r);
    std::vector<std::string> names;
    for (const auto& t : r.value())
        names.push_back(t.display);
    EXPECT_THAT(names, ::testing::Contains("b.txt"));
    EXPECT_THAT(names, ::testing::Not(::testing::Contains(".hidden.txt")));
}

} // namespace yams::tests::resolve
