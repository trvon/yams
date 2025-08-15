#pragma once

// AppServices interfaces: shared service layer for CLI and MCP.
// These interfaces define stable contracts for search, grep, document, and restore operations,
// enabling strict feature parity between CLI tools and the MCP server.

#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_executor.h>

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::app::services {

// Shared application context for services. Construct once and pass to service implementations.
struct AppContext {
    std::shared_ptr<api::IContentStore> store;
    std::shared_ptr<search::SearchExecutor> searchExecutor;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo;
    std::shared_ptr<search::HybridSearchEngine> hybridEngine;
};

// ===========================
// Search Service
// ===========================

struct SearchRequest {
    std::string query;
    std::size_t limit{10};

    // Modes and ergonomics
    bool fuzzy{false};
    float similarity{0.7f};
    std::string hash;           // explicit hash search (full or partial)
    std::string type{"hybrid"}; // "keyword" | "semantic" | "hybrid"
    bool verbose{false};

    // LLM ergonomics / output shaping
    bool pathsOnly{false};
    bool lineNumbers{false};
    int beforeContext{0};
    int afterContext{0};
    int context{0};                 // if >0, overrides before/after
    std::string colorMode{"never"}; // "always" | "never" | "auto"
};

struct SearchItem {
    int64_t id{0};
    std::string hash;
    std::string title;
    std::string path;
    double score{0.0};
    std::string snippet;

    // Optional score breakdowns if verbose=true
    std::optional<double> vectorScore;
    std::optional<double> keywordScore;
    std::optional<double> kgEntityScore;
    std::optional<double> structuralScore;
};

struct SearchResponse {
    std::size_t total{0};
    std::string type; // actual search type used
    int64_t executionTimeMs{0};
    std::vector<SearchItem> results; // normal detailed results
    std::vector<std::string> paths;  // pathsOnly=true output
    bool usedHybrid{false};
};

class ISearchService {
public:
    virtual ~ISearchService() = default;
    virtual Result<SearchResponse> search(const SearchRequest& req) = 0;
};

// ===========================
// Grep Service
// ===========================

struct GrepRequest {
    std::string pattern;
    std::vector<std::string> paths; // optional subset to search (files/dirs)

    // Context
    int beforeContext{0};
    int afterContext{0};
    int context{0}; // if >0, overrides before/after

    // Pattern options
    bool ignoreCase{false};
    bool word{false}; // match whole words
    bool invert{false};

    // Output modes
    bool lineNumbers{false};
    bool withFilename{true};
    bool count{false};
    bool filesWithMatches{false};
    bool filesWithoutMatch{false};
    std::string colorMode{"never"}; // "always" | "never" | "auto"

    // Limits
    int maxCount{0}; // stop after N matches per file (0 => unlimited)
};

struct GrepMatch {
    std::size_t lineNumber{0};
    std::size_t columnStart{0};
    std::size_t columnEnd{0};
    std::string line;
    std::vector<std::string> before; // context lines
    std::vector<std::string> after;  // context lines
};

struct GrepFileResult {
    std::string file;
    std::size_t matchCount{0};
    std::vector<GrepMatch> matches;
};

struct GrepResponse {
    std::vector<GrepFileResult> results;

    // Aggregates suitable for count/files_with_matches/files_without_match modes
    std::vector<std::string> filesWith;    // file paths with >=1 match
    std::vector<std::string> filesWithout; // file paths with 0 matches
    std::size_t totalMatches{0};
};

class IGrepService {
public:
    virtual ~IGrepService() = default;
    virtual Result<GrepResponse> grep(const GrepRequest& req) = 0;
};

// ===========================
// Document Service
// ===========================

struct StoreDocumentRequest {
    // Provide either path, or (content + name)
    std::string path;
    std::string content;
    std::string name; // used when content is provided directly
    std::string mimeType;

    // Extra attributes
    std::vector<std::string> tags;                         // tag list
    std::unordered_map<std::string, std::string> metadata; // key-value metadata
};

struct StoreDocumentResponse {
    std::string hash;
    std::uint64_t bytesStored{0};
    std::uint64_t bytesDeduped{0};
};

struct RetrieveDocumentRequest {
    std::string hash;
    std::string outputPath; // optional: if non-empty, write to this path

    // Graph options
    bool graph{false};
    int depth{1};
    bool includeContent{false};
};

struct RetrievedDocument {
    std::string hash;
    std::string path;
    std::string name;
    std::uint64_t size{0};
    std::string mimeType;
    std::optional<std::string> content; // set if includeContent=true and available
};

struct RelatedDocument {
    std::string hash;
    std::string path;
    std::optional<std::string> relationship;
    int distance{1};
};

struct RetrieveDocumentResponse {
    bool graphEnabled{false};
    std::optional<RetrievedDocument> document;
    std::vector<RelatedDocument> related; // optional; non-empty if graphEnabled=true
};

struct ListDocumentsRequest {
    int limit{100};
    int offset{0};

    std::string pattern;           // glob-like filename filter
    std::vector<std::string> tags; // filter by tags
    std::string type;              // "text" | "binary" | ""
    std::string mime;              // MIME pattern
    std::string extension;         // "md" or ".md"

    bool binary{false};
    bool text{false};

    // Time-based filters: ISO 8601 or relative times (interpreted by implementation)
    std::string createdAfter;
    std::string createdBefore;
    std::string modifiedAfter;
    std::string modifiedBefore;
    std::string indexedAfter;
    std::string indexedBefore;

    // Recent N documents
    std::optional<int> recent;

    // Sorting
    std::string sortBy{"indexed"}; // "name" | "size" | "created" | "modified" | "indexed"
    std::string sortOrder{"desc"}; // "asc" | "desc"
};

struct DocumentEntry {
    std::string name;
    std::string hash;
    std::string path;
    std::string extension;
    std::uint64_t size{0};
    std::string mimeType;
    std::string fileType; // "text" | "binary" | ""

    // Unix epoch seconds for simplicity in interfaces
    std::int64_t created{0};
    std::int64_t modified{0};
    std::int64_t indexed{0};

    std::vector<std::string> tags;
};

struct ListDocumentsResponse {
    std::vector<DocumentEntry> documents;
    std::size_t count{0};
    std::size_t totalFound{0};

    // Echo filters used (for traceability / debugging)
    std::optional<std::string> pattern;
    std::vector<std::string> filteredByTags;
    std::string sortBy;
    std::string sortOrder;
};

struct UpdateMetadataRequest {
    // Target by hash OR by name. Implementations should enforce exactly one selector.
    std::string hash;
    std::string name;

    // Accept either key-value map, or array of "key=value" pairs (CLI-compatible)
    std::unordered_map<std::string, std::string> keyValues;
    std::vector<std::string> pairs;

    bool verbose{false};
};

struct UpdateMetadataResponse {
    bool success{false};
    std::string hash; // resolved hash (when available)
    std::size_t updatesApplied{0};
    std::optional<int64_t> documentId;
};

struct CatDocumentRequest {
    std::string hash;
    std::string name; // alternative selector
};

struct CatDocumentResponse {
    std::string content;
    std::size_t size{0};
    std::string hash;
    std::string name;
};

struct DeleteByNameRequest {
    std::string name;
    std::vector<std::string> names;
    std::string pattern; // glob-like
    bool dryRun{false};
};

struct DeleteByNameResult {
    std::string name;
    std::string hash;
    bool deleted{false};
    std::optional<std::string> error;
};

struct DeleteByNameResponse {
    bool dryRun{false};
    std::vector<DeleteByNameResult> deleted;
    std::size_t count{0};
    std::vector<DeleteByNameResult> errors;
};

class IDocumentService {
public:
    virtual ~IDocumentService() = default;

    // Storage operations
    virtual Result<StoreDocumentResponse> store(const StoreDocumentRequest& req) = 0;
    virtual Result<RetrieveDocumentResponse> retrieve(const RetrieveDocumentRequest& req) = 0;
    virtual Result<CatDocumentResponse> cat(const CatDocumentRequest& req) = 0;

    // Listing and metadata
    virtual Result<ListDocumentsResponse> list(const ListDocumentsRequest& req) = 0;
    virtual Result<UpdateMetadataResponse> updateMetadata(const UpdateMetadataRequest& req) = 0;

    // Name-based helpers
    virtual Result<std::string> resolveNameToHash(const std::string& name) = 0;
    virtual Result<DeleteByNameResponse> deleteByName(const DeleteByNameRequest& req) = 0;
};

// ===========================
// Restore Service
// ===========================

struct RestoreSummary {
    std::size_t documentsFound{0};
    std::size_t documentsRestored{0};
    std::size_t documentsFailed{0};
    std::size_t documentsSkipped{0};
    bool dryRun{false};
};

struct RestoreItemResult {
    std::string documentName;
    std::string hash;
    std::string outputPath;
    std::string status; // "restored" | "failed" | "skipped" | "would_restore"
    std::optional<std::string> error;
    std::optional<std::uint64_t> size;
};

struct RestoreCommonOptions {
    std::string outputDirectory{"."};
    std::string layoutTemplate{"{path}"};
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    bool overwrite{false};
    bool createDirs{true};
    bool dryRun{false};
};

struct RestoreCollectionRequest {
    std::string collection;
    RestoreCommonOptions options;
};

struct RestoreSnapshotRequest {
    std::string snapshotId;    // Either snapshotId...
    std::string snapshotLabel; // ...or label (exactly one required)
    RestoreCommonOptions options;
};

struct RestoreResponse {
    RestoreSummary summary;
    std::string scope; // description string ("collection: X" / "snapshot ID: Y", etc.)
    std::string outputDirectory;
    std::string layoutTemplate;
    std::vector<RestoreItemResult> results;
};

class IRestoreService {
public:
    virtual ~IRestoreService() = default;

    virtual Result<RestoreResponse> restoreCollection(const RestoreCollectionRequest& req) = 0;
    virtual Result<RestoreResponse> restoreSnapshot(const RestoreSnapshotRequest& req) = 0;
};

/**
 * Factory to construct default SearchService implementation from shared context.
 */
std::shared_ptr<ISearchService> makeSearchService(const AppContext& ctx);

} // namespace yams::app::services