#pragma once

// AppServices interfaces: shared service layer for CLI and MCP.
// These interfaces define stable contracts for search, grep, document, and restore operations,
// enabling strict feature parity between CLI tools and the MCP server.

#include <boost/asio/awaitable.hpp>
#include <yams/api/content_store.h>
#include <yams/core/task.h>
#include <yams/core/types.h>
#include <yams/downloader/downloader.hpp>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_executor.h>
// Required for yams::extraction::IContentExtractor
#include <yams/extraction/content_extractor.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <regex>
#include <string>
#include <unordered_map>
#include <vector>
#include <boost/asio/any_io_executor.hpp>

// Forward declarations for LLM-optimized utilities
// Forward declare daemon ServiceManager used in AppContext
namespace yams::daemon {
class ServiceManager;
}

// Forward declare KnowledgeGraphStore for AppContext
namespace yams::metadata {
class KnowledgeGraphStore;
}

// Forward declare VectorDatabase for AppContext
namespace yams::vector {
class VectorDatabase;
}

namespace yams::app::services::utils {

/// Parse natural language time expressions into Unix epoch seconds
/// Supports: ISO 8601, relative times ("7d", "2h", "30m"), natural language ("yesterday", "last
/// week") Returns Unix epoch seconds, or error if parsing fails
Result<std::int64_t> parseTimeExpression(const std::string& timeExpr);

/// Check if a string looks like a content hash (hex string, 8-64 chars)
bool looksLikeHash(const std::string& str);

/// Classify file type from MIME type and extension for enhanced filtering
/// Returns: "text", "binary", "image", "document", "archive", "audio", "video", "executable"
std::string classifyFileType(const std::string& mimeType, const std::string& extension);

/// Create content snippet with configurable length and word boundaries
std::string createSnippet(const std::string& content, size_t maxLength,
                          bool preserveWordBoundary = true);

/// Format output as structured JSON for LLM consumption
template <typename T> std::string formatAsJson(const T& data, bool pretty = false);

/// Escape regex special characters for literal text search
std::string escapeRegex(const std::string& text);

/// Match glob patterns (supporting * and ? wildcards)
bool matchGlob(const std::string& text, const std::string& pattern);

struct NormalizedLookupPath {
    std::string original;
    std::string normalized;
    bool changed{false};
    bool hasWildcards{false};
};

/// Normalize a user-supplied document path for lookup against daemon-stored paths.
/// Converts relative inputs to absolute canonical form when no glob characters are present.
NormalizedLookupPath normalizeLookupPath(const std::string& path);

} // namespace yams::app::services::utils

namespace yams::app::services {

// Forward declare IGraphQueryService
class IGraphQueryService;

// Shared application context for services. Construct once and pass to service implementations.

struct AppContext {
    yams::daemon::ServiceManager* service_manager = nullptr;
    boost::asio::any_io_executor workerExecutor;
    std::shared_ptr<api::IContentStore> store;
    std::shared_ptr<search::SearchExecutor> searchExecutor;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo;
    std::shared_ptr<search::SearchEngine> searchEngine;     // New SearchEngine
    std::shared_ptr<vector::VectorDatabase> vectorDatabase; // For SearchEngineBuilder
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore; // PBI-043: tree diff KG integration
    std::shared_ptr<IGraphQueryService> graphQueryService;  // PBI-009: centralized graph queries
    // Optional: externally-provided content extractors (plugins)
    std::vector<std::shared_ptr<yams::extraction::IContentExtractor>> contentExtractors;

    // Degraded search / repair status (allows services to fallback during repairs)
    bool searchRepairInProgress{false};
    std::string searchRepairDetails{};
    int searchRepairProgress{0}; // 0-100%
};

// ===========================
// Extraction (PBI-006 DTOs)
// ===========================
struct ExtractionBBox {
    int x{0};
    int y{0};
    int w{0};
    int h{0};
};

struct ExtractionMatch {
    std::optional<int> page; // page index (when applicable)
    std::optional<int> line; // line index (when applicable)
    std::size_t start{0};
    std::size_t end{0};
    std::vector<ExtractionBBox> bboxes; // optional bounding boxes
};

struct ExtractionQuery {
    // Core fields (format-agnostic)
    std::string scope{"all"};             // "all" | "range" | "section" | "selector"
    std::string range;                    // e.g., "1-3,5" (pages/lines/rows depending on format)
    std::vector<std::string> sectionPath; // e.g., ["Chapter 2","Section 2.3"]
    std::string selector;                 // CSS/XPath/JSONPath (format-specific)
    std::string search;                   // string or regex (format-specific)
    int maxMatches{50};
    bool includeBBoxes{false};

    // Output formatting
    std::string format{"text"}; // "text" | "markdown" | "json"

    // Format-specific knobs (simple key/value map for Phase 1)
    std::unordered_map<std::string, std::string> formatOptions;
};

struct ExtractionResult {
    std::string mime;                     // normalized mime of result
    std::optional<std::string> text;      // extracted text/markdown
    std::optional<std::string> json;      // structured JSON payload (as string for Phase 1)
    std::vector<ExtractionMatch> matches; // match locations (when search applied)
    std::vector<int> pages;               // page indices included (when applicable)
    std::vector<std::string> sections;    // section titles included (when applicable)
};

// ===========================
// Search Service
// ===========================

struct SearchRequest {
    std::string query;
    std::size_t limit{10};

    // Search modes
    bool fuzzy{false};
    float similarity{0.7f};     // minimum similarity for fuzzy search (0.0-1.0)
    std::string hash;           // explicit hash search (full or partial, min 8 chars)
    std::string type{"hybrid"}; // "keyword" | "semantic" | "hybrid" | "hash"
    bool autoDetectHash{true};  // auto-detect if query looks like a hash

    // Query processing
    bool verbose{false};
    bool literalText{false}; // escape regex special characters
    bool showHash{false};    // show document hashes in results

    // Output formats (LLM ergonomics)
    bool pathsOnly{false};       // output only file paths (for scripting/LLM)
    bool jsonOutput{false};      // structured JSON output for LLM parsing
    std::string format{"table"}; // "table" | "json" | "minimal" | "paths"

    // Session-isolated memory (PBI-082)
    bool useSession{true};    // scope to active session when true (default)
    std::string sessionName;  // explicit session to search (empty = current)
    bool globalSearch{false}; // bypass session isolation, search global docs only

    // Line-level context (like grep)
    bool showLineNumbers{false};    // show line numbers with matches
    int beforeContext{0};           // show N lines before match
    int afterContext{0};            // show N lines after match
    int context{0};                 // show N lines before and after (overrides before/after)
    std::string colorMode{"never"}; // "always" | "never" | "auto"

    // Filtering
    std::string pathPattern;               // glob-like filename/path filter (legacy)
    std::vector<std::string> pathPatterns; // multiple glob patterns (preferred)
    std::vector<std::string> tags;         // filter by tags (presence-based)
    bool matchAllTags{false};              // require all specified tags

    // File type filters
    std::string fileType;  // "image", "document", "text", etc.
    std::string mimeType;  // MIME type filter
    std::string extension; // file extension filter
    bool textOnly{false};
    bool binaryOnly{false};

    // Time filters (for comprehensive search)
    std::string createdAfter; // ISO 8601, relative, or natural language
    std::string createdBefore;
    std::string modifiedAfter;
    std::string modifiedBefore;
    std::string indexedAfter;
    std::string indexedBefore;

    // Performance budgets (0 == unlimited)
    int vectorStageTimeoutMs{0};
    int keywordStageTimeoutMs{0};
    int snippetHydrationTimeoutMs{0};
};

struct SearchMatchContext {
    std::size_t lineNumber{0};            // line number of the match
    std::string line;                     // the matching line
    std::vector<std::string> beforeLines; // context lines before
    std::vector<std::string> afterLines;  // context lines after
    std::size_t columnStart{0};           // start column of match
    std::size_t columnEnd{0};             // end column of match
};

struct SearchItem {
    int64_t id{0};
    std::string hash;
    std::string title;
    std::string path;
    std::string fileName;
    double score{0.0};
    std::string snippet; // content preview

    // Metadata for LLM context
    std::string mimeType;
    std::string fileType;
    std::uint64_t size{0};
    std::int64_t created{0};
    std::int64_t modified{0};
    std::int64_t indexed{0};
    std::vector<std::string> tags;
    std::unordered_map<std::string, std::string> metadata;

    // Line-level matches (when line context is requested)
    std::vector<SearchMatchContext> matches;

    // Optional score breakdowns if verbose=true
    std::optional<double> vectorScore;
    std::optional<double> keywordScore;
    std::optional<double> kgEntityScore;
    std::optional<double> structuralScore;

    // Match explanation for LLM understanding
    std::optional<std::string> matchReason;  // why this result matched
    std::optional<std::string> searchMethod; // how it was found
};

struct SearchResponse {
    std::size_t total{0};       // total results found
    std::string type;           // actual search type used ("keyword", "semantic", "hybrid", "hash")
    int64_t executionTimeMs{0}; // query execution time
    std::vector<SearchItem> results; // detailed results

    // LLM-optimized outputs
    std::vector<std::string> paths; // pathsOnly=true output
    std::string jsonOutput;         // pre-formatted JSON for LLM parsing

    // Search metadata
    bool usedHybrid{false};
    bool wasHashSearch{false};     // true if query was treated as hash
    std::string detectedHashQuery; // if hash was auto-detected
    std::string appliedFormat;     // format used for output

    // Performance and debugging
    std::string queryInfo; // description of how query was processed
    std::unordered_map<std::string, std::string> searchStats; // detailed stats for debugging

    // Degraded mode indicators
    bool isDegraded{false};
    std::vector<std::string> timedOutComponents;
    std::vector<std::string> failedComponents;
    std::vector<std::string> contributingComponents;
};

class ISearchService {
public:
    virtual ~ISearchService() = default;
    virtual boost::asio::awaitable<Result<SearchResponse>> search(const SearchRequest& req) = 0;
    // Perform a lightweight text extraction + FTS5/fuzzy indexing for a newly-added
    // document so searches work instantly. Intended for small text-like files.
    // Skips heavy plugins (e.g., PDF) and large binaries.
    // Returns ok when indexing was performed or safely skipped.
    virtual Result<void> lightIndexForHash(const std::string& hash,
                                           std::size_t maxBytes = 2 * 1024 * 1024) = 0;
};

// ===========================
// Grep Service
// ===========================

struct GrepRequest {
    std::string pattern;
    std::vector<std::string> paths; // optional subset to search (files/dirs)

    // File selection
    std::vector<std::string> includePatterns; // file patterns to include
    bool recursive{true};                     // recursive directory search

    // Context
    int beforeContext{0};
    int afterContext{0};
    int context{0}; // if >0, overrides before/after

    // Pattern options
    bool ignoreCase{false};
    bool word{false}; // match whole words
    bool invert{false};
    bool literalText{false}; // escape regex special characters

    // Output modes
    bool lineNumbers{false};
    bool withFilename{true};
    bool count{false};
    bool filesWithMatches{false};
    bool filesWithoutMatch{false};
    bool pathsOnly{false};          // output paths only, no content
    std::string colorMode{"never"}; // "always" | "never" | "auto"

    // Hybrid search options
    bool regexOnly{false}; // disable semantic search
    int semanticLimit{10}; // limit for semantic results

    // Tag filtering
    std::vector<std::string> tags; // filter by tags
    bool matchAllTags{false};      // require all specified tags

    // Session-isolated memory (PBI-082)
    bool useSession{true};    // scope to active session when true (default)
    std::string sessionName;  // explicit session to search (empty = current)
    bool globalSearch{false}; // bypass session isolation, search global docs only

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

    // Enhanced match info
    std::string matchText;  // the actual matched text
    double confidence{1.0}; // match confidence (for semantic results)
    std::string matchType;  // "regex" | "semantic" | "hybrid"
};

struct GrepFileResult {
    std::string file;
    std::string fileName;
    std::size_t matchCount{0};
    std::vector<GrepMatch> matches;

    // File metadata for LLM context
    std::string mimeType;
    std::string fileType;
    std::uint64_t size{0};
    std::vector<std::string> tags;
    std::unordered_map<std::string, std::string> metadata;

    // Search method used for this file
    std::string searchMethod;      // how this file was searched
    bool wasSemanticSearch{false}; // true if semantic search was used
};

struct GrepResponse {
    std::vector<GrepFileResult> results;

    // Aggregates for different output modes
    std::vector<std::string> filesWith;    // file paths with >=1 match
    std::vector<std::string> filesWithout; // file paths with 0 matches
    std::vector<std::string> pathsOnly;    // paths-only output for LLM/scripting
    std::size_t totalMatches{0};
    std::size_t filesSearched{0};

    // Hybrid search breakdown
    std::size_t regexMatches{0};    // matches from regex search
    std::size_t semanticMatches{0}; // matches from semantic search

    // LLM-optimized output
    std::string jsonOutput;      // pre-formatted JSON
    std::string format{"table"}; // format used

    // Performance info
    std::int64_t executionTimeMs{0};                          // search execution time
    std::string queryInfo;                                    // description of search strategy
    std::unordered_map<std::string, std::string> searchStats; // detailed performance stats
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
    bool disableAutoMime{false}; // disable automatic MIME type detection

    // Extra attributes
    std::vector<std::string> tags;                         // tag list
    std::unordered_map<std::string, std::string> metadata; // key-value metadata

    // Collection and snapshot support
    std::string collection;    // collection name for organizing documents
    std::string snapshotId;    // unique snapshot identifier
    std::string snapshotLabel; // user-friendly snapshot label

    // Session-isolated memory (PBI-082)
    std::string sessionId;     // session to associate document with
    bool bypassSession{false}; // skip session tagging even if session is active

    // Embedding control
    bool noEmbeddings{false}; // disable embedding generation

    // Directory recursion options (when path is a directory)
    bool recursive{false};                    // recursively add files from directories
    std::vector<std::string> includePatterns; // glob patterns to include
    std::vector<std::string> excludePatterns; // glob patterns to exclude
};

struct StoreDocumentResponse {
    std::string hash;
    std::uint64_t bytesStored{0};
    std::uint64_t bytesDeduped{0};
};

struct RetrieveDocumentRequest {
    // Target selection (exactly one must be specified)
    std::string hash;               // full or partial hash
    std::string name;               // document name/pattern
    std::vector<std::string> names; // multiple names for batch retrieval
    std::string pattern;            // glob pattern for multiple docs

    // Output options
    std::string outputPath;     // optional: write to this path instead of memory
    bool metadataOnly{false};   // return only metadata, no content
    uint64_t maxBytes{0};       // max bytes to transfer (0 = unlimited)
    uint32_t chunkSize{262144}; // streaming chunk size

    // Content options
    bool includeContent{true};   // include document content
    bool raw{false};             // raw content without text extraction
    bool extract{false};         // force text extraction
    bool acceptCompressed{true}; // request compressed payload when supported

    // Knowledge graph options
    bool graph{false}; // show related documents
    int depth{1};      // graph traversal depth (1-5)

    // Selection criteria (for pattern/multiple selection)
    bool latest{false}; // get most recent matching doc
    bool oldest{false}; // get oldest matching doc

    // File type filters (for pattern matching)
    std::string fileType;  // "image", "document", "archive", etc.
    std::string mimeType;  // MIME type filter
    std::string extension; // file extension filter
    bool binaryOnly{false};
    bool textOnly{false};

    // Time filters (for pattern matching)
    std::string createdAfter; // ISO 8601, relative, or natural language
    std::string createdBefore;
    std::string modifiedAfter;
    std::string modifiedBefore;
    std::string indexedAfter;
    std::string indexedBefore;

    // Display options
    bool verbose{false};                            // detailed output
    std::optional<ExtractionQuery> extractionQuery; // PBI-006: optional rich extraction query
};

struct RetrievedDocument {
    std::string hash;
    std::string path;
    std::string name;
    std::string fileName;
    std::uint64_t size{0};
    std::string mimeType;
    std::string fileType; // "text", "binary", etc.

    // Time information
    std::int64_t created{0}; // Unix epoch seconds
    std::int64_t modified{0};
    std::int64_t indexed{0};

    // Content
    std::optional<std::string> content;       // set if includeContent=true
    std::optional<std::string> extractedText; // text extraction result
    bool compressed{false};
    std::optional<uint8_t> compressionAlgorithm;
    std::optional<uint8_t> compressionLevel;
    std::optional<uint64_t> uncompressedSize;
    std::optional<uint32_t> compressedCrc32;
    std::optional<uint32_t> uncompressedCrc32;
    std::vector<uint8_t> compressionHeader;
    std::optional<uint32_t> centroidWeight;
    std::optional<uint32_t> centroidDims;
    std::vector<float> centroidPreview; // truncated preview of centroid values

    // Metadata
    std::unordered_map<std::string, std::string> metadata;
    std::vector<std::string> tags;

    // Streaming info
    bool isStreaming{false};      // true if content is being streamed
    uint64_t bytesTransferred{0}; // for streaming progress
};

struct RelatedDocument {
    std::string hash;
    std::string path;
    std::string name;
    std::optional<std::string> relationship;
    int distance{1};
    double relevanceScore{0.0};
};

struct RetrieveDocumentResponse {
    // Single document result (for hash/name queries)
    std::optional<RetrievedDocument> document;

    // Multiple documents result (for pattern/batch queries)
    std::vector<RetrievedDocument> documents;

    // Graph traversal results
    bool graphEnabled{false};
    std::vector<RelatedDocument> related;

    // Result metadata
    std::size_t totalFound{0}; // total matches for pattern queries
    bool hasMore{false};       // more results available

    // Output information
    std::optional<std::string> outputPath; // if written to file
    uint64_t totalBytes{0};                // total bytes processed
};

struct ListDocumentsRequest {
    // Pagination
    int limit{100};
    int offset{0};
    std::optional<int> recent; // show N most recent documents

    // Filtering
    std::string pattern;           // glob-like filename filter
    std::vector<std::string> tags; // filter by tags
    bool matchAllTags{false};      // require all tags vs any tag

    // File type filters
    std::string type;      // "image" | "document" | "archive" | "audio" | "video" | "text" |
                           // "executable" | "binary"
    std::string mime;      // MIME type filter (exact match)
    std::string extension; // file extension filter (e.g., ".jpg,.png")
    bool binary{false};    // show only binary files
    bool text{false};      // show only text files

    // Time-based filters (ISO 8601, relative like "7d", or natural like "yesterday")
    std::string createdAfter;
    std::string createdBefore;
    std::string modifiedAfter;
    std::string modifiedBefore;
    std::string indexedAfter;
    std::string indexedBefore;

    // Change tracking
    bool changes{false};     // show documents with recent modifications (last 24h)
    std::string since;       // show documents changed since specified time
    bool diffTags{false};    // show documents grouped by change type (added/modified/deleted)
    bool showDeleted{false}; // include documents deleted from filesystem
    std::string changeWindow{"24h"}; // time window for "recent changes"

    // Display options
    std::string format{"table"}; // "table" | "json" | "csv" | "minimal" | "paths"
    bool showSnippets{true};     // show content previews
    int snippetLength{50};       // preview length in characters
    bool showMetadata{false};    // show all metadata for each document
    bool showTags{true};         // show document tags
    bool groupBySession{false};  // group documents by time periods
    bool verbose{false};         // show detailed information
    bool pathsOnly{false};       // output only file paths (for LLM/scripting)

    // Sorting
    std::string sortBy{
        "indexed"}; // "name" | "size" | "created" | "modified" | "indexed" | "date" | "hash"
    std::string sortOrder{"desc"}; // "asc" | "desc"
    bool reverse{false};           // reverse sort order (alternative to sortOrder)
};

struct DocumentEntry {
    std::string name;
    std::string fileName;
    std::string hash;
    std::string path;
    std::string extension;
    std::uint64_t size{0};
    std::string mimeType;
    std::string fileType; // "text" | "binary" | "image" | "document" | etc.

    // Unix epoch seconds for simplicity in interfaces
    std::int64_t created{0};
    std::int64_t modified{0};
    std::int64_t indexed{0};

    std::vector<std::string> tags;
    std::unordered_map<std::string, std::string> metadata; // full metadata for verbose mode

    // Content preview
    std::optional<std::string> snippet; // content preview

    // Change tracking info
    std::optional<std::string> changeType;  // "added" | "modified" | "deleted"
    std::optional<std::int64_t> changeTime; // when the change was detected

    // Scoring/ranking info
    double relevanceScore{0.0};             // search/filter relevance
    std::optional<std::string> matchReason; // why this document matched
};

struct ListDocumentsResponse {
    std::vector<DocumentEntry> documents;
    std::size_t count{0};      // documents returned in this response
    std::size_t totalFound{0}; // total matching documents
    bool hasMore{false};       // more results available via pagination

    // Paths-only mode (for LLM/scripting efficiency)
    std::vector<std::string> paths; // populated when pathsOnly=true

    // Change tracking results
    std::vector<DocumentEntry> addedDocuments; // when diffTags=true
    std::vector<DocumentEntry> modifiedDocuments;
    std::vector<DocumentEntry> deletedDocuments;

    // Grouping results (when groupBySession=true)
    std::unordered_map<std::string, std::vector<DocumentEntry>> groupedResults;

    // Echo filters used (for traceability / debugging / LLM context)
    std::optional<std::string> pattern;
    std::vector<std::string> filteredByTags;
    std::string sortBy;
    std::string sortOrder;
    std::string appliedFormat; // format used

    // Performance info
    std::int64_t executionTimeMs{0}; // query execution time
    std::string queryInfo;           // description of query for debugging
};

struct UpdateMetadataRequest {
    // Target by hash OR by name. Implementations should enforce exactly one selector.
    std::string hash;
    std::string name;

    // Accept either key-value map, or array of "key=value" pairs (CLI-compatible)
    std::unordered_map<std::string, std::string> keyValues;
    std::vector<std::string> pairs;

    // Enhanced update capabilities
    std::string newContent;              // Optional: update document content
    std::vector<std::string> addTags;    // Tags to add
    std::vector<std::string> removeTags; // Tags to remove
    bool atomic{true};                   // All-or-nothing transaction
    bool createBackup{false};            // Create backup before updating

    bool verbose{false};
};

struct UpdateMetadataResponse {
    bool success{false};
    std::string hash; // resolved hash (when available)
    std::size_t updatesApplied{0};
    std::optional<int64_t> documentId;

    // Enhanced response fields
    bool contentUpdated{false};
    std::size_t tagsAdded{0};
    std::size_t tagsRemoved{0};
    std::string backupHash; // if backup was created
};

struct CatDocumentRequest {
    std::string hash;
    std::string name; // alternative selector
    std::optional<ExtractionQuery> extractionQuery;
};

struct CatDocumentResponse {
    std::string content;
    std::size_t size{0};
    std::string hash;
    std::string name;
    std::optional<ExtractionResult> extraction;
};

struct DeleteByNameRequest {
    std::string name;
    std::string hash; // delete by hash (full or partial)
    std::vector<std::string> names;
    std::string pattern; // glob-like

    bool dryRun{false};
    bool force{false};     // skip confirmation
    bool keepRefs{false};  // keep reference counts (don't decrement)
    bool recursive{false}; // delete directory contents recursively
    bool verbose{false};   // verbose output
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
    virtual Result<std::string> resolveNameToHash(const std::string& name, bool oldest = false) = 0;
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

// ===========================
// Download Service
// ===========================

struct DownloadServiceRequest {
    std::string url;
    std::vector<downloader::Header> headers;
    std::optional<downloader::Checksum> checksum;
    int concurrency{4};
    std::size_t chunkSizeBytes{8'388'608};
    std::chrono::milliseconds timeout{60'000};
    downloader::RetryPolicy retry;
    downloader::RateLimit rateLimit;
    bool resume{true};
    std::optional<std::string> proxy;
    downloader::TlsConfig tls;
    bool followRedirects{true};
    bool storeOnly{true};
    std::optional<std::string> exportPath;
    downloader::OverwritePolicy overwrite{downloader::OverwritePolicy::Never};
    // User-supplied annotations
    std::vector<std::string> tags;                         // repeated --tag
    std::unordered_map<std::string, std::string> metadata; // repeated --meta key=value
};

struct DownloadServiceResponse {
    std::string url;
    std::string hash;
    std::filesystem::path storedPath;
    std::uint64_t sizeBytes{0};
    bool success{false};
    std::optional<int> httpStatus;
    std::optional<std::string> etag;
    std::optional<std::string> lastModified;
    std::optional<bool> checksumOk;
    // Friendly retrieval hint: suggested index/name for discovery
    std::string indexName; // e.g., host/path basename or Content-Disposition filename
};

class IDownloadService {
public:
    virtual ~IDownloadService() = default;
    virtual Result<DownloadServiceResponse> download(const DownloadServiceRequest& req) = 0;
};

// ===========================
// Indexing Service
// ===========================

struct AddDirectoryRequest {
    std::string directoryPath;
    std::string collection;
    std::vector<std::string> tags; // tags to apply to each stored document
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    std::unordered_map<std::string, std::string> metadata;
    bool recursive{true};
    bool followSymlinks{false};
    // Post-add verification: when true, verify stored content exists and matches expected hash
    bool verify{false};
    // Optional: verify indexes (FTS/vector) presence when enabled
    bool verifyIndexes{false};
    std::string snapshotLabel; // Optional human-friendly label for automatic snapshot

    // Session-isolated memory (PBI-082)
    std::string sessionId;     // session to associate documents with
    bool bypassSession{false}; // skip session tagging even if session is active
};

struct IndexedFileResult {
    std::string path;
    std::string hash;
    std::uint64_t sizeBytes{0};
    bool success{false};
    std::optional<std::string> error;
};

struct AddDirectoryResponse {
    std::string directoryPath;
    std::string collection;
    std::size_t filesProcessed{0};
    std::size_t filesIndexed{0};
    std::size_t filesSkipped{0};
    std::size_t filesFailed{0};
    std::vector<IndexedFileResult> results;
    std::string snapshotId;    // Auto-generated timestamp ID (server-side)
    std::string snapshotLabel; // Optional human-friendly label
    std::string treeRootHash;  // Merkle tree root hash (PBI-043)
};

class IIndexingService {
public:
    virtual ~IIndexingService() = default;
    virtual Result<AddDirectoryResponse> addDirectory(const AddDirectoryRequest& req) = 0;
};

// ===========================
// Embedding Service
// ===========================

struct EmbedDocumentsServiceRequest {
    std::vector<std::string> documentHashes;
    std::string modelName;
    uint32_t batchSize{32};
    bool skipExisting{true};
};

struct EmbedDocumentsServiceResponse {
    uint64_t requested{0};
    uint64_t embedded{0};
    uint64_t skipped{0};
    uint64_t failed{0};
};

class IEmbeddingService {
public:
    virtual ~IEmbeddingService() = default;
    virtual boost::asio::awaitable<Result<EmbedDocumentsServiceResponse>>
    embedDocuments(const EmbedDocumentsServiceRequest& req) = 0;
};

// ===========================
// Stats Service
// ===========================

struct StatsRequest {
    bool fileTypes{false};
    bool verbose{false};
};

struct FileTypeStats {
    std::string extension;
    std::size_t count{0};
    std::uint64_t totalBytes{0};
};

struct StatsResponse {
    std::uint64_t totalObjects{0};
    std::uint64_t totalBytes{0};
    std::uint64_t uniqueHashes{0};
    std::uint64_t deduplicationSavings{0};
    std::vector<FileTypeStats> fileTypes;
    std::unordered_map<std::string, std::uint64_t> additionalStats;
};

class IStatsService {
public:
    virtual ~IStatsService() = default;
    virtual Result<StatsResponse> getStats(const StatsRequest& req) = 0;
};

/**
 * Factory functions to construct service implementations from shared context.
 */
std::shared_ptr<ISearchService> makeSearchService(const AppContext& ctx);
std::shared_ptr<IEmbeddingService> makeEmbeddingService(const AppContext& ctx);
std::shared_ptr<IGrepService> makeGrepService(const AppContext& ctx);
std::shared_ptr<IDocumentService> makeDocumentService(const AppContext& ctx);
std::shared_ptr<IDownloadService> makeDownloadService(const AppContext& ctx);
std::shared_ptr<IIndexingService> makeIndexingService(const AppContext& ctx);
std::shared_ptr<IStatsService> makeStatsService(const AppContext& ctx);

} // namespace yams::app::services
