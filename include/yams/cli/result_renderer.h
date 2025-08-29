#pragma once

#include <nlohmann/json.hpp>
#include <algorithm>
#include <array>
#include <chrono>
#include <concepts>
#include <iomanip>
#include <iostream>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>
#include <yams/metadata/document_metadata.h>
#include <yams/search/search_results.h>

namespace yams::cli {

using json = nlohmann::json;

// ============================================================================
// Generic Search Result Rendering System
// ====================================================================================

/**
 * @brief concept for basic result adapters
 * Defines the minimum interface requirements for any result type
 */
template <typename T>
concept ResultAdapter = requires(const T& t) {
    { t.path() } -> std::convertible_to<std::string_view>;
    { t.title() } -> std::convertible_to<std::string_view>;
    { t.id() } -> std::convertible_to<std::string>;
    { t.hash() } -> std::convertible_to<std::string_view>;
    { t.size() } -> std::convertible_to<int64_t>;
    { t.mimeType() } -> std::convertible_to<std::string_view>;
};

/**
 * @brief concept for search result adapters with score and snippet
 */
template <typename T>
concept SearchResultAdapter = ResultAdapter<T> && requires(const T& t) {
    { t.score() } -> std::convertible_to<double>;
    { t.snippet() } -> std::convertible_to<std::string_view>;
};

/**
 * @brief concept for document info adapters with timestamps
 */
template <typename T>
concept DocumentInfoAdapter = ResultAdapter<T> && requires(const T& t) {
    { t.createdTime() } -> std::convertible_to<std::chrono::system_clock::time_point>;
    { t.modifiedTime() } -> std::convertible_to<std::chrono::system_clock::time_point>;
    { t.indexedTime() } -> std::convertible_to<std::chrono::system_clock::time_point>;
};

// ============================================================================
// Concrete Adapters for Different Data Types
// ==========================================================================

/**
 * @brief Adapter for metadata::SearchResult (search results with scores)
 */
class MetadataSearchResultAdapter {
private:
    const metadata::SearchResult& result_;

public:
    // Allow implicit conversion from SearchResult for ranges compatibility
    MetadataSearchResultAdapter(const metadata::SearchResult& result) : result_(result) {}

    [[nodiscard]] std::string_view path() const noexcept {
        return (!result_.document.filePath.empty() && result_.document.filePath != "stdin")
                   ? std::string_view{result_.document.filePath}
                   : std::string_view{result_.document.fileName};
    }

    [[nodiscard]] std::string_view title() const noexcept { return result_.document.fileName; }

    [[nodiscard]] std::string id() const { return std::to_string(result_.document.id); }

    [[nodiscard]] double score() const noexcept { return result_.score; }

    [[nodiscard]] std::string_view snippet() const noexcept { return result_.snippet; }

    [[nodiscard]] std::string_view hash() const noexcept { return result_.document.sha256Hash; }

    [[nodiscard]] int64_t size() const noexcept { return result_.document.fileSize; }

    [[nodiscard]] std::string_view mimeType() const noexcept { return result_.document.mimeType; }
};

/**
 * @brief Adapter for metadata::DocumentInfo (document listings)
 */
class DocumentInfoResultAdapter {
private:
    const metadata::DocumentInfo& doc_;

public:
    // Allow implicit conversion from DocumentInfo for ranges compatibility
    DocumentInfoResultAdapter(const metadata::DocumentInfo& doc) : doc_(doc) {}

    [[nodiscard]] std::string_view path() const noexcept {
        return (!doc_.filePath.empty() && doc_.filePath != "stdin")
                   ? std::string_view{doc_.filePath}
                   : std::string_view{doc_.fileName};
    }

    [[nodiscard]] std::string_view title() const noexcept { return doc_.fileName; }

    [[nodiscard]] std::string id() const { return std::to_string(doc_.id); }

    [[nodiscard]] std::string_view hash() const noexcept { return doc_.sha256Hash; }

    [[nodiscard]] int64_t size() const noexcept { return doc_.fileSize; }

    [[nodiscard]] std::string_view mimeType() const noexcept { return doc_.mimeType; }

    [[nodiscard]] constexpr std::chrono::system_clock::time_point createdTime() const noexcept {
        return doc_.createdTime;
    }

    [[nodiscard]] constexpr std::chrono::system_clock::time_point modifiedTime() const noexcept {
        return doc_.modifiedTime;
    }

    [[nodiscard]] constexpr std::chrono::system_clock::time_point indexedTime() const noexcept {
        return doc_.indexedTime;
    }
};

/**
 * @brief Adapter for search::SearchResultItem (hybrid search engine results)
 */
class HybridSearchResultAdapter {
private:
    const search::SearchResultItem& item_;

public:
    // Allow implicit conversion from SearchResultItem for ranges compatibility
    HybridSearchResultAdapter(const search::SearchResultItem& item) : item_(item) {}

    [[nodiscard]] std::string_view path() const noexcept { return item_.path; }

    [[nodiscard]] std::string_view title() const noexcept { return item_.title; }

    [[nodiscard]] std::string id() const { return std::to_string(item_.documentId); }

    [[nodiscard]] double score() const noexcept {
        return static_cast<double>(item_.relevanceScore);
    }

    [[nodiscard]] std::string_view snippet() const noexcept { return item_.contentPreview; }

    [[nodiscard]] std::string_view hash() const noexcept {
        // Search results don't typically have hashes, return empty
        return "";
    }

    [[nodiscard]] int64_t size() const noexcept { return static_cast<int64_t>(item_.fileSize); }

    [[nodiscard]] std::string_view mimeType() const noexcept { return item_.contentType; }

    [[nodiscard]] constexpr std::chrono::system_clock::time_point createdTime() const noexcept {
        return item_.lastModified; // Use lastModified as creation time
    }

    [[nodiscard]] constexpr std::chrono::system_clock::time_point modifiedTime() const noexcept {
        return item_.lastModified;
    }

    [[nodiscard]] constexpr std::chrono::system_clock::time_point indexedTime() const noexcept {
        return item_.indexedAt;
    }
};

/**
 * @brief Simple adapter for file paths (e.g., grep results)
 */
class FilePathAdapter {
private:
    std::string path_;

public:
    explicit FilePathAdapter(std::string path) : path_(std::move(path)) {}

    [[nodiscard]] std::string_view path() const noexcept { return path_; }

    [[nodiscard]] std::string_view title() const noexcept {
        auto pos = path_.find_last_of('/');
        return pos != std::string::npos ? std::string_view{path_}.substr(pos + 1)
                                        : std::string_view{path_};
    }

    [[nodiscard]] std::string id() const { return path_; }

    [[nodiscard]] constexpr std::string_view hash() const noexcept {
        return ""; // No hash for simple paths
    }

    [[nodiscard]] constexpr int64_t size() const noexcept {
        return 0; // Size not available for simple paths
    }

    [[nodiscard]] constexpr std::string_view mimeType() const noexcept {
        return ""; // MIME type not available for simple paths
    }
};

// Verify our adapters satisfy the concepts
static_assert(SearchResultAdapter<MetadataSearchResultAdapter>);
static_assert(DocumentInfoAdapter<DocumentInfoResultAdapter>);
static_assert(SearchResultAdapter<HybridSearchResultAdapter>);
static_assert(DocumentInfoAdapter<HybridSearchResultAdapter>);
static_assert(ResultAdapter<FilePathAdapter>);

// ============================================================================
// Generic Result Renderer
// ==========================================================================

/**
 * @brief Output format modes supported by the renderer
 */
enum class OutputFormat {
    PathsOnly, ///< Show only file paths, one per line
    JSON,      ///< JSON format output
    Table,     ///< Formatted table output
    Verbose,   ///< Detailed verbose output
    Minimal    ///< Concise single-line output
};

/**
 * @brief Configuration for result rendering
 */
struct RenderConfig {
    std::string pathFilter; ///< Filter results by path pattern
    OutputFormat format = OutputFormat::Verbose;
    bool showHash = false;         ///< Show document hashes
    bool showFullHash = false;     ///< Show full hash vs truncated
    bool showScores = false;       ///< Show relevance scores (if available)
    bool showTimestamps = false;   ///< Show creation/modification times
    size_t snippetMaxLength = 200; ///< Maximum snippet length
};

/**
 * @brief Genericxresult renderer with ranges and path filtering
 */
template <ResultAdapter AdapterType> class ResultRenderer {
private:
    RenderConfig config_;

    [[nodiscard]] bool matchesPathFilter(std::string_view path) const noexcept {
        return config_.pathFilter.empty() ||
               path.find(config_.pathFilter) != std::string_view::npos;
    }

    [[nodiscard]] static std::string truncateSnippet(std::string_view snippet, size_t maxLength) {
        if (snippet.length() <= maxLength) {
            return std::string{snippet};
        }

        std::string result{snippet.substr(0, maxLength)};

        // Try to find a good breaking point
        auto lastSpace = result.find_last_of(" \t\n");
        if (lastSpace != std::string::npos && lastSpace > maxLength * 0.8) {
            result = result.substr(0, lastSpace);
        }

        return result + "...";
    }

public:
    [[nodiscard]] static std::string formatSize(int64_t bytes) {
        constexpr std::array<std::string_view, 5> units = {"B", "KB", "MB", "GB", "TB"};

        if (bytes == 0)
            return "0 B";

        double size = static_cast<double>(bytes);
        size_t unitIndex = 0;

        while (size >= 1024.0 && unitIndex < units.size() - 1) {
            size /= 1024.0;
            ++unitIndex;
        }

        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << size << " " << units[unitIndex];
        return oss.str();
    }

    [[nodiscard]] static std::string formatTimestamp(std::chrono::system_clock::time_point tp) {
        auto time_t = std::chrono::system_clock::to_time_t(tp);
        std::ostringstream oss;
        oss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }

public:
    explicit ResultRenderer(RenderConfig config) : config_(std::move(config)) {}

    /**
     * @brief Render results using ranges with path filtering
     */
    template <std::ranges::range ResultRange>
    void render(const std::string& query, const std::string& searchType, const ResultRange& results,
                int64_t totalCount = 0, int64_t executionTimeMs = 0)
    requires std::convertible_to<std::ranges::range_value_t<ResultRange>, AdapterType>
    {
        // Apply path filtering using ranges
        auto filtered_results =
            results | std::views::transform([](const auto& item) { return AdapterType{item}; }) |
            std::views::filter(
                [this](const auto& adapter) { return matchesPathFilter(adapter.path()); });

        switch (config_.format) {
            case OutputFormat::PathsOnly:
                renderPathsOnly(filtered_results);
                break;
            case OutputFormat::JSON:
                renderJson(query, searchType, filtered_results, totalCount, executionTimeMs);
                break;
            case OutputFormat::Table:
                renderTable(query, filtered_results, totalCount);
                break;
            case OutputFormat::Verbose:
                renderVerbose(query, searchType, filtered_results, totalCount);
                break;
            case OutputFormat::Minimal:
                renderMinimal(query, filtered_results, totalCount);
                break;
        }
    }

private:
    template <std::ranges::range FilteredRange>
    void renderPathsOnly(FilteredRange& filtered_results) {
        for (const auto& adapter : filtered_results) {
            std::cout << adapter.path() << std::endl;
        }
    }

    template <std::ranges::range FilteredRange>
    void renderJson(const std::string& query, const std::string& searchType,
                    FilteredRange& filtered_results, int64_t totalCount, int64_t executionTimeMs) {
        json output;
        output["query"] = query;
        output["type"] = searchType;
        output["total_results"] = totalCount;
        output["execution_time_ms"] = executionTimeMs;

        json items = json::array();
        for (const auto& adapter : filtered_results) {
            json item;
            item["id"] = adapter.id();
            if (!adapter.hash().empty()) {
                item["hash"] = adapter.hash();
            }
            item["title"] = adapter.title();
            item["path"] = adapter.path();
            if (adapter.size() > 0) {
                item["size"] = adapter.size();
                item["size_formatted"] = formatSize(adapter.size());
            }
            if (!adapter.mimeType().empty()) {
                item["mime_type"] = adapter.mimeType();
            }

            // Add score if this is a SearchResultAdapter
            if constexpr (SearchResultAdapter<AdapterType>) {
                item["score"] = adapter.score();
                if (!adapter.snippet().empty()) {
                    item["snippet"] = adapter.snippet();
                }
            }

            // Add timestamps if this is a DocumentInfoAdapter
            if constexpr (DocumentInfoAdapter<AdapterType>) {
                item["created"] = std::chrono::duration_cast<std::chrono::seconds>(
                                      adapter.createdTime().time_since_epoch())
                                      .count();
                item["modified"] = std::chrono::duration_cast<std::chrono::seconds>(
                                       adapter.modifiedTime().time_since_epoch())
                                       .count();
                item["indexed"] = std::chrono::duration_cast<std::chrono::seconds>(
                                      adapter.indexedTime().time_since_epoch())
                                      .count();
                item["created_formatted"] = formatTimestamp(adapter.createdTime());
                item["modified_formatted"] = formatTimestamp(adapter.modifiedTime());
                item["indexed_formatted"] = formatTimestamp(adapter.indexedTime());
            }

            items.push_back(item);
        }

        output["results"] = items;
        output["returned"] = items.size();
        std::cout << output.dump(2) << std::endl;
    }

    template <std::ranges::range FilteredRange>
    void renderTable(const std::string& query, FilteredRange& filtered_results,
                     int64_t totalCount) {
        // Convert to vector to get size and allow multiple iterations
        std::vector<AdapterType> results_vec(filtered_results.begin(), filtered_results.end());

        if (results_vec.empty()) {
            std::cout << "No results found for: " << query << std::endl;
            return;
        }

        std::cout << "Found " << (totalCount > 0 ? totalCount : results_vec.size())
                  << " result(s) for: " << query << std::endl;
        std::cout << std::endl;

        // Table headers
        std::cout << std::left;
        std::cout << std::setw(4) << "#";
        std::cout << std::setw(30) << "Title";
        std::cout << std::setw(12) << "Size";
        if (config_.showHash) {
            std::cout << std::setw(16) << "Hash";
        }
        if constexpr (SearchResultAdapter<AdapterType>) {
            if (config_.showScores) {
                std::cout << std::setw(8) << "Score";
            }
        }
        std::cout << "Path" << std::endl;

        // Separator line
        std::cout << std::string(4, '-') << " ";
        std::cout << std::string(29, '-') << " ";
        std::cout << std::string(11, '-') << " ";
        if (config_.showHash) {
            std::cout << std::string(15, '-') << " ";
        }
        if constexpr (SearchResultAdapter<AdapterType>) {
            if (config_.showScores) {
                std::cout << std::string(7, '-') << " ";
            }
        }
        std::cout << std::string(20, '-') << std::endl;

        // Table rows
        for (size_t i = 0; i < results_vec.size(); ++i) {
            const auto& adapter = results_vec[i];

            std::cout << std::setw(4) << (i + 1);

            // Truncate title if too long
            std::string title{adapter.title()};
            if (title.length() > 29) {
                title = title.substr(0, 26) + "...";
            }
            std::cout << std::setw(30) << title;

            std::cout << std::setw(12) << formatSize(adapter.size());

            if (config_.showHash && !adapter.hash().empty()) {
                std::string_view hash = adapter.hash();
                std::string displayHash =
                    config_.showFullHash
                        ? std::string{hash}
                        : std::string{hash.substr(0, std::min(hash.size(), size_t{12}))} + "...";
                std::cout << std::setw(16) << displayHash;
            }

            if constexpr (SearchResultAdapter<AdapterType>) {
                if (config_.showScores) {
                    std::cout << std::setw(8) << std::fixed << std::setprecision(2)
                              << adapter.score();
                }
            }

            std::cout << adapter.path() << std::endl;
        }
    }

    template <std::ranges::range FilteredRange>
    void renderVerbose(const std::string& query, const std::string& /* searchType */,
                       FilteredRange& filtered_results, int64_t totalCount) {
        // Convert to vector to get size
        std::vector<AdapterType> results_vec(filtered_results.begin(), filtered_results.end());

        if (results_vec.empty()) {
            std::cout << "No results found for: " << query << std::endl;
            return;
        }

        std::cout << "Found " << (totalCount > 0 ? totalCount : results_vec.size())
                  << " result(s) for: " << query << std::endl;
        std::cout << std::endl;

        for (size_t i = 0; i < results_vec.size(); ++i) {
            const auto& adapter = results_vec[i];

            std::cout << (i + 1) << ". " << adapter.title();

            // Show score for search results
            if constexpr (SearchResultAdapter<AdapterType>) {
                std::cout << " [score: " << std::fixed << std::setprecision(2) << adapter.score()
                          << "]";
            }
            std::cout << std::endl;

            // Show hash
            if (config_.showHash && !adapter.hash().empty()) {
                if (config_.showFullHash) {
                    std::cout << "   Hash: " << adapter.hash() << std::endl;
                } else {
                    std::string_view hash = adapter.hash();
                    std::cout << "   Hash: " << hash.substr(0, std::min(hash.size(), size_t{12}))
                              << "..." << std::endl;
                }
            }

            // Show path if different from title
            if (adapter.path() != adapter.title() && adapter.path() != "stdin") {
                std::cout << "   Path: " << adapter.path() << std::endl;
            }

            // Show size and type
            if (adapter.size() > 0) {
                std::cout << "   Size: " << formatSize(adapter.size());
                if (!adapter.mimeType().empty()) {
                    std::cout << " | Type: " << adapter.mimeType();
                }
                std::cout << std::endl;
            }

            // Show timestamps for document info
            if constexpr (DocumentInfoAdapter<AdapterType>) {
                if (config_.showTimestamps) {
                    std::cout << "   Created: " << formatTimestamp(adapter.createdTime())
                              << std::endl;
                    std::cout << "   Modified: " << formatTimestamp(adapter.modifiedTime())
                              << std::endl;
                    std::cout << "   Indexed: " << formatTimestamp(adapter.indexedTime())
                              << std::endl;
                }
            }

            // Show snippet for search results
            if constexpr (SearchResultAdapter<AdapterType>) {
                if (!adapter.snippet().empty()) {
                    std::cout << std::endl
                              << "   "
                              << truncateSnippet(adapter.snippet(), config_.snippetMaxLength)
                              << std::endl;
                }
            }

            std::cout << std::endl;
        }
    }

    template <std::ranges::range FilteredRange>
    void renderMinimal(const std::string& query, FilteredRange& filtered_results,
                       int64_t totalCount) {
        // Convert to vector to get size
        std::vector<AdapterType> results_vec(filtered_results.begin(), filtered_results.end());

        if (results_vec.empty()) {
            std::cout << "No results found for: " << query << std::endl;
            return;
        }

        std::cout << "Found " << (totalCount > 0 ? totalCount : results_vec.size())
                  << " result(s) for: " << query << std::endl;
        std::cout << std::endl;

        for (size_t i = 0; i < results_vec.size(); ++i) {
            const auto& adapter = results_vec[i];

            std::cout << (i + 1) << ". " << adapter.title();

            if (config_.showHash && !adapter.hash().empty()) {
                std::string_view hash = adapter.hash();
                std::cout << " [" << hash.substr(0, std::min(hash.size(), size_t{8})) << "...]";
            }

            if constexpr (SearchResultAdapter<AdapterType>) {
                if (config_.showScores) {
                    std::cout << " (" << std::fixed << std::setprecision(2) << adapter.score()
                              << ")";
                }
            }

            std::cout << std::endl;

            // Show brief snippet for search results
            if constexpr (SearchResultAdapter<AdapterType>) {
                if (!adapter.snippet().empty()) {
                    std::cout << "   " << truncateSnippet(adapter.snippet(), 100) << std::endl;
                }
            }

            std::cout << std::endl;
        }
    }
};

// ============================================================================
// Convenience Factory Functions
// ==========================================================================

/**
 * @brief Create a renderer for search results with default configuration
 */
inline auto createSearchResultRenderer(const std::string& pathFilter = "",
                                       OutputFormat format = OutputFormat::Verbose,
                                       bool showHash = false, bool showFullHash = false) {
    RenderConfig config;
    config.pathFilter = pathFilter;
    config.format = format;
    config.showHash = showHash;
    config.showFullHash = showFullHash;
    config.showScores = true;
    return ResultRenderer<MetadataSearchResultAdapter>{config};
}

/**
 * @brief Create a renderer for document listings with default configuration
 */
inline auto createDocumentRenderer(const std::string& pathFilter = "",
                                   OutputFormat format = OutputFormat::Table, bool showHash = false,
                                   bool showTimestamps = false) {
    RenderConfig config;
    config.pathFilter = pathFilter;
    config.format = format;
    config.showHash = showHash;
    config.showTimestamps = showTimestamps;
    return ResultRenderer<DocumentInfoResultAdapter>{config};
}

/**
 * @brief Create a renderer for hybrid search results with default configuration
 */
inline auto createHybridSearchResultRenderer(const std::string& pathFilter = "",
                                             OutputFormat format = OutputFormat::Verbose,
                                             bool showHash = false, bool showFullHash = false,
                                             bool showTimestamps = false) {
    RenderConfig config;
    config.pathFilter = pathFilter;
    config.format = format;
    config.showHash = showHash;
    config.showFullHash = showFullHash;
    config.showScores = true;
    config.showTimestamps = showTimestamps;
    return ResultRenderer<HybridSearchResultAdapter>{config};
}

/**
 * @brief Create a renderer for simple file paths
 */
inline auto createPathRenderer(const std::string& pathFilter = "",
                               OutputFormat format = OutputFormat::PathsOnly) {
    RenderConfig config;
    config.pathFilter = pathFilter;
    config.format = format;
    return ResultRenderer<FilePathAdapter>{config};
}

} // namespace yams::cli
