#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/search/search_executor.h>
#include <yams/metadata/metadata_repository.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <iomanip>
#include <sstream>

namespace yams::cli {

using json = nlohmann::json;

class SearchCommand : public ICommand {
public:
    std::string getName() const override { return "search"; }
    
    std::string getDescription() const override { 
        return "Search documents by query";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("search", getDescription());
        cmd->add_option("query", query_, "Search query")
            ->required();
        
        cmd->add_option("-l,--limit", limit_, "Maximum number of results")
            ->default_val(20);
        
        cmd->add_option("-t,--type", searchType_, "Search type (keyword, semantic, hybrid)")
            ->default_val("keyword");
        
        cmd->add_flag("-f,--fuzzy", fuzzySearch_, "Enable fuzzy search for approximate matching");
        cmd->add_option("--similarity", minSimilarity_, "Minimum similarity for fuzzy search (0.0-1.0)")
            ->default_val(0.7f);
        
        cmd->add_option("--hash", hashQuery_, "Search by file hash (full or partial, minimum 8 characters)");
        
        cmd->callback([this]() { 
            auto result = execute();
            if (!result) {
                spdlog::error("Command failed: {}", result.error().message);
                exit(1);
            }
        });
    }
    
    Result<void> execute() override {
        try {
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return ensured;
            }
            
            // Handle hash search if --hash flag is provided
            if (!hashQuery_.empty()) {
                if (!isValidHash(hashQuery_)) {
                    return Error{ErrorCode::InvalidArgument, 
                               "Invalid hash format. Must be 8-64 hexadecimal characters."};
                }
                searchByHash(hashQuery_);
                return Result<void>();
            }
            
            // Auto-detect hash format in query for backward compatibility
            if (query_.length() >= 8 && query_.length() <= 64 && isValidHash(query_)) {
                searchByHash(query_);
                return Result<void>();
            }
            
            // Check if we should use fuzzy search via metadata repository
            if (fuzzySearch_) {
                auto metadataRepo = cli_->getMetadataRepository();
                if (!metadataRepo) {
                    return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
                }
                
                // Execute fuzzy search
                auto searchResult = metadataRepo->fuzzySearch(query_, minSimilarity_, limit_);
                if (!searchResult) {
                    return Error{searchResult.error().code, searchResult.error().message};
                }
                
                outputFuzzyResults(searchResult.value());
                return Result<void>();
            }
            
            // Use metadata repository for regular search if available
            auto metadataRepo = cli_->getMetadataRepository();
            if (metadataRepo) {
                // Execute metadata-based search
                auto searchResult = metadataRepo->search(query_, limit_, 0);
                if (!searchResult) {
                    return Error{searchResult.error().code, searchResult.error().message};
                }
                
                outputMetadataResults(searchResult.value());
                return Result<void>();
            }
            
            // Fall back to search executor if metadata repo not available
            auto searchExecutor = cli_->getSearchExecutor();
            if (!searchExecutor) {
                return Error{ErrorCode::NotInitialized, "Search executor not initialized"};
            }
            
            // Build search request
            search::SearchRequest request;
            request.query = query_;
            request.limit = limit_;
            request.offset = 0;
            request.includeHighlights = true;
            request.includeSnippets = true;
            
            // Execute search
            auto result = searchExecutor->search(request);
            if (!result) {
                return Error{result.error().code, result.error().message};
            }
            
            auto& response = result.value();
            
            // Output results using helper
            outputSearchResults(response);
            
            return Result<void>();
            
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }
    
private:
    void outputSearchResults(const search::SearchResults& response) {
        // Output in JSON if --json flag is set, or if --verbose is set
        if (cli_->getJsonOutput() || cli_->getVerbose()) {
            json output;
            output["query"] = response.getStatistics().originalQuery;
            output["total_results"] = response.getStatistics().totalResults;
            output["returned"] = response.getItems().size();
            
            json results = json::array();
            for (const auto& item : response.getItems()) {
                json doc;
                doc["id"] = item.documentId;
                doc["title"] = item.title;
                doc["path"] = item.path;
                doc["score"] = item.relevanceScore;
                doc["snippet"] = item.contentPreview;
                
                if (!item.highlights.empty()) {
                    json highlights = json::array();
                    for (const auto& h : item.highlights) {
                        json highlight;
                        highlight["field"] = h.field;
                        highlight["snippet"] = h.snippet;
                        highlights.push_back(highlight);
                    }
                    doc["highlights"] = highlights;
                }
                
                results.push_back(doc);
            }
            output["results"] = results;
            
            // Performance metrics
            json metrics;
            metrics["query_parse_ms"] = response.getStatistics().queryTime.count();
            metrics["search_ms"] = response.getStatistics().searchTime.count();
            metrics["ranking_ms"] = 0; // rankingTime not available in SearchStatistics
            metrics["total_ms"] = response.getStatistics().totalTime.count();
            output["metrics"] = metrics;
            
            std::cout << output.dump(2) << std::endl;
        } else {
            // Simple, concise output by default
            const auto& items = response.getItems();
            if (items.empty()) {
                std::cout << "No results found for: " << response.getStatistics().originalQuery << std::endl;
            } else {
                std::cout << "Found " << response.getStatistics().totalResults << " result(s) for: " << response.getStatistics().originalQuery << std::endl;
                std::cout << std::endl;
                
                for (size_t i = 0; i < items.size(); i++) {
                    const auto& item = items[i];
                    std::cout << (i + 1) << ". " << item.title;
                    if (!item.path.empty()) {
                        std::cout << " (" << item.path << ")";
                    }
                    std::cout << std::endl;
                    
                    if (!item.contentPreview.empty()) {
                        std::cout << "   " << item.contentPreview << std::endl;
                    }
                    std::cout << std::endl;
                }
            }
        }
    }
    
    // Helper function to format file size in human-readable format
    std::string formatSize(int64_t bytes) {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = static_cast<double>(bytes);
        
        while (size >= 1024 && unitIndex < 4) {
            size /= 1024;
            unitIndex++;
        }
        
        std::stringstream ss;
        if (unitIndex == 0) {
            ss << static_cast<int>(size) << " " << units[unitIndex];
        } else {
            ss << std::fixed << std::setprecision(1) << size << " " << units[unitIndex];
        }
        return ss.str();
    }
    
    // Helper function to detect if a string is a SHA256 hash
    bool isValidHash(const std::string& str) {
        // Must be 8-64 hex characters
        if (str.length() < 8 || str.length() > 64) {
            return false;
        }
        
        // Check if all characters are hex
        for (char c : str) {
            if (!std::isxdigit(c)) {
                return false;
            }
        }
        
        return true;
    }
    
    // Helper function to search by hash with support for partial matching
    void searchByHash(const std::string& hash) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            std::cerr << "Error: Metadata repository not initialized" << std::endl;
            return;
        }
        
        // If it's a full 64-character hash, do exact lookup
        if (hash.length() == 64) {
            auto docResult = metadataRepo->getDocumentByHash(hash);
            if (!docResult) {
                std::cerr << "Error: " << docResult.error().message << std::endl;
                return;
            }
            
            if (!docResult.value()) {
                std::cout << "No document found with hash: " << hash << std::endl;
                return;
            }
            
            // Display the single result
            const auto& doc = docResult.value().value();
            std::cout << "Found document for hash: " << hash.substr(0, 8) << "..." << std::endl;
            std::cout << std::endl;
            
            if (cli_->getVerbose()) {
                std::cout << "1. " << doc.fileName << std::endl;
                std::cout << "   Path: " << doc.filePath << std::endl;
                std::cout << "   Hash: " << doc.sha256Hash << std::endl;
                std::cout << "   Size: " << formatSize(doc.fileSize) 
                          << " | Type: " << doc.mimeType << std::endl;
            } else {
                std::cout << "1. " << doc.fileName << std::endl;
                std::cout << "   " << doc.sha256Hash.substr(0, 16) << "..." << std::endl;
            }
            std::cout << std::endl;
        } else {
            // Partial hash search - need to search all documents
            auto queryResult = metadataRepo->findDocumentsByPath("%");
            if (!queryResult) {
                std::cerr << "Error: " << queryResult.error().message << std::endl;
                return;
            }
            
            std::vector<metadata::DocumentInfo> matches;
            for (const auto& doc : queryResult.value()) {
                if (doc.sha256Hash.substr(0, hash.length()) == hash) {
                    matches.push_back(doc);
                    if (matches.size() >= static_cast<size_t>(limit_)) break; // Respect limit
                }
            }
            
            if (matches.empty()) {
                std::cout << "No documents found with hash prefix: " << hash << std::endl;
                return;
            }
            
            std::cout << "Found " << matches.size() << " document(s) with hash prefix: " << hash << std::endl;
            std::cout << std::endl;
            
            for (size_t i = 0; i < matches.size(); i++) {
                const auto& doc = matches[i];
                if (cli_->getVerbose()) {
                    std::cout << (i + 1) << ". " << doc.fileName << std::endl;
                    std::cout << "   Path: " << doc.filePath << std::endl;
                    std::cout << "   Hash: " << doc.sha256Hash << std::endl;
                    std::cout << "   Size: " << formatSize(doc.fileSize) 
                              << " | Type: " << doc.mimeType << std::endl;
                } else {
                    std::cout << (i + 1) << ". " << doc.fileName << std::endl;
                    std::cout << "   " << doc.sha256Hash.substr(0, 16) << "..." << std::endl;
                }
                std::cout << std::endl;
            }
        }
    }
    
    // Helper function to truncate snippet to a maximum length at word boundary
    std::string truncateSnippet(const std::string& snippet, size_t maxLength) {
        // Remove HTML tags and clean up the snippet
        std::string cleaned;
        bool inTag = false;
        for (char c : snippet) {
            if (c == '<') {
                inTag = true;
            } else if (c == '>') {
                inTag = false;
            } else if (!inTag) {
                // Replace newlines and multiple spaces with single space
                if (c == '\n' || c == '\r' || c == '\t') {
                    if (!cleaned.empty() && cleaned.back() != ' ') {
                        cleaned += ' ';
                    }
                } else {
                    cleaned += c;
                }
            }
        }
        
        // Remove multiple consecutive spaces
        std::string result;
        bool lastWasSpace = false;
        for (char c : cleaned) {
            if (c == ' ') {
                if (!lastWasSpace) {
                    result += c;
                    lastWasSpace = true;
                }
            } else {
                result += c;
                lastWasSpace = false;
            }
        }
        
        // Trim to max length at word boundary
        if (result.length() <= maxLength) {
            return result;
        }
        
        // Find last space before maxLength
        size_t lastSpace = result.rfind(' ', maxLength);
        if (lastSpace != std::string::npos && lastSpace > maxLength * 0.7) {
            // Found a reasonable word boundary
            return result.substr(0, lastSpace);
        }
        
        // No good word boundary, just truncate
        return result.substr(0, maxLength);
    }
    
    void outputFuzzyResults(const metadata::SearchResults& results) {
        // Handle error case
        if (!results.errorMessage.empty()) {
            std::cerr << "Fuzzy search error: " << results.errorMessage << std::endl;
            return;
        }
        
        // Output in JSON if --json flag is set
        if (cli_->getJsonOutput()) {
            json output;
            output["query"] = results.query;
            output["type"] = "fuzzy";
            output["total_results"] = results.totalCount;
            output["returned"] = results.results.size();
            output["execution_time_ms"] = results.executionTimeMs;
            
            json items = json::array();
            for (const auto& result : results.results) {
                json item;
                item["id"] = result.document.id;
                item["hash"] = result.document.sha256Hash;
                item["file_name"] = result.document.fileName;
                item["file_path"] = result.document.filePath;
                item["size"] = result.document.fileSize;
                item["mime_type"] = result.document.mimeType;
                item["score"] = result.score;
                item["snippet"] = result.snippet;
                items.push_back(item);
            }
            output["results"] = items;
            
            std::cout << output.dump(2) << std::endl;
        } else if (cli_->getVerbose()) {
            // Verbose output with full details
            if (results.results.empty()) {
                std::cout << "No fuzzy matches found for: " << results.query << std::endl;
            } else {
                std::cout << "Found " << results.totalCount << " fuzzy match(es) for: " << results.query << std::endl;
                std::cout << std::endl;
                
                for (size_t i = 0; i < results.results.size(); i++) {
                    const auto& result = results.results[i];
                    std::cout << (i + 1) << ". " << result.document.fileName;
                    std::cout << " [fuzzy score: " << std::fixed << std::setprecision(2) << result.score << "]" << std::endl;
                    
                    // Show full path and metadata in verbose mode
                    if (result.document.filePath != "stdin") {
                        std::cout << "   Path: " << result.document.filePath << std::endl;
                    }
                    std::cout << "   Size: " << formatSize(result.document.fileSize) 
                              << " | Type: " << result.document.mimeType << std::endl;
                    
                    // Show full snippet in verbose mode
                    if (!result.snippet.empty()) {
                        std::cout << std::endl;
                        std::cout << "   " << result.snippet << std::endl;
                    }
                    std::cout << std::endl;
                }
            }
        } else {
            // Default concise output with truncated snippets
            if (results.results.empty()) {
                std::cout << "No fuzzy matches found for: " << results.query << std::endl;
            } else {
                std::cout << "Found " << results.totalCount << " fuzzy match(es) for: " << results.query << std::endl;
                std::cout << std::endl;
                
                for (size_t i = 0; i < results.results.size(); i++) {
                    const auto& result = results.results[i];
                    std::cout << (i + 1) << ". " << result.document.fileName << std::endl;
                    
                    // Show truncated snippet (first 150 chars max, single line)
                    if (!result.snippet.empty()) {
                        std::string truncated = truncateSnippet(result.snippet, 150);
                        std::cout << "   " << truncated;
                        if (truncated.length() < result.snippet.length()) {
                            std::cout << "...";
                        }
                        std::cout << std::endl;
                    }
                    std::cout << std::endl;
                }
            }
        }
    }
    
    void outputMetadataResults(const metadata::SearchResults& results) {
        // Handle error case
        if (!results.errorMessage.empty()) {
            std::cerr << "Search error: " << results.errorMessage << std::endl;
            return;
        }
        
        // Output in JSON if --json flag is set
        if (cli_->getJsonOutput()) {
            json output;
            output["query"] = results.query;
            output["type"] = "full-text";
            output["total_results"] = results.totalCount;
            output["returned"] = results.results.size();
            output["execution_time_ms"] = results.executionTimeMs;
            
            json items = json::array();
            for (const auto& result : results.results) {
                json item;
                item["id"] = result.document.id;
                item["hash"] = result.document.sha256Hash;
                item["file_name"] = result.document.fileName;
                item["file_path"] = result.document.filePath;
                item["size"] = result.document.fileSize;
                item["mime_type"] = result.document.mimeType;
                item["score"] = result.score;
                item["snippet"] = result.snippet;
                items.push_back(item);
            }
            output["results"] = items;
            
            std::cout << output.dump(2) << std::endl;
        } else if (cli_->getVerbose()) {
            // Verbose output with full details
            if (results.results.empty()) {
                std::cout << "No results found for: " << results.query << std::endl;
            } else {
                std::cout << "Found " << results.totalCount << " result(s) for: " << results.query << std::endl;
                std::cout << std::endl;
                
                for (size_t i = 0; i < results.results.size(); i++) {
                    const auto& result = results.results[i];
                    std::cout << (i + 1) << ". " << result.document.fileName;
                    std::cout << " [score: " << std::fixed << std::setprecision(2) << result.score << "]" << std::endl;
                    
                    // Show full path and metadata in verbose mode
                    if (result.document.filePath != "stdin") {
                        std::cout << "   Path: " << result.document.filePath << std::endl;
                    }
                    std::cout << "   Size: " << formatSize(result.document.fileSize) 
                              << " | Type: " << result.document.mimeType << std::endl;
                    
                    // Show full snippet in verbose mode
                    if (!result.snippet.empty()) {
                        std::cout << std::endl;
                        std::cout << "   " << result.snippet << std::endl;
                    }
                    std::cout << std::endl;
                }
            }
        } else {
            // Default concise output with truncated snippets
            if (results.results.empty()) {
                std::cout << "No results found for: " << results.query << std::endl;
            } else {
                std::cout << "Found " << results.totalCount << " result(s) for: " << results.query << std::endl;
                std::cout << std::endl;
                
                for (size_t i = 0; i < results.results.size(); i++) {
                    const auto& result = results.results[i];
                    std::cout << (i + 1) << ". " << result.document.fileName << std::endl;
                    
                    // Show truncated snippet (first 150 chars max, single line)
                    if (!result.snippet.empty()) {
                        std::string truncated = truncateSnippet(result.snippet, 150);
                        std::cout << "   " << truncated;
                        if (truncated.length() < result.snippet.length()) {
                            std::cout << "...";
                        }
                        std::cout << std::endl;
                    }
                    std::cout << std::endl;
                }
            }
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    std::string query_;
    size_t limit_ = 20;
    std::string searchType_ = "keyword";
    bool fuzzySearch_ = false;
    float minSimilarity_ = 0.7f;
    std::string hashQuery_;
};

// Factory function
std::unique_ptr<ICommand> createSearchCommand() {
    return std::make_unique<SearchCommand>();
}

} // namespace yams::cli