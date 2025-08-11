#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/search/search_executor.h>
#include <yams/metadata/metadata_repository.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <iostream>

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
            
            // Output results
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
            
            return Result<void>();
            
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }
    
private:
    void outputFuzzyResults(const metadata::SearchResults& results) {
        json output;
        output["query"] = results.query;
        output["type"] = "fuzzy";
        output["total_results"] = results.totalCount;
        output["returned"] = results.results.size();
        output["execution_time_ms"] = results.executionTimeMs;
        
        if (!results.errorMessage.empty()) {
            output["error"] = results.errorMessage;
        }
        
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
    }
    
    void outputMetadataResults(const metadata::SearchResults& results) {
        json output;
        output["query"] = results.query;
        output["type"] = "full-text";
        output["total_results"] = results.totalCount;
        output["returned"] = results.results.size();
        output["execution_time_ms"] = results.executionTimeMs;
        
        if (!results.errorMessage.empty()) {
            output["error"] = results.errorMessage;
        }
        
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
    }

private:
    YamsCLI* cli_ = nullptr;
    std::string query_;
    size_t limit_ = 20;
    std::string searchType_ = "keyword";
    bool fuzzySearch_ = false;
    float minSimilarity_ = 0.7f;
};

// Factory function
std::unique_ptr<ICommand> createSearchCommand() {
    return std::make_unique<SearchCommand>();
}

} // namespace yams::cli