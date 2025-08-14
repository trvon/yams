#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/metadata/metadata_repository.h>
#include <spdlog/spdlog.h>
#include <iostream>
#include <regex>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <set>
#include <unistd.h>

namespace yams::cli {

class GrepCommand : public ICommand {
public:
    std::string getName() const override { return "grep"; }
    
    std::string getDescription() const override { 
        return "Search for regex patterns within file contents";
    }
    
    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        
        auto* cmd = app.add_subcommand("grep", getDescription());
        
        cmd->add_option("pattern", pattern_, "Regular expression pattern to search for")
            ->required();
        
        cmd->add_option("paths", paths_, "Files or directories to search (default: all indexed files)");
        
        // Context options
        cmd->add_option("-A,--after", afterContext_, "Show N lines after match")
            ->default_val(0);
        cmd->add_option("-B,--before", beforeContext_, "Show N lines before match")
            ->default_val(0);
        cmd->add_option("-C,--context", context_, "Show N lines before and after match")
            ->default_val(0);
        
        // Search options
        cmd->add_flag("-i,--ignore-case", ignoreCase_, "Case-insensitive search");
        cmd->add_flag("-w,--word", wholeWord_, "Match whole words only");
        cmd->add_flag("-v,--invert", invertMatch_, "Invert match (show non-matching lines)");
        cmd->add_flag("-n,--line-numbers", showLineNumbers_, "Show line numbers");
        cmd->add_flag("-H,--with-filename", showFilename_, "Show filename with matches");
        cmd->add_flag("--no-filename", noFilename_, "Never show filename");
        cmd->add_flag("-c,--count", countOnly_, "Show only count of matching lines");
        cmd->add_flag("-l,--files-with-matches", filesOnly_, "Show only filenames with matches");
        cmd->add_flag("-L,--files-without-match", filesWithoutMatch_, "Show only filenames without matches");
        
        // Output options
        cmd->add_option("--color", colorMode_, "Color mode: always, never, auto")
            ->default_val("auto")
            ->check(CLI::IsMember({"always", "never", "auto"}));
        
        cmd->add_option("-m,--max-count", maxCount_, "Stop after N matches per file")
            ->default_val(0);
        
        cmd->add_option("--limit", maxCount_, "Alias: stop after N matches per file (same as --max-count)");
        
        cmd->callback([this]() { 
            auto result = execute();
            if (!result) {
                spdlog::error("Grep failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }
    
    Result<void> execute() override {
        try {
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return ensured;
            }
            
            auto metadataRepo = cli_->getMetadataRepository();
            if (!metadataRepo) {
                return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
            }
            
            auto store = cli_->getContentStore();
            if (!store) {
                return Error{ErrorCode::NotInitialized, "Content store not initialized"};
            }
            
            // Handle context options
            if (context_ > 0) {
                beforeContext_ = afterContext_ = context_;
            }
            
            // Determine if we should show filenames
            bool multipleFiles = paths_.size() != 1;
            if (!noFilename_ && (showFilename_ || multipleFiles)) {
                showFilename_ = true;
            }
            
            // Build regex pattern
            std::regex_constants::syntax_option_type flags = std::regex_constants::ECMAScript;
            if (ignoreCase_) {
                flags |= std::regex_constants::icase;
            }
            
            std::string regexPattern = pattern_;
            if (wholeWord_) {
                regexPattern = "\\b" + regexPattern + "\\b";
            }
            
            std::regex regex;
            try {
                regex = std::regex(regexPattern, flags);
            } catch (const std::regex_error& e) {
                return Error{ErrorCode::InvalidArgument, "Invalid regex pattern: " + std::string(e.what())};
            }
            
            // Get documents to search
            std::vector<metadata::DocumentInfo> documents;
            
            if (paths_.empty()) {
                // Search all indexed files
                auto docsResult = metadataRepo->findDocumentsByPath("%");
                if (!docsResult) {
                    return Error{ErrorCode::DatabaseError, "Failed to query documents: " + docsResult.error().message};
                }
                documents = docsResult.value();
            } else {
                // Search specific paths
                for (const auto& path : paths_) {
                    auto docsResult = metadataRepo->findDocumentsByPath(path);
                    if (!docsResult) {
                        continue; // Skip if path not found
                    }
                    
                    for (const auto& doc : docsResult.value()) {
                        documents.push_back(doc);
                    }
                    
                    // Also try path suffix match
                    if (docsResult.value().empty()) {
                        auto suffixResult = metadataRepo->findDocumentsByPath("%/" + path);
                        if (suffixResult && !suffixResult.value().empty()) {
                            for (const auto& doc : suffixResult.value()) {
                                documents.push_back(doc);
                            }
                        }
                    }
                }
            }
            
            if (documents.empty()) {
                std::cerr << "No files to search" << std::endl;
                return Result<void>();
            }
            
            // Process each document
            size_t totalMatches = 0;
            std::vector<std::string> matchingFiles;
            std::vector<std::string> nonMatchingFiles;
            
            for (const auto& doc : documents) {
                // Retrieve document content
                auto contentResult = store->retrieveBytes(doc.sha256Hash);
                if (!contentResult) {
                    continue; // Skip if can't retrieve
                }
                
                std::string content(
                    reinterpret_cast<const char*>(contentResult.value().data()),
                    contentResult.value().size()
                );
                
                // Process the file
                auto matches = processFile(doc.filePath, content, regex);
                // Per-file limit: trim matches to maxCount_ if specified
                if (maxCount_ > 0 && matches.size() > static_cast<size_t>(maxCount_)) {
                    matches.resize(maxCount_);
                }
                
                if (!matches.empty()) {
                    matchingFiles.push_back(doc.filePath);
                    totalMatches += matches.size();
                    
                    if (filesOnly_) {
                        std::cout << doc.filePath << std::endl;
                    } else if (countOnly_) {
                        if (showFilename_) {
                            std::cout << doc.filePath << ":";
                        }
                        std::cout << matches.size() << std::endl;
                    } else if (!filesWithoutMatch_) {
                        printMatches(doc.filePath, content, matches);
                    }
                    
                    
                } else {
                    nonMatchingFiles.push_back(doc.filePath);
                }
            }
            
            // Handle files-without-match option
            if (filesWithoutMatch_) {
                for (const auto& file : nonMatchingFiles) {
                    std::cout << file << std::endl;
                }
            }
            
            return Result<void>();
            
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }
    
private:
    struct Match {
        size_t lineNumber;
        size_t columnStart;
        size_t columnEnd;
        std::string line;
    };
    
    std::vector<Match> processFile(const std::string& filename, const std::string& content, const std::regex& regex) {
        std::vector<Match> matches;
        std::istringstream stream(content);
        std::string line;
        size_t lineNumber = 1;
        
        while (std::getline(stream, line)) {
            bool hasMatch = false;
            std::smatch match;
            std::string searchLine = line;
            size_t columnOffset = 0;
            
            while (std::regex_search(searchLine, match, regex)) {
                if (!invertMatch_) {
                    Match m;
                    m.lineNumber = lineNumber;
                    m.columnStart = columnOffset + match.position();
                    m.columnEnd = m.columnStart + match.length();
                    m.line = line;
                    matches.push_back(m);
                    hasMatch = true;
                }
                
                columnOffset += match.position() + match.length();
                searchLine = match.suffix();
                
                // For count/files only modes, one match per line is enough
                if (countOnly_ || filesOnly_ || filesWithoutMatch_) {
                    break;
                }
            }
            
            // Handle inverted match
            if (invertMatch_ && !hasMatch) {
                Match m;
                m.lineNumber = lineNumber;
                m.columnStart = 0;
                m.columnEnd = 0;
                m.line = line;
                matches.push_back(m);
            }
            
            lineNumber++;
        }
        
        return matches;
    }
    
    void printMatches(const std::string& filename, const std::string& content, const std::vector<Match>& matches) {
        // Split content into lines for context printing
        std::vector<std::string> lines;
        std::istringstream stream(content);
        std::string line;
        while (std::getline(stream, line)) {
            lines.push_back(line);
        }
        
        // Track which lines we've already printed (for context overlap)
        std::set<size_t> printedLines;
        
        for (const auto& match : matches) {
            // Calculate context range
            size_t startLine = (match.lineNumber > beforeContext_) 
                ? match.lineNumber - beforeContext_ : 1;
            size_t endLine = std::min(match.lineNumber + afterContext_, lines.size());
            
            // Print separator if needed
            if (!printedLines.empty() && startLine > *printedLines.rbegin() + 1) {
                std::cout << "--" << std::endl;
            }
            
            // Print context and match
            for (size_t i = startLine; i <= endLine; ++i) {
                if (printedLines.count(i) > 0) {
                    continue; // Already printed this line
                }
                printedLines.insert(i);
                
                if (i - 1 >= lines.size()) {
                    continue;
                }
                
                // Print filename if needed
                if (showFilename_) {
                    std::cout << filename << ":";
                }
                
                // Print line number if needed
                if (showLineNumbers_) {
                    std::cout << std::setw(6) << i << ":";
                }
                
                // Print the line with highlighting if it's a match line
                if (i == match.lineNumber && !invertMatch_) {
                    printHighlightedLine(lines[i - 1], match);
                } else {
                    std::cout << lines[i - 1] << std::endl;
                }
            }
        }
    }
    
    void printHighlightedLine(const std::string& line, const Match& match) {
        // Simple highlighting with color codes if enabled
        bool useColor = (colorMode_ == "always") || 
                       (colorMode_ == "auto" && isatty(fileno(stdout)));
        
        if (useColor && match.columnEnd > match.columnStart) {
            std::cout << line.substr(0, match.columnStart);
            std::cout << "\033[1;31m"; // Bold red
            std::cout << line.substr(match.columnStart, match.columnEnd - match.columnStart);
            std::cout << "\033[0m"; // Reset
            std::cout << line.substr(match.columnEnd);
        } else {
            std::cout << line;
        }
        std::cout << std::endl;
    }
    
private:
    YamsCLI* cli_ = nullptr;
    std::string pattern_;
    std::vector<std::string> paths_;
    
    // Context options
    size_t beforeContext_ = 0;
    size_t afterContext_ = 0;
    size_t context_ = 0;
    
    // Search options
    bool ignoreCase_ = false;
    bool wholeWord_ = false;
    bool invertMatch_ = false;
    bool showLineNumbers_ = false;
    bool showFilename_ = false;
    bool noFilename_ = false;
    bool countOnly_ = false;
    bool filesOnly_ = false;
    bool filesWithoutMatch_ = false;
    
    // Output options
    std::string colorMode_ = "auto";
    size_t maxCount_ = 0;
};

// Factory function
std::unique_ptr<ICommand> createGrepCommand() {
    return std::make_unique<GrepCommand>();
}

} // namespace yams::cli
