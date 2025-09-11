#include <spdlog/spdlog.h>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/extraction/html_text_extractor.h>
#include <yams/extraction/text_extractor.h>
#include <yams/profiling.h>

namespace yams::cli {

class CatCommand : public ICommand {
public:
    std::string getName() const override { return "cat"; }

    std::string getDescription() const override { return "Display document content to stdout"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("cat", getDescription());

        // Create option group for retrieval methods (only one can be used at a time)
        auto* group = cmd->add_option_group("retrieval_method");
        group->add_option("hash", hash_, "Hash of the document to display");
        group->add_option("--name", name_, "Name of the document to display");
        group->require_option(1);
        // Disambiguation flags: select newest/oldest when multiple matches exist
        cmd->add_flag(
            "--latest", getLatest_,
            "Select the most recently indexed match when multiple documents share the same name");
        cmd->add_flag(
            "--oldest", getOldest_,
            "Select the oldest indexed match when multiple documents share the same name");

        // Add flag for raw content output
        cmd->add_flag("--raw", raw_, "Output raw content without text extraction");

        // No output option - cat always goes to stdout
        // This is intentional for piping and viewing content directly

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Cat failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
        YAMS_ZONE_SCOPED_N("CatCommand::execute");

        try {
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return ensured;
            }
            auto store = cli_->getContentStore();
            if (!store) {
                return Error{ErrorCode::NotInitialized, "Content store not initialized"};
            }

            // Resolve the hash to display
            std::string hashToDisplay;

            if (!hash_.empty()) {
                YAMS_ZONE_SCOPED_N("CatCommand::hashResolution");
                // Direct hash display - support partial hashes
                if (isValidHashPrefix(hash_) && hash_.length() < 64) {
                    auto resolveResult = resolvePartialHash(hash_);
                    if (!resolveResult) {
                        return resolveResult.error();
                    }
                    hashToDisplay = resolveResult.value();
                } else {
                    hashToDisplay = hash_;
                }
            } else if (!name_.empty()) {
                YAMS_ZONE_SCOPED_N("CatCommand::nameResolution");
                // Name-based display
                auto resolveResult = resolveNameToHash(name_);
                if (!resolveResult) {
                    // If document not found in YAMS, check if it's a local file
                    if (resolveResult.error().code == ErrorCode::NotFound &&
                        std::filesystem::exists(name_)) {
                        // Fall back to reading local file
                        std::ifstream file(name_, std::ios::binary);
                        if (!file) {
                            return Error{ErrorCode::FileNotFound,
                                         "Cannot read local file: " + name_};
                        }

                        // Output file contents directly to stdout
                        std::cout << file.rdbuf();

                        // Successfully displayed local file
                        return Result<void>();
                    }
                    // Not a local file either, return original error
                    return resolveResult.error();
                }
                hashToDisplay = resolveResult.value();
            } else {
                return Error{ErrorCode::InvalidArgument, "No document specified"};
            }

            // Check if document exists
            auto existsResult = store->exists(hashToDisplay);
            if (!existsResult) {
                return Error{existsResult.error().code, existsResult.error().message};
            }

            if (!existsResult.value()) {
                return Error{ErrorCode::NotFound, "Document not found: " + hashToDisplay};
            }

            // Retrieve content
            std::vector<std::byte> contentBytes;
            {
                YAMS_ZONE_SCOPED_N("CatCommand::retrieveBytes");
                auto result = store->retrieveBytes(hashToDisplay);
                if (!result) {
                    return Error{result.error().code, result.error().message};
                }
                contentBytes = std::move(result.value());
            }

            // Apply text extraction if not in raw mode
            if (!raw_) {
                // Try to determine file extension from name or metadata
                std::string extension;
                std::string fileName;

                // Always try to get the actual fileName from metadata using the resolved hash
                auto metadataRepo = cli_->getMetadataRepository();
                if (metadataRepo) {
                    auto docResult = metadataRepo->getDocumentByHash(hashToDisplay);
                    if (docResult && docResult.value().has_value()) {
                        fileName = docResult.value()->fileName;
                    }
                }

                // Fallback to using the search term if no metadata found
                if (fileName.empty() && !name_.empty()) {
                    fileName = name_;
                }

                // Extract extension from file name
                if (!fileName.empty()) {
                    auto lastDot = fileName.find_last_of('.');
                    if (lastDot != std::string::npos) {
                        extension = fileName.substr(lastDot);
                    }
                }

                // Try text extraction if we have an extension
                if (!extension.empty()) {
                    auto& factory = extraction::TextExtractorFactory::instance();
                    auto extractor = factory.create(extension);

                    if (extractor) {
                        // Create extraction config
                        extraction::ExtractionConfig config;
                        config.maxFileSize = 100 * 1024 * 1024; // 100MB max

                        // Convert to byte span
                        auto dataSpan =
                            std::span<const std::byte>(contentBytes.data(), contentBytes.size());

                        // Extract text
                        auto extractResult = extractor->extractFromBuffer(dataSpan, config);
                        if (extractResult) {
                            // Output extracted text
                            std::cout << extractResult.value().text;
                            return Result<void>();
                        }
                        // If extraction fails, fall back to raw output
                    } else if (extension == ".html" || extension == ".htm") {
                        // Try HTML extraction even without factory support
                        std::string content(reinterpret_cast<const char*>(contentBytes.data()),
                                            contentBytes.size());
                        std::string extracted =
                            extraction::HtmlTextExtractor::extractTextFromHtml(content);
                        std::cout << extracted;
                        return Result<void>();
                    }
                } else {
                    // No extension, try content-based detection for HTML
                    std::string content(reinterpret_cast<const char*>(contentBytes.data()),
                                        std::min(size_t(1000), contentBytes.size()));
                    std::transform(content.begin(), content.end(), content.begin(), ::tolower);

                    if (content.find("<!doctype html") != std::string::npos ||
                        content.find("<html") != std::string::npos ||
                        content.find("<head") != std::string::npos ||
                        content.find("<body") != std::string::npos) {
                        // Detected HTML content
                        std::string fullContent(reinterpret_cast<const char*>(contentBytes.data()),
                                                contentBytes.size());
                        std::string extracted =
                            extraction::HtmlTextExtractor::extractTextFromHtml(fullContent);
                        std::cout << extracted;
                        return Result<void>();
                    }
                }
            }

            // Output raw content
            std::cout.write(reinterpret_cast<const char*>(contentBytes.data()),
                            contentBytes.size());

            // Cat command should not output any status messages
            // This allows clean piping: yams cat --name file.txt | grep something

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }

private:
    bool isValidHashPrefix(const std::string& input) const {
        // Must be at least 6 characters for unambiguous hash prefixes
        if (input.length() < 6 || input.length() > 64) {
            return false;
        }

        // Must contain only hexadecimal characters (case insensitive)
        return std::all_of(input.begin(), input.end(), [](char c) { return std::isxdigit(c); });
    }

    Result<std::string> resolvePartialHash(const std::string& hashPrefix) {
        YAMS_ZONE_SCOPED_N("CatCommand::resolvePartialHash");

        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        // Convert to lowercase for consistent comparison
        std::string lowerPrefix = hashPrefix;
        std::transform(lowerPrefix.begin(), lowerPrefix.end(), lowerPrefix.begin(), ::tolower);

        // Get all documents and find matches
        auto documentsResult = metadataRepo->findDocumentsByPath("%");
        if (!documentsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query documents: " + documentsResult.error().message};
        }

        std::vector<std::string> matchingHashes;
        std::vector<std::string> matchingPaths;

        for (const auto& doc : documentsResult.value()) {
            std::string docHash = doc.sha256Hash;
            std::transform(docHash.begin(), docHash.end(), docHash.begin(), ::tolower);

            if (docHash.substr(0, lowerPrefix.length()) == lowerPrefix) {
                matchingHashes.push_back(doc.sha256Hash);
                matchingPaths.push_back(doc.filePath);
            }
        }

        if (matchingHashes.empty()) {
            return Error{ErrorCode::NotFound, "No document found with hash prefix: " + hashPrefix};
        }

        if (matchingHashes.size() > 1) {
            std::cerr << "Ambiguous hash prefix '" << hashPrefix << "' matches "
                      << matchingHashes.size() << " documents:" << std::endl;
            for (size_t i = 0; i < std::min(size_t(5), matchingHashes.size()); ++i) {
                std::cerr << "  " << matchingHashes[i].substr(0, 12) << "... - " << matchingPaths[i]
                          << std::endl;
            }
            if (matchingHashes.size() > 5) {
                std::cerr << "  ... and " << (matchingHashes.size() - 5) << " more" << std::endl;
            }
            std::cerr << "Please use a longer hash prefix to disambiguate." << std::endl;
            return Error{ErrorCode::InvalidOperation, "Ambiguous hash prefix. Use longer prefix."};
        }

        return matchingHashes[0];
    }

    Result<std::string> resolveNameToHash(const std::string& name) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        // Try as partial hash first (if it looks like hex and is 6+ characters)
        if (isValidHashPrefix(name)) {
            auto hashResult = resolvePartialHash(name);
            if (hashResult) {
                return hashResult.value();
            }
            // If hash resolution fails, continue with name-based resolution
        }

        // First try as a path suffix (for real files)
        auto documentsResult = metadataRepo->findDocumentsByPath("%/" + name);
        if (documentsResult && !documentsResult.value().empty()) {
            auto results = documentsResult.value();
            if (results.size() > 1) {
                if (getLatest_ || getOldest_) {
                    std::sort(
                        results.begin(), results.end(),
                        [this](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                            return getOldest_ ? (a.indexedTime < b.indexedTime)
                                              : (a.indexedTime > b.indexedTime);
                        });
                    return results.front().sha256Hash;
                }
                std::cerr << "Multiple documents found with name '" << name << "':" << std::endl;
                for (const auto& doc : results) {
                    std::cerr << "  " << doc.sha256Hash.substr(0, 12) << "... - " << doc.filePath
                              << std::endl;
                }
                return Error{
                    ErrorCode::InvalidOperation,
                    "Multiple documents with the same name. Please use hash to specify which one."};
            }
            return results[0].sha256Hash;
        }

        // Try exact path match
        documentsResult = metadataRepo->findDocumentsByPath(name);
        if (documentsResult && !documentsResult.value().empty()) {
            auto docs = documentsResult.value();
            if (docs.size() > 1 && (getLatest_ || getOldest_)) {
                std::sort(docs.begin(), docs.end(),
                          [this](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                              return getOldest_ ? (a.indexedTime < b.indexedTime)
                                                : (a.indexedTime > b.indexedTime);
                          });
                return docs.front().sha256Hash;
            }
            return docs[0].sha256Hash;
        }

        // Try fuzzy path matching (partial path components)
        auto fuzzyResult = resolveFuzzyPath(name);
        if (fuzzyResult) {
            return fuzzyResult.value();
        }

        // For stdin documents or when path search fails, use search
        auto searchResult = metadataRepo->search(name, 100, 0);
        if (searchResult) {
            std::vector<std::string> matchingHashes;
            std::vector<std::string> matchingPaths;

            for (const auto& result : searchResult.value().results) {
                // SearchResult contains document directly
                const auto& doc = result.document;
                // Check if fileName matches exactly
                if (doc.fileName == name) {
                    matchingHashes.push_back(doc.sha256Hash);
                    matchingPaths.push_back(doc.filePath);
                }
            }

            if (!matchingHashes.empty()) {
                if (matchingHashes.size() > 1) {
                    std::cerr << "Multiple documents found with name '" << name
                              << "':" << std::endl;
                    for (size_t i = 0; i < matchingHashes.size(); ++i) {
                        std::cerr << "  " << matchingHashes[i].substr(0, 12) << "... - "
                                  << matchingPaths[i] << std::endl;
                    }
                    return Error{ErrorCode::InvalidOperation,
                                 "Multiple documents with the same name. Please use hash to "
                                 "specify which one."};
                }
                return matchingHashes[0];
            }
        }

        return Error{ErrorCode::NotFound, "No document found with name: " + name};
    }

    Result<std::string> resolveFuzzyPath(const std::string& pathQuery) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        // Get all documents
        auto documentsResult = metadataRepo->findDocumentsByPath("%");
        if (!documentsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query documents: " + documentsResult.error().message};
        }

        std::vector<std::pair<std::string, int>> candidatesWithScores;

        // Split query into path components
        std::vector<std::string> queryComponents;
        std::istringstream ss(pathQuery);
        std::string component;
        while (std::getline(ss, component, '/')) {
            if (!component.empty()) {
                queryComponents.push_back(component);
            }
        }

        if (queryComponents.empty()) {
            return Error{ErrorCode::InvalidArgument, "Empty path query"};
        }

        // Score each document path
        for (const auto& doc : documentsResult.value()) {
            std::filesystem::path docPath(doc.filePath);

            // Split document path into components
            std::vector<std::string> docComponents;
            for (const auto& part : docPath) {
                if (part != "/" && !part.string().empty()) {
                    docComponents.push_back(part.string());
                }
            }

            // Calculate fuzzy match score
            int score = calculateFuzzyPathScore(queryComponents, docComponents);
            if (score > 0) {
                candidatesWithScores.emplace_back(doc.sha256Hash, score);
            }
        }

        if (candidatesWithScores.empty()) {
            return Error{ErrorCode::NotFound, "No documents match fuzzy path: " + pathQuery};
        }

        // Sort by score (highest first)
        std::sort(candidatesWithScores.begin(), candidatesWithScores.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });

        // If multiple matches with same top score, show ambiguity
        if (candidatesWithScores.size() > 1 &&
            candidatesWithScores[0].second == candidatesWithScores[1].second) {
            if (getLatest_ || getOldest_) {
                // Collect tied matches
                std::vector<metadata::DocumentInfo> tiedMatches;
                for (size_t i = 0; i < candidatesWithScores.size() &&
                                   candidatesWithScores[i].second == candidatesWithScores[0].second;
                     ++i) {
                    for (const auto& doc : documentsResult.value()) {
                        if (doc.sha256Hash == candidatesWithScores[i].first) {
                            tiedMatches.push_back(doc);
                            break;
                        }
                    }
                }
                // Sort by indexed time according to flags
                std::sort(tiedMatches.begin(), tiedMatches.end(),
                          [this](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                              return getOldest_ ? (a.indexedTime < b.indexedTime)
                                                : (a.indexedTime > b.indexedTime);
                          });
                return tiedMatches.front().sha256Hash;
            }

            std::cerr << "Ambiguous fuzzy path '" << pathQuery
                      << "' matches multiple documents:" << std::endl;

            // Find document paths for display
            for (size_t i = 0; i < std::min(size_t(5), candidatesWithScores.size()) &&
                               candidatesWithScores[i].second == candidatesWithScores[0].second;
                 ++i) {
                // Find the document path for this hash
                for (const auto& doc : documentsResult.value()) {
                    if (doc.sha256Hash == candidatesWithScores[i].first) {
                        std::cerr << "  " << candidatesWithScores[i].first.substr(0, 12) << "... - "
                                  << doc.filePath << " (score: " << candidatesWithScores[i].second
                                  << ")" << std::endl;
                        break;
                    }
                }
            }

            return Error{ErrorCode::InvalidOperation,
                         "Ambiguous fuzzy path match. Please be more specific."};
        }

        return candidatesWithScores[0].first;
    }

    int calculateFuzzyPathScore(const std::vector<std::string>& queryComponents,
                                const std::vector<std::string>& docComponents) {
        int score = 0;

        // Exact suffix match gets highest score
        if (queryComponents.size() <= docComponents.size()) {
            bool exactSuffixMatch = true;
            for (size_t i = 0; i < queryComponents.size(); ++i) {
                size_t queryIdx = queryComponents.size() - 1 - i;
                size_t docIdx = docComponents.size() - 1 - i;
                if (queryComponents[queryIdx] != docComponents[docIdx]) {
                    exactSuffixMatch = false;
                    break;
                }
            }
            if (exactSuffixMatch) {
                return 1000 + static_cast<int>(queryComponents.size());
            }
        }

        // Partial matches - give points for each matching component
        for (const auto& queryComp : queryComponents) {
            for (const auto& docComp : docComponents) {
                if (queryComp == docComp) {
                    score += 100; // Exact component match
                } else if (docComp.find(queryComp) != std::string::npos) {
                    score += 50; // Substring match
                } else if (queryComp.find(docComp) != std::string::npos) {
                    score += 30; // Query contains doc component
                } else {
                    // Check for case-insensitive match
                    std::string lowerQuery = queryComp;
                    std::string lowerDoc = docComp;
                    std::transform(lowerQuery.begin(), lowerQuery.end(), lowerQuery.begin(),
                                   ::tolower);
                    std::transform(lowerDoc.begin(), lowerDoc.end(), lowerDoc.begin(), ::tolower);

                    if (lowerQuery == lowerDoc) {
                        score += 80; // Case-insensitive exact match
                    } else if (lowerDoc.find(lowerQuery) != std::string::npos) {
                        score += 40; // Case-insensitive substring match
                    }
                }
            }
        }

        // Bonus for matching last component (filename)
        if (!queryComponents.empty() && !docComponents.empty()) {
            const auto& lastQuery = queryComponents.back();
            const auto& lastDoc = docComponents.back();

            if (lastQuery == lastDoc) {
                score += 200; // Exact filename match
            } else if (lastDoc.find(lastQuery) != std::string::npos) {
                score += 100; // Filename contains query
            }
        }

        return score;
    }

    YamsCLI* cli_ = nullptr;
    std::string hash_;
    std::string name_;
    bool raw_ = false;       // Flag to output raw content without text extraction
    bool getLatest_ = false; // When ambiguous, select newest
    bool getOldest_ = false; // When ambiguous, select oldest
};

// Factory function
std::unique_ptr<ICommand> createCatCommand() {
    return std::make_unique<CatCommand>();
}

} // namespace yams::cli