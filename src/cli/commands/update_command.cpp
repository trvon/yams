#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <iostream>
#include <sstream>
#include <yams/cli/commands/update_command.h>
#include <yams/cli/yams_cli.h>
// Daemon client API for daemon-first update
#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>

namespace yams::cli {

using json = nlohmann::json;

std::string UpdateCommand::getName() const {
    return "update";
}

std::string UpdateCommand::getDescription() const {
    return "Update metadata for an existing document";
}

void UpdateCommand::registerCommand(CLI::App& app, YamsCLI* cli) {
    cli_ = cli;

    auto* cmd = app.add_subcommand("update", getDescription());

    // Add option group for document selection (hash or name)
    auto* group = cmd->add_option_group("document_selection");
    group->add_option("hash", hash_, "Hash of the document to update");
    group->add_option("--name", name_, "Name of the document to update");
    group->require_option(1);

    // Metadata options
    cmd->add_option("-m,--metadata", metadata_, "Metadata key=value pairs")->required();

    // Flags
    cmd->add_flag("-v,--verbose", verbose_, "Enable verbose output");

    cmd->callback([this]() {
        auto result = execute();
        if (!result) {
            spdlog::error("Command failed: {}", result.error().message);
            throw CLI::RuntimeError(1);
        }
    });
}

Result<void> UpdateCommand::execute() {
    try {
        // Attempt daemon-first update; fall back to local on failure
        {
            yams::daemon::UpdateDocumentRequest dreq;
            dreq.hash = hash_;
            dreq.name = name_;

            // Parse metadata key=value pairs
            for (const auto& kv : metadata_) {
                auto pos = kv.find('=');
                if (pos != std::string::npos) {
                    std::string key = kv.substr(0, pos);
                    std::string value = kv.substr(pos + 1);
                    dreq.metadata[key] = value;
                }
            }

            auto render = [&](const yams::daemon::UpdateDocumentResponse& resp) -> Result<void> {
                // Output results based on format
                if (cli_ && (cli_->getJsonOutput() || cli_->getVerbose())) {
                    json output;
                    output["document_hash"] = resp.hash;
                    output["metadata_updated"] = resp.metadataUpdated;
                    std::cout << output.dump(2) << std::endl;
                } else {
                    std::cout << "Document metadata updated successfully!" << std::endl;
                    std::cout << "Hash: " << resp.hash.substr(0, 12) << "..." << std::endl;
                    std::cout << "Metadata updated: " << resp.metadataUpdated << std::endl;
                }
                return Result<void>();
            };

            auto fallback = [&]() -> Result<void> {
                // Fall back to local execution
                return executeLocal();
            };

            if (auto d = daemon_first(dreq, fallback, render); d) {
                return Result<void>();
            }
        }

        // Fall back to local execution if daemon failed
        return executeLocal();

    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
    }
}

Result<void> UpdateCommand::executeLocal() {
    // Use injected repository if available (for testing)
    auto metadataRepo = metadataRepo_;

    // Otherwise get from CLI
    if (!metadataRepo && cli_) {
        // Ensure storage is initialized
        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured) {
            return ensured;
        }

        metadataRepo = cli_->getMetadataRepository();
    }

    if (!metadataRepo) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
    }

    // Determine document to update
    std::string docHash;
    int64_t docId = -1;

    if (!hash_.empty()) {
        docHash = hash_;
        // Get document by hash to get its ID
        auto docResult = metadataRepo->getDocumentByHash(docHash);
        if (!docResult) {
            return Error{docResult.error().code, docResult.error().message};
        }
        if (!docResult.value().has_value()) {
            return Error{ErrorCode::NotFound, "Document not found with hash: " + docHash};
        }
        docId = docResult.value()->id;
    } else if (!name_.empty()) {
        // Resolve name to document
        auto resolveResult = resolveNameToDocument(name_);
        if (!resolveResult) {
            return resolveResult.error();
        }
        docId = resolveResult.value().id;
        docHash = resolveResult.value().sha256Hash;
    } else {
        return Error{ErrorCode::InvalidArgument, "No document identifier specified"};
    }

    // Apply metadata updates
    size_t successCount = 0;
    size_t failureCount = 0;

    for (const auto& kv : metadata_) {
        auto pos = kv.find('=');
        if (pos != std::string::npos) {
            std::string key = kv.substr(0, pos);
            std::string value = kv.substr(pos + 1);

            // Set the metadata
            auto setResult = metadataRepo->setMetadata(docId, key, metadata::MetadataValue(value));
            if (setResult) {
                successCount++;
                if (verbose_ || (cli_ && cli_->getVerbose())) {
                    std::cout << "Set " << key << " = " << value << std::endl;
                }
            } else {
                failureCount++;
                spdlog::warn("Failed to set metadata {}: {}", key, setResult.error().message);
            }
        } else {
            spdlog::warn("Invalid metadata format: {} (expected key=value)", kv);
            failureCount++;
        }
    }

    // Update fuzzy index if needed
    metadataRepo->updateFuzzyIndex(docId);

    // Output results
    if (cli_ && (cli_->getJsonOutput() || cli_->getVerbose())) {
        json output;
        output["document_hash"] = docHash;
        output["document_id"] = docId;
        output["metadata_updated"] = successCount;
        output["metadata_failed"] = failureCount;
        std::cout << output.dump(2) << std::endl;
    } else {
        std::cout << "Document metadata updated successfully!" << std::endl;
        std::cout << "Hash: " << docHash.substr(0, 12) << "..." << std::endl;
        std::cout << "Metadata updated: " << successCount << std::endl;
        if (failureCount > 0) {
            std::cout << "Metadata failed: " << failureCount << std::endl;
        }
    }

    return Result<void>();
}

Result<metadata::MetadataValue> UpdateCommand::parseMetadataValue(const std::string& value) {
    // Simple implementation - in reality this might parse JSON or other formats
    return metadata::MetadataValue(value);
}

void UpdateCommand::parseArguments(const std::vector<std::string>& args) {
    // Simple argument parsing for testing
    for (size_t i = 0; i < args.size(); i++) {
        if (args[i] == "--hash" && i + 1 < args.size()) {
            hash_ = args[++i];
        } else if (args[i] == "--name" && i + 1 < args.size()) {
            name_ = args[++i];
        } else if (args[i] == "--key" && i + 1 < args.size()) {
            if (i + 2 < args.size() && args[i + 2] == "--value") {
                metadata_.push_back(args[i + 1] + "=" + args[i + 3]);
                i += 3;
            }
        }
    }
}

Result<metadata::DocumentInfo> UpdateCommand::resolveNameToDocument(const std::string& name) {
    // Use injected repository if available (for testing)
    auto metadataRepo = metadataRepo_;
    if (!metadataRepo && cli_) {
        metadataRepo = cli_->getMetadataRepository();
    }
    if (!metadataRepo) {
        return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
    }

    // First try as a path suffix (for real files)
    auto documentsResult = metadataRepo->findDocumentsByPath("%/" + name);
    if (documentsResult && !documentsResult.value().empty()) {
        const auto& results = documentsResult.value();
        if (results.size() > 1) {
            // Multiple documents with the same name
            std::cerr << "Multiple documents found with name '" << name << "':" << std::endl;
            for (const auto& doc : results) {
                std::cerr << "  " << doc.sha256Hash.substr(0, 12) << "... - " << doc.filePath
                          << std::endl;
            }
            return Error{
                ErrorCode::InvalidOperation,
                "Multiple documents with the same name. Please use hash to specify which one."};
        }
        return results[0];
    }

    // Try exact path match
    documentsResult = metadataRepo->findDocumentsByPath(name);
    if (documentsResult && !documentsResult.value().empty()) {
        return documentsResult.value()[0];
    }

    // For stdin documents or when path search fails, search all documents and match by fileName
    auto allDocsResult = metadataRepo->getDocumentCount();
    if (!allDocsResult) {
        return Error{allDocsResult.error().code, allDocsResult.error().message};
    }

    // Use search to find by name (works for both file and stdin documents)
    auto searchResult = metadataRepo->search(name, 100, 0);
    if (searchResult) {
        std::vector<metadata::DocumentInfo> matchingDocs;
        for (const auto& result : searchResult.value().results) {
            // SearchResult contains document directly
            const auto& doc = result.document;
            // Check if fileName matches exactly
            if (doc.fileName == name) {
                matchingDocs.push_back(doc);
            }
        }

        if (!matchingDocs.empty()) {
            if (matchingDocs.size() > 1) {
                std::cerr << "Multiple documents found with name '" << name << "':" << std::endl;
                for (const auto& doc : matchingDocs) {
                    std::cerr << "  " << doc.sha256Hash.substr(0, 12) << "... - " << doc.filePath
                              << std::endl;
                }
                return Error{
                    ErrorCode::InvalidOperation,
                    "Multiple documents with the same name. Please use hash to specify which one."};
            }
            return matchingDocs[0];
        }
    }

    return Error{ErrorCode::NotFound, "No document found with name: " + name};
}

// Factory function
std::unique_ptr<ICommand> createUpdateCommand() {
    return std::make_unique<UpdateCommand>();
}

} // namespace yams::cli