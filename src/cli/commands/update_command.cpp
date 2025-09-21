#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <iostream>
#include <sstream>
#include <yams/cli/commands/update_command.h>
#include <yams/cli/yams_cli.h>
// Daemon client API for daemon-first update
#include <future>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>
// Smart name resolution
#include <filesystem>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>

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
    cmd->add_flag("--latest", latest_, "When multiple matches, pick the latest");
    cmd->add_flag("--oldest", oldest_, "When multiple matches, pick the oldest");
    cmd->add_flag("--no-session", noSession_, "Bypass session scoping for name resolution");

    // Basic conflict: latest vs oldest
    cmd->callback([this]() {
        if (latest_ && oldest_) {
            throw CLI::ValidationError("update", "--latest and --oldest are mutually exclusive");
        }
        cli_->setPendingCommand(this);
    });
}

Result<void> UpdateCommand::execute() {
    try {
        // If called by name, resolve to hash first using smart resolver (daemon path later uses
        // hash)
        if (hash_.empty() && !name_.empty()) {
            auto hr = resolveNameToHashSmart(name_);
            if (!hr)
                return hr.error();
            hash_ = hr.value();
        }
        // Attempt daemon-first update; fall back to local on failure
        if (cli_) {
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

            try {
                yams::daemon::ClientConfig cfg;
                if (cli_ && cli_->hasExplicitDataDir())
                    cfg.dataDir = cli_->getDataPath();
                cfg.enableChunkedResponses = false;
                cfg.requestTimeout = std::chrono::milliseconds(30000);
                auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                if (!leaseRes) {
                    spdlog::warn("Update: unable to acquire daemon client: {}",
                                 leaseRes.error().message);
                    throw std::runtime_error("daemon unavailable");
                }
                auto leaseHandle = std::move(leaseRes.value());
                std::promise<Result<yams::daemon::UpdateDocumentResponse>> prom;
                auto fut = prom.get_future();
                boost::asio::co_spawn(
                    boost::asio::system_executor{},
                    [leaseHandle, dreq, &prom]() mutable -> boost::asio::awaitable<void> {
                        auto& client = **leaseHandle;
                        auto r = co_await client.call(dreq);
                        prom.set_value(std::move(r));
                        co_return;
                    },
                    boost::asio::detached);
                if (fut.wait_for(std::chrono::seconds(30)) == std::future_status::ready) {
                    auto result = fut.get();
                    if (result) {
                        auto r = render(result.value());
                        if (!r)
                            return r.error();
                        return Result<void>();
                    }
                }
            } catch (...) {
                // fall through to local execution
            }
        }

        // Fall back to local execution if daemon failed
        return executeLocal();

    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
    }
}

// New: non-blocking async implementation leveraging DaemonClient coroutines
boost::asio::awaitable<Result<void>> UpdateCommand::executeAsync() {
    try {
        if (cli_) {
            yams::daemon::UpdateDocumentRequest dreq;
            dreq.hash = hash_;
            dreq.name = name_;

            for (const auto& kv : metadata_) {
                auto pos = kv.find('=');
                if (pos != std::string::npos) {
                    std::string key = kv.substr(0, pos);
                    std::string value = kv.substr(pos + 1);
                    dreq.metadata[key] = value;
                }
            }

            auto render = [&](const yams::daemon::UpdateDocumentResponse& resp) -> Result<void> {
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

            // Daemon-first using a short, safe header timeout; fallback to local if unavailable
            yams::daemon::ClientConfig cfg;
            if (cli_)
                cfg.dataDir = cli_->getDataPath();
            cfg.enableChunkedResponses = false;
            cfg.requestTimeout = std::chrono::milliseconds(30000);
            cfg.headerTimeout = std::chrono::milliseconds(3000);
            cfg.bodyTimeout = std::chrono::milliseconds(20000);
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
            if (!leaseRes) {
                spdlog::warn("Update: unable to acquire daemon client: {}",
                             leaseRes.error().message);
                co_return executeLocal();
            }
            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;

            auto result = co_await client.call(dreq);
            if (result) {
                auto r = render(result.value());
                if (!r)
                    co_return r.error();
                co_return Result<void>();
            }
            spdlog::warn("Update: daemon unavailable or failed ({}); using local path.",
                         result.error().message);
        }

        co_return executeLocal();

    } catch (const std::exception& e) {
        co_return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
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
        // Resolve name using smart resolver
        auto hr = resolveNameToHashSmart(name_);
        if (!hr)
            return hr.error();
        docHash = hr.value();
        auto docResult = metadataRepo->getDocumentByHash(docHash);
        if (!docResult)
            return Error{docResult.error()};
        if (!docResult.value().has_value()) {
            return Error{ErrorCode::NotFound, "Document not found with resolved hash: " + docHash};
        }
        docId = docResult.value()->id;
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

// Smart resolution mirroring get_by_name behavior used in MCP server
Result<std::string> UpdateCommand::resolveNameToHashSmart(const std::string& name) {
    // Normalize: expand leading '~'
    std::string nm = name;
    try {
        if (!nm.empty() && nm.front() == '~') {
            if (const char* home = std::getenv("HOME")) {
                nm = std::string(home) + nm.substr(1);
            }
        }
    } catch (...) {
    }

    // Use RetrievalService to resolve by name via daemon when available; fall back to service
    yams::app::services::RetrievalService rsvc;
    yams::app::services::RetrievalOptions ropts;
    if (cli_ && cli_->hasExplicitDataDir())
        ropts.explicitDataDir = cli_->getDataPath();

    bool pickOldest = oldest_;
    bool includeContent = false;
    bool useSession = !noSession_;
    std::string sessionName{};

    // Provide resolver via DocumentService for parity
    std::function<Result<std::string>(const std::string&)> resolver;
    if (cli_) {
        if (auto appContext = cli_->getAppContext()) {
            auto documentService = yams::app::services::makeDocumentService(*appContext);
            if (documentService) {
                resolver = [documentService](const std::string& nm2) {
                    return documentService->resolveNameToHash(nm2);
                };
            }
        }
    }

    auto grr = rsvc.getByNameSmart(nm, pickOldest, includeContent, useSession, sessionName, ropts,
                                   resolver);
    if (grr) {
        return grr.value().hash;
    }

    // Fallback: try common list patterns via RetrievalService (daemon)
    auto tryList = [&](const std::string& pat) -> std::optional<yams::daemon::ListResponse> {
        yams::daemon::ListRequest lreq;
        lreq.namePattern = pat;
        lreq.limit = 500;
        lreq.pathsOnly = false;
        auto lr = rsvc.list(lreq, ropts);
        if (lr && !lr.value().items.empty())
            return lr.value();
        return std::nullopt;
    };

    std::optional<yams::daemon::ListResponse> lr;
    lr = tryList(std::string("%/") + nm);
    if (!lr) {
        std::string stem = nm;
        try {
            stem = std::filesystem::path(nm).stem().string();
        } catch (...) {
        }
        lr = tryList(std::string("%/") + stem + "%");
    }
    if (!lr)
        lr = tryList(std::string("%") + nm + "%");
    if (!lr || lr->items.empty())
        return Error{ErrorCode::NotFound, "No matching documents"};

    // Disambiguate
    const yams::daemon::ListEntry* chosen = nullptr;
    if (latest_ || oldest_) {
        for (const auto& it : lr->items) {
            if (!chosen)
                chosen = &it;
            else if (oldest_) {
                if (it.indexed < chosen->indexed)
                    chosen = &it;
            } else {
                if (it.indexed > chosen->indexed)
                    chosen = &it;
            }
        }
    } else {
        auto scoreName = [&](const std::string& base) -> int {
            if (base == nm)
                return 1000;
            if (base.size() >= nm.size() && base.rfind(nm, 0) == 0)
                return 800;
            if (base.find(nm) != std::string::npos)
                return 600;
            int dl = static_cast<int>(std::abs((long)(base.size() - nm.size())));
            return 400 - std::min(200, dl * 10);
        };
        int best = -1;
        for (const auto& it : lr->items) {
            std::string b;
            try {
                b = std::filesystem::path(it.path).filename().string();
            } catch (...) {
                b = it.name;
            }
            int sc = scoreName(b);
            if (sc > best) {
                best = sc;
                chosen = &it;
            }
        }
    }
    if (!chosen)
        return Error{ErrorCode::NotFound, "No matching documents"};
    return chosen->hash;
}

// Factory function
std::unique_ptr<ICommand> createUpdateCommand() {
    return std::make_unique<UpdateCommand>();
}

} // namespace yams::cli
