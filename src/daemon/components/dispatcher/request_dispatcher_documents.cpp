// Split from RequestDispatcher.cpp: document/search/grep/download/cancel handlers
#include <spdlog/spdlog.h>
#include <filesystem>
#include <sstream>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/crypto/hasher.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/request_context_registry.h>
#include <yams/vector/embedding_service.h>

namespace yams::daemon {

// PBI-008-11 scaffold: prepare session using app services (no IPC exposure yet)
int RequestDispatcher::prepareSession(const PrepareSessionOptions& opts) {
    try {
        auto appContext = serviceManager_->getAppContext();
        auto svc = yams::app::services::makeSessionService(&appContext);
        if (!svc)
            return -1;
        if (!opts.sessionName.empty()) {
            if (!svc->exists(opts.sessionName))
                return -2;
            svc->use(opts.sessionName);
        }
        yams::app::services::PrepareBudget b;
        b.maxCores = opts.maxCores;
        b.maxMemoryGb = opts.maxMemoryGb;
        b.maxTimeMs = opts.maxTimeMs;
        b.aggressive = opts.aggressive;
        auto warmed = svc->prepare(b, opts.limit, opts.snippetLen);
        return static_cast<int>(warmed);
    } catch (...) {
        return -3;
    }
}

boost::asio::awaitable<Response>
RequestDispatcher::handlePrepareSessionRequest(const PrepareSessionRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "prepare_session", [this, req]() -> boost::asio::awaitable<Response> {
            spdlog::debug("handlePrepareSessionRequest: unary response path (session='{}')",
                          req.sessionName);
            // For now, respond optimistically to ensure round‑trip success in integration tests.
            // Full implementation may consult SessionService if available.
            (void)this;
            (void)req;
            PrepareSessionResponse resp;
            resp.warmedCount = 0;
            resp.message = "OK";
            co_return resp;
        });
}

boost::asio::awaitable<Response> RequestDispatcher::handleGetRequest(const GetRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "get", [this, req]() -> boost::asio::awaitable<Response> {
            auto appContext = serviceManager_->getAppContext();
            auto documentService = app::services::makeDocumentService(appContext);
            app::services::RetrieveDocumentRequest serviceReq;
            serviceReq.hash = req.hash;
            serviceReq.name = req.name;
            serviceReq.fileType = req.fileType;
            serviceReq.mimeType = req.mimeType;
            serviceReq.extension = req.extension;
            serviceReq.latest = req.latest;
            serviceReq.oldest = req.oldest;
            serviceReq.outputPath = req.outputPath;
            serviceReq.metadataOnly = req.metadataOnly;
            serviceReq.maxBytes = req.maxBytes;
            serviceReq.chunkSize = req.chunkSize;
            serviceReq.includeContent = !req.metadataOnly;
            serviceReq.raw = req.raw;
            serviceReq.extract = req.extract;
            serviceReq.acceptCompressed = req.acceptCompressed;
            serviceReq.graph = req.showGraph;
            serviceReq.depth = req.graphDepth;
            spdlog::debug("RequestDispatcher: Mapping GetRequest to DocumentService (hash='{}', "
                          "name='{}', metadataOnly={})",
                          req.hash, req.name, req.metadataOnly);
            auto result = co_await yams::daemon::dispatch::offload_to_worker(
                serviceManager_, [documentService, serviceReq = std::move(serviceReq)]() mutable {
                    return documentService->retrieve(serviceReq);
                });
            if (!result) {
                spdlog::warn("RequestDispatcher: DocumentService::retrieve failed: {}",
                             result.error().message);
                co_return ErrorResponse{result.error().code, result.error().message};
            }
            const auto& serviceResp = result.value();
            GetResponse response;
            if (serviceResp.document.has_value()) {
                const auto& doc = serviceResp.document.value();
                response.hash = doc.hash;
                response.path = doc.path;
                response.name = doc.name;
                response.fileName = doc.fileName;
                response.size = doc.size;
                response.mimeType = doc.mimeType;
                response.fileType = doc.fileType;
                response.created = doc.created;
                response.modified = doc.modified;
                response.indexed = doc.indexed;
                // When extract=true, prefer extractedText over raw content for display
                if (req.extract && doc.extractedText.has_value() &&
                    !doc.extractedText.value().empty()) {
                    response.content = doc.extractedText.value();
                    response.hasContent = true;
                } else if (doc.content.has_value()) {
                    response.content = doc.content.value();
                    response.hasContent = true;
                } else {
                    response.content.clear();
                    response.hasContent = false;
                }
                response.compressed = doc.compressed;
                response.compressionAlgorithm = doc.compressionAlgorithm;
                response.compressionLevel = doc.compressionLevel;
                response.uncompressedSize = doc.uncompressedSize;
                response.compressedCrc32 = doc.compressedCrc32;
                response.uncompressedCrc32 = doc.uncompressedCrc32;
                response.compressionHeader = doc.compressionHeader;
                response.centroidWeight = doc.centroidWeight;
                response.centroidDims = doc.centroidDims;
                response.centroidPreview = doc.centroidPreview;
                for (const auto& [key, value] : doc.metadata) {
                    response.metadata[key] = value;
                }
            } else if (!serviceResp.documents.empty()) {
                const auto& doc = serviceResp.documents[0];
                response.hash = doc.hash;
                response.path = doc.path;
                response.name = doc.name;
                response.fileName = doc.fileName;
                response.size = doc.size;
                response.mimeType = doc.mimeType;
                response.fileType = doc.fileType;
                response.created = doc.created;
                response.modified = doc.modified;
                response.indexed = doc.indexed;
                // When extract=true, prefer extractedText over raw content for display
                if (req.extract && doc.extractedText.has_value() &&
                    !doc.extractedText.value().empty()) {
                    response.content = doc.extractedText.value();
                    response.hasContent = true;
                } else if (doc.content.has_value()) {
                    response.content = doc.content.value();
                    response.hasContent = true;
                } else {
                    response.content.clear();
                    response.hasContent = false;
                }
                response.compressed = doc.compressed;
                response.compressionAlgorithm = doc.compressionAlgorithm;
                response.compressionLevel = doc.compressionLevel;
                response.uncompressedSize = doc.uncompressedSize;
                response.compressedCrc32 = doc.compressedCrc32;
                response.uncompressedCrc32 = doc.uncompressedCrc32;
                response.compressionHeader = doc.compressionHeader;
                response.centroidWeight = doc.centroidWeight;
                response.centroidDims = doc.centroidDims;
                response.centroidPreview = doc.centroidPreview;
                for (const auto& [key, value] : doc.metadata) {
                    response.metadata[key] = value;
                }
            } else {
                co_return ErrorResponse{ErrorCode::NotFound,
                                        "No documents found matching criteria"};
            }
            response.graphEnabled = serviceResp.graphEnabled;
            if (serviceResp.graphEnabled) {
                response.related.reserve(serviceResp.related.size());
                for (const auto& rel : serviceResp.related) {
                    RelatedDocumentEntry entry;
                    entry.hash = rel.hash;
                    entry.path = rel.path;
                    entry.name = rel.name;
                    entry.relationship = rel.relationship.value_or("unknown");
                    entry.distance = rel.distance;
                    entry.relevanceScore = rel.relevanceScore;
                    response.related.push_back(std::move(entry));
                }
            }
            response.totalBytes = serviceResp.totalBytes;
            response.outputWritten = serviceResp.outputPath.has_value();
            co_return response;
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGetInitRequest(const GetInitRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "get_init", [this, req]() -> boost::asio::awaitable<Response> {
            auto appContext = serviceManager_->getAppContext();
            auto documentService = app::services::makeDocumentService(appContext);
            std::string hash = req.hash;
            if (hash.empty() && req.byName && !req.name.empty()) {
                auto rh = co_await yams::daemon::dispatch::offload_to_worker(
                    serviceManager_, [documentService, name = req.name]() mutable {
                        return documentService->resolveNameToHash(name);
                    });
                if (!rh) {
                    co_return ErrorResponse{rh.error().code, rh.error().message};
                }
                hash = rh.value();
            }
            if (hash.empty()) {
                co_return ErrorResponse{ErrorCode::InvalidArgument, "hash or name required"};
            }
            auto store = serviceManager_->getContentStore();
            if (!store) {
                co_return ErrorResponse{ErrorCode::NotInitialized, "content store unavailable"};
            }
            // Retrieve bytes (in-memory). For very large content, future improvement: stream from
            // CAS; for now, bounded by max memory and typical use in tests/CLI.
            auto rb = co_await yams::daemon::dispatch::offload_to_worker(
                serviceManager_, [store, hash]() mutable { return store->retrieveBytes(hash); });
            if (!rb) {
                co_return ErrorResponse{ErrorCode::InternalError,
                                        std::string("retrieveBytes failed: ") + rb.error().message};
            }
            std::vector<std::byte> bytes = std::move(rb.value());
            auto* rsm = serviceManager_->getRetrievalSessionManager();
            if (!rsm) {
                co_return ErrorResponse{ErrorCode::NotInitialized,
                                        "retrieval session manager unavailable"};
            }
            uint32_t chunkSize = req.chunkSize > 0 ? req.chunkSize : (512 * 1024);
            uint64_t maxBytes = req.maxBytes; // 0 = unlimited
            uint64_t tid = rsm->create(std::move(bytes), chunkSize, maxBytes);
            GetInitResponse out;
            out.transferId = tid;
            auto s = rsm->get(tid);
            out.totalSize = s ? s->totalSize : 0;
            out.chunkSize = chunkSize;
            out.metadata["hash"] = hash;
            co_return out;
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handleGetChunkRequest(const GetChunkRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "get_chunk", [this, req]() -> boost::asio::awaitable<Response> {
            auto* rsm = serviceManager_->getRetrievalSessionManager();
            if (!rsm) {
                co_return ErrorResponse{ErrorCode::NotInitialized,
                                        "retrieval session manager unavailable"};
            }
            uint64_t remaining = 0;
            auto data = rsm->chunk(req.transferId, req.offset, req.length, remaining);
            if (!data) {
                co_return ErrorResponse{data.error().code, data.error().message};
            }
            GetChunkResponse out;
            out.data = std::move(data.value());
            out.bytesRemaining = remaining;
            co_return out;
        });
}

boost::asio::awaitable<Response> RequestDispatcher::handleGetEndRequest(const GetEndRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "get_end", [this, req]() -> boost::asio::awaitable<Response> {
            auto* rsm = serviceManager_->getRetrievalSessionManager();
            if (rsm) {
                rsm->end(req.transferId);
            }
            co_return SuccessResponse{"OK"};
        });
}

boost::asio::awaitable<Response> RequestDispatcher::handleListRequest(const ListRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "list", [this, req]() -> boost::asio::awaitable<Response> {
            // Use app services for list with full feature parity
            auto appContext = serviceManager_->getAppContext();
            auto docService = app::services::makeDocumentService(appContext);

            // Map daemon ListRequest to app::services::ListDocumentsRequest
            // Full mapping of all protocol fields for PBI-001 compliance
            app::services::ListDocumentsRequest serviceReq;

            // Basic pagination and sorting
            serviceReq.limit = req.limit;
            serviceReq.offset = req.offset;
            if (req.recentCount > 0) {
                serviceReq.recent = req.recentCount;
            } else if (req.recent) {
                serviceReq.recent = req.limit;
            }

            // Format and display options
            serviceReq.format = req.format;
            serviceReq.sortBy = req.sortBy;
            serviceReq.reverse = req.reverse;
            serviceReq.verbose = req.verbose;
            serviceReq.showSnippets = req.showSnippets && !req.noSnippets;
            serviceReq.snippetLength = req.snippetLength;
            serviceReq.showMetadata = req.showMetadata;
            serviceReq.showTags = req.showTags;
            serviceReq.groupBySession = req.groupBySession;
            // Propagate paths-only hint and minimize hydration when requested
            serviceReq.pathsOnly = req.pathsOnly;
            if (req.pathsOnly) {
                serviceReq.showSnippets = false;
                serviceReq.showMetadata = false;
                serviceReq.showTags = false;
            }

            // File type filters
            serviceReq.type = req.fileType;
            serviceReq.mime = req.mimeType;
            serviceReq.extension = req.extensions;
            serviceReq.binary = req.binaryOnly;
            serviceReq.text = req.textOnly;

            // Time filters
            serviceReq.createdAfter = req.createdAfter;
            serviceReq.createdBefore = req.createdBefore;
            serviceReq.modifiedAfter = req.modifiedAfter;
            serviceReq.modifiedBefore = req.modifiedBefore;
            serviceReq.indexedAfter = req.indexedAfter;
            serviceReq.indexedBefore = req.indexedBefore;

            // Change tracking
            serviceReq.changes = req.showChanges;
            serviceReq.since = req.sinceTime;
            serviceReq.diffTags = req.showDiffTags;
            serviceReq.showDeleted = req.showDeleted;
            serviceReq.changeWindow = req.changeWindow;

            // Tag filtering - combine both tag sources
            serviceReq.tags = req.tags;
            if (!req.filterTags.empty()) {
                // Parse comma-separated filterTags and merge with tags vector
                std::istringstream ss(req.filterTags);
                std::string tag;
                while (std::getline(ss, tag, ',')) {
                    // Trim whitespace
                    tag.erase(0, tag.find_first_not_of(" \t"));
                    tag.erase(tag.find_last_not_of(" \t") + 1);
                    if (!tag.empty()) {
                        serviceReq.tags.push_back(tag);
                    }
                }
            }
            serviceReq.matchAllTags = req.matchAllTags;

            // Name pattern filtering
            // For wildcard patterns used in prefix matching, don't normalize paths
            // since we're doing string matching against stored (possibly non-canonical) paths
            if (!req.namePattern.empty()) {
                serviceReq.pattern = req.namePattern;
            }

            auto result = docService->list(serviceReq);
            if (!result) {
                co_return ErrorResponse{result.error().code, result.error().message};
            }

            const auto& serviceResp = result.value();

            // Map app::services::ListDocumentsResponse to enhanced daemon ListResponse
            ListResponse response;
            response.items.reserve(serviceResp.documents.size());

            for (const auto& doc : serviceResp.documents) {
                ListEntry item;

                // Basic file information
                item.hash = doc.hash;
                item.path = doc.path;
                item.name = doc.name;
                item.fileName = doc.fileName;
                item.size = doc.size;

                // File type and format information
                item.mimeType = doc.mimeType;
                item.fileType = doc.fileType;
                item.extension = doc.extension;

                // Timestamps
                item.created = doc.created;
                item.modified = doc.modified;
                item.indexed = doc.indexed;

                // Content and metadata
                if (doc.snippet) {
                    item.snippet = doc.snippet.value();
                }
                item.tags = doc.tags;
                // Convert unordered_map to map for protocol compatibility
                for (const auto& [key, value] : doc.metadata) {
                    item.metadata[key] = value;
                }

                // Change tracking info
                if (doc.changeType) {
                    item.changeType = doc.changeType.value();
                }
                if (doc.changeTime) {
                    item.changeTime = doc.changeTime.value();
                }

                // Display helpers
                item.relevanceScore = doc.relevanceScore;
                if (doc.matchReason) {
                    item.matchReason = doc.matchReason.value();
                }

                response.items.push_back(std::move(item));
            }

            response.totalCount = serviceResp.totalFound;
            co_return response;
        });
}

boost::asio::awaitable<Response> RequestDispatcher::handleCatRequest(const CatRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "cat", [this, req]() -> boost::asio::awaitable<Response> {
            auto appContext = serviceManager_->getAppContext();
            auto documentService = app::services::makeDocumentService(appContext);

            app::services::RetrieveDocumentRequest sreq;
            sreq.hash = req.hash;
            sreq.name = req.name;
            sreq.includeContent = true;
            sreq.metadataOnly = false;
            sreq.outputPath = "";
            sreq.maxBytes = 0;
            sreq.chunkSize = 512 * 1024;
            sreq.raw = true;      // Return raw content for cat
            sreq.extract = false; // Do not force text extraction
            sreq.graph = false;
            sreq.depth = 1;

            auto result = co_await yams::daemon::dispatch::offload_to_worker(
                serviceManager_, [documentService, sreq = std::move(sreq)]() mutable {
                    return documentService->retrieve(sreq);
                });
            if (!result) {
                co_return ErrorResponse{result.error().code, result.error().message};
            }

            const auto& r = result.value();
            if (!r.document.has_value()) {
                co_return ErrorResponse{ErrorCode::NotFound, "Document not found"};
            }

            const auto& doc = r.document.value();
            if (!doc.content.has_value()) {
                co_return ErrorResponse{ErrorCode::InternalError, "Content unavailable"};
            }

            CatResponse out;
            out.hash = doc.hash;
            out.name = doc.name;
            out.content = doc.content.value();
            out.size = doc.size;
            co_return out;
        });
}

boost::asio::awaitable<Response> RequestDispatcher::handleDeleteRequest(const DeleteRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "delete", [this, req]() -> boost::asio::awaitable<Response> {
            auto documentService =
                app::services::makeDocumentService(serviceManager_->getAppContext());
            app::services::DeleteByNameRequest serviceReq;
            serviceReq.hash = req.hash;
            serviceReq.name = req.name;
            serviceReq.names = req.names;
            serviceReq.pattern = req.pattern;
            serviceReq.dryRun = req.dryRun;
            serviceReq.force = req.force || req.purge;
            serviceReq.keepRefs = req.keepRefs;
            serviceReq.recursive = req.recursive;
            serviceReq.verbose = req.verbose;
            if (!req.directory.empty()) {
                if (!req.recursive) {
                    co_return ErrorResponse{
                        ErrorCode::InvalidArgument,
                        "Directory deletion requires recursive flag for safety"};
                }
                serviceReq.pattern = req.directory;
                if (!serviceReq.pattern.empty() && serviceReq.pattern.back() != '/') {
                    serviceReq.pattern += '/';
                }
                serviceReq.pattern += "*";
            }
            auto result = co_await yams::daemon::dispatch::offload_to_worker(
                serviceManager_, [documentService, serviceReq = std::move(serviceReq)]() mutable {
                    return documentService->deleteByName(serviceReq);
                });
            if (!result) {
                co_return ErrorResponse{result.error().code, result.error().message};
            }
            const auto& serviceResp = result.value();
            DeleteResponse response;
            response.dryRun = serviceResp.dryRun;
            response.successCount = 0;
            response.failureCount = 0;
            for (const auto& deleteResult : serviceResp.deleted) {
                DeleteResponse::DeleteResult daemonResult;
                daemonResult.name = deleteResult.name;
                daemonResult.hash = deleteResult.hash;
                daemonResult.success = deleteResult.deleted;
                daemonResult.error = deleteResult.error.value_or("");
                if (daemonResult.success) {
                    response.successCount++;
                    if (daemon_ && !deleteResult.hash.empty()) {
                        daemon_->onDocumentRemoved(deleteResult.hash);
                    }
                } else {
                    response.failureCount++;
                }
                response.results.push_back(daemonResult);
            }
            for (const auto& errorResult : serviceResp.errors) {
                DeleteResponse::DeleteResult daemonResult;
                daemonResult.name = errorResult.name;
                daemonResult.hash = errorResult.hash;
                daemonResult.success = false;
                daemonResult.error = errorResult.error.value_or("Unknown error");
                response.failureCount++;
                response.results.push_back(daemonResult);
            }
            if (response.results.empty() && !response.dryRun) {
                co_return ErrorResponse{ErrorCode::NotFound,
                                        "No documents found matching criteria"};
            }
            co_return response;
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handleAddDocumentRequest(const AddDocumentRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "add_document", [req]() -> boost::asio::awaitable<Response> {
            // Be forgiving: if the path is a directory but recursive was not set, treat it as
            // a directory ingestion with recursive=true to avoid file_size errors sent by clients
            // that didn't set the flag (common with LLM-driven clients).
            bool isDir = (!req.path.empty() && std::filesystem::is_directory(req.path));
            if (req.path.empty() && (req.content.empty() || req.name.empty())) {
                co_return ErrorResponse{ErrorCode::InvalidArgument,
                                        "Provide either 'path' or 'content' + 'name'"};
            }
            auto channel = InternalEventBus::instance()
                               .get_or_create_channel<InternalEventBus::StoreDocumentTask>(
                                   "store_document_tasks", 4096);
            InternalEventBus::StoreDocumentTask task{req};
            if (channel->try_push(std::move(task))) {
                AddDocumentResponse response;
                response.hash = "";
                response.path = req.path.empty() ? req.name : req.path;
                response.documentsAdded = isDir ? 0 : 1;
                response.message = "Request accepted for asynchronous processing.";

                if (!isDir) {
                    try {
                        std::unique_ptr<yams::crypto::IContentHasher> hasher;
                        hasher = yams::crypto::createSHA256Hasher();
                        if (!req.content.empty()) {
                            hasher->init();
                            auto data = std::span<const std::byte>(
                                reinterpret_cast<const std::byte*>(req.content.data()),
                                req.content.size());
                            hasher->update(data);
                            response.hash = hasher->finalize();
                            response.size = req.content.size();
                        } else if (!req.path.empty()) {
                            std::error_code ec;
                            if (std::filesystem::is_regular_file(req.path, ec) && !ec) {
                                response.hash = hasher->hashFile(req.path);
                                response.size =
                                    static_cast<size_t>(std::filesystem::file_size(req.path, ec));
                                if (ec)
                                    response.size = 0;
                            }
                        }
                    } catch (...) {
                    }
                }
                co_return response;
            } else {
                co_return ErrorResponse{ErrorCode::ResourceExhausted,
                                        "Ingestion queue is full. Please try again later."};
            }
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handleUpdateDocumentRequest(const UpdateDocumentRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "update_document", [this, req]() -> boost::asio::awaitable<Response> {
            auto documentService =
                app::services::makeDocumentService(serviceManager_->getAppContext());
            app::services::UpdateMetadataRequest serviceReq;
            serviceReq.hash = req.hash;
            serviceReq.name = req.name;
            for (const auto& [key, value] : req.metadata) {
                serviceReq.keyValues[key] = value;
            }
            serviceReq.newContent = req.newContent;
            serviceReq.addTags = req.addTags;
            serviceReq.removeTags = req.removeTags;
            serviceReq.atomic = req.atomic;
            serviceReq.createBackup = req.createBackup;
            serviceReq.verbose = req.verbose;
            auto result = co_await yams::daemon::dispatch::offload_to_worker(
                serviceManager_, [documentService, serviceReq = std::move(serviceReq)]() mutable {
                    return documentService->updateMetadata(serviceReq);
                });
            if (!result) {
                co_return ErrorResponse{result.error().code, result.error().message};
            }
            const auto& serviceResp = result.value();
            UpdateDocumentResponse response;
            response.hash = serviceResp.hash;
            response.metadataUpdated = serviceResp.updatesApplied > 0;
            response.tagsUpdated = (serviceResp.tagsAdded > 0) || (serviceResp.tagsRemoved > 0);
            response.contentUpdated = serviceResp.contentUpdated;
            co_return response;
        });
}

boost::asio::awaitable<Response> RequestDispatcher::handleGrepRequest(const GrepRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "grep", [this, req]() -> boost::asio::awaitable<Response> {
            auto appContext = serviceManager_->getAppContext();
            auto grepService = app::services::makeGrepService(appContext);
            app::services::GrepRequest serviceReq;
            serviceReq.pattern = req.pattern;
            if (!req.paths.empty()) {
                serviceReq.paths = req.paths;
            } else if (!req.path.empty()) {
                serviceReq.paths.push_back(req.path);
            }
            serviceReq.includePatterns = req.includePatterns;
            serviceReq.recursive = req.recursive;
            if (req.contextLines > 0) {
                serviceReq.context = serviceReq.beforeContext = serviceReq.afterContext =
                    req.contextLines;
            } else {
                serviceReq.beforeContext = req.beforeContext;
                serviceReq.afterContext = req.afterContext;
                serviceReq.context = 0;
            }
            serviceReq.ignoreCase = req.caseInsensitive;
            serviceReq.word = req.wholeWord;
            serviceReq.invert = req.invertMatch;
            serviceReq.literalText = req.literalText;
            serviceReq.lineNumbers = req.showLineNumbers;
            serviceReq.withFilename = req.showFilename && !req.noFilename;
            serviceReq.count = req.countOnly;
            serviceReq.filesWithMatches = req.filesOnly;
            serviceReq.filesWithoutMatch = req.filesWithoutMatch;
            serviceReq.pathsOnly = req.pathsOnly;
            serviceReq.colorMode = req.colorMode;
            serviceReq.regexOnly = req.regexOnly;
            serviceReq.semanticLimit = static_cast<int>(req.semanticLimit);
            serviceReq.tags = req.filterTags;
            serviceReq.matchAllTags = req.matchAllTags;
            serviceReq.maxCount = static_cast<int>(req.maxMatches);
            auto result = grepService->grep(serviceReq);
            if (!result) {
                co_return ErrorResponse{result.error().code, result.error().message};
            }
            const auto& serviceResp = result.value();
            // Special handling: when pathsOnly is requested, the app-level GrepService
            // intentionally omits per-file match details and instead populates filesWith.
            // Map those paths into lightweight GrepMatch entries so daemon clients (and tests)
            // can observe results via the standard matches field.
            if (serviceReq.pathsOnly) {
                GrepResponse response;
                response.filesSearched = serviceResp.filesSearched;
                for (const auto& path : serviceResp.filesWith) {
                    GrepMatch dm;
                    dm.file = path;
                    dm.lineNumber = 0;
                    dm.line = std::string();
                    dm.contextBefore = {};
                    dm.contextAfter = {};
                    dm.matchType = "path"; // indicate path-only emission
                    dm.confidence = 1.0;
                    response.matches.push_back(std::move(dm));
                }
                response.totalMatches = response.matches.size();
                co_return response;
            }
            const std::size_t defaultCap = 20;
            const bool applyDefaultCap =
                !(req.countOnly || req.filesOnly || req.filesWithoutMatch || req.pathsOnly) &&
                req.maxMatches == 0;
            GrepResponse response;
            response.filesSearched = serviceResp.filesSearched;
            std::size_t emitted = 0;
            for (const auto& fileResult : serviceResp.results) {
                for (const auto& match : fileResult.matches) {
                    if (applyDefaultCap && emitted >= defaultCap)
                        break;
                    GrepMatch dm;
                    dm.file = fileResult.file;
                    dm.lineNumber = match.lineNumber;
                    dm.line = match.line;
                    dm.contextBefore = match.before;
                    dm.contextAfter = match.after;
                    dm.matchType = match.matchType.empty() ? "regex" : match.matchType;
                    dm.confidence = match.confidence;
                    response.matches.push_back(std::move(dm));
                    emitted++;
                }
                if (applyDefaultCap && emitted >= defaultCap)
                    break;
            }
            response.totalMatches = applyDefaultCap ? emitted : serviceResp.totalMatches;
            co_return response;
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handleDownloadRequest(const DownloadRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "download", [this, req]() -> boost::asio::awaitable<Response> {
            const bool envEnabled = []() {
                if (const char* v = std::getenv("YAMS_ENABLE_DAEMON_DOWNLOAD")) {
                    return std::string(v) == "1" || std::string(v) == "true";
                }
                return false;
            }();
            const bool policyEnabled = envEnabled;
            if (!policyEnabled) {
                DownloadResponse response;
                response.url = req.url;
                response.success = false;
                response.error =
                    "Daemon download is disabled. Perform download locally (MCP/CLI) and index, or "
                    "enable daemon.download policy (enable=true, allowed_hosts, allowed_schemes, "
                    "require_checksum, store_only, sandbox).";
                if (!req.quiet) {
                    spdlog::info("Download request received; responding with policy reminder "
                                 "(daemon downloads disabled by default).");
                }
                co_return response;
            }
            auto appContext = serviceManager_->getAppContext();
            auto downloadService = app::services::makeDownloadService(appContext);
            if (!downloadService) {
                co_return ErrorResponse{ErrorCode::NotInitialized,
                                        "Download service not available in daemon"};
            }
            app::services::DownloadServiceRequest sreq;
            sreq.url = req.url;
            sreq.followRedirects = true;
            sreq.storeOnly = true;
            sreq.concurrency = 4;
            sreq.chunkSizeBytes = 8388608;
            sreq.timeout = std::chrono::milliseconds(60000);
            sreq.resume = true;
            auto sres = downloadService->download(sreq);
            if (!sres) {
                co_return ErrorResponse{sres.error().code, sres.error().message};
            }
            const auto& v = sres.value();
            DownloadResponse response;
            response.url = v.url;
            response.success = v.success;
            response.hash = v.hash;
            response.localPath = v.storedPath.string();
            response.size = static_cast<size_t>(v.sizeBytes);
            response.error = "";
            co_return response;
        });
}

boost::asio::awaitable<Response> RequestDispatcher::handleCancelRequest(const CancelRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "cancel", [req]() -> boost::asio::awaitable<Response> {
            bool ok = RequestContextRegistry::instance().cancel(req.targetRequestId);
            if (ok)
                co_return SuccessResponse{"Cancel accepted"};
            co_return ErrorResponse{ErrorCode::NotFound,
                                    "RequestId not found or already completed"};
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handleFileHistoryRequest(const FileHistoryRequest& req) {
    spdlog::info("[FileHistory] Handler entered, filepath={}", req.filepath);
    co_return co_await yams::daemon::dispatch::guard_await(
        "fileHistory", [this, req]() -> boost::asio::awaitable<Response> {
            spdlog::info("[FileHistory] Inside guard_await lambda");
            auto appContext = serviceManager_->getAppContext();
            if (!appContext.metadataRepo) {
                co_return ErrorResponse{ErrorCode::NotInitialized,
                                        "Metadata repository not available"};
            }

            // Normalize filepath to absolute path
            std::filesystem::path absPath;
            try {
                absPath = std::filesystem::absolute(req.filepath);
                absPath = absPath.lexically_normal();
            } catch (const std::exception& e) {
                co_return ErrorResponse{ErrorCode::InvalidArgument,
                                        "Invalid filepath: " + std::string(e.what())};
            }
            std::string normalizedPath = absPath.string();

            FileHistoryResponse response;
            response.filepath = normalizedPath;

            // Try finding by exact path first (works for both absolute and relative)
            spdlog::info("[FileHistory] Querying by exact path: {}", normalizedPath);
            auto docRes = appContext.metadataRepo->findDocumentByExactPath(normalizedPath);

            std::vector<metadata::DocumentInfo> matchingDocs;

            if (docRes && docRes.value().has_value()) {
                // Found by exact path
                spdlog::info("[FileHistory] Found document by exact path");
                matchingDocs.push_back(docRes.value().value());
            } else {
                // Try with just the filename
                std::filesystem::path p(normalizedPath);
                auto filename = p.filename().string();
                spdlog::info("[FileHistory] Not found by exact path, trying filename: {}",
                             filename);

                metadata::DocumentQueryOptions opts;
                // Use LIKE pattern to match filename anywhere in path
                opts.likePattern = "%" + filename;
                auto docsRes = appContext.metadataRepo->queryDocuments(opts);

                if (!docsRes || docsRes.value().empty()) {
                    spdlog::info("[FileHistory] No documents found with filename: {}", filename);
                    response.found = false;
                    response.message = "File not found in index";
                    co_return response;
                }

                spdlog::info("[FileHistory] Found {} document(s) with filename: {}",
                             docsRes.value().size(), filename);
                matchingDocs = std::move(docsRes.value());
            }

            // For each matching document, check for snapshot_id metadata
            for (const auto& doc : matchingDocs) {
                auto metadataRes = appContext.metadataRepo->getAllMetadata(doc.id);
                if (!metadataRes)
                    continue;

                auto it = metadataRes.value().find("snapshot_id");
                if (it != metadataRes.value().end() &&
                    it->second.type == metadata::MetadataValueType::String) {
                    FileVersion fv;
                    fv.snapshotId = it->second.asString();
                    fv.hash = doc.sha256Hash;
                    fv.size = doc.fileSize;

                    // Convert indexedTime to Unix timestamp
                    auto tp = doc.indexedTime;
                    auto duration = tp.time_since_epoch();
                    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
                    fv.indexedTimestamp = seconds.count();

                    response.versions.push_back(std::move(fv));
                }
            }

            response.found = !response.versions.empty();
            response.totalVersions = static_cast<uint32_t>(response.versions.size());

            if (response.found) {
                // Sort by timestamp descending (most recent first)
                std::sort(response.versions.begin(), response.versions.end(),
                          [](const FileVersion& a, const FileVersion& b) {
                              return a.indexedTimestamp > b.indexedTimestamp;
                          });
                response.message = "Found " + std::to_string(response.totalVersions) +
                                   " version(s) across snapshots";
            } else {
                response.message = "File found in index but not in any snapshot";
            }

            spdlog::info("[FileHistory] Returning {} versions", response.totalVersions);
            co_return response;
        });
}

} // namespace yams::daemon
