#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/async_ipc_server.h>
#include <yams/daemon/resource/model_provider.h>
// Search + metadata
#include <yams/api/content_store_builder.h>
#include <yams/common/name_resolver.h>
#include <yams/daemon/ipc/retrieval_session.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_engine_builder.h>
#include <yams/search/search_executor.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_index_manager.h>
// Download functionality
#include <yams/downloader/downloader.hpp>

#include <spdlog/spdlog.h>
#include <cstdlib>
#include <filesystem>
#include <regex>
#include <set>
#include <sstream>

#include <cerrno>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace yams::daemon {

// Forward declarations for helper functions
double getMemoryUsage();
double getCpuUsage();

// Static member for signal handling
YamsDaemon* YamsDaemon::instance_ = nullptr;

YamsDaemon::YamsDaemon(const DaemonConfig& config) : config_(config) {
    stats_.startTime = std::chrono::steady_clock::now();

    // Resolve paths if not explicitly set
    if (config_.socketPath.empty()) {
        config_.socketPath = resolveSystemPath(PathType::Socket);
    }
    if (config_.pidFile.empty()) {
        config_.pidFile = resolveSystemPath(PathType::PidFile);
    }
    if (config_.logFile.empty()) {
        config_.logFile = resolveSystemPath(PathType::LogFile);
    }

    pidFile_ = config_.pidFile;

    // Log resolved paths for debugging
    spdlog::info("Daemon configuration:");
    if (!config_.dataDir.empty()) {
        spdlog::info("  Data dir: {}", config_.dataDir.string());
    }
    spdlog::info("  Socket: {}", config_.socketPath.string());
    spdlog::info("  PID file: {}", config_.pidFile.string());
    spdlog::info("  Log file: {}", config_.logFile.string());
}

YamsDaemon::~YamsDaemon() {
    if (running_) {
        stop();
    }
    removePidFile();
}

Result<void> YamsDaemon::start() {
    if (running_.exchange(true)) {
        return Error{ErrorCode::InvalidState, "Daemon already running"};
    }

    spdlog::info("Starting YAMS daemon...");

    // Check if another daemon is already running
    if (isDaemonRunning()) {
        running_ = false;
        return Error{ErrorCode::InvalidState, "Another daemon instance is already running"};
    }

    // Create PID file
    if (auto result = createPidFile(); !result) {
        running_ = false;
        return result;
    }

    // Initialize resources
    if (auto result = initializeResources(); !result) {
        removePidFile();
        running_ = false;
        return result;
    }

    // Setup signal handlers
    setupSignalHandlers();

    // Verify core resource readiness before starting IPC server
    if (!(contentStore_ && database_ && metadataRepo_ && searchExecutor_)) {
        spdlog::error(
            "Daemon not ready - core resources missing (store: {}, db: {}, repo: {}, search: {})",
            contentStore_ ? "yes" : "no", database_ ? "yes" : "no", metadataRepo_ ? "yes" : "no",
            searchExecutor_ ? "yes" : "no");
        removePidFile();
        running_ = false;
        return Error{ErrorCode::NotInitialized, "Daemon core resources not ready"};
    }
    // Start IPC server
    AsyncIpcServer::Config serverConfig;
    serverConfig.socket_path = config_.socketPath;
    serverConfig.worker_threads = config_.workerThreads;
    serverConfig.max_connections = 100;
    serverConfig.connection_timeout = std::chrono::seconds(30);

    ipcServer_ = std::make_unique<AsyncIpcServer>(serverConfig);

    // Set request handler
    ipcServer_->set_handler([this](const Request& req) -> Response {
        stats_.requestsProcessed++;

        // Handle different request types
        return std::visit(
            [this](auto&& r) -> Response {
                using T = std::decay_t<decltype(r)>;

                if constexpr (std::is_same_v<T, StatusRequest>) {
                    auto uptime = std::chrono::steady_clock::now() - stats_.startTime;
                    StatusResponse res;
                    res.running = true;
                    res.uptimeSeconds =
                        std::chrono::duration_cast<std::chrono::seconds>(uptime).count();
                    res.requestsProcessed = stats_.requestsProcessed.load();
                    res.activeConnections = stats_.activeConnections.load();
                    res.memoryUsageMb = getMemoryUsage();
                    res.cpuUsagePercent = getCpuUsage();
                    res.version = "0.4.0";
                    res.ready = static_cast<bool>(contentStore_ && database_ && metadataRepo_ &&
                                                  searchExecutor_);

                    // Add model information if available
                    if (modelProvider_) {
                        auto loadedModels = modelProvider_->getLoadedModels();
                        for (const auto& modelName : loadedModels) {
                            StatusResponse::ModelInfo info;
                            info.name = modelName;
                            info.type = "ONNX";
                            info.memoryMb = 0;     // TODO: Get actual memory usage
                            info.requestCount = 0; // TODO: Get actual request count
                            res.models.push_back(info);
                        }
                    }

                    return res;
                } else if constexpr (std::is_same_v<T, ShutdownRequest>) {
                    spdlog::info("Received shutdown request (graceful={})", r.graceful);

                    // Schedule shutdown after a brief delay to allow response to be sent
                    std::thread([this, graceful = r.graceful]() {
                        // Wait for response to be sent
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));

                        spdlog::info("Executing shutdown sequence");

                        if (!graceful) {
                            // Force immediate shutdown by setting stopRequested only
                            stopRequested_ = true;
                            if (ipcServer_) {
                                ipcServer_->stop();
                            }
                        } else {
                            // Graceful shutdown - call the full stop() method
                            auto stopResult = stop();
                            if (!stopResult) {
                                spdlog::error("Shutdown failed: {}", stopResult.error().message);
                            }
                        }
                    }).detach();

                    return SuccessResponse{"Shutdown initiated"};
                } else if constexpr (std::is_same_v<T, PingRequest>) {
                    PongResponse res;
                    res.serverTime = std::chrono::steady_clock::now();
                    return res;
                } else if constexpr (std::is_same_v<T, SearchRequest>) {
                    // Ensure search resources are initialized
                    if (!metadataRepo_ || (!searchExecutor_ && !vectorIndexManager_)) {
                        spdlog::warn("Search unavailable: resources not initialized (store: {}, "
                                     "db: {}, repo: {}, search: {}, vecIndex: {}).",
                                     contentStore_ ? "yes" : "no", database_ ? "yes" : "no",
                                     metadataRepo_ ? "yes" : "no", searchExecutor_ ? "yes" : "no",
                                     vectorIndexManager_ ? "yes" : "no");
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Search not available (daemon not ready; see daemon "
                                             "logs for readiness details)"};
                    }
                    try {
                        // If fuzzy explicitly requested, prefer fuzzy path over hybrid
                        if (r.fuzzy) {
                            auto fuzzyRes = metadataRepo_->fuzzySearch(
                                r.query, static_cast<float>(r.similarity),
                                static_cast<int>(r.limit));
                            if (!fuzzyRes) {
                                return ErrorResponse{fuzzyRes.error().code,
                                                     fuzzyRes.error().message};
                            }
                            SearchResponse out;
                            const auto& fr = fuzzyRes.value();
                            out.totalCount = static_cast<size_t>(fr.totalCount);
                            out.elapsed = std::chrono::milliseconds(fr.executionTimeMs);
                            out.results.reserve(fr.results.size());
                            for (const auto& ritem : fr.results) {
                                SearchResult sr;
                                sr.id = std::to_string(ritem.document.id);
                                sr.path = ritem.document.filePath;
                                sr.title = ritem.document.fileName;
                                sr.snippet = ritem.snippet;
                                sr.score = ritem.score;
                                sr.metadata["path"] = ritem.document.filePath;
                                sr.metadata["contentType"] = ritem.document.mimeType;
                                sr.metadata["hash"] = ritem.document.sha256Hash;
                                out.results.push_back(std::move(sr));
                            }
                            return out;
                        }

                        // Prefer hybrid engine when vector index is available; otherwise fallback
                        // to FTS
                        if (vectorIndexManager_) {
                            // Ensure an embedding generator is available (lazy singleton)
                            auto gen = ensureEmbeddingGenerator();
                            yams::search::SearchEngineBuilder builder;
                            builder.withVectorIndex(vectorIndexManager_)
                                .withMetadataRepo(metadataRepo_);
                            if (knowledgeGraphStore_) {
                                builder.withKGStore(
                                    std::shared_ptr<yams::metadata::KnowledgeGraphStore>(
                                        knowledgeGraphStore_.get(), [](auto*) {}));
                            }
                            auto opts =
                                yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
                            opts.hybrid.final_top_k = static_cast<size_t>(r.limit);
                            opts.hybrid.generate_explanations = false;
                            auto engRes = builder.buildEmbedded(opts);
                            if (!engRes) {
                                spdlog::warn("Hybrid engine build failed: {}",
                                             engRes.error().message);
                            } else {
                                auto eng = engRes.value();
                                auto hres = eng->search(r.query, opts.hybrid.final_top_k);
                                if (hres) {
                                    const auto& items = hres.value();
                                    SearchResponse out;
                                    out.totalCount = items.size();
                                    out.elapsed = std::chrono::milliseconds(0);
                                    out.results.reserve(items.size());
                                    for (const auto& it : items) {
                                        SearchResult sr;
                                        sr.id = it.id;
                                        auto itPath = it.metadata.find("path");
                                        if (itPath != it.metadata.end())
                                            sr.path = itPath->second;
                                        auto itTitle = it.metadata.find("title");
                                        if (itTitle != it.metadata.end())
                                            sr.title = itTitle->second;
                                        sr.snippet = it.content;
                                        sr.score = static_cast<double>(it.hybrid_score);
                                        if (itPath != it.metadata.end())
                                            sr.metadata["path"] = itPath->second;
                                        out.results.push_back(std::move(sr));
                                    }
                                    return out;
                                }
                            }
                        }

                        // Standard search via SearchExecutor (FTS), honor literalText
                        // Standard search via SearchExecutor (FTS)
                        yams::search::SearchRequest sreq;
                        sreq.query = r.query;
                        sreq.limit = r.limit;
                        sreq.literalText = r.literalText;
                        auto sres = searchExecutor_
                                        ? searchExecutor_->search(sreq)
                                        : Result<yams::search::SearchResults>(Error{
                                              ErrorCode::NotInitialized, "SearchExecutor missing"});
                        if (!sres) {
                            return ErrorResponse{sres.error().code, sres.error().message};
                        }
                        const auto& items = sres.value().getItems();
                        SearchResponse out;
                        out.totalCount = sres.value().getStatistics().totalResults;
                        out.elapsed = sres.value().getStatistics().totalTime;
                        out.results.reserve(items.size());
                        for (const auto& it : items) {
                            SearchResult sr;
                            sr.id = std::to_string(it.documentId);
                            sr.path = it.path;
                            sr.title = it.title;
                            if (!it.contentPreview.empty()) {
                                sr.snippet = it.contentPreview;
                            } else if (!it.highlights.empty()) {
                                sr.snippet = it.highlights.front().snippet;
                            }
                            sr.score = static_cast<double>(it.relevanceScore);
                            auto mt = it.metadata;
                            auto pit = mt.find("path");
                            if (pit == mt.end() && !it.path.empty()) {
                                sr.metadata["path"] = it.path;
                            } else if (pit != mt.end()) {
                                sr.metadata["path"] = pit->second;
                            }
                            sr.metadata["title"] = it.title;
                            sr.metadata["contentType"] = it.contentType;
                            out.results.push_back(std::move(sr));
                        }
                        return out;
                    } catch (const std::exception& e) {
                        return ErrorResponse{ErrorCode::InternalError,
                                             std::string("Search failed: ") + e.what()};
                    }
                } else if constexpr (std::is_same_v<T, GetRequest>) {
                    using yams::common::resolve_hash_or_name;
                    if (!contentStore_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Content store not available"};
                    }
                    if (!metadataRepo_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Metadata repo not available"};
                    }
                    auto rh = resolve_hash_or_name(*metadataRepo_, r.hash,
                                                   r.byName ? r.name : std::string{});
                    if (!rh)
                        return ErrorResponse{rh.error().code, rh.error().message};
                    std::string resolvedHash = std::move(rh.value());

                    auto bytesRes = contentStore_->retrieveBytes(resolvedHash);
                    if (!bytesRes) {
                        return ErrorResponse{bytesRes.error().code, bytesRes.error().message};
                    }
                    const auto& bytes = bytesRes.value();
                    constexpr size_t kMaxIpcPayload = 700 * 1024;
                    if (bytes.size() > kMaxIpcPayload) {
                        return ErrorResponse{
                            ErrorCode::ResourceExhausted,
                            "Content too large for IPC transfer (" + std::to_string(bytes.size()) +
                                " bytes). Use file retrieval or future chunked API."};
                    }

                    GetResponse gres;
                    gres.hash = resolvedHash;
                    gres.content.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                    if (metadataRepo_) {
                        auto di = metadataRepo_->getDocumentByHash(resolvedHash);
                        if (di && di.value()) {
                            const auto& doc = *di.value();
                            gres.metadata["path"] = doc.filePath;
                            gres.metadata["name"] = doc.fileName;
                            gres.metadata["mime"] = doc.mimeType;
                            gres.metadata["size"] = std::to_string(doc.fileSize);
                        }
                    }
                    return gres;
                } else if constexpr (std::is_same_v<T, GetInitRequest>) {
                    using yams::common::resolve_hash_or_name;
                    if (!contentStore_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Content store not available"};
                    }
                    if (!metadataRepo_)
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Metadata repo not available"};
                    auto rh = resolve_hash_or_name(*metadataRepo_, r.hash,
                                                   r.byName ? r.name : std::string{});
                    if (!rh)
                        return ErrorResponse{rh.error().code, rh.error().message};
                    std::string resolvedHash = std::move(rh.value());
                    // Load bytes (for now we buffer in memory)
                    auto bytesRes = contentStore_->retrieveBytes(resolvedHash);
                    if (!bytesRes) {
                        return ErrorResponse{bytesRes.error().code, bytesRes.error().message};
                    }
                    auto bytes = std::move(bytesRes.value());
                    uint64_t total = bytes.size();
                    // metadataOnly path returns only metadata without starting a session
                    GetInitResponse ir;
                    ir.totalSize = total;
                    ir.chunkSize = r.chunkSize;
                    if (metadataRepo_) {
                        auto di = metadataRepo_->getDocumentByHash(resolvedHash);
                        if (di && di.value()) {
                            const auto& doc = *di.value();
                            ir.metadata["path"] = doc.filePath;
                            ir.metadata["name"] = doc.fileName;
                            ir.metadata["mime"] = doc.mimeType;
                            ir.metadata["size"] = std::to_string(doc.fileSize);
                        }
                    }
                    if (r.metadataOnly) {
                        ir.transferId = 0;
                        return ir;
                    }
                    // Enforce a sensible upper bound to avoid memory blow-ups in daemon
                    constexpr uint64_t kMaxSessionBytes = 16ull * 1024ull * 1024ull; // 16MB
                    uint64_t limit = (r.maxBytes > 0)
                                         ? std::min<uint64_t>(r.maxBytes, kMaxSessionBytes)
                                         : kMaxSessionBytes;
                    if (total > limit) {
                        return ErrorResponse{ErrorCode::ResourceExhausted,
                                             "Content too large for session"};
                    }
                    // Move bytes into session manager
                    uint64_t tid = 0;
                    {
                        std::vector<std::byte> buf(bytes.size());
                        std::memcpy(buf.data(), bytes.data(), bytes.size());
                        tid = retrievalSessions_->create(std::move(buf), r.chunkSize, r.maxBytes);
                    }
                    ir.transferId = tid;
                    return ir;
                } else if constexpr (std::is_same_v<T, GetChunkRequest>) {
                    if (!retrievalSessions_) {
                        return ErrorResponse{ErrorCode::InvalidState, "No active sessions"};
                    }
                    uint64_t remaining = 0;
                    auto cr =
                        retrievalSessions_->chunk(r.transferId, r.offset, r.length, remaining);
                    if (!cr) {
                        return ErrorResponse{cr.error().code, cr.error().message};
                    }
                    GetChunkResponse resp;
                    resp.data = std::move(cr.value());
                    resp.bytesRemaining = remaining;
                    return resp;
                } else if constexpr (std::is_same_v<T, GetEndRequest>) {
                    if (retrievalSessions_)
                        retrievalSessions_->end(r.transferId);
                    return SuccessResponse{"Transfer closed"};
                } else if constexpr (std::is_same_v<T, ListRequest>) {
                    if (!metadataRepo_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Metadata repo not available"};
                    }
                    auto docsRes = metadataRepo_->findDocumentsByPath("%");
                    if (!docsRes) {
                        return ErrorResponse{docsRes.error().code, docsRes.error().message};
                    }
                    auto docs = std::move(docsRes.value());
                    // Sort by indexed time if recent flag
                    if (r.recent) {
                        std::sort(docs.begin(), docs.end(), [](const auto& a, const auto& b) {
                            return a.indexedTime > b.indexedTime;
                        });
                    }
                    // Apply limit
                    size_t count = std::min(docs.size(), r.limit);
                    ListResponse lr;
                    lr.totalCount = docs.size();
                    lr.items.reserve(count);
                    for (size_t i = 0; i < count; ++i) {
                        const auto& d = docs[i];
                        ListEntry e;
                        e.hash = d.sha256Hash;
                        e.path = d.filePath;
                        e.name = d.fileName;
                        e.size = d.fileSize;
                        lr.items.push_back(std::move(e));
                    }
                    return lr;
                } else if constexpr (std::is_same_v<T, DeleteRequest>) {
                    if (!contentStore_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Content store not available"};
                    }
                    auto exists = contentStore_->exists(r.hash);
                    if (!exists) {
                        return ErrorResponse{exists.error().code, exists.error().message};
                    }
                    if (!exists.value()) {
                        return ErrorResponse{ErrorCode::NotFound, "Hash not found in storage"};
                    }
                    auto del = contentStore_->remove(r.hash);
                    if (!del) {
                        return ErrorResponse{del.error().code, del.error().message};
                    }
                    if (metadataRepo_ && r.purge) {
                        auto di = metadataRepo_->getDocumentByHash(r.hash);
                        if (di && di.value()) {
                            auto rm = metadataRepo_->deleteDocument(di.value()->id);
                            if (!rm) {
                                spdlog::warn("Failed to purge metadata for {}: {}", r.hash,
                                             rm.error().message);
                            }
                        }
                    }
                    return SuccessResponse{"Deleted"};
                } else if constexpr (std::is_same_v<T, LoadModelRequest>) {
                    if (!modelProvider_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Model provider not available"};
                    }

                    auto startTime = std::chrono::steady_clock::now();
                    auto result = modelProvider_->loadModel(r.modelName);
                    auto loadTime = std::chrono::steady_clock::now() - startTime;

                    ModelLoadResponse res;
                    res.success = result.has_value();
                    res.modelName = r.modelName;
                    res.loadTimeMs =
                        std::chrono::duration_cast<std::chrono::milliseconds>(loadTime).count();
                    res.memoryUsageMb = 0; // TODO: Get actual memory usage

                    if (!result) {
                        spdlog::warn("Failed to load model {}: {}", r.modelName,
                                     result.error().message);
                        return ErrorResponse{result.error().code, result.error().message};
                    }

                    spdlog::info("Successfully loaded model: {}", r.modelName);
                    return res;
                } else if constexpr (std::is_same_v<T, GenerateEmbeddingRequest>) {
                    if (!modelProvider_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Model provider not available"};
                    }

                    auto startTime = std::chrono::steady_clock::now();

                    // Generate embedding using provider
                    auto embeddingResult = modelProvider_->generateEmbedding(r.text);
                    if (!embeddingResult) {
                        return ErrorResponse{embeddingResult.error().code,
                                             "Failed to generate embedding: " +
                                                 embeddingResult.error().message};
                    }

                    auto processingTime = std::chrono::steady_clock::now() - startTime;

                    EmbeddingResponse res;
                    res.embedding = std::move(embeddingResult.value());
                    res.dimensions = res.embedding.size();
                    res.modelUsed = r.modelName;
                    res.processingTimeMs =
                        std::chrono::duration_cast<std::chrono::milliseconds>(processingTime)
                            .count();

                    return res;
                } else if constexpr (std::is_same_v<T, BatchEmbeddingRequest>) {
                    if (!modelProvider_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Model provider not available"};
                    }

                    auto startTime = std::chrono::steady_clock::now();

                    // Generate batch embeddings using provider
                    auto embeddingsResult = modelProvider_->generateBatchEmbeddings(r.texts);
                    if (!embeddingsResult) {
                        return ErrorResponse{embeddingsResult.error().code,
                                             "Failed to generate embeddings: " +
                                                 embeddingsResult.error().message};
                    }

                    auto processingTime = std::chrono::steady_clock::now() - startTime;

                    if (!embeddingsResult) {
                        return ErrorResponse{embeddingsResult.error().code,
                                             embeddingsResult.error().message};
                    }

                    BatchEmbeddingResponse res;
                    res.embeddings = std::move(embeddingsResult.value());
                    res.dimensions = res.embeddings.empty() ? 0 : res.embeddings[0].size();
                    res.modelUsed = r.modelName;
                    res.processingTimeMs =
                        std::chrono::duration_cast<std::chrono::milliseconds>(processingTime)
                            .count();
                    res.successCount = res.embeddings.size();
                    res.failureCount = r.texts.size() - res.embeddings.size();

                    return res;
                } else if constexpr (std::is_same_v<T, ModelStatusRequest>) {
                    if (!modelProvider_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Model provider not available"};
                    }

                    ModelStatusResponse res;
                    auto loadedModels = modelProvider_->getLoadedModels();
                    // Stats from provider (if available)

                    if (r.modelName.empty()) {
                        // Return status for all models
                        for (const auto& modelName : loadedModels) {
                            ModelStatusResponse::ModelDetails details;
                            details.name = modelName;
                            details.loaded = true;
                            details.isHot = true;            // TODO: Get actual hot status
                            details.memoryMb = 0;            // TODO: Get actual memory usage
                            details.embeddingDim = 384;      // TODO: Get actual dimensions
                            details.maxSequenceLength = 512; // TODO: Get actual max length
                            details.requestCount = 0;        // TODO: Get actual request count
                            details.errorCount = 0;          // TODO: Get actual error count
                            details.loadTime =
                                std::chrono::system_clock::now(); // TODO: Get actual load time
                            details.lastAccess =
                                std::chrono::steady_clock::now(); // TODO: Get actual last access

                            res.models.push_back(details);
                        }
                    } else {
                        // Return status for specific model
                        if (modelProvider_->isModelLoaded(r.modelName)) {
                            ModelStatusResponse::ModelDetails details;
                            details.name = r.modelName;
                            details.loaded = true;
                            details.isHot = true; // TODO: Get actual hot status
                            // TODO: Fill in other details
                            res.models.push_back(details);
                        }
                    }

                    res.totalMemoryMb = modelProvider_->getMemoryUsage() / (1024 * 1024);
                    res.maxMemoryMb = config_.maxMemoryGb * 1024;

                    return res;
                }
                // Phase 3: Add document handler
                else if constexpr (std::is_same_v<T, AddDocumentRequest>) {
                    if (!contentStore_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Content store not initialized"};
                    }

                    AddDocumentResponse res;

                    // Handle single file or directory
                    if (!r.path.empty()) {
                        std::filesystem::path p(r.path);

                        if (std::filesystem::is_directory(p) && r.recursive) {
                            // Recursive directory add
                            size_t count = 0;
                            for (const auto& entry :
                                 std::filesystem::recursive_directory_iterator(p)) {
                                if (entry.is_regular_file()) {
                                    if (!r.includeHidden &&
                                        entry.path().filename().string()[0] == '.') {
                                        continue;
                                    }

                                    auto result = contentStore_->store(entry.path());
                                    if (result) {
                                        count++;
                                        if (res.hash.empty()) {
                                            res.hash = result.value().contentHash;
                                            res.path = entry.path().string();
                                        }
                                    }
                                }
                            }
                            res.documentsAdded = count;
                        } else if (std::filesystem::is_regular_file(p)) {
                            // Single file add
                            auto result = contentStore_->store(std::filesystem::path(r.path));
                            if (!result) {
                                return ErrorResponse{result.error().code, result.error().message};
                            }
                            res.hash = result.value().contentHash;
                            res.path = r.path;
                            res.size = std::filesystem::file_size(p);
                            res.documentsAdded = 1;
                        }
                    } else if (!r.content.empty() && !r.name.empty()) {
                        // Store content directly
                        std::vector<uint8_t> bytes(r.content.begin(), r.content.end());
                        // Convert vector<uint8_t> to span<const std::byte>
                        std::span<const std::byte> byteSpan(
                            reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
                        auto result = contentStore_->storeBytes(byteSpan);
                        if (!result) {
                            return ErrorResponse{result.error().code, result.error().message};
                        }
                        res.hash = result.value().contentHash;
                        res.path = r.name;
                        res.size = r.content.size();
                        res.documentsAdded = 1;
                    }

                    return res;
                }
                // Phase 3: Download handler
                else if constexpr (std::is_same_v<T, DownloadRequest>) {
                    spdlog::info("[DAEMON] Received DownloadRequest for URL: {}", r.url);

                    if (!contentStore_) {
                        spdlog::error("[DAEMON] Content store not initialized");
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Content store not initialized"};
                    }
                    if (!metadataRepo_) {
                        spdlog::error("[DAEMON] Metadata repository not initialized");
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Metadata repository not initialized"};
                    }

                    DownloadResponse res;
                    res.url = r.url;

                    try {
                        spdlog::debug("[DAEMON] Configuring storage for download");

                        // Configure storage and downloader
                        yams::downloader::StorageConfig storageConfig;
                        storageConfig.objectsDir = config_.dataDir / "objects";
                        storageConfig.stagingDir = config_.dataDir / "staging";

                        // Ensure directories exist
                        std::error_code ec;
                        std::filesystem::create_directories(storageConfig.objectsDir, ec);
                        if (ec) {
                            spdlog::error("[DAEMON] Failed to create objects directory: {}",
                                          ec.message());
                        }
                        std::filesystem::create_directories(storageConfig.stagingDir, ec);
                        if (ec) {
                            spdlog::error("[DAEMON] Failed to create staging directory: {}",
                                          ec.message());
                        }

                        spdlog::debug("[DAEMON] Storage config: objects={}, staging={}",
                                      storageConfig.objectsDir.string(),
                                      storageConfig.stagingDir.string());

                        // Initialize downloader config with defaults
                        yams::downloader::DownloaderConfig downloaderConfig;
                        downloaderConfig.defaultConcurrency = 4;
                        downloaderConfig.defaultChunkSizeBytes = 8 * 1024 * 1024; // 8MB
                        downloaderConfig.defaultTimeout = std::chrono::milliseconds(60000);
                        downloaderConfig.followRedirects = true;
                        downloaderConfig.resume = true;
                        downloaderConfig.storeOnly = true;
                        downloaderConfig.defaultChecksumAlgo = yams::downloader::HashAlgo::Sha256;

                        spdlog::debug("[DAEMON] Creating download manager");

                        // Create download manager
                        auto downloadManager =
                            yams::downloader::makeDownloadManager(storageConfig, downloaderConfig);
                        if (!downloadManager) {
                            spdlog::error("[DAEMON] Failed to create download manager");
                            res.success = false;
                            res.error = "Failed to create download manager";
                            return res;
                        }

                        spdlog::debug("[DAEMON] Creating download request for URL: {}", r.url);

                        // Create download request
                        yams::downloader::DownloadRequest downloadReq;
                        downloadReq.url = r.url;
                        downloadReq.storeOnly = true; // We only want to store in CAS

                        spdlog::info("[DAEMON] Starting download of: {}", r.url);

                        // Perform download
                        auto downloadResult = downloadManager->download(downloadReq);

                        spdlog::debug("[DAEMON] Download operation completed, checking result");

                        if (!downloadResult.ok()) {
                            spdlog::error("[DAEMON] Download failed: {}",
                                          downloadResult.error().message);
                            res.success = false;
                            res.error = downloadResult.error().message;
                            return res;
                        }

                        const auto& finalResult = downloadResult.value();

                        spdlog::debug("[DAEMON] Download result: success={}, hash={}, size={}",
                                      finalResult.success, finalResult.hash, finalResult.sizeBytes);

                        if (!finalResult.success) {
                            spdlog::error("[DAEMON] Download marked as failed: {}",
                                          finalResult.error ? finalResult.error->message
                                                            : "Unknown error");
                            res.success = false;
                            res.error =
                                finalResult.error ? finalResult.error->message : "Download failed";
                            return res;
                        }

                        // File is already stored in CAS by the downloader
                        // Ingest downloaded file into ContentStore to ensure metadata/indexing can
                        // see it
                        std::string hashStr;
                        spdlog::debug("[DAEMON] Ingesting downloaded file into ContentStore: {}",
                                      finalResult.storedPath.string());
                        auto storeRes = contentStore_->store(finalResult.storedPath);
                        if (!storeRes) {
                            spdlog::error("[DAEMON] ContentStore store() failed: {}",
                                          storeRes.error().message);
                            res.success = false;
                            res.error = std::string("ContentStore store failed: ") +
                                        storeRes.error().message;
                            return res;
                        }
                        hashStr = storeRes.value().contentHash;
                        spdlog::debug("[DAEMON] ContentStore stored hash: {}", hashStr);

                        spdlog::debug("[DAEMON] Storing metadata in database");

                        // Store metadata in database
                        metadata::DocumentInfo docInfo;

                        // Extract filename from URL
                        std::string filename = finalResult.url;
                        auto lastSlash = filename.find_last_of('/');
                        if (lastSlash != std::string::npos) {
                            filename = filename.substr(lastSlash + 1);
                        }
                        auto questionMark = filename.find('?');
                        if (questionMark != std::string::npos) {
                            filename = filename.substr(0, questionMark);
                        }
                        if (filename.empty()) {
                            filename = "downloaded_file";
                        }

                        spdlog::debug("[DAEMON] Extracted filename: {}", filename);

                        docInfo.filePath = finalResult.url; // Use URL as logical path
                        docInfo.fileName = filename;
                        docInfo.fileExtension = "";
                        auto dotPos = filename.rfind('.');
                        if (dotPos != std::string::npos) {
                            docInfo.fileExtension = filename.substr(dotPos);
                        }
                        docInfo.fileSize = static_cast<int64_t>(finalResult.sizeBytes);
                        docInfo.sha256Hash = hashStr;
                        docInfo.mimeType = "application/octet-stream"; // TODO: Detect MIME type

                        auto now = std::chrono::system_clock::now();
                        docInfo.createdTime = now;
                        docInfo.modifiedTime = now;
                        docInfo.indexedTime = now;

                        spdlog::debug("[DAEMON] Inserting document metadata: hash={}, size={}",
                                      docInfo.sha256Hash, docInfo.fileSize);

                        auto insertResult = metadataRepo_->insertDocument(docInfo);
                        if (!insertResult) {
                            if (!r.quiet) {
                                spdlog::warn("Failed to store metadata for downloaded file: {}",
                                             insertResult.error().message);
                            }
                        } else {
                            int64_t docId = insertResult.value();

                            // Add tags as metadata
                            for (const auto& tag : r.tags) {
                                metadataRepo_->setMetadata(docId, "tag",
                                                           metadata::MetadataValue(tag));
                            }

                            // Add custom metadata
                            for (const auto& [key, value] : r.metadata) {
                                metadataRepo_->setMetadata(docId, key,
                                                           metadata::MetadataValue(value));
                            }

                            // Add download-specific metadata
                            metadataRepo_->setMetadata(docId, "source_url",
                                                       metadata::MetadataValue(r.url));
                            metadataRepo_->setMetadata(
                                docId, "download_time",
                                metadata::MetadataValue(
                                    std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                                                       now.time_since_epoch())
                                                       .count())));

                            // Add HTTP metadata if available
                            if (finalResult.etag) {
                                metadataRepo_->setMetadata(
                                    docId, "etag", metadata::MetadataValue(*finalResult.etag));
                            }
                            if (finalResult.lastModified) {
                                metadataRepo_->setMetadata(
                                    docId, "last_modified",
                                    metadata::MetadataValue(*finalResult.lastModified));
                            }
                            if (finalResult.httpStatus) {
                                metadataRepo_->setMetadata(docId, "http_status",
                                                           metadata::MetadataValue(std::to_string(
                                                               *finalResult.httpStatus)));
                            }

                            // Index document content for full-text search
                            // Read the file from CAS using the content store
                            try {
                                auto contentBytes = contentStore_->retrieveBytes(hashStr);
                                if (contentBytes) {
                                    std::string fileContent(
                                        reinterpret_cast<const char*>(contentBytes.value().data()),
                                        contentBytes.value().size());

                                    auto indexResult = metadataRepo_->indexDocumentContent(
                                        docId, filename, fileContent, docInfo.mimeType);
                                    if (!indexResult && !r.quiet) {
                                        spdlog::warn(
                                            "Failed to index downloaded document content: {}",
                                            indexResult.error().message);
                                    }
                                } else if (!r.quiet) {
                                    spdlog::warn("Failed to retrieve downloaded file content for "
                                                 "indexing: {}",
                                                 contentBytes.error().message);
                                }
                            } catch (const std::exception& e) {
                                if (!r.quiet) {
                                    spdlog::warn("Failed to read downloaded file for indexing: {}",
                                                 e.what());
                                }
                            }

                            // Update fuzzy index
                            metadataRepo_->updateFuzzyIndex(docId);
                        }

                        // Set response values
                        res.success = true;
                        res.hash = hashStr;
                        res.localPath = finalResult.storedPath.string();
                        res.size = static_cast<size_t>(finalResult.sizeBytes);

                        // Log success if not quiet
                        if (!r.quiet) {
                            spdlog::info("[DAEMON] Downloaded and indexed file: {} -> {} ({})",
                                         r.url, res.hash.substr(0, 16) + "...", filename);
                        }

                        spdlog::debug("[DAEMON] Returning successful DownloadResponse");

                    } catch (const std::exception& e) {
                        spdlog::error("[DAEMON] Exception in download handler: {}", e.what());
                        res.success = false;
                        res.error = std::string("Download failed: ") + e.what();
                    } catch (...) {
                        spdlog::error("[DAEMON] Unknown exception in download handler");
                        res.success = false;
                        res.error = "Download failed: Unknown error";
                    }

                    return res;
                }
                // Phase 3: Grep handler
                else if constexpr (std::is_same_v<T, GrepRequest>) {
                    if (!searchExecutor_ || !contentStore_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Search executor not initialized"};
                    }

                    GrepResponse res;

                    // Get all documents (use % to match all)
                    auto docsResult = metadataRepo_->findDocumentsByPath("%");
                    if (!docsResult) {
                        return ErrorResponse{docsResult.error().code, docsResult.error().message};
                    }

                    // Apply path filter if specified
                    std::vector<metadata::DocumentInfo> documents;
                    for (const auto& doc : docsResult.value()) {
                        if (r.path.empty() || doc.filePath.find(r.path) != std::string::npos) {
                            documents.push_back(doc);
                        }
                    }

                    // Compile regex
                    std::regex_constants::syntax_option_type flags =
                        std::regex_constants::ECMAScript;
                    if (r.caseInsensitive) {
                        flags |= std::regex_constants::icase;
                    }

                    std::regex regex;
                    try {
                        regex = std::regex(r.pattern, flags);
                    } catch (const std::regex_error& e) {
                        return ErrorResponse{ErrorCode::InvalidArgument,
                                             "Invalid regex: " + std::string(e.what())};
                    }

                    // Search each document
                    for (const auto& doc : documents) {
                        auto contentResult = contentStore_->retrieveBytes(doc.sha256Hash);
                        if (!contentResult)
                            continue;

                        std::string content(
                            reinterpret_cast<const char*>(contentResult.value().data()),
                            contentResult.value().size());

                        // Split into lines for matching
                        std::vector<std::string> lines;
                        std::istringstream iss(content);
                        std::string line;
                        while (std::getline(iss, line)) {
                            lines.push_back(line);
                        }

                        // Find matches
                        for (size_t i = 0; i < lines.size(); ++i) {
                            bool matches = std::regex_search(lines[i], regex);
                            if (matches != r.invertMatch) {
                                GrepMatch match;
                                match.file = doc.filePath;
                                match.lineNumber = i + 1;
                                match.line = lines[i];

                                // Add context if requested
                                if (r.contextLines > 0) {
                                    for (int j = std::max(0, static_cast<int>(i) - r.contextLines);
                                         j < static_cast<int>(i); ++j) {
                                        match.contextBefore.push_back(lines[j]);
                                    }
                                    for (size_t j = i + 1;
                                         j < std::min(lines.size(), i + 1 + r.contextLines); ++j) {
                                        match.contextAfter.push_back(lines[j]);
                                    }
                                }

                                res.matches.push_back(match);
                                res.totalMatches++;

                                if (r.maxMatches > 0 && res.matches.size() >= r.maxMatches) {
                                    break;
                                }
                            }
                        }
                        res.filesSearched++;
                    }

                    return res;
                }
                // Phase 3: Update document handler
                else if constexpr (std::is_same_v<T, UpdateDocumentRequest>) {
                    if (!metadataRepo_ || !contentStore_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Repository not initialized"};
                    }

                    UpdateDocumentResponse res;

                    // Find document by hash or name
                    std::string hash = r.hash;
                    if (hash.empty() && !r.name.empty()) {
                        auto docs = metadataRepo_->findDocumentsByPath(r.name);
                        if (docs && !docs.value().empty()) {
                            hash = docs.value()[0].sha256Hash;
                        }
                    }

                    if (hash.empty()) {
                        return ErrorResponse{ErrorCode::NotFound, "Document not found"};
                    }

                    res.hash = hash;

                    // Update content if provided
                    if (!r.newContent.empty()) {
                        std::vector<uint8_t> bytes(r.newContent.begin(), r.newContent.end());
                        // Convert vector<uint8_t> to span<const std::byte>
                        std::span<const std::byte> byteSpan(
                            reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
                        auto storeResult = contentStore_->storeBytes(byteSpan);
                        if (storeResult) {
                            res.hash = storeResult.value().contentHash;
                            res.contentUpdated = true;
                        }
                    }

                    // Update metadata
                    if (!r.metadata.empty()) {
                        for (const auto& [key, value] : r.metadata) {
                            // Get document ID first
                            auto docResult = metadataRepo_->getDocumentByHash(hash);
                            if (docResult && docResult.value()) {
                                metadataRepo_->setMetadata(docResult.value()->id, key,
                                                           metadata::MetadataValue(value));
                            }
                        }
                        res.metadataUpdated = true;
                    }

                    // Update tags
                    if (!r.addTags.empty() || !r.removeTags.empty()) {
                        // Get document ID first for tags
                        auto docResult = metadataRepo_->getDocumentByHash(hash);
                        if (!docResult || !docResult.value()) {
                            return ErrorResponse{ErrorCode::NotFound, "Document not found"};
                        }
                        int64_t docId = docResult.value()->id;
                        auto currentTags = metadataRepo_->getDocumentTags(docId);
                        std::set<std::string> tags;
                        if (currentTags) {
                            tags.insert(currentTags.value().begin(), currentTags.value().end());
                        }

                        for (const auto& tag : r.addTags) {
                            tags.insert(tag);
                        }
                        for (const auto& tag : r.removeTags) {
                            tags.erase(tag);
                        }

                        std::vector<std::string> newTags(tags.begin(), tags.end());
                        // Tags are managed through metadata, set each tag
                        for (const auto& tag : newTags) {
                            metadataRepo_->setMetadata(docId, "tag", metadata::MetadataValue(tag));
                        }
                        res.tagsUpdated = true;
                    }

                    return res;
                }
                // Phase 3: Stats handler
                else if constexpr (std::is_same_v<T, GetStatsRequest>) {
                    if (!metadataRepo_) {
                        return ErrorResponse{ErrorCode::NotInitialized,
                                             "Repository not initialized"};
                    }

                    GetStatsResponse res;

                    // Get basic stats
                    auto docCount = metadataRepo_->getDocumentCount();
                    if (docCount) {
                        res.totalDocuments = static_cast<size_t>(docCount.value());
                        res.indexedDocuments = static_cast<size_t>(docCount.value());
                    }

                    // Get content store stats if available
                    if (contentStore_) {
                        auto storeStats = contentStore_->getStats();
                        res.totalSize = storeStats.totalBytes;
                        res.compressionRatio = storeStats.dedupRatio();
                    }

                    // Get detailed breakdown if requested
                    if (r.detailed) {
                        auto typeStats = metadataRepo_->getDocumentCountsByExtension();
                        if (typeStats) {
                            for (const auto& [ext, count] : typeStats.value()) {
                                res.documentsByType[ext] = static_cast<size_t>(count);
                            }
                        }
                    }

                    // Add cache stats if requested
                    if (r.includeCache) {
                        // Add cache-specific statistics
                        res.additionalStats["cache_hits"] = "0"; // TODO: Track actual cache hits
                        res.additionalStats["cache_misses"] = "0";
                        res.additionalStats["cache_size"] = "0";
                    }

                    // Calculate compression ratio if available
                    if (res.totalSize > 0 && contentStore_) {
                        // Compression ratio already set from content store stats above
                    }

                    return res;
                } else {
                    spdlog::warn("Received unsupported request type");
                    return ErrorResponse{ErrorCode::NotImplemented,
                                         "Request type not yet implemented"};
                }
            },
            req);
    });

    if (auto result = ipcServer_->start(); !result) {
        removePidFile();
        running_ = false;
        return result;
    }

    // Model preloading is now handled by the model provider
    // The provider will lazy-load models as needed

    // Start daemon thread
    daemonThread_ = std::jthread([this](std::stop_token token) { run(token); });

    spdlog::info("YAMS daemon started successfully (PID: {})", getpid());
    return Result<void>();
}

Result<void> YamsDaemon::stop() {
    if (!running_.exchange(false)) {
        return Error{ErrorCode::InvalidState, "Daemon not running"};
    }

    spdlog::info("Stopping YAMS daemon...");

    stopRequested_ = true;

    // Notify the daemon thread to wake up
    stop_cv_.notify_all();

    // Stop IPC server
    if (ipcServer_) {
        ipcServer_->stop();
    }

    // Model provider handles its own resource management

    // Stop daemon thread
    if (daemonThread_.joinable()) {
        spdlog::debug("Requesting daemon thread stop...");
        daemonThread_.request_stop();
        spdlog::debug("Waiting for daemon thread to join...");
        daemonThread_.join();
        spdlog::debug("Daemon thread joined");
    }

    // Cleanup resources
    shutdownResources();
    cleanupSignalHandlers();
    removePidFile();

    spdlog::info("YAMS daemon stopped");
    return Result<void>();
}

void YamsDaemon::run(std::stop_token stopToken) {
    spdlog::debug("Daemon main loop started");

    while (!stopToken.stop_requested() && !stopRequested_.load()) {
        // Main daemon loop
        // TODO: Add periodic tasks (cleanup, health checks, etc.)
        // Use interruptible sleep that responds to stop requests
        std::unique_lock<std::mutex> lock(stop_mutex_);
        bool stop = stop_cv_.wait_for(lock, std::chrono::seconds(1), [&]() {
            return stopToken.stop_requested() || stopRequested_.load();
        });
        if (stop) {
            spdlog::debug("Daemon main loop received stop signal");
            break;
        }
    }

    spdlog::debug("Daemon main loop exiting (stopToken={}, stopRequested={})",
                  stopToken.stop_requested(), stopRequested_.load());
}

Result<void> YamsDaemon::createPidFile() {
    // Use O_CREAT | O_EXCL for atomic creation - fails if file exists
    int fd = open(pidFile_.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
    if (fd < 0) {
        if (errno == EEXIST) {
            // File exists, check if daemon is still running
            if (isDaemonRunning()) {
                return Error{ErrorCode::InvalidState, "Daemon already running (PID file exists)"};
            }
            // Stale PID file, remove and retry once
            spdlog::info("Removing stale PID file: {}", pidFile_.string());
            std::filesystem::remove(pidFile_);

            fd = open(pidFile_.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
            if (fd < 0) {
                return Error{ErrorCode::WriteError,
                             "Failed to create PID file after removing stale: " +
                                 std::string(strerror(errno))};
            }
        } else {
            return Error{ErrorCode::WriteError,
                         "Failed to create PID file: " + std::string(strerror(errno))};
        }
    }

    // Write PID to file
    std::string pid_str = std::to_string(getpid()) + "\n";
    if (write(fd, pid_str.c_str(), pid_str.size()) != static_cast<ssize_t>(pid_str.size())) {
        close(fd);
        std::filesystem::remove(pidFile_);
        return Error{ErrorCode::WriteError, "Failed to write PID to file"};
    }

    close(fd);
    spdlog::debug("Created PID file: {} with PID: {}", pidFile_.string(), getpid());
    return Result<void>();
}

Result<void> YamsDaemon::removePidFile() {
    if (std::filesystem::exists(pidFile_)) {
        std::error_code ec;
        std::filesystem::remove(pidFile_, ec);
        if (ec) {
            return Error{ErrorCode::WriteError, "Failed to remove PID file: " + ec.message()};
        }
        spdlog::debug("Removed PID file: {}", pidFile_.string());
    }
    return Result<void>();
}

bool YamsDaemon::isDaemonRunning() const {
    using namespace std;
    namespace fs = std::filesystem;
    // If no PID file, assume not running
    if (!fs::exists(pidFile_)) {
        return false;
    }

    // Parse PID
    ifstream pidFile(pidFile_);
    if (!pidFile) {
        return false;
    }
    pid_t pid = 0;
    pidFile >> pid;
    if (pid <= 0) {
        // Corrupt PID file
        std::error_code ec;
        fs::remove(pidFile_, ec);
        return false;
    }

    // Check if process exists
    if (kill(pid, 0) != 0) {
        // Stale PID file, remove it
        std::error_code ec;
        fs::remove(pidFile_, ec);
        return false;
    }

    // Extra verification: try connecting to the configured socket
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return true; // Process exists; assume running
    }
    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, config_.socketPath.c_str(), sizeof(addr.sun_path) - 1);
    bool connect_ok = (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
    close(fd);

    if (!connect_ok) {
        // Socket not available; treat as stale and remove PID file
        std::error_code ec;
        fs::remove(pidFile_, ec);
        return false;
    }
    return true;
}

void YamsDaemon::setupSignalHandlers() {
    instance_ = this;

    std::signal(SIGTERM, &YamsDaemon::signalHandler);
    std::signal(SIGINT, &YamsDaemon::signalHandler);
    std::signal(SIGHUP, &YamsDaemon::signalHandler);

    spdlog::debug("Signal handlers installed");
}

void YamsDaemon::cleanupSignalHandlers() {
    std::signal(SIGTERM, SIG_DFL);
    std::signal(SIGINT, SIG_DFL);
    std::signal(SIGHUP, SIG_DFL);

    instance_ = nullptr;
}

void YamsDaemon::signalHandler(int signal) {
    if (instance_) {
        instance_->handleSignal(signal);
    }
}

void YamsDaemon::handleSignal(int signal) {
    switch (signal) {
        case SIGTERM:
        case SIGINT:
            spdlog::info("Received signal {}, initiating shutdown", signal);
            stopRequested_ = true;
            break;

        case SIGHUP:
            spdlog::info("Received SIGHUP, reloading configuration");
            // TODO: Implement configuration reload
            break;

        default:
            spdlog::warn("Received unexpected signal: {}", signal);
            break;
    }
}

Result<void> YamsDaemon::initializeResources() {
    spdlog::debug("Initializing daemon resources");

    // Initialize model provider based on configuration
    if (config_.enableModelProvider) {
        try {
            spdlog::info("Initializing model provider with config: max_models={}, preload_count={}",
                         config_.modelPoolConfig.maxLoadedModels,
                         config_.modelPoolConfig.preloadModels.size());

            modelProvider_ = createModelProvider(config_.modelPoolConfig);
            if (modelProvider_) {
                spdlog::info("Initialized {} model provider", modelProvider_->getProviderName());

                // Defer model preloading until after storage/DB/search init
                spdlog::debug("Deferring model preloading until core resources are ready");
            } else {
                spdlog::warn("Model provider creation returned nullptr");
            }
        } catch (const std::exception& e) {
            spdlog::warn("Model provider initialization failed: {} - features disabled", e.what());
            modelProvider_ = nullptr;
        }
    } else {
        spdlog::debug("Model provider disabled in configuration");
        modelProvider_ = nullptr;
    }

    // Initialize database/storage and search primitives
    try {
        namespace fs = std::filesystem;
        // Resolve data dir: prefer config.dataDir, then YAMS_STORAGE env, then defaults
        fs::path dataDir = config_.dataDir;
        if (dataDir.empty()) {
            if (const char* storageEnv = std::getenv("YAMS_STORAGE")) {
                dataDir = fs::path(storageEnv);
            }
        }
        if (dataDir.empty()) {
            if (const char* xdgDataHome = std::getenv("XDG_DATA_HOME")) {
                dataDir = fs::path(xdgDataHome) / "yams";
            } else if (const char* homeEnv = std::getenv("HOME")) {
                dataDir = fs::path(homeEnv) / ".local" / "share" / "yams";
            } else {
                dataDir = fs::path(".") / "yams_data";
            }
        }
        std::error_code ec;
        fs::create_directories(dataDir, ec);
        if (ec) {
            spdlog::warn("Failed to create data dir {}: {}", dataDir.string(), ec.message());
        }

        // Initialize content store under dataDir/storage
        try {
            auto storeRes = yams::api::ContentStoreBuilder::createDefault(dataDir / "storage");
            if (!storeRes) {
                spdlog::warn("Content store init failed: {}", storeRes.error().message);
            } else {
                auto uniqueStore = std::move(
                    const_cast<std::unique_ptr<yams::api::IContentStore>&>(storeRes.value()));
                contentStore_ = std::shared_ptr<yams::api::IContentStore>(uniqueStore.release());
            }
        } catch (const std::exception& e) {
            spdlog::warn("Content store init exception: {}", e.what());
        }

        auto dbPath = dataDir / "yams.db";
        database_ = std::make_shared<metadata::Database>();
        auto dbRes = database_->open(dbPath.string(), metadata::ConnectionMode::Create);
        if (!dbRes) {
            spdlog::warn("Daemon DB open failed: {}", dbRes.error().message);
        } else {
            // Run migrations
            metadata::MigrationManager mm(*database_);
            auto initRes = mm.initialize();
            if (!initRes) {
                spdlog::warn("Migration init failed: {}", initRes.error().message);
            } else {
                mm.registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());
                auto migRes = mm.migrate();
                if (!migRes) {
                    spdlog::warn("Migrations failed: {}", migRes.error().message);
                }
            }

            // Connection pool + repo
            metadata::ConnectionPoolConfig poolCfg;
            poolCfg.maxConnections = 8;
            poolCfg.minConnections = 1;
            poolCfg.connectTimeout = std::chrono::seconds(10);
            connectionPool_ = std::make_shared<metadata::ConnectionPool>(dbPath.string(), poolCfg);
            auto poolInit = connectionPool_->initialize();
            if (!poolInit) {
                spdlog::warn("ConnectionPool init failed: {}", poolInit.error().message);
            } else {
                metadataRepo_ = std::make_shared<metadata::MetadataRepository>(*connectionPool_);
                // SearchExecutor (keyword/FTS fallback)
                searchExecutor_ =
                    std::make_shared<search::SearchExecutor>(database_, metadataRepo_);
            }

            // Initialize hybrid search resources
            // Vector index manager (persistent between requests)
            vectorIndexManager_ = std::make_shared<yams::vector::VectorIndexManager>();
            // Configure persistence path under dataDir if not set
            if (vectorIndexManager_) {
                try {
                    auto cfg = vectorIndexManager_->getConfig();
                    if (cfg.enable_persistence && cfg.index_path.empty() &&
                        !config_.dataDir.empty()) {
                        auto indexPath = (config_.dataDir / "vector_index.bin").string();
                        spdlog::info("Setting vector index persistence path to '{}'", indexPath);
                        cfg.index_path = indexPath;
                        vectorIndexManager_->setConfig(cfg);
                    }
                    // Load persisted index when enabled and path configured
                    cfg = vectorIndexManager_->getConfig();
                    if (cfg.enable_persistence && !cfg.index_path.empty()) {
                        spdlog::info("Loading vector index from '{}'", cfg.index_path);
                        auto loadRes = vectorIndexManager_->loadIndex(cfg.index_path);
                        if (!loadRes) {
                            spdlog::warn("Failed to load vector index '{}': {}", cfg.index_path,
                                         vectorIndexManager_->getLastError());
                        } else {
                            spdlog::info("Vector index loaded successfully");
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Vector index load exception: {}", e.what());
                }
            }

            // Embedding generator (if model provider is available, ensure models will be ready)
            // For now, construct a default generator that can be replaced by provider-managed pool.
            embeddingGenerator_ = std::shared_ptr<yams::vector::EmbeddingGenerator>();
            // Knowledge graph store (SQLite-backed using existing pool)
            if (connectionPool_) {
                auto kgRes = yams::metadata::makeSqliteKnowledgeGraphStore(*connectionPool_);
                if (!kgRes) {
                    spdlog::warn("KnowledgeGraphStore init failed: {}", kgRes.error().message);
                } else {
                    knowledgeGraphStore_ = std::move(kgRes).value();
                }
            }

            // Optionally preload models after core init
            if (modelProvider_ && std::getenv("YAMS_TEST_MODE") == nullptr &&
                !config_.modelPoolConfig.preloadModels.empty()) {
                spdlog::info("Preloading {} configured models after core init...",
                             config_.modelPoolConfig.preloadModels.size());
                preloadModels();
            }

            // Prepare a reusable builder
            searchBuilder_ = std::make_shared<yams::search::SearchEngineBuilder>();
        }
    } catch (const std::exception& e) {
        spdlog::warn("Daemon search init exception: {}", e.what());
    }

    // Readiness log for core daemon resources
    spdlog::info("Daemon resources ready (store: {}, db: {}, repo: {}, search: {}, models: {})",
                 contentStore_ ? "yes" : "no", database_ ? "yes" : "no",
                 metadataRepo_ ? "yes" : "no", searchExecutor_ ? "yes" : "no",
                 modelProvider_ ? "yes" : "no");

    // Initialize retrieval session manager
    retrievalSessions_ = std::make_unique<RetrievalSessionManager>();

    return Result<void>();
}

std::shared_ptr<yams::vector::EmbeddingGenerator> YamsDaemon::ensureEmbeddingGenerator() noexcept {
    // Fast path: already created
    if (embeddingGenerator_) {
        return embeddingGenerator_;
    }

    try {
        std::scoped_lock lock(embeddingMutex_);
        if (embeddingGenerator_) {
            return embeddingGenerator_;
        }

        // Prefer provider-backed generator when model provider is available
        if (modelProvider_ && modelProvider_->isAvailable()) {
            yams::vector::EmbeddingConfig cfg{};
            cfg.backend =
                yams::vector::EmbeddingConfig::Backend::Daemon; // indicate provider-backed
            cfg.normalize_embeddings = true;
            cfg.num_threads = std::max(1, config_.modelPoolConfig.numThreads);
            // Resolve model name from config (preload list) or default
            if (!config_.modelPoolConfig.preloadModels.empty()) {
                cfg.model_name = config_.modelPoolConfig.preloadModels.front();
            }

            // Ensure at least one model is loaded in provider
            if (!modelProvider_->isModelLoaded(cfg.model_name)) {
                auto load = modelProvider_->loadModel(cfg.model_name);
                if (!load) {
                    spdlog::warn("Model provider failed to load '{}': {}", cfg.model_name,
                                 load.error().message);
                }
            }

            // Create a generator; backend Hybrid/Daemon path will call provider through Daemon
            // backend
            auto gen = yams::vector::createEmbeddingGenerator(cfg);
            if (gen && gen->initialize()) {
                embeddingGenerator_.reset(gen.release());
                spdlog::info("Daemon embedding generator initialized (backend=Daemon, model='{}')",
                             cfg.model_name);
                return embeddingGenerator_;
            }

            spdlog::warn("Provider-backed embedding generator initialization failed; attempting "
                         "local fallback");
        }

        // Local ONNX fallback generator
        yams::vector::EmbeddingConfig cfg{};
        cfg.backend = yams::vector::EmbeddingConfig::Backend::Local;
        if (!config_.dataDir.empty()) {
            const auto modelsDir = (config_.dataDir / "models").string();
            cfg.model_path = modelsDir + "/all-MiniLM-L6-v2.onnx";
            cfg.tokenizer_path = modelsDir + "/tokenizer.json";
        }
        cfg.normalize_embeddings = true;
        cfg.num_threads = std::max(1, config_.modelPoolConfig.numThreads);

        auto gen = yams::vector::createEmbeddingGenerator(cfg);
        if (gen && gen->initialize()) {
            embeddingGenerator_.reset(gen.release());
            spdlog::info("Daemon embedding generator initialized (backend=Local, model='{}')",
                         cfg.model_name);
            return embeddingGenerator_;
        }

        spdlog::warn(
            "Failed to initialize any embedding generator; hybrid search will omit vector scoring");
        return nullptr;
    } catch (const std::exception& e) {
        spdlog::error("ensureEmbeddingGenerator() failed: {}", e.what());
        return nullptr;
    }
}

void YamsDaemon::preloadModels() {
    // Preload models configured in daemon.models.preload_models
    const auto& modelsToPreload = config_.modelPoolConfig.preloadModels;

    if (modelsToPreload.empty()) {
        spdlog::info("No models configured for preloading");
        return;
    }

    if (!modelProvider_) {
        spdlog::warn("Cannot preload models - model provider not initialized");
        return;
    }

    spdlog::info("Starting preload of {} models...", modelsToPreload.size());
    size_t successCount = 0;

    for (size_t i = 0; i < modelsToPreload.size(); ++i) {
        const auto& modelName = modelsToPreload[i];
        spdlog::info("[{}/{}] Preloading model: {}...", i + 1, modelsToPreload.size(), modelName);

        auto startTime = std::chrono::steady_clock::now();
        auto result = modelProvider_->loadModel(modelName);
        auto loadTime = std::chrono::steady_clock::now() - startTime;

        if (result) {
            auto loadMs = std::chrono::duration_cast<std::chrono::milliseconds>(loadTime).count();
            spdlog::info("[{}/{}] Successfully preloaded model '{}' in {}ms", i + 1,
                         modelsToPreload.size(), modelName, loadMs);
            successCount++;
        } else {
            spdlog::warn("[{}/{}] Failed to preload model '{}': {}", i + 1, modelsToPreload.size(),
                         modelName, result.error().message);
        }
    }

    spdlog::info("Model preloading complete: {}/{} models loaded successfully", successCount,
                 modelsToPreload.size());
}

void YamsDaemon::shutdownResources() {
    spdlog::debug("Shutting down daemon resources");

    // Persist vector index when enabled
    if (vectorIndexManager_) {
        try {
            auto cfg = vectorIndexManager_->getConfig();
            if (cfg.enable_persistence && !cfg.index_path.empty()) {
                spdlog::info("Saving vector index to '{}'", cfg.index_path);
                auto saveRes = vectorIndexManager_->saveIndex(cfg.index_path);
                if (!saveRes) {
                    spdlog::warn("Failed to save vector index '{}': {}", cfg.index_path,
                                 vectorIndexManager_->getLastError());
                } else {
                    spdlog::info("Vector index saved successfully");
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("Vector index save exception: {}", e.what());
        }
    }

    // Shutdown embedding generator (if any)
    if (embeddingGenerator_) {
        embeddingGenerator_->shutdown();
        embeddingGenerator_.reset();
    }

    // Shutdown model provider
    if (modelProvider_) {
        modelProvider_->shutdown();
        modelProvider_.reset();
    }

    // Release hybrid resources
    knowledgeGraphStore_.reset();
    vectorIndexManager_.reset();
    searchBuilder_.reset();
}

// Path resolution helpers
std::filesystem::path YamsDaemon::resolveSystemPath(PathType type) {
    namespace fs = std::filesystem;

    // Check if running as root
    bool isRoot = (geteuid() == 0);

    // Get user ID for user-specific paths
    uid_t uid = getuid();

    switch (type) {
        case PathType::Socket: {
            if (isRoot) {
                return fs::path("/var/run/yams-daemon.sock");
            }

            // Try XDG_RUNTIME_DIR first
            auto xdgRuntime = getXDGRuntimeDir();
            if (!xdgRuntime.empty() && canWriteToDirectory(xdgRuntime)) {
                return xdgRuntime / "yams-daemon.sock";
            }

            // Fall back to /tmp with user ID
            return fs::path("/tmp") / ("yams-daemon-" + std::to_string(uid) + ".sock");
        }

        case PathType::PidFile: {
            if (isRoot) {
                return fs::path("/var/run/yams-daemon.pid");
            }

            // Try XDG_RUNTIME_DIR first
            auto xdgRuntime = getXDGRuntimeDir();
            if (!xdgRuntime.empty() && canWriteToDirectory(xdgRuntime)) {
                return xdgRuntime / "yams-daemon.pid";
            }

            // Fall back to /tmp with user ID
            return fs::path("/tmp") / ("yams-daemon-" + std::to_string(uid) + ".pid");
        }

        case PathType::LogFile: {
            if (isRoot && canWriteToDirectory("/var/log")) {
                return fs::path("/var/log/yams-daemon.log");
            }

            // Try XDG_STATE_HOME for user logs
            auto xdgState = getXDGStateHome();
            if (!xdgState.empty()) {
                auto logDir = xdgState / "yams";
                std::error_code ec;
                fs::create_directories(logDir, ec);
                if (!ec && canWriteToDirectory(logDir)) {
                    return logDir / "daemon.log";
                }
            }

            // Fall back to /tmp with user ID
            return fs::path("/tmp") / ("yams-daemon-" + std::to_string(uid) + ".log");
        }
    }

    return fs::path(); // Should never reach here
}

bool YamsDaemon::canWriteToDirectory(const std::filesystem::path& dir) {
    namespace fs = std::filesystem;

    if (!fs::exists(dir)) {
        return false;
    }

    // Try to create a temporary file to test write permissions
    auto testFile = dir / (".yams-test-" + std::to_string(getpid()));
    std::ofstream test(testFile);
    if (test.good()) {
        test.close();
        fs::remove(testFile);
        return true;
    }

    return false;
}

std::filesystem::path YamsDaemon::getXDGRuntimeDir() {
    const char* xdgRuntime = std::getenv("XDG_RUNTIME_DIR");
    if (xdgRuntime) {
        return std::filesystem::path(xdgRuntime);
    }
    return std::filesystem::path();
}

std::filesystem::path YamsDaemon::getXDGStateHome() {
    const char* xdgState = std::getenv("XDG_STATE_HOME");
    if (xdgState) {
        return std::filesystem::path(xdgState);
    }

    // Default to ~/.local/state
    const char* home = std::getenv("HOME");
    if (home) {
        return std::filesystem::path(home) / ".local" / "state";
    }

    return std::filesystem::path();
}

// Helper functions for system stats (platform-specific implementations needed)
double getMemoryUsage() {
    // TODO: Implement platform-specific memory usage retrieval
    return 0.0;
}

double getCpuUsage() {
    // TODO: Implement platform-specific CPU usage retrieval
    return 0.0;
}

} // namespace yams::daemon
