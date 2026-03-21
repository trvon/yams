#include <spdlog/spdlog.h>
#include <chrono>
#include <filesystem>
#include <yams/app/services/services.hpp>
#include <yams/detection/file_type_detector.h>
#include <yams/downloader/downloader.hpp>
#include <yams/extraction/extraction_util.h>

namespace fs = std::filesystem;

namespace yams::app::services {

namespace {
constexpr size_t kMaxIndexedDownloadTextBytes = 16 * 1024 * 1024;
}

class DownloadServiceImpl : public IDownloadService {
public:
    explicit DownloadServiceImpl(const AppContext& ctx) : ctx_(ctx) {
        // Initialize download manager once with proper config
        // Get storage path from content store or environment
        fs::path storagePath;
        if (const char* storageEnv = std::getenv("YAMS_STORAGE")) {
            storagePath = fs::path(storageEnv);
        } else if (const char* homeEnv = std::getenv("HOME")) {
            storagePath = fs::path(homeEnv) / "yams";
        } else {
            storagePath = fs::current_path() / "yams";
        }

        // Set up storage paths for downloader (CAS paths)
        storageCfg_.objectsDir = storagePath / "storage" / "objects";
        storageCfg_.stagingDir = storagePath / "storage" / "staging";

        // Ensure directories exist
        fs::create_directories(storageCfg_.objectsDir);
        fs::create_directories(storageCfg_.stagingDir);

        spdlog::debug("DownloadService initialized with storage path: {}", storagePath.string());
        spdlog::debug("Objects directory: {}", storageCfg_.objectsDir.string());
        spdlog::debug("Staging directory: {}", storageCfg_.stagingDir.string());

        dlCfg_.defaultConcurrency = 4;
        dlCfg_.defaultChunkSizeBytes = 8 * 1024 * 1024;
        dlCfg_.defaultTimeout = std::chrono::milliseconds(60000);
        dlCfg_.followRedirects = true;
        dlCfg_.resume = true;
        dlCfg_.storeOnly = true;

        // Create and cache the manager
        manager_ = downloader::makeDownloadManager(storageCfg_, dlCfg_);
        if (!manager_) {
            spdlog::error("DownloadService: Failed to create download manager");
            throw std::runtime_error("Failed to initialize download manager");
        }
        spdlog::info("DownloadService: Download manager initialized successfully");
    }

    Result<DownloadServiceResponse> download(const DownloadServiceRequest& req) override {
        try {
            spdlog::info("DownloadService: Starting download of URL: {}", req.url);

            // Convert app service request to downloader request
            downloader::DownloadRequest downloaderReq;
            downloaderReq.url = req.url;
            downloaderReq.headers = req.headers;
            downloaderReq.checksum = req.checksum;
            downloaderReq.concurrency = req.concurrency;
            downloaderReq.chunkSizeBytes = req.chunkSizeBytes;
            downloaderReq.timeout = req.timeout;
            downloaderReq.retry = req.retry;
            downloaderReq.rateLimit = req.rateLimit;
            downloaderReq.resume = req.resume;
            downloaderReq.proxy = req.proxy;
            downloaderReq.tls = req.tls;
            downloaderReq.followRedirects = req.followRedirects;
            downloaderReq.storeOnly = req.storeOnly;
            if (req.exportPath) {
                downloaderReq.exportPath = std::filesystem::path(*req.exportPath);
            }
            downloaderReq.overwrite = req.overwrite;

            // Set up callbacks
            auto progressCb = req.progressCallback ? req.progressCallback
                                                   : [](const downloader::ProgressEvent&) {};
            auto shouldCancel = req.shouldCancel ? req.shouldCancel : []() { return false; };
            auto logCb = [](std::string_view) { /* ignore for now */ };

            // Execute download using cached manager
            spdlog::debug("DownloadService: Executing download with manager");
            auto result = manager_->download(downloaderReq, progressCb, shouldCancel, logCb);
            if (!result.ok()) {
                spdlog::error("DownloadService: Download failed: {}", result.error().message);
                if (result.error().code == downloader::ErrorCode::OperationCancelled) {
                    return Error{ErrorCode::OperationCancelled, result.error().message};
                }
                return Error{ErrorCode::NetworkError, result.error().message};
            }

            // Convert downloader result to service response
            const auto& finalResult = result.value();
            spdlog::info("DownloadService: Download completed successfully. DownloadedHash: {}, "
                         "Size: {} bytes",
                         finalResult.hash, finalResult.sizeBytes);
            DownloadServiceResponse response;
            // Ingest into ContentStore and index metadata/tags for MCP/CLI parity
            std::optional<std::string> ingestedHash;
            try {
                if (ctx_.store) {
                    auto storeRes = ctx_.store->store(finalResult.storedPath);
                    if (!storeRes) {
                        spdlog::warn("DownloadService: ContentStore store() failed: {}",
                                     storeRes.error().message);
                    } else if (ctx_.metadataRepo) {
                        ingestedHash = storeRes.value().contentHash;
                        metadata::DocumentInfo docInfo;

                        // Derive friendly filename/indexName (server-suggested or URL basename)
                        std::string filename;
                        if (finalResult.suggestedName && !finalResult.suggestedName->empty()) {
                            filename = *finalResult.suggestedName;
                        } else {
                            filename = finalResult.url;
                            auto lastSlash = filename.find_last_of('/');
                            if (lastSlash != std::string::npos) {
                                filename = filename.substr(lastSlash + 1);
                            }
                            auto questionMark = filename.find('?');
                            if (questionMark != std::string::npos) {
                                filename.resize(questionMark);
                            }
                            if (filename.empty()) {
                                filename = "downloaded_file";
                            }
                        }

                        // Use the filename as fileName; store actual CAS path in filePath
                        docInfo.filePath = finalResult.storedPath.string();
                        docInfo.fileName = filename;
                        docInfo.fileExtension = "";
                        auto dotPos = filename.rfind('.');
                        if (dotPos != std::string::npos) {
                            docInfo.fileExtension = filename.substr(dotPos);
                        }
                        docInfo.fileSize = static_cast<int64_t>(finalResult.sizeBytes);
                        docInfo.sha256Hash = storeRes.value().contentHash;
                        docInfo.mimeType = finalResult.contentType.value_or("");
                        try {
                            (void)detection::FileTypeDetector::initializeWithMagicNumbers();
                            auto& detector = detection::FileTypeDetector::instance();
                            if (auto sig = detector.detectFromFile(finalResult.storedPath)) {
                                if (!sig.value().mimeType.empty())
                                    docInfo.mimeType = sig.value().mimeType;
                            }
                            if ((docInfo.mimeType.empty() ||
                                 docInfo.mimeType == "application/octet-stream") &&
                                !docInfo.fileExtension.empty()) {
                                docInfo.mimeType =
                                    detection::FileTypeDetector::getMimeTypeFromExtension(
                                        docInfo.fileExtension);
                            }
                        } catch (...) {
                        }
                        if (docInfo.mimeType.empty()) {
                            docInfo.mimeType = "application/octet-stream";
                        }
                        using std::chrono::floor;
                        using namespace std::chrono;
                        auto now = std::chrono::system_clock::now();
                        auto now_s = floor<seconds>(now);
                        docInfo.createdTime = now_s;
                        docInfo.modifiedTime = now_s;
                        docInfo.indexedTime = now_s;

                        auto ins = ctx_.metadataRepo->insertDocument(docInfo);
                        if (ins) {
                            const int64_t docId = ins.value();

                            // Core provenance metadata
                            ctx_.metadataRepo->setMetadata(
                                docId, "source_url", metadata::MetadataValue(finalResult.url));
                            if (finalResult.etag) {
                                ctx_.metadataRepo->setMetadata(
                                    docId, "etag", metadata::MetadataValue(*finalResult.etag));
                            }
                            if (finalResult.lastModified) {
                                ctx_.metadataRepo->setMetadata(
                                    docId, "last_modified",
                                    metadata::MetadataValue(*finalResult.lastModified));
                            }
                            // Surface content type and suggested filename when available
                            if (finalResult.contentType) {
                                ctx_.metadataRepo->setMetadata(
                                    docId, "content_type",
                                    metadata::MetadataValue(*finalResult.contentType));
                            }
                            if (finalResult.suggestedName) {
                                ctx_.metadataRepo->setMetadata(
                                    docId, "suggested_name",
                                    metadata::MetadataValue(*finalResult.suggestedName));
                            }
                            if (finalResult.httpStatus) {
                                ctx_.metadataRepo->setMetadata(
                                    docId, "http_status",
                                    metadata::MetadataValue(
                                        std::to_string(*finalResult.httpStatus)));
                            }
                            if (finalResult.checksumOk) {
                                ctx_.metadataRepo->setMetadata(
                                    docId, "checksum_ok",
                                    metadata::MetadataValue(*finalResult.checksumOk ? "true"
                                                                                    : "false"));
                            }

                            // Apply user-provided tags and metadata first
                            for (const auto& t : req.tags) {
                                if (!t.empty())
                                    ctx_.metadataRepo->setMetadata(docId, "tag",
                                                                   metadata::MetadataValue(t));
                            }
                            for (const auto& [k, v] : req.metadata) {
                                if (!k.empty())
                                    ctx_.metadataRepo->setMetadata(docId, k,
                                                                   metadata::MetadataValue(v));
                            }

                            // Derive and index helper tags for discoverability
                            // downloaded, host:..., scheme:...
                            ctx_.metadataRepo->setMetadata(docId, "tag",
                                                           metadata::MetadataValue("downloaded"));
                            // parse host/scheme from URL
                            try {
                                std::string scheme, host;
                                const std::string& u = finalResult.url;
                                auto pos = u.find("://");
                                if (pos != std::string::npos) {
                                    scheme = u.substr(0, pos);
                                    auto rest = u.substr(pos + 3);
                                    auto slash = rest.find('/');
                                    host =
                                        (slash == std::string::npos) ? rest : rest.substr(0, slash);
                                }
                                if (!host.empty()) {
                                    ctx_.metadataRepo->setMetadata(
                                        docId, "tag", metadata::MetadataValue("host:" + host));
                                }
                                if (!scheme.empty()) {
                                    ctx_.metadataRepo->setMetadata(
                                        docId, "tag", metadata::MetadataValue("scheme:" + scheme));
                                }
                                if (finalResult.httpStatus) {
                                    int code = *finalResult.httpStatus;
                                    std::string bucket = (code >= 200 && code < 300)   ? "2xx"
                                                         : (code >= 400 && code < 500) ? "4xx"
                                                                                       : "5xx";
                                    ctx_.metadataRepo->setMetadata(
                                        docId, "tag", metadata::MetadataValue("status:" + bucket));
                                }
                            } catch (...) {
                                // best-effort tag derivation
                            }

                            auto extractedText = extraction::util::extractDocumentText(
                                ctx_.store, storeRes.value().contentHash, docInfo.mimeType,
                                docInfo.fileExtension, ctx_.contentExtractors);
                            if (extractedText && !extractedText->empty()) {
                                metadata::DocumentContent content;
                                content.documentId = docId;
                                if (extractedText->size() > kMaxIndexedDownloadTextBytes) {
                                    content.contentText =
                                        extractedText->substr(0, kMaxIndexedDownloadTextBytes);
                                } else {
                                    content.contentText = *extractedText;
                                }
                                content.extractionMethod = "download_ingest";

                                (void)ctx_.metadataRepo->insertContent(content);
                                (void)ctx_.metadataRepo->indexDocumentContent(
                                    docId, filename, content.contentText, docInfo.mimeType);
                                (void)ctx_.metadataRepo->updateDocumentExtractionStatus(
                                    docId, true, metadata::ExtractionStatus::Success);
                            } else {
                                bool textLike = false;
                                try {
                                    (void)detection::FileTypeDetector::initializeWithMagicNumbers();
                                    textLike =
                                        detection::FileTypeDetector::instance().isTextMimeType(
                                            docInfo.mimeType);
                                } catch (...) {
                                }

                                (void)ctx_.metadataRepo->updateDocumentExtractionStatus(
                                    docId, false,
                                    textLike ? metadata::ExtractionStatus::Pending
                                             : metadata::ExtractionStatus::Skipped,
                                    textLike ? "Download stored; extraction pending"
                                             : "No extractable text");
                            }
                        } else {
                            spdlog::warn("DownloadService: Failed to insert document metadata: {}",
                                         ins.error().message);
                        }
                    }
                } else {
                    spdlog::debug("DownloadService: ContentStore not available; skipping ingest");
                }
            } catch (const std::exception& ex) {
                spdlog::warn("DownloadService: Ingest/metadata exception: {}", ex.what());
            }
            // Prefer the ingested content hash for downstream retrieval; fall back to downloaded
            // hash
            if (ingestedHash) {
                spdlog::info("DownloadService: Ingested content hash: {} (Downloaded: {})",
                             *ingestedHash, finalResult.hash);
                response.hash = *ingestedHash;
            } else {
                response.hash = finalResult.hash;
            }
            response.url = finalResult.url;
            response.storedPath = finalResult.storedPath;
            response.sizeBytes = finalResult.sizeBytes;
            response.success = finalResult.success;
            response.httpStatus = finalResult.httpStatus;
            response.etag = finalResult.etag;
            response.lastModified = finalResult.lastModified;
            response.checksumOk = finalResult.checksumOk;
            // Friendly retrieval hint for CLI/MCP parity
            if (finalResult.suggestedName && !finalResult.suggestedName->empty()) {
                response.indexName = *finalResult.suggestedName;
            } else {
                std::string fname = finalResult.url;
                auto lastSlash = fname.find_last_of('/');
                if (lastSlash != std::string::npos)
                    fname = fname.substr(lastSlash + 1);
                auto q = fname.find('?');
                if (q != std::string::npos)
                    fname.resize(q);
                if (fname.empty())
                    fname = "downloaded_file";
                response.indexName = std::move(fname);
            }

            return response;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError, std::string("Download failed: ") + e.what()};
        }
    }

private:
    AppContext ctx_;
    downloader::StorageConfig storageCfg_;
    downloader::DownloaderConfig dlCfg_;
    std::unique_ptr<downloader::IDownloadManager> manager_;
};

std::shared_ptr<IDownloadService> makeDownloadService(const AppContext& ctx) {
    return std::make_shared<DownloadServiceImpl>(ctx);
}

} // namespace yams::app::services
