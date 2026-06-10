#include <spdlog/spdlog.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string_view>
#include <yams/app/services/download_metadata_entries.hpp>
#include <yams/app/services/services.hpp>
#include <yams/common/fs_utils.h>
#include <yams/detection/file_type_detector.h>
#include <yams/downloader/downloader.hpp>
#include <yams/extraction/extraction_util.h>

namespace fs = std::filesystem;

namespace yams::app::services {

namespace {
constexpr std::size_t kMaxIndexedDownloadTextBytes = std::size_t{16} * 1024 * 1024;

std::string getenvCopy(std::string_view key) {
    static std::mutex envMutex;
    std::lock_guard<std::mutex> lock(envMutex);
    if (const char* value =
            std::getenv(std::string(key).c_str())) { // NOLINT(concurrency-mt-unsafe)
        return value;
    }
    return {};
}
} // namespace

class DownloadServiceImpl : public IDownloadService {
public:
    DownloadServiceImpl(const AppContext& ctx, const DownloadServiceOptions& opts)
        : DownloadServiceImpl(ctx) {
        if (opts.maxFileBytes > 0 && manager_) {
            dlCfg_.maxFileBytes = opts.maxFileBytes;
            manager_ = downloader::makeDownloadManager(storageCfg_, dlCfg_);
            if (!manager_) {
                spdlog::error("DownloadService: Failed to rebuild download manager with cap");
                throw std::runtime_error("Failed to initialize download manager with cap");
            }
        }
    }

    explicit DownloadServiceImpl(const AppContext& ctx) : ctx_(ctx) {
        // Initialize download manager once with proper config
        // Get storage path from content store or environment
        fs::path storagePath;
        if (const std::string storageEnv = getenvCopy("YAMS_STORAGE"); !storageEnv.empty()) {
            storagePath = fs::path(storageEnv);
        } else if (const std::string homeEnv = getenvCopy("HOME"); !homeEnv.empty()) {
            storagePath = fs::path(homeEnv) / "yams";
        } else {
            storagePath = fs::current_path() / "yams";
        }

        // Set up storage paths for downloader (CAS paths)
        storageCfg_.objectsDir = storagePath / "storage" / "objects";
        storageCfg_.stagingDir = storagePath / "storage" / "staging";

        // Ensure directories exist
        yams::common::ensureDirectories(storageCfg_.objectsDir);
        yams::common::ensureDirectories(storageCfg_.stagingDir);

        spdlog::debug("DownloadService initialized with storage path: {}", storagePath.string());
        spdlog::debug("Objects directory: {}", storageCfg_.objectsDir.string());
        spdlog::debug("Staging directory: {}", storageCfg_.stagingDir.string());

        dlCfg_.defaultConcurrency = 4;
        dlCfg_.defaultChunkSizeBytes = std::size_t{8} * 1024 * 1024;
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
                        } catch (const std::exception& ex) {
                            spdlog::debug("DownloadService: MIME sniff failed: {}", ex.what());
                        } catch (...) {
                            spdlog::debug("DownloadService: MIME sniff failed with unknown error");
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
                            YAMS_DCHECK(docId > 0,
                                        "download ingest should only batch metadata for persisted "
                                        "documents");

                            const auto metadataEntries =
                                buildDownloadMetadataEntries(docId, req, finalResult);
                            if (!metadataEntries.empty()) {
                                // Download metadata enrichments are advisory: if the DB is under
                                // write contention, keep the successful download/ingest moving and
                                // let the scoped retry policy bound how long we wait here.
                                metadata::MetadataOpScope metadataScope(
                                    "app_download_metadata_burst");
                                auto metadataResult =
                                    ctx_.metadataRepo->setMetadataBatch(metadataEntries);
                                if (!metadataResult) {
                                    spdlog::warn("DownloadService: Failed to batch downloaded "
                                                 "document metadata: {}",
                                                 metadataResult.error().message);
                                }
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
                                } catch (const std::exception& ex) {
                                    spdlog::debug("DownloadService: text MIME probe failed: {}",
                                                  ex.what());
                                } catch (...) {
                                    spdlog::debug("DownloadService: text MIME probe failed with "
                                                  "unknown error");
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

std::shared_ptr<IDownloadService> makeDownloadService(const AppContext& ctx,
                                                      const DownloadServiceOptions& opts) {
    return std::make_shared<DownloadServiceImpl>(ctx, opts);
}

} // namespace yams::app::services
