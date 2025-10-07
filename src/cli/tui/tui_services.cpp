#include <yams/cli/tui/tui_services.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/api/content_store.h>
#include <yams/cli/yams_cli.h>
#include <yams/extraction/content_extractor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/extraction/text_extractor.h>
#include <yams/metadata/document_metadata.h>

#include <spdlog/spdlog.h>

#include <chrono>
#include <filesystem>
#include <future>

namespace yams::cli::tui {

namespace {
using yams::Result;

template <typename T> Result<T> futureGet(std::future<Result<T>>& future) {
    try {
        return future.get();
    } catch (const std::exception& ex) {
        return Error{ErrorCode::InternalError, std::string{"Service future failed: "} + ex.what()};
    } catch (...) {
        return Error{ErrorCode::InternalError, "Service future failed with unknown error"};
    }
}
} // namespace

void TUIServices::invalidate() noexcept {
    services_ = {};
    metadataRepo_.reset();
    contentStore_.reset();
    appContext_.reset();
    ready_ = false;
}

yams::Result<void> TUIServices::ensureReady() {
    if (ready_) {
        return Result<void>();
    }
    if (!cli_) {
        return Error{ErrorCode::InvalidOperation, "CLI context not attached"};
    }
    auto loadResult = loadServices();
    if (!loadResult) {
        return loadResult;
    }
    ready_ = true;
    return Result<void>();
}

yams::Result<void> TUIServices::loadServices() {
    auto ensureStorage = cli_->ensureStorageInitialized();
    if (!ensureStorage) {
        return ensureStorage;
    }

    appContext_ = cli_->getAppContext();
    if (!appContext_) {
        return Error{ErrorCode::NotInitialized, "App context unavailable"};
    }

    metadataRepo_ = appContext_->metadataRepo;
    contentStore_ = appContext_->store;

    services_ = app::services::makeServices(*appContext_);

    if (!services_.search) {
        spdlog::warn("TUI services: search service unavailable");
    }
    if (!services_.document) {
        spdlog::warn("TUI services: document service unavailable");
    }
    if (!services_.indexing) {
        spdlog::debug("TUI services: indexing service unavailable");
    }
    if (!metadataRepo_) {
        spdlog::warn("TUI services: metadata repository unavailable");
    }
    if (!contentStore_) {
        spdlog::warn("TUI services: content store unavailable");
    }

    return Result<void>();
}

yams::Result<app::services::SearchResponse>
TUIServices::search(const app::services::SearchRequest& request) {
    auto ready = ensureReady();
    if (!ready) {
        return ready.error();
    }
    if (!services_.search) {
        return Error{ErrorCode::NotImplemented, "Search service not available"};
    }
    if (!appContext_) {
        return Error{ErrorCode::NotInitialized, "Worker executor not available"};
    }

    std::promise<Result<app::services::SearchResponse>> promise;
    auto future = promise.get_future();

    boost::asio::co_spawn(
        appContext_->workerExecutor,
        [svc = services_.search, req = request,
         pr = std::move(promise)]() mutable -> boost::asio::awaitable<void> {
            auto result = co_await svc->search(req);
            try {
                pr.set_value(std::move(result));
            } catch (...) {
                // set_value may throw on broken promise; swallow to avoid terminate
            }
            co_return;
        },
        boost::asio::detached);

    return futureGet(future);
}

yams::Result<void> TUIServices::lightIndexForHash(const std::string& hash) {
    if (hash.empty()) {
        return Error{ErrorCode::InvalidArgument, "Missing document hash"};
    }

    auto ready = ensureReady();
    if (!ready) {
        return ready.error();
    }

    if (!services_.search) {
        return Error{ErrorCode::NotInitialized, "Search service unavailable"};
    }

    return services_.search->lightIndexForHash(hash);
}

yams::Result<void> TUIServices::reindexDocument(const std::string& hash) {
    if (hash.empty()) {
        return Error{ErrorCode::InvalidArgument, "Missing document hash"};
    }

    auto ready = ensureReady();
    if (!ready) {
        return ready.error();
    }

    if (!metadataRepo_) {
        return Error{ErrorCode::NotInitialized, "Metadata repository unavailable"};
    }

    if (!contentStore_) {
        return Error{ErrorCode::NotInitialized, "Content store unavailable"};
    }

    auto docInfoResult = metadataRepo_->getDocumentByHash(hash);
    if (!docInfoResult) {
        return docInfoResult.error();
    }

    if (!docInfoResult.value().has_value()) {
        return Error{ErrorCode::NotFound, "Document not found"};
    }

    auto info = docInfoResult.value().value();

    std::string extension = info.fileExtension;
    if (extension.empty() && !info.fileName.empty()) {
        extension = std::filesystem::path(info.fileName).extension().string();
    }

    const auto extractors =
        appContext_ ? appContext_->contentExtractors : yams::extraction::ContentExtractorList{};
    auto extractedOpt = yams::extraction::util::extractDocumentText(
        contentStore_, hash, info.mimeType, extension, extractors);

    metadata::DocumentInfo updated = info;
    updated.indexedTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

    if (extractedOpt && !extractedOpt->empty()) {
        metadata::DocumentContent contentRow;
        contentRow.documentId = info.id;
        contentRow.contentText = *extractedOpt;
        contentRow.contentLength = static_cast<int64_t>(contentRow.contentText.size());
        contentRow.extractionMethod = "tui-reindex";
        double langConfidence = 0.0;
        contentRow.language = yams::extraction::LanguageDetector::detectLanguage(
            contentRow.contentText, &langConfidence);

        auto insertContent = metadataRepo_->insertContent(contentRow);
        if (!insertContent) {
            auto updateContent = metadataRepo_->updateContent(contentRow);
            if (!updateContent) {
                spdlog::warn("TUI reindex: failed to persist extracted content for {}: {}", hash,
                             insertContent.error().message);
            }
        }

        auto indexResult = metadataRepo_->indexDocumentContent(info.id, info.fileName,
                                                               *extractedOpt, info.mimeType);
        if (!indexResult) {
            return indexResult;
        }

        auto fuzzyResult = metadataRepo_->updateFuzzyIndex(info.id);
        if (!fuzzyResult) {
            return fuzzyResult;
        }

        updated.contentExtracted = true;
        updated.extractionStatus = metadata::ExtractionStatus::Success;
    } else {
        updated.contentExtracted = false;
        updated.extractionStatus = metadata::ExtractionStatus::Skipped;
        auto removeResult = metadataRepo_->removeFromIndex(info.id);
        if (!removeResult) {
            spdlog::debug("TUI reindex: failed to remove {} from index: {}", hash,
                          removeResult.error().message);
        }
    }

    auto updateDocument = metadataRepo_->updateDocument(updated);
    if (!updateDocument) {
        return updateDocument;
    }

    return Result<void>();
}

} // namespace yams::cli::tui
