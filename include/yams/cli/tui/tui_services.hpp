#pragma once

#include <memory>
#include <optional>

#include <yams/app/services/factory.hpp>
#include <yams/app/services/services.hpp>
#include <yams/core/types.h>

namespace yams::api {
class IContentStore;
}

namespace yams::metadata {
class MetadataRepository;
}

namespace yams::cli {
class YamsCLI;
}

namespace yams::cli::tui {

/**
 * Thin adapter that provides the Browse/TUI layer with access to the shared
 * application services (document, search, indexing, etc.) backed by the
 * AppContext. The adapter centralises initialization, error handling, and
 * synchronous wrappers for async services so the UI code remains focused on
 * presentation logic.
 */
class TUIServices {
public:
    TUIServices() noexcept = default;
    explicit TUIServices(yams::cli::YamsCLI* cli) noexcept { attach(cli); }

    void attach(yams::cli::YamsCLI* cli) noexcept {
        cli_ = cli;
        invalidate();
    }

    [[nodiscard]] yams::Result<void> ensureReady();

    [[nodiscard]] std::shared_ptr<app::services::IDocumentService>
    documentService() const noexcept {
        return services_.document;
    }

    [[nodiscard]] std::shared_ptr<app::services::IGrepService> grepService() const noexcept {
        return services_.grep;
    }

    [[nodiscard]] std::shared_ptr<app::services::IIndexingService>
    indexingService() const noexcept {
        return services_.indexing;
    }

    [[nodiscard]] std::shared_ptr<app::services::IDownloadService>
    downloadService() const noexcept {
        return services_.download;
    }

    [[nodiscard]] std::shared_ptr<app::services::IStatsService> statsService() const noexcept {
        return services_.stats;
    }

    [[nodiscard]] std::shared_ptr<metadata::MetadataRepository> metadataRepo() const noexcept {
        return metadataRepo_;
    }

    [[nodiscard]] std::shared_ptr<api::IContentStore> contentStore() const noexcept {
        return contentStore_;
    }

    [[nodiscard]] std::shared_ptr<app::services::AppContext> appContext() const noexcept {
        return appContext_;
    }

    [[nodiscard]] yams::Result<app::services::SearchResponse>
    search(const app::services::SearchRequest& request);

    [[nodiscard]] yams::Result<void> lightIndexForHash(const std::string& hash);

    [[nodiscard]] yams::Result<void> reindexDocument(const std::string& hash);

private:
    void invalidate() noexcept;
    [[nodiscard]] yams::Result<void> loadServices();

private:
    yams::cli::YamsCLI* cli_{nullptr};
    std::shared_ptr<app::services::AppContext> appContext_;
    app::services::ServiceBundle services_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<api::IContentStore> contentStore_;
    bool ready_{false};
};

} // namespace yams::cli::tui
