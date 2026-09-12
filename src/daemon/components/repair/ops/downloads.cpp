// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "downloads" (wire code 4); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operations_internal.h"

#include <spdlog/spdlog.h>
#include <yams/daemon/components/MetadataWriteFacade.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

#include <atomic>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace yams::daemon::repair {

namespace {

RepairOperationResult repairDownloads(OperationEnv& env, bool dryRun, bool verbose,
                                      RepairService::ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "downloads";

    auto meta = env.ctx.getMetadataRepo ? env.ctx.getMetadataRepo() : nullptr;
    if (!meta) {
        result.message = "Metadata not available";
        return result;
    }

    auto* wc = env.ctx.getWriteCoordinator ? env.ctx.getWriteCoordinator() : nullptr;
    MetadataWriteFacade metaFacade(wc, meta.get());

    auto docsResult = metadata::queryDocumentsByPattern(*meta, "%");
    if (!docsResult) {
        result.message = "Failed to query";
        return result;
    }

    auto is_url = [](const std::string& s) { return s.find("://") != std::string::npos; };
    auto extract_host = [](const std::string& url) -> std::string {
        auto p = url.find("://");
        if (p == std::string::npos)
            return {};
        auto rest = url.substr(p + 3);
        auto slash = rest.find('/');
        return (slash == std::string::npos) ? rest : rest.substr(0, slash);
    };
    auto extract_scheme = [](const std::string& url) -> std::string {
        auto p = url.find("://");
        return (p == std::string::npos) ? std::string{} : url.substr(0, p);
    };
    auto filename_from_url = [](std::string url) -> std::string {
        auto lastSlash = url.find_last_of('/');
        if (lastSlash != std::string::npos)
            url = url.substr(lastSlash + 1);
        auto q = url.find('?');
        if (q != std::string::npos)
            url = url.substr(0, q);
        if (url.empty())
            url = "downloaded_file";
        return url;
    };

    for (auto& doc : docsResult.value()) {
        std::string sourceUrl;
        if (is_url(doc.filePath))
            sourceUrl = doc.filePath;
        if (sourceUrl.empty())
            continue;

        result.processed++;
        if (dryRun) {
            result.skipped++;
            continue;
        }

        std::string filename = filename_from_url(sourceUrl);
        std::string ext;
        auto dotPos = filename.rfind('.');
        if (dotPos != std::string::npos)
            ext = filename.substr(dotPos);
        doc.fileName = filename;
        doc.filePath = std::move(filename);
        doc.fileExtension = std::move(ext);

        {
            metadata::MetadataOpScope opScope("repair_download_url_update");
            if (auto up = meta->updateDocument(doc); up) {
                result.succeeded++;
            } else {
                result.failed++;
            }
        }

        try {
            metaFacade.setMetadata(doc.id, "source_url", metadata::MetadataValue(sourceUrl));
            metaFacade.setMetadata(doc.id, "tag:downloaded", metadata::MetadataValue("downloaded"));
            auto host = extract_host(sourceUrl);
            auto scheme = extract_scheme(sourceUrl);
            if (!host.empty())
                metaFacade.setMetadata(doc.id, "tag:host:" + host,
                                       metadata::MetadataValue("host:" + host));
            if (!scheme.empty())
                metaFacade.setMetadata(doc.id, "tag:scheme:" + scheme,
                                       metadata::MetadataValue("scheme:" + scheme));
        } catch (const std::exception& e) {
            spdlog::debug("RepairService: failed to persist download metadata for {}: {}",
                          sourceUrl, e.what());
        } catch (...) {
            spdlog::debug("RepairService: failed to persist download metadata for {}", sourceUrl);
        }
    }

    result.message = "Updated " + std::to_string(result.succeeded) + " download documents";
    metaFacade.flush();
    return result;
}

class DownloadsOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "downloads"; }
    std::uint64_t code() const noexcept override { return 4; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        (void)cancelRequested;
        return repairDownloads(env, req.dryRun, req.verbose, progress);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeDownloadsOperation() {
    return std::make_unique<DownloadsOperation>();
}

} // namespace yams::daemon::repair
