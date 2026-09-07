// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "downloads" (wire code 4); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operation_support.h"
#include "../repair_operations_internal.h"

#include <spdlog/spdlog.h>
#include <yams/core/repair_fsm.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/db_salvage.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/MetadataWriteFacade.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/daemon/metric_keys.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/content_extractor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/profiling.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include <sqlite3.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

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
