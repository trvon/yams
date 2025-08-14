#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <iostream>
#include <algorithm>

namespace yams::cli {

namespace fs = std::filesystem;
using json = nlohmann::json;

class RepairMimeCommand : public ICommand {
public:
    std::string getName() const override { return "repair-mime"; }

    std::string getDescription() const override {
        return "Repair MIME types for existing documents using signature and extension detection";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand(getName(), getDescription());

        cmd->add_flag("--all", processAll_, "Repair all documents (not just those with application/octet-stream or empty MIME)");
        cmd->add_flag("--dry-run", dryRun_, "Analyze and report changes without modifying the database");
        cmd->add_option("--limit", limit_, "Maximum number of documents to process (0 = no limit)")
            ->default_val(0);
        cmd->add_option("--path-contains", pathFilter_, "Only consider documents whose file_path contains this substring");
        cmd->add_flag("--verbose", verbose_, "Verbose per-document logging");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("repair-mime failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
        // 1) Ensure storage/DB is ready
        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured) return ensured;

        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        // 2) Initialize detector (best-effort)
        {
            detection::FileTypeDetectorConfig cfg;
            cfg.patternsFile = YamsCLI::findMagicNumbersFile();
            cfg.useCustomPatterns = !cfg.patternsFile.empty();
            cfg.cacheResults = true;
            detection::FileTypeDetector::instance().initialize(cfg);
        }

        // 3) Gather documents (filter by path if requested)
        std::vector<metadata::DocumentInfo> docs;
        {
            std::string like = "%";
            if (!pathFilter_.empty()) {
                // Use a LIKE filter that matches anywhere; DB expects '%' wildcards
                like = "%" + pathFilter_ + "%";
            }
            auto res = metadataRepo->findDocumentsByPath(like);
            if (!res) return res.error();
            docs = std::move(res.value());
        }

        // 4) Iterate and repair
        uint64_t totalConsidered = 0;
        uint64_t wouldUpdate = 0;
        uint64_t updated = 0;
        uint64_t skipped = 0;
        uint64_t failed = 0;

        auto needsRepair = [this](const metadata::DocumentInfo& d) -> bool {
            if (processAll_) return true;
            if (d.mimeType.empty()) return true;
            if (d.mimeType == "application/octet-stream") return true;
            return false;
        };

        auto detectMimeFor = [](const metadata::DocumentInfo& d) -> std::string {
            // Attempt signature-based detection from file if possible
            try {
                if (!d.filePath.empty() && d.filePath != "stdin") {
                    fs::path p(d.filePath);
                    std::error_code ec;
                    if (fs::exists(p, ec) && fs::is_regular_file(p, ec)) {
                        auto sig = detection::FileTypeDetector::instance().detectFromFile(p);
                        if (sig && !sig.value().mimeType.empty()) {
                            return sig.value().mimeType;
                        }
                    }
                }
            } catch (...) {
                // best-effort; ignore
            }

            // Fallback to extension-based mapping
            if (!d.fileExtension.empty()) {
                return detection::FileTypeDetector::getMimeTypeFromExtension(d.fileExtension);
            }
            if (!d.fileName.empty()) {
                auto pos = d.fileName.rfind('.');
                if (pos != std::string::npos) {
                    return detection::FileTypeDetector::getMimeTypeFromExtension(d.fileName.substr(pos));
                }
            }

            // Default
            return "application/octet-stream";
        };

        for (const auto& doc : docs) {
            if (!needsRepair(doc)) {
                skipped++;
                continue;
            }

            totalConsidered++;

            std::string newMime = detectMimeFor(doc);

            // If nothing better than octet-stream, only update when original was empty
            bool isImprovement = (doc.mimeType.empty() && !newMime.empty()) ||
                                 (!newMime.empty() && newMime != "application/octet-stream" && newMime != doc.mimeType);

            if (dryRun_) {
                if (isImprovement) {
                    wouldUpdate++;
                    if (verbose_) {
                        std::cout << "[DRY-RUN] " << doc.filePath << " : "
                                  << (doc.mimeType.empty() ? "(empty)" : doc.mimeType)
                                  << " -> " << newMime << "\n";
                    }
                } else {
                    if (verbose_) {
                        std::cout << "[DRY-RUN] " << doc.filePath << " : no change ("
                                  << (doc.mimeType.empty() ? "(empty)" : doc.mimeType) << ")\n";
                    }
                }
            } else {
                if (isImprovement) {
                    auto updatedDoc = doc;
                    updatedDoc.mimeType = newMime;
                    auto r = metadataRepo->updateDocument(updatedDoc);
                    if (r) {
                        updated++;
                        if (verbose_) {
                            std::cout << "[UPDATED] " << updatedDoc.filePath << " : "
                                      << (doc.mimeType.empty() ? "(empty)" : doc.mimeType)
                                      << " -> " << newMime << "\n";
                        }
                        // Refresh fuzzy index metadata for this doc
                        metadataRepo->updateFuzzyIndex(updatedDoc.id);
                    } else {
                        failed++;
                        spdlog::warn("Failed to update MIME for {}: {}", doc.filePath, r.error().message);
                    }
                } else {
                    skipped++;
                    if (verbose_) {
                        std::cout << "[SKIP] " << doc.filePath << " : no better MIME detected\n";
                    }
                }
            }

            if (limit_ > 0 && (updated + skipped + failed + wouldUpdate) >= static_cast<uint64_t>(limit_)) {
                break;
            }
        }

        // 5) Output summary
        if (cli_->getJsonOutput()) {
            json out;
            out["dry_run"] = dryRun_;
            out["all"] = processAll_;
            out["path_filter"] = pathFilter_;
            out["limit"] = limit_;
            out["total_examined"] = totalConsidered;
            out["would_update"] = dryRun_ ? wouldUpdate : 0;
            out["updated"] = dryRun_ ? 0 : updated;
            out["skipped"] = skipped;
            out["failed"] = failed;
            std::cout << out.dump(2) << std::endl;
        } else {
            std::cout << "repair-mime summary\n";
            std::cout << "  Mode        : " << (dryRun_ ? "DRY-RUN" : "APPLY") << "\n";
            std::cout << "  Scope       : " << (processAll_ ? "all documents" : "empty or octet-stream only") << "\n";
            if (!pathFilter_.empty()) {
                std::cout << "  Path filter : " << pathFilter_ << "\n";
            }
            if (limit_ > 0) {
                std::cout << "  Limit       : " << limit_ << "\n";
            }
            std::cout << "  Examined    : " << totalConsidered << "\n";
            if (dryRun_) {
                std::cout << "  Would update: " << wouldUpdate << "\n";
            } else {
                std::cout << "  Updated     : " << updated << "\n";
            }
            std::cout << "  Skipped     : " << skipped << "\n";
            std::cout << "  Failed      : " << failed << "\n";
        }

        return Result<void>();
    }

private:
    YamsCLI* cli_ = nullptr;
    bool processAll_ = false;
    bool dryRun_ = false;
    bool verbose_ = false;
    int64_t limit_ = 0;
    std::string pathFilter_;
};

// Factory function
std::unique_ptr<ICommand> createRepairMimeCommand() {
    return std::make_unique<RepairMimeCommand>();
}

} // namespace yams::cli