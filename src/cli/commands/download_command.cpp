#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <iostream>
#include <optional>
#include <regex>
#include <vector>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/downloader/downloader.hpp>

namespace yams::cli {

namespace fs = std::filesystem;
using json = nlohmann::json;

class DownloadCommand : public ICommand {
public:
    std::string getName() const override { return "download"; }

    std::string getDescription() const override {
        return "Download artifact(s) and store directly into YAMS CAS (store-only by default).";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("download", getDescription());

        // Inputs: URL or --list (mutually exclusive)
        cmd->add_option("url", url_,
                        "Source URL (HTTPS recommended; TLS verification on by default).");
        cmd->add_option("--list", listPath_, "Path to a manifest file (one URL per line).");

        // Headers and auth
        cmd->add_option("-H,--header", headers_,
                        "Custom header (repeatable), e.g., 'Authorization: Bearer <token>'.");

        // Integrity
        cmd->add_option("--checksum", checksum_,
                        "Expected checksum '<algo>:<hex>' (algo: sha256|sha512|md5).")
            ->check(CLI::Validator(
                [](std::string& s) {
                    static const std::regex re(R"(^(sha256|sha512|md5):[0-9a-fA-F]+$)");
                    return std::regex_match(s, re)
                               ? std::string{}
                               : std::string{"invalid checksum format (expected '<algo>:<hex>')"};
                },
                "checksum"));

        // Concurrency and performance
        cmd->add_option("-c,--concurrency", concurrency_, "Parallel connection count (default 4).")
            ->check(CLI::Range(1, 64));
        cmd->add_option("--chunk-size", chunkSize_, "Chunk size in bytes (default 8388608).")
            ->check(CLI::Range((std::uint64_t)64 * 1024, (std::uint64_t)1024 * 1024 * 1024));
        cmd->add_option("--timeout", timeoutMs_, "Per-connection timeout in ms (default 60000).")
            ->check(CLI::Range(1000, 3600 * 1000));

        cmd->add_option("--retry", retryAttempts_, "Max retry attempts (default 5).")
            ->check(CLI::Range(0, 20));
        cmd->add_option("--backoff", backoffMs_, "Initial backoff in ms (default 500).")
            ->check(CLI::Range(0, 60000));
        cmd->add_option("--backoff-mult", backoffMult_, "Backoff multiplier (default 2.0).")
            ->check(CLI::PositiveNumber);
        cmd->add_option("--max-backoff", maxBackoffMs_, "Max backoff in ms (default 15000).")
            ->check(CLI::Range(0, 5 * 60 * 1000));

        cmd->add_option("--rate-limit-global", rateLimitGlobalBps_,
                        "Global rate limit (bytes/sec, 0=unlimited).");
        cmd->add_option("--rate-limit-per-conn", rateLimitPerConnBps_,
                        "Per-connection rate limit (bytes/sec, 0=unlimited).");

        // Resume and networking
        cmd->add_flag("--no-resume", noResume_, "Disable resume (Range requests).");
        cmd->add_option("--proxy", proxy_, "Proxy URL (e.g., http://user:pass@host:port).");
        cmd->add_flag("--tls-insecure", tlsInsecure_,
                      "Disable TLS verification (NOT RECOMMENDED).");
        cmd->add_option("--tls-ca", tlsCaPath_, "Path to CA bundle for TLS verification.");
        cmd->add_flag("--no-follow-redirects", noFollowRedirects_,
                      "Disable following HTTP redirects.");

        // Export policy (applies only if export is requested)
        cmd->add_option("--export", exportPath_,
                        "Export a copy/link from CAS to a user path (optional).");
        cmd->add_option("--export-dir", exportDir_, "Export directory for --list mode (optional).");
        cmd->add_option("--overwrite", overwritePolicy_,
                        "Overwrite policy for export only: [never|if-different-etag|always] "
                        "(default: never).")
            ->check(CLI::IsMember({"never", "if-different-etag", "always"}));

        // Annotations
        cmd->add_option("--tag", tags_, "Tag to attach to the stored document (repeatable). ")
            ->take_all();
        cmd->add_option("--meta", metadataKVs_,
                        "Metadata key=value to attach (repeatable). Keys are strings; values are "
                        "stored as strings.")
            ->take_all();

        // Output / UX
        cmd->add_option("--progress", progress_,
                        "Progress format: [human|json|none] (default: human).")
            ->check(CLI::IsMember({"human", "json", "none"}));
        cmd->add_flag("--json", jsonOutput_,
                      "Emit final result as JSON to stdout (progress to stderr).");
        cmd->add_flag("--quiet", quiet_, "Suppress non-critical logs.");

        // Action
        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("download command failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });

        // Help epilogue clarifying store-only behavior
        cmd->footer(R"(Behavior:
  - Default is store-only. The downloader verifies content and atomically finalizes it into CAS:
      data/objects/sha256/aa/bb/<full_hash>
  - No user filesystem writes unless --export/--export-dir is explicitly provided.
  - TLS verification is ON by default; use --tls-insecure at your own risk.
  - Resume enabled by default (Range + If-Range). Use --no-resume to disable.
  - Use --json for machine-readable output (final result on stdout; logs on stderr).)");
    }

    Result<void> execute() override {
        // Validate inputs
        if (!url_ && !listPath_) {
            return Error{ErrorCode::InvalidArgument, "Either <url> or --list must be provided"};
        }
        if (url_ && listPath_) {
            return Error{ErrorCode::InvalidArgument, "Specify only one of <url> or --list"};
        }

        // Get app context and download service
        auto appContext = cli_->getAppContext();
        if (!appContext) {
            return Error{ErrorCode::NotInitialized, "Failed to initialize app context"};
        }
        auto downloadService = app::services::makeDownloadService(*appContext);
        if (!downloadService) {
            return Error{ErrorCode::NotInitialized, "Failed to create download service"};
        }

        // Build service request from CLI options
        app::services::DownloadServiceRequest serviceReq;
        if (url_) {
            serviceReq.url = *url_;
        } else {
            // TODO: Handle --list option
            return Error{ErrorCode::NotImplemented, "--list option is not yet implemented"};
        }

        // Headers
        for (const auto& h : headers_) {
            auto pos = h.find(':');
            if (pos != std::string::npos) {
                std::string key = h.substr(0, pos);
                std::string value = h.substr(pos + 1);
                // trim leading space from value
                if (!value.empty() && value[0] == ' ') {
                    value.erase(0, 1);
                }
                serviceReq.headers.push_back({key, value});
            }
        }

        // Checksum
        if (checksum_) {
            auto pos = checksum_->find(':');
            if (pos != std::string::npos) {
                std::string algo = checksum_->substr(0, pos);
                std::string hex = checksum_->substr(pos + 1);
                downloader::Checksum cs;
                if (algo == "sha256")
                    cs.algo = downloader::HashAlgo::Sha256;
                else if (algo == "sha512")
                    cs.algo = downloader::HashAlgo::Sha512;
                else if (algo == "md5")
                    cs.algo = downloader::HashAlgo::Md5;
                cs.hex = hex;
                serviceReq.checksum = cs;
            }
        }

        // Concurrency and performance
        serviceReq.concurrency = concurrency_;
        serviceReq.chunkSizeBytes = chunkSize_;
        serviceReq.timeout = std::chrono::milliseconds(timeoutMs_);
        serviceReq.retry.maxAttempts = retryAttempts_;
        serviceReq.retry.initialBackoff = std::chrono::milliseconds(backoffMs_);
        serviceReq.retry.multiplier = backoffMult_;
        serviceReq.retry.maxBackoff = std::chrono::milliseconds(maxBackoffMs_);
        serviceReq.rateLimit.globalBps = rateLimitGlobalBps_;
        serviceReq.rateLimit.perConnectionBps = rateLimitPerConnBps_;

        // Resume and networking
        serviceReq.resume = !noResume_;
        serviceReq.proxy = proxy_;
        serviceReq.tls.insecure = tlsInsecure_;
        if (tlsCaPath_) {
            serviceReq.tls.caPath = *tlsCaPath_;
        }
        serviceReq.followRedirects = !noFollowRedirects_;

        // Export policy
        serviceReq.storeOnly = !exportPath_ && !exportDir_;
        if (exportPath_) {
            serviceReq.exportPath = exportPath_->string();
        }
        if (overwritePolicy_ == "never") {
            serviceReq.overwrite = downloader::OverwritePolicy::Never;
        } else if (overwritePolicy_ == "if-different-etag") {
            serviceReq.overwrite = downloader::OverwritePolicy::IfDifferentEtag;
        } else if (overwritePolicy_ == "always") {
            serviceReq.overwrite = downloader::OverwritePolicy::Always;
        }

        // User-supplied annotations
        serviceReq.tags = tags_;
        // Parse key=value pairs
        for (const auto& kv : metadataKVs_) {
            auto pos = kv.find('=');
            if (pos == std::string::npos || pos == 0) {
                spdlog::warn("Ignoring --meta without '=' or empty key: {}", kv);
                continue;
            }
            std::string key = kv.substr(0, pos);
            std::string value = kv.substr(pos + 1);
            // Trim spaces around key/value
            auto trim = [](std::string& s) {
                if (s.empty())
                    return;
                s.erase(0, s.find_first_not_of(" \t"));
                auto p = s.find_last_not_of(" \t");
                if (p != std::string::npos)
                    s.erase(p + 1);
            };
            trim(key);
            trim(value);
            if (key.empty()) {
                spdlog::warn("Ignoring --meta with empty key: {}", kv);
                continue;
            }
            serviceReq.metadata[key] = value;
        }

        // Call the download service
        auto result = downloadService->download(serviceReq);

        // Handle result
        if (!result) {
            spdlog::error("Download failed: {}", result.error().message);
            if (jsonOutput_) {
                json j;
                j["success"] = false;
                j["url"] = serviceReq.url;
                j["error"] = result.error().message;
                std::cout << j.dump(2) << std::endl;
            }
            return result.error();
        }

        const auto& resp = result.value();

        if (jsonOutput_) {
            json j;
            j["success"] = resp.success;
            j["url"] = resp.url;
            j["hash"] = resp.hash;
            j["stored_path"] = resp.storedPath.string();
            j["size_bytes"] = resp.sizeBytes;
            if (resp.httpStatus)
                j["http_status"] = *resp.httpStatus;
            if (resp.etag)
                j["etag"] = *resp.etag;
            if (resp.lastModified)
                j["last_modified"] = *resp.lastModified;
            if (resp.checksumOk)
                j["checksum_ok"] = *resp.checksumOk;
            if (!resp.indexName.empty())
                j["index_name"] = resp.indexName;
            if (!tags_.empty())
                j["tags"] = tags_;
            if (!serviceReq.metadata.empty())
                j["metadata"] = serviceReq.metadata;
            std::cout << j.dump(2) << std::endl;
        } else {
            std::cout << "Download successful!" << std::endl;
            std::cout << "  URL: " << resp.url << std::endl;
            // Show the ingested content hash (DownloadServiceResponse.hash prefers ingested hash)
            std::cout << "  Content Hash: " << resp.hash << std::endl;
            std::cout << "  Size: " << resp.sizeBytes << " bytes" << std::endl;
            std::cout << "  Stored at: " << resp.storedPath.string() << std::endl;
            if (!resp.indexName.empty()) {
                std::cout << "  Name: " << resp.indexName << std::endl;
                std::cout << "  Tip: yams get --name \"" << resp.indexName << "\"  (or: yams get "
                          << resp.hash << ")" << std::endl;
            } else {
                std::cout << "  Tip: yams get " << resp.hash << std::endl;
            }
        }

        return Result<void>();
    }

private:
    YamsCLI* cli_{nullptr};
    std::optional<std::string> url_{};
    std::optional<fs::path> listPath_{};

    // Headers and auth
    std::vector<std::string> headers_{};

    // Integrity
    std::optional<std::string> checksum_{}; // "<algo>:<hex>"

    // Concurrency and performance
    int concurrency_{4};
    std::uint64_t chunkSize_{8ull * 1024ull * 1024ull}; // 8 MiB
    int timeoutMs_{60000};
    int retryAttempts_{5};
    int backoffMs_{500};
    double backoffMult_{2.0};
    int maxBackoffMs_{15000};
    std::uint64_t rateLimitGlobalBps_{0};
    std::uint64_t rateLimitPerConnBps_{0};

    // Resume and networking
    bool noResume_{false};
    std::optional<std::string> proxy_{};
    bool tlsInsecure_{false};
    std::optional<std::string> tlsCaPath_{};
    bool noFollowRedirects_{false};

    // Export policy
    std::optional<fs::path> exportPath_{};
    std::optional<fs::path> exportDir_{};
    std::string overwritePolicy_{"never"};

    // Output / UX
    std::string progress_{"human"}; // "human" | "json" | "none"
    bool jsonOutput_{false};
    bool quiet_{false};

    // Annotations
    std::vector<std::string> tags_{};
    std::vector<std::string> metadataKVs_{}; // raw key=value pairs
};

// Factory function for registry
std::unique_ptr<ICommand> createDownloadCommand() {
    return std::make_unique<DownloadCommand>();
}

} // namespace yams::cli
