#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <iostream>
#include <optional>
#include <regex>
#include <vector>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/ipc/ipc_protocol.h>

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

        // Build daemon request (using only protocol fields)
        yams::daemon::DownloadRequest dreq;
        if (url_) {
            dreq.url = *url_;
        } else {
            return Error{ErrorCode::InvalidArgument, "URL is required for download"};
        }

        if (exportPath_) {
            dreq.outputPath = exportPath_->string();
        }

        // Note: tags and metadata would be added here if CLI options existed
        // dreq.tags = tags_;
        // dreq.metadata = metadata_;

        dreq.quiet = quiet_;

        // Render lambda for results (unused since we skip daemon)
        [[maybe_unused]] auto render =
            [&](const yams::daemon::DownloadResponse& resp) -> Result<void> {
            if (jsonOutput_) {
                json j;
                j["success"] = resp.success;
                j["url"] = resp.url;
                j["hash"] = resp.hash;
                j["local_path"] = resp.localPath;
                j["size"] = resp.size;
                if (!resp.error.empty()) {
                    j["error"] = resp.error;
                }
                std::cout << j.dump(2) << std::endl;
            } else {
                if (resp.success) {
                    std::cout << "Downloaded: " << resp.url << std::endl;
                    std::cout << "Hash: " << resp.hash << std::endl;
                    std::cout << "Size: " << resp.size << " bytes" << std::endl;
                    if (!resp.localPath.empty()) {
                        std::cout << "Stored at: " << resp.localPath << std::endl;
                    }
                } else {
                    std::cout << "Download failed: " << resp.error << std::endl;
                }
            }
            return Result<void>();
        };

        // Downloads should always be handled locally by CLI with rich functionality
        // The daemon doesn't have HTTP client, progress indicators, resume support, etc.
        // Skip daemon entirely and use local download implementation
        spdlog::info("Using local download implementation (daemon not suited for downloads)");

        // For now, return a clear message that local download needs implementation
        if (jsonOutput_) {
            json j;
            j["success"] = false;
            j["url"] = dreq.url;
            j["error"] = "Local download implementation needed - daemon not suitable for downloads";
            std::cout << j.dump(2) << std::endl;
        } else {
            std::cout << "Download failed: Local download implementation needed" << std::endl;
            std::cout
                << "The daemon is not suitable for downloads (missing HTTP client, progress, etc.)"
                << std::endl;
        }

        return Error{ErrorCode::NotImplemented, "Local download implementation required"};
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
};

// Factory function for registry
std::unique_ptr<ICommand> createDownloadCommand() {
    return std::make_unique<DownloadCommand>();
}

} // namespace yams::cli