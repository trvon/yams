/*
 * yams/src/cli/cmd_download.cpp
 *
 * CLI stub for `yams download` reflecting the store-only plan.
 * - Store-only default: artifacts are finalized into YAMS CAS (content-addressed storage).
 * - Optional export (--export/--export-dir) to place a copy/link outside the store.
 * - This is a stub: it does not perform network IO. It returns a placeholder JSON result and logs a
 * TODO.
 *
 * Integration notes:
 * - Exposes: void yams::cli::registerDownloadCommand(CLI::App& app)
 * - The main CLI should call this to register the "download" subcommand.
 *
 * TODO:
 * - Wire to the downloader implementation (yams::downloader::IDownloadManager).
 * - Load defaults from config ([storage] and [downloader] sections).
 * - Emit real progress and final CAS hash/stored_path.
 */

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;
using json = nlohmann::json;

namespace yams::cli {

namespace {

struct RetryOpts {
    int max_attempts{5};
    int backoff_ms{500};
    double backoff_mult{2.0};
    int max_backoff_ms{15000};
};

enum class OverwritePolicy { Never, IfDifferentEtag, Always };

struct DownloadOpts {
    // Inputs
    std::optional<std::string> url;
    std::optional<fs::path> list_path;

    // Headers and auth
    std::vector<std::string> headers;

    // Integrity
    std::optional<std::string> checksum; // "<algo>:<hex>"

    // Concurrency and performance
    int concurrency{4};
    std::uint64_t chunk_size{8ull * 1024ull * 1024ull}; // 8 MiB
    int timeout_ms{60000};
    RetryOpts retry{};
    std::uint64_t rate_limit_global_bps{0};
    std::uint64_t rate_limit_per_conn_bps{0};

    // Resume and networking
    bool resume{true};
    std::optional<std::string> proxy;
    bool tls_insecure{false};
    std::optional<std::string> tls_ca;
    bool follow_redirects{true};

    // Export policy (applies only if export is requested)
    std::optional<fs::path> export_path;
    std::optional<fs::path> export_dir;
    OverwritePolicy overwrite{OverwritePolicy::Never};

    // Output / UX
    std::string progress{"human"}; // "human" | "json" | "none"
    bool emit_json{false};
    bool quiet{false};
};

// Map string to OverwritePolicy
std::optional<OverwritePolicy> parse_overwrite(std::string_view s) {
    if (s == "never")
        return OverwritePolicy::Never;
    if (s == "if-different-etag")
        return OverwritePolicy::IfDifferentEtag;
    if (s == "always")
        return OverwritePolicy::Always;
    return std::nullopt;
}

// Validate checksum format "algo:hex"
bool valid_checksum_format(const std::string& s) {
    static const std::regex re(R"(^(sha256|sha512|md5):[0-9a-fA-F]+$)");
    return std::regex_match(s, re);
}

void print_result_stub(const DownloadOpts& opts) {
    // This is a stubbed result that reflects the store-only plan.
    // It returns placeholders and indicates that the operation is TODO.
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();

    json result = {
        {"type", "result"},
        {"url", opts.url.value_or(std::string{})},
        // Placeholders to reflect CAS finalize output shape:
        {"hash", "sha256:0000000000000000000000000000000000000000000000000000000000000000"},
        {"stored_path", "data/objects/sha256/00/00/"
                        "0000000000000000000000000000000000000000000000000000000000000000"},
        {"size_bytes", 0},
        {"success", false},
        {"http_status", nullptr},
        {"etag", nullptr},
        {"last_modified", nullptr},
        {"checksum_ok", nullptr},
        {"error",
         {{"code", "E_TODO"},
          {"message",
           "Downloader not implemented yet. This is a CLI stub reflecting the store-only plan."}}},
        {"elapsed_ms", 0},
        {"_meta",
         {{"store_only", true},
          {"export_requested", opts.export_path.has_value() || opts.export_dir.has_value()},
          {"concurrency", opts.concurrency},
          {"chunk_size_bytes", opts.chunk_size},
          {"timeout_ms", opts.timeout_ms},
          {"resume", opts.resume},
          {"follow_redirects", opts.follow_redirects},
          {"rate_limit_global_bps", opts.rate_limit_global_bps},
          {"rate_limit_per_conn_bps", opts.rate_limit_per_conn_bps},
          {"timestamp_ms", now_ms}}}};

    if (opts.emit_json) {
        // Strictly JSON to stdout
        fmt::print("{}\n", result.dump());
    } else {
        // Human-readable summary, JSON result preview
        spdlog::info("yams download (stub) - store-only default enabled");
        if (opts.url) {
            spdlog::info("URL: {}", *opts.url);
        } else if (opts.list_path) {
            spdlog::info("Manifest: {}", opts.list_path->string());
        }
        spdlog::info("Concurrency: {}, ChunkSize: {} bytes, Timeout: {} ms", opts.concurrency,
                     opts.chunk_size, opts.timeout_ms);
        spdlog::info("TLS: verify={}, ca='{}'", !opts.tls_insecure, opts.tls_ca.value_or(""));
        if (opts.export_path) {
            spdlog::info("Export path requested: {}", opts.export_path->string());
        }
        if (opts.export_dir) {
            spdlog::info("Export dir requested: {}", opts.export_dir->string());
        }
        spdlog::warn("Downloader is not implemented yet. Returning placeholder result.");
        fmt::print("{}\n", result.dump(2));
    }
}

} // namespace

void registerDownloadCommand(CLI::App& app) {
    auto* sub = app.add_subcommand(
        "download",
        "Download artifact(s) and store directly into YAMS CAS (store-only by default). "
        "Optionally export a copy to a user path with --export/--export-dir.");

    auto opts = std::make_shared<DownloadOpts>();

    // Inputs: URL or --list (mutually exclusive)
    sub->add_option("url", opts->url,
                    "Source URL (HTTPS recommended; default policy verifies TLS).")
        ->check(CLI::NonEmpty());
    sub->add_option("--list", opts->list_path, "Path to a manifest file (one URL per line).");

    // Headers and auth
    sub->add_option("-H,--header", opts->headers,
                    "Custom request header (repeatable), e.g., 'Authorization: Bearer <token>'.");

    // Integrity
    sub->add_option("--checksum", opts->checksum,
                    "Expected checksum '<algo>:<hex>' (algo: sha256|sha512|md5).")
        ->check(CLI::Validator(
            [](std::string& s) {
                return valid_checksum_format(s)
                           ? std::string{}
                           : std::string{"invalid checksum format (expected '<algo>:<hex>')"};
            },
            "checksum"));

    // Concurrency and performance
    sub->add_option("-c,--concurrency", opts->concurrency, "Parallel connection count (default 4).")
        ->check(CLI::Range(1, 64));
    sub->add_option("--chunk-size", opts->chunk_size, "Chunk size in bytes (default 8388608).")
        ->check(CLI::Range<std::uint64_t>(64 * 1024, 1024ull * 1024ull * 1024ull));
    sub->add_option("--timeout", opts->timeout_ms, "Per-connection timeout in ms (default 60000).")
        ->check(CLI::Range(1000, 3600 * 1000));

    sub->add_option("--retry", opts->retry.max_attempts, "Max retry attempts (default 5).")
        ->check(CLI::Range(0, 20));
    sub->add_option("--backoff", opts->retry.backoff_ms, "Initial backoff in ms (default 500).")
        ->check(CLI::Range(0, 60000));
    sub->add_option("--backoff-mult", opts->retry.backoff_mult, "Backoff multiplier (default 2.0).")
        ->check(CLI::PositiveNumber);
    sub->add_option("--max-backoff", opts->retry.max_backoff_ms,
                    "Max backoff in ms (default 15000).")
        ->check(CLI::Range(0, 5 * 60 * 1000));

    sub->add_option("--rate-limit-global", opts->rate_limit_global_bps,
                    "Global rate limit (bytes/sec, 0=unlimited).");
    sub->add_option("--rate-limit-per-conn", opts->rate_limit_per_conn_bps,
                    "Per-connection rate limit (bytes/sec, 0=unlimited).");

    // Resume and networking
    sub->add_flag("--no-resume", opts->resume, "Disable resume (Range requests).")
        ->expected(0)
        ->trigger_on_parse();
    sub->get_option("--no-resume")->callback([opts]() { opts->resume = false; });

    sub->add_option("--proxy", opts->proxy, "Proxy URL (e.g., http://user:pass@host:port).");
    sub->add_flag("--tls-insecure", opts->tls_insecure,
                  "Disable TLS verification (NOT RECOMMENDED).");
    sub->add_option("--tls-ca", opts->tls_ca, "Path to CA bundle for TLS verification.");
    sub->add_flag("--no-follow-redirects", opts->follow_redirects,
                  "Disable following HTTP redirects.")
        ->expected(0)
        ->trigger_on_parse();
    sub->get_option("--no-follow-redirects")->callback([opts]() {
        opts->follow_redirects = false;
    });

    // Export policy
    sub->add_option("--export", opts->export_path,
                    "Export a copy/link from CAS to a user path (optional).");
    sub->add_option("--export-dir", opts->export_dir,
                    "Export directory for --list mode (optional).");

    std::string overwrite_str{"never"};
    sub->add_option(
           "--overwrite", overwrite_str,
           "Overwrite policy for export only: [never|if-different-etag|always] (default: never).")
        ->check(CLI::IsMember({"never", "if-different-etag", "always"}));

    // Output / UX
    sub->add_option("--progress", opts->progress,
                    "Progress format: [human|json|none] (default: human).")
        ->check(CLI::IsMember({"human", "json", "none"}));
    sub->add_flag("--json", opts->emit_json,
                  "Emit final result as JSON to stdout (progress to stderr).");
    sub->add_flag("--quiet", opts->quiet, "Suppress non-critical logs.");

    // Validation and action
    sub->callback([opts, overwrite_str]() {
        // Basic validation
        if (!opts->url && !opts->list_path) {
            throw CLI::ValidationError("download", "Either <url> or --list must be provided.");
        }
        if (opts->url && opts->list_path) {
            throw CLI::ValidationError("download", "Specify only one of <url> or --list.");
        }

        // Overwrite policy
        if (auto ow = parse_overwrite(overwrite_str)) {
            opts->overwrite = *ow;
        } else {
            throw CLI::ValidationError("download", "Invalid --overwrite value.");
        }

        // Quiet mode reduces spdlog level
        if (opts->quiet) {
            spdlog::set_level(spdlog::level::warn);
        } else {
            spdlog::set_level(spdlog::level::info);
        }

        // Progress hint (no-op in stub)
        if (opts->progress == "json") {
            spdlog::info("Progress will be emitted as JSON events (stubbed).");
        } else if (opts->progress == "none") {
            spdlog::info("Progress output disabled.");
        } else {
            spdlog::info("Progress will be human-readable (stubbed).");
        }

        // Store-only plan explained
        spdlog::info("Store-only default: artifacts will be verified and finalized into YAMS CAS.");
        if (opts->export_path || opts->export_dir) {
            spdlog::info(
                "Export requested: a copy/link will be made from CAS after successful store.");
        }

        // TODO: Replace this with actual downloader invocation and progress streaming.
        // For now, print a placeholder final result.
        print_result_stub(*opts);
    });

    // Help epilogue to clarify store-only behavior
    sub->footer(R"(Behavior:
  - Default is store-only. The downloader verifies content and atomically finalizes it into CAS:
      data/objects/sha256/aa/bb/<full_hash>
  - No user filesystem writes unless --export/--export-dir is explicitly provided.
  - TLS verification is ON by default; use --tls-insecure at your own risk.
  - Resume enabled by default (Range + If-Range). Use --no-resume to disable.
  - Use --json for machine-readable output (final result on stdout; logs on stderr).)");
}

} // namespace yams::cli