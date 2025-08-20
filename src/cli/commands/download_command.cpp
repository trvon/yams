/*
 * yams/src/cli/commands/download_command.cpp
 *
 * Download command implementation (stub).
 *
 * Store-only by default:
 * - Downloads will be verified and finalized into YAMS CAS (content-addressed storage).
 * - No user filesystem writes unless --export or --export-dir is explicitly provided.
 *
 * This is a non-functional stub that validates arguments and outputs a placeholder result.
 * Wiring to the downloader implementation (libcurl-based) will be added next.
 */

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/core/types.h>
#include <yams/detection/file_type_detector.h>
#include <yams/downloader/downloader.hpp>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
// Daemon client API for daemon-first download
#include <fstream>
#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>

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
        // Validate inputs: either URL or --list must be provided, but not both.
        if (!url_ && !listPath_) {
            return Error{ErrorCode::InvalidArgument, "Either <url> or --list must be provided"};
        }
        if (url_ && listPath_) {
            return Error{ErrorCode::InvalidArgument, "Specify only one of <url> or --list"};
        }

        // Logging levels
        if (quiet_) {
            spdlog::set_level(spdlog::level::warn);
        } else {
            spdlog::set_level(spdlog::level::info);
        }

        // Local helpers for config v2 loading
        auto getConfigPath = []() -> fs::path {
            const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME");
            const char* homeEnv = std::getenv("HOME");
            fs::path configHome;
            if (xdgConfigHome) {
                configHome = fs::path(xdgConfigHome);
            } else if (homeEnv) {
                configHome = fs::path(homeEnv) / ".config";
            } else {
                return fs::path("~/.config") / "yams" / "config.toml";
            }
            return configHome / "yams" / "config.toml";
        };

        auto parseSimpleToml = [](const fs::path& path) -> std::map<std::string, std::string> {
            std::map<std::string, std::string> config;
            std::ifstream file(path);
            if (!file)
                return config;
            std::string line, currentSection;
            while (std::getline(file, line)) {
                if (line.empty() || line[0] == '#')
                    continue;
                // Trim left/right spaces
                line.erase(0, line.find_first_not_of(" \t"));
                if (line.empty() || line[0] == '#')
                    continue;
                // Section?
                if (line[0] == '[') {
                    size_t end = line.find(']');
                    if (end != std::string::npos) {
                        currentSection = line.substr(1, end - 1);
                        if (!currentSection.empty())
                            currentSection += ".";
                    }
                    continue;
                }
                size_t eq = line.find('=');
                if (eq != std::string::npos) {
                    std::string key = line.substr(0, eq);
                    std::string value = line.substr(eq + 1);
                    // Trim
                    key.erase(0, key.find_first_not_of(" \t"));
                    key.erase(key.find_last_not_of(" \t") + 1);
                    value.erase(0, value.find_first_not_of(" \t"));
                    value.erase(value.find_last_not_of(" \t") + 1);
                    // Remove inline comment
                    size_t comment = value.find('#');
                    if (comment != std::string::npos) {
                        value = value.substr(0, comment);
                        value.erase(value.find_last_not_of(" \t") + 1);
                    }
                    // Strip quotes
                    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
                        value = value.substr(1, value.size() - 2);
                    }
                    config[currentSection + key] = value;
                }
            }
            return config;
        };

        auto toInt = [](const std::string& s, int def) -> int {
            try {
                return s.empty() ? def : std::stoi(s);
            } catch (...) {
                return def;
            }
        };
        auto toU64 = [](const std::string& s, std::uint64_t def) -> std::uint64_t {
            try {
                return s.empty() ? def : static_cast<std::uint64_t>(std::stoll(s));
            } catch (...) {
                return def;
            }
        };
        auto toBool = [](const std::string& s, bool def) -> bool {
            if (s.empty())
                return def;
            std::string v = s;
            std::transform(v.begin(), v.end(), v.begin(), ::tolower);
            if (v == "true" || v == "1" || v == "yes" || v == "on")
                return true;
            if (v == "false" || v == "0" || v == "no" || v == "off")
                return false;
            return def;
        };

        // Load config v2
        const fs::path cfgPath = getConfigPath();
        const auto cfg = parseSimpleToml(cfgPath);

        // Resolve storage base path and CAS directories
        fs::path basePath = cli_ ? cli_->getDataPath() : fs::current_path();
        if (auto it = cfg.find("storage.base_path"); it != cfg.end() && !it->second.empty()) {
            basePath = fs::path(it->second);
        }
        std::string objectsDir = "objects";
        if (auto it = cfg.find("storage.objects_dir"); it != cfg.end() && !it->second.empty()) {
            objectsDir = it->second;
        }
        std::string stagingDir = "staging";
        if (auto it = cfg.find("storage.staging_dir"); it != cfg.end() && !it->second.empty()) {
            stagingDir = it->second;
        }

        yams::downloader::StorageConfig storageCfg{.objectsDir = fs::path(basePath) / objectsDir,
                                                   .stagingDir = fs::path(basePath) / stagingDir};

        // Downloader defaults from config
        yams::downloader::DownloaderConfig dlCfg{};
        dlCfg.defaultConcurrency = toInt(cfg.count("downloader.default_concurrency")
                                             ? cfg.at("downloader.default_concurrency")
                                             : "",
                                         4);
        dlCfg.defaultChunkSizeBytes = toU64(cfg.count("downloader.default_chunk_size_bytes")
                                                ? cfg.at("downloader.default_chunk_size_bytes")
                                                : "",
                                            8ull * 1024ull * 1024ull);
        dlCfg.defaultTimeout = std::chrono::milliseconds(toInt(
            cfg.count("downloader.default_timeout_ms") ? cfg.at("downloader.default_timeout_ms")
                                                       : "",
            60000));
        dlCfg.followRedirects = toBool(
            cfg.count("downloader.follow_redirects") ? cfg.at("downloader.follow_redirects") : "",
            true);
        dlCfg.resume =
            toBool(cfg.count("downloader.resume") ? cfg.at("downloader.resume") : "", true);
        dlCfg.rateLimit.globalBps = toU64(cfg.count("downloader.rate_limit_global_bps")
                                              ? cfg.at("downloader.rate_limit_global_bps")
                                              : "",
                                          0);
        dlCfg.rateLimit.perConnectionBps = toU64(cfg.count("downloader.rate_limit_per_conn_bps")
                                                     ? cfg.at("downloader.rate_limit_per_conn_bps")
                                                     : "",
                                                 0);
        dlCfg.defaultChecksumAlgo = yams::downloader::HashAlgo::Sha256; // from config if needed
        dlCfg.storeOnly =
            toBool(cfg.count("downloader.store_only") ? cfg.at("downloader.store_only") : "", true);
        dlCfg.maxFileBytes = toU64(
            cfg.count("downloader.max_file_bytes") ? cfg.at("downloader.max_file_bytes") : "", 0);

        // TLS and proxy
        bool cfgTlsVerify =
            toBool(cfg.count("downloader.tls.verify") ? cfg.at("downloader.tls.verify") : "", true);
        std::string cfgCaPath =
            cfg.count("downloader.tls.ca_path") ? cfg.at("downloader.tls.ca_path") : "";
        std::string cfgProxy =
            cfg.count("downloader.proxy.url") ? cfg.at("downloader.proxy.url") : "";

        // Summarize intent (only if not quiet)
        if (!quiet_) {
            spdlog::info("yams download - store-only default enabled");
            if (url_) {
                spdlog::info("URL: {}", *url_);
            } else {
                spdlog::info("Manifest: {}", listPath_->string());
            }
            spdlog::info("Concurrency: {}, ChunkSize: {} bytes, Timeout: {} ms", concurrency_,
                         chunkSize_, timeoutMs_);
            spdlog::info("TLS verify: {}, CA: '{}'",
                         (tlsInsecure_ ? "OFF" : (cfgTlsVerify ? "ON" : "OFF")),
                         tlsCaPath_.value_or(cfgCaPath));
        }

        if (!quiet_) {
            if (exportPath_)
                spdlog::info("Export path requested: {}", exportPath_->string());
            if (exportDir_)
                spdlog::info("Export dir requested: {}", exportDir_->string());
        }

        // Build request (single URL MVP)
        if (!url_) {
            // Batch mode: --list supplied. TODO(concurrency): process in parallel.
            if (!listPath_ || !std::filesystem::exists(*listPath_)) {
                return Error{ErrorCode::FileNotFound, "List file not found or not specified"};
            }

            // Prepare manager
            namespace dl = yams::downloader;
            std::unique_ptr<dl::IDownloadManager> mgr = dl::makeDownloadManager(storageCfg, dlCfg);

            // Read manifest (one URL per line; ignore blanks and comments)
            std::ifstream in(*listPath_);
            if (!in) {
                return Error{ErrorCode::IOError, "Failed to open list file"};
            }

            auto progressCb = [&](const dl::ProgressEvent& ev) {
                if (progress_ == "json") {
                    json pj;
                    pj["type"] = "progress";
                    pj["url"] = ev.url;
                    pj["downloaded_bytes"] = ev.downloadedBytes;
                    if (ev.totalBytes)
                        pj["total_bytes"] = *ev.totalBytes;
                    if (ev.percentage)
                        pj["percentage"] = *ev.percentage;
                    std::cerr << pj.dump() << std::endl;
                } else if (progress_ == "human" && !quiet_) {
                    if (ev.percentage) {
                        spdlog::info("Downloaded: {} bytes ({:.2f}%) [{}]", ev.downloadedBytes,
                                     *ev.percentage, ev.url);
                    } else {
                        spdlog::info("Downloaded: {} bytes [{}]", ev.downloadedBytes, ev.url);
                    }
                }
            };
            auto shouldCancel = []() { return false; };
            auto logCb = [](std::string_view msg) { spdlog::debug("downloader: {}", msg); };

            std::string line;
            while (std::getline(in, line)) {
                // Trim whitespace
                auto start = line.find_first_not_of(" \t\r\n");
                if (start == std::string::npos)
                    continue;
                auto end = line.find_last_not_of(" \t\r\n");
                std::string u = line.substr(start, end - start + 1);
                if (u.empty() || u[0] == '#')
                    continue;

                dl::DownloadRequest req{};
                req.url = u;

                // Headers
                for (const auto& h : headers_) {
                    auto pos = h.find(':');
                    if (pos != std::string::npos) {
                        dl::Header hh;
                        hh.name = std::string(h.substr(0, pos));
                        std::string val = h.substr(pos + 1);
                        if (!val.empty() && val.front() == ' ')
                            val.erase(0, 1);
                        hh.value = std::move(val);
                        req.headers.push_back(std::move(hh));
                    }
                }
                // Checksum (applies same value to all, if provided)
                if (checksum_) {
                    auto pos = checksum_->find(':');
                    if (pos != std::string::npos) {
                        dl::Checksum cs;
                        auto algo = checksum_->substr(0, pos);
                        auto hex = checksum_->substr(pos + 1);
                        std::string al = algo;
                        std::transform(al.begin(), al.end(), al.begin(), ::tolower);
                        if (al == "sha256")
                            cs.algo = dl::HashAlgo::Sha256;
                        else if (al == "sha512")
                            cs.algo = dl::HashAlgo::Sha512;
                        else if (al == "md5")
                            cs.algo = dl::HashAlgo::Md5;
                        cs.hex = hex;
                        req.checksum = cs;
                    }
                }

                // Performance/policies
                req.concurrency = concurrency_;
                req.chunkSizeBytes = chunkSize_;
                req.timeout = std::chrono::milliseconds(timeoutMs_);
                req.retry.maxAttempts = retryAttempts_;
                req.retry.initialBackoff = std::chrono::milliseconds(backoffMs_);
                req.retry.multiplier = backoffMult_;
                req.retry.maxBackoff = std::chrono::milliseconds(maxBackoffMs_);
                req.rateLimit.globalBps = rateLimitGlobalBps_;
                req.rateLimit.perConnectionBps = rateLimitPerConnBps_;
                req.resume = !noResume_;
                req.proxy = proxy_.has_value()
                                ? proxy_
                                : (cfgProxy.empty() ? std::optional<std::string>{}
                                                    : std::optional<std::string>{cfgProxy});
                req.tls.insecure = tlsInsecure_;
                req.tls.caPath = tlsCaPath_.value_or(cfgCaPath);
                req.followRedirects = !noFollowRedirects_ && dlCfg.followRedirects;
                req.storeOnly = true;
                // Optional export (single URL): pass through to manager; TODO(export):
                // IfDifferentEtag policy enforcement happens in manager
                if (exportPath_) {
                    req.exportPath = *exportPath_;
                }

                // Export handling for batch: derive filename from URL if --export-dir provided
                if (exportDir_) {
                    try {
                        std::string name = u;
                        // crude extraction of path tail
                        auto slash = name.find_last_of('/');
                        if (slash != std::string::npos && slash + 1 < name.size()) {
                            name = name.substr(slash + 1);
                        }
                        if (name.empty())
                            name = "download.bin";
                        std::filesystem::path out = *exportDir_ / name;
                        req.exportPath = out;
                    } catch (...) {
                        // ignore export path errors here; manager will report on copy failure
                    }
                }

                auto t0 = std::chrono::steady_clock::now();
                auto res = mgr->download(req, progressCb, shouldCancel, logCb);
                auto t1 = std::chrono::steady_clock::now();
                auto elapsed_ms =
                    std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

                if (!res.ok()) {
                    const auto& err = res.error();
                    json out = {
                        {"type", "result"},
                        {"url", req.url},
                        {"success", false},
                        {"error", {{"code", static_cast<int>(err.code)}, {"message", err.message}}},
                        {"elapsed_ms", elapsed_ms}};
                    if (jsonOutput_)
                        std::cout << out.dump() << std::endl;
                    else
                        std::cout << out.dump(2) << std::endl;
                    continue;
                }

                const auto& fr = res.value();

                // Always log this to verify our code is reached
                spdlog::warn("METADATA_TEST: Download completed, success={}, cli_ptr={}",
                             fr.success, static_cast<void*>(cli_));

                // Insert metadata for the downloaded file (same as single file mode)
                if (fr.success && cli_) {
                    spdlog::debug("Download successful, attempting to insert metadata");
                    auto metadataRepo = cli_->getMetadataRepository();
                    if (metadataRepo) {
                        spdlog::debug("Metadata repository available, creating document info");
                        metadata::DocumentInfo docInfo;

                        // Extract filename from URL (last segment after /)
                        std::string filename = fr.url;
                        auto lastSlash = filename.find_last_of('/');
                        if (lastSlash != std::string::npos) {
                            filename = filename.substr(lastSlash + 1);
                        }
                        // Remove query parameters if present
                        auto questionMark = filename.find('?');
                        if (questionMark != std::string::npos) {
                            filename = filename.substr(0, questionMark);
                        }
                        if (filename.empty()) {
                            filename = "downloaded_file";
                        }

                        docInfo.filePath = fr.url; // Use URL as path
                        docInfo.fileName = filename;
                        docInfo.fileExtension = "";
                        auto dotPos = filename.rfind('.');
                        if (dotPos != std::string::npos) {
                            docInfo.fileExtension = filename.substr(dotPos);
                        }
                        docInfo.fileSize = static_cast<int64_t>(fr.sizeBytes);

                        // Remove "sha256:" prefix from hash if present
                        std::string hashStr = fr.hash;
                        if (hashStr.substr(0, 7) == "sha256:") {
                            hashStr = hashStr.substr(7);
                        }
                        docInfo.sha256Hash = hashStr;

                        // Try to detect MIME type from extension
                        docInfo.mimeType = detection::FileTypeDetector::getMimeTypeFromExtension(
                            docInfo.fileExtension);

                        auto now = std::chrono::system_clock::now();
                        docInfo.createdTime = now;
                        docInfo.modifiedTime = now;
                        docInfo.indexedTime = now;

                        spdlog::debug("Inserting document with hash: {}, filename: {}, size: {}",
                                      docInfo.sha256Hash, docInfo.fileName, docInfo.fileSize);
                        auto insertResult = metadataRepo->insertDocument(docInfo);
                        if (insertResult) {
                            int64_t docId = insertResult.value();
                            spdlog::debug("Successfully inserted document with ID: {}", docId);

                            // Add metadata about the download
                            metadataRepo->setMetadata(docId, "source_url",
                                                      metadata::MetadataValue(fr.url));
                            metadataRepo->setMetadata(
                                docId, "download_date",
                                metadata::MetadataValue(
                                    std::chrono::duration_cast<std::chrono::seconds>(
                                        now.time_since_epoch())
                                        .count()));

                            if (fr.etag) {
                                metadataRepo->setMetadata(docId, "etag",
                                                          metadata::MetadataValue(*fr.etag));
                            }
                            if (fr.lastModified) {
                                metadataRepo->setMetadata(
                                    docId, "last_modified",
                                    metadata::MetadataValue(*fr.lastModified));
                            }

                            // Add a tag to identify downloaded files
                            metadataRepo->setMetadata(docId, "tag",
                                                      metadata::MetadataValue("downloaded"));

                            if (!quiet_) {
                                spdlog::debug("Stored download metadata with document ID: {}",
                                              docId);
                            }
                        } else {
                            spdlog::warn("Failed to store download metadata: {}",
                                         insertResult.error().message);
                        }
                    } else {
                        spdlog::debug(
                            "Metadata repository not available, skipping metadata insertion");
                    }
                } else {
                    if (!fr.success) {
                        spdlog::debug("Download not successful, skipping metadata insertion");
                    }
                    if (!cli_) {
                        spdlog::debug("CLI context not available, skipping metadata insertion");
                    }
                }

                json out = {
                    {"type", "result"},
                    {"url", fr.url},
                    {"hash", fr.hash},
                    {"stored_path", fr.storedPath.string()},
                    {"size_bytes", fr.sizeBytes},
                    {"success", fr.success},
                    {"http_status", fr.httpStatus ? json(*fr.httpStatus) : json(nullptr)},
                    {"etag", fr.etag ? json(*fr.etag) : json(nullptr)},
                    {"last_modified", fr.lastModified ? json(*fr.lastModified) : json(nullptr)},
                    {"checksum_ok", fr.checksumOk ? json(*fr.checksumOk) : json(nullptr)},
                    {"elapsed_ms", elapsed_ms}};
                if (jsonOutput_)
                    std::cout << out.dump() << std::endl;
                else
                    std::cout << out.dump(2) << std::endl;
            }

            return Result<void>();
        }

        // Single URL download: use daemon-first approach which includes fallback
        return tryDaemonDownload(*url_);
    }

private:
    // Perform local download for single URL (extracted from execute())
    Result<void> performLocalDownload(const std::string& url) {
        // Local helpers for config v2 loading (same as in execute())
        auto getConfigPath = []() -> fs::path {
            const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME");
            const char* homeEnv = std::getenv("HOME");
            fs::path configHome;
            if (xdgConfigHome) {
                configHome = fs::path(xdgConfigHome);
            } else if (homeEnv) {
                configHome = fs::path(homeEnv) / ".config";
            } else {
                return fs::path("~/.config") / "yams" / "config.toml";
            }
            return configHome / "yams" / "config.toml";
        };

        auto parseSimpleToml = [](const fs::path& path) -> std::map<std::string, std::string> {
            std::map<std::string, std::string> config;
            std::ifstream file(path);
            if (!file)
                return config;
            std::string line, currentSection;
            while (std::getline(file, line)) {
                if (line.empty() || line[0] == '#')
                    continue;
                line.erase(0, line.find_first_not_of(" \t"));
                if (line.empty() || line[0] == '#')
                    continue;
                if (line[0] == '[') {
                    size_t end = line.find(']');
                    if (end != std::string::npos) {
                        currentSection = line.substr(1, end - 1);
                        if (!currentSection.empty())
                            currentSection += ".";
                    }
                    continue;
                }
                size_t eq = line.find('=');
                if (eq != std::string::npos) {
                    std::string key = line.substr(0, eq);
                    std::string value = line.substr(eq + 1);
                    key.erase(0, key.find_first_not_of(" \t"));
                    key.erase(key.find_last_not_of(" \t") + 1);
                    value.erase(0, value.find_first_not_of(" \t"));
                    value.erase(value.find_last_not_of(" \t") + 1);
                    size_t comment = value.find('#');
                    if (comment != std::string::npos) {
                        value = value.substr(0, comment);
                        value.erase(value.find_last_not_of(" \t") + 1);
                    }
                    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
                        value = value.substr(1, value.size() - 2);
                    }
                    config[currentSection + key] = value;
                }
            }
            return config;
        };

        auto toInt = [](const std::string& s, int def) -> int {
            try {
                return s.empty() ? def : std::stoi(s);
            } catch (...) {
                return def;
            }
        };
        auto toU64 = [](const std::string& s, std::uint64_t def) -> std::uint64_t {
            try {
                return s.empty() ? def : static_cast<std::uint64_t>(std::stoll(s));
            } catch (...) {
                return def;
            }
        };
        auto toBool = [](const std::string& s, bool def) -> bool {
            if (s.empty())
                return def;
            std::string v = s;
            std::transform(v.begin(), v.end(), v.begin(), ::tolower);
            if (v == "true" || v == "1" || v == "yes" || v == "on")
                return true;
            if (v == "false" || v == "0" || v == "no" || v == "off")
                return false;
            return def;
        };

        // Load config v2
        const fs::path cfgPath = getConfigPath();
        const auto cfg = parseSimpleToml(cfgPath);

        // Resolve storage base path and CAS directories
        fs::path basePath = cli_ ? cli_->getDataPath() : fs::current_path();
        if (auto it = cfg.find("storage.base_path"); it != cfg.end() && !it->second.empty()) {
            basePath = fs::path(it->second);
        }
        std::string objectsDir = "objects";
        if (auto it = cfg.find("storage.objects_dir"); it != cfg.end() && !it->second.empty()) {
            objectsDir = it->second;
        }
        std::string stagingDir = "staging";
        if (auto it = cfg.find("storage.staging_dir"); it != cfg.end() && !it->second.empty()) {
            stagingDir = it->second;
        }

        yams::downloader::StorageConfig storageCfg{.objectsDir = fs::path(basePath) / objectsDir,
                                                   .stagingDir = fs::path(basePath) / stagingDir};

        // Downloader defaults from config
        yams::downloader::DownloaderConfig dlCfg{};
        dlCfg.defaultConcurrency = toInt(cfg.count("downloader.default_concurrency")
                                             ? cfg.at("downloader.default_concurrency")
                                             : "",
                                         4);
        dlCfg.defaultChunkSizeBytes = toU64(cfg.count("downloader.default_chunk_size_bytes")
                                                ? cfg.at("downloader.default_chunk_size_bytes")
                                                : "",
                                            8ull * 1024ull * 1024ull);
        dlCfg.defaultTimeout = std::chrono::milliseconds(toInt(
            cfg.count("downloader.default_timeout_ms") ? cfg.at("downloader.default_timeout_ms")
                                                       : "",
            60000));
        dlCfg.followRedirects = toBool(
            cfg.count("downloader.follow_redirects") ? cfg.at("downloader.follow_redirects") : "",
            true);
        dlCfg.resume =
            toBool(cfg.count("downloader.resume") ? cfg.at("downloader.resume") : "", true);
        dlCfg.rateLimit.globalBps = toU64(cfg.count("downloader.rate_limit_global_bps")
                                              ? cfg.at("downloader.rate_limit_global_bps")
                                              : "",
                                          0);
        dlCfg.rateLimit.perConnectionBps = toU64(cfg.count("downloader.rate_limit_per_conn_bps")
                                                     ? cfg.at("downloader.rate_limit_per_conn_bps")
                                                     : "",
                                                 0);
        dlCfg.defaultChecksumAlgo = yams::downloader::HashAlgo::Sha256;
        dlCfg.storeOnly =
            toBool(cfg.count("downloader.store_only") ? cfg.at("downloader.store_only") : "", true);
        dlCfg.maxFileBytes = toU64(
            cfg.count("downloader.max_file_bytes") ? cfg.at("downloader.max_file_bytes") : "", 0);

        // TLS and proxy
        bool cfgTlsVerify =
            toBool(cfg.count("downloader.tls.verify") ? cfg.at("downloader.tls.verify") : "", true);
        std::string cfgCaPath =
            cfg.count("downloader.tls.ca_path") ? cfg.at("downloader.tls.ca_path") : "";
        std::string cfgProxy =
            cfg.count("downloader.proxy.url") ? cfg.at("downloader.proxy.url") : "";

        yams::downloader::DownloadRequest req{};
        req.url = url;
        // Headers: "Key: Value"
        for (const auto& h : headers_) {
            auto pos = h.find(':');
            if (pos != std::string::npos) {
                yams::downloader::Header hh;
                hh.name = std::string(h.substr(0, pos));
                std::string val = h.substr(pos + 1);
                if (!val.empty() && val.front() == ' ')
                    val.erase(0, 1);
                hh.value = std::move(val);
                req.headers.push_back(std::move(hh));
            }
        }
        // Checksum
        if (checksum_) {
            auto pos = checksum_->find(':');
            if (pos != std::string::npos) {
                yams::downloader::Checksum cs;
                auto algo = checksum_->substr(0, pos);
                auto hex = checksum_->substr(pos + 1);
                std::string al = algo;
                std::transform(al.begin(), al.end(), al.begin(), ::tolower);
                if (al == "sha256")
                    cs.algo = yams::downloader::HashAlgo::Sha256;
                else if (al == "sha512")
                    cs.algo = yams::downloader::HashAlgo::Sha512;
                else if (al == "md5")
                    cs.algo = yams::downloader::HashAlgo::Md5;
                cs.hex = hex;
                req.checksum = cs;
            }
        }
        // Performance and policies
        req.concurrency = concurrency_;
        req.chunkSizeBytes = chunkSize_;
        req.timeout = std::chrono::milliseconds(timeoutMs_);
        req.retry.maxAttempts = retryAttempts_;
        req.retry.initialBackoff = std::chrono::milliseconds(backoffMs_);
        req.retry.multiplier = backoffMult_;
        req.retry.maxBackoff = std::chrono::milliseconds(maxBackoffMs_);
        req.rateLimit.globalBps = rateLimitGlobalBps_;
        req.rateLimit.perConnectionBps = rateLimitPerConnBps_;
        req.resume = !noResume_;
        req.proxy = proxy_.has_value() ? proxy_
                                       : (cfgProxy.empty() ? std::optional<std::string>{}
                                                           : std::optional<std::string>{cfgProxy});
        req.tls.insecure = tlsInsecure_;
        req.tls.caPath = tlsCaPath_.value_or(cfgCaPath);
        req.followRedirects = !noFollowRedirects_ && dlCfg.followRedirects;
        req.storeOnly = true;

        // Progress callback
        auto progressCb = [&](const yams::downloader::ProgressEvent& ev) {
            if (progress_ == "json") {
                json pj;
                pj["type"] = "progress";
                pj["url"] = ev.url;
                pj["downloaded_bytes"] = ev.downloadedBytes;
                if (ev.totalBytes)
                    pj["total_bytes"] = *ev.totalBytes;
                if (ev.percentage)
                    pj["percentage"] = *ev.percentage;
                std::cerr << pj.dump() << std::endl;
            } else if (progress_ == "human" && !quiet_) {
                if (ev.percentage) {
                    spdlog::info("Downloaded: {} bytes ({:.2f}%)", ev.downloadedBytes,
                                 *ev.percentage);
                } else {
                    spdlog::info("Downloaded: {} bytes", ev.downloadedBytes);
                }
            }
        };

        auto shouldCancel = []() -> bool { return false; };
        auto logCb = [](std::string_view msg) { spdlog::debug("downloader: {}", msg); };

        namespace dl = yams::downloader;
        std::unique_ptr<dl::IDownloadManager> mgr = dl::makeDownloadManager(storageCfg, dlCfg);

        auto t0 = std::chrono::steady_clock::now();
        auto res = mgr->download(req, progressCb, shouldCancel, logCb);
        auto t1 = std::chrono::steady_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

        if (!res.ok()) {
            const auto& err = res.error();
            json out = {{"type", "result"},
                        {"url", req.url},
                        {"success", false},
                        {"error", {{"code", static_cast<int>(err.code)}, {"message", err.message}}},
                        {"elapsed_ms", elapsed_ms}};
            if (jsonOutput_)
                std::cout << out.dump() << std::endl;
            else
                std::cout << out.dump(2) << std::endl;
            return Error{ErrorCode::Unknown, err.message};
        }

        const auto& fr = res.value();

        // Insert metadata for the downloaded file if successful and cli is available
        if (fr.success && cli_) {
            auto metadataRepo = cli_->getMetadataRepository();
            if (metadataRepo) {
                metadata::DocumentInfo docInfo;

                // Extract filename from URL
                std::string filename = fr.url;
                auto lastSlash = filename.find_last_of('/');
                if (lastSlash != std::string::npos) {
                    filename = filename.substr(lastSlash + 1);
                }
                auto questionMark = filename.find('?');
                if (questionMark != std::string::npos) {
                    filename = filename.substr(0, questionMark);
                }
                if (filename.empty()) {
                    filename = "downloaded_file";
                }

                docInfo.filePath = fr.url;
                docInfo.fileName = filename;
                docInfo.fileExtension = "";
                auto dotPos = filename.rfind('.');
                if (dotPos != std::string::npos) {
                    docInfo.fileExtension = filename.substr(dotPos);
                }
                docInfo.fileSize = static_cast<int64_t>(fr.sizeBytes);

                // Remove "sha256:" prefix from hash if present
                std::string hashStr = fr.hash;
                if (hashStr.substr(0, 7) == "sha256:") {
                    hashStr = hashStr.substr(7);
                }
                docInfo.sha256Hash = hashStr;

                docInfo.mimeType =
                    detection::FileTypeDetector::getMimeTypeFromExtension(docInfo.fileExtension);

                auto now = std::chrono::system_clock::now();
                docInfo.createdTime = now;
                docInfo.modifiedTime = now;
                docInfo.indexedTime = now;

                auto insertResult = metadataRepo->insertDocument(docInfo);
                if (insertResult) {
                    int64_t docId = insertResult.value();
                    metadataRepo->setMetadata(docId, "source_url", metadata::MetadataValue(fr.url));
                    metadataRepo->setMetadata(
                        docId, "download_date",
                        metadata::MetadataValue(
                            std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch())
                                .count()));

                    if (fr.etag) {
                        metadataRepo->setMetadata(docId, "etag", metadata::MetadataValue(*fr.etag));
                    }
                    if (fr.lastModified) {
                        metadataRepo->setMetadata(docId, "last_modified",
                                                  metadata::MetadataValue(*fr.lastModified));
                    }

                    metadataRepo->setMetadata(docId, "tag", metadata::MetadataValue("downloaded"));

                    if (!quiet_) {
                        spdlog::debug("Stored download metadata with document ID: {}", docId);
                    }
                } else {
                    spdlog::warn("Failed to store download metadata: {}",
                                 insertResult.error().message);
                }
            }
        }

        // Log download completion
        if (!quiet_) {
            if (fr.success) {
                spdlog::info("Downloaded {} -> {} ({} bytes)", fr.url,
                             fr.hash.substr(0, 16) + "...", fr.sizeBytes);
            } else {
                spdlog::error("Download failed: {}", fr.url);
            }
        }

        json out = {{"type", "result"},
                    {"url", fr.url},
                    {"hash", fr.hash},
                    {"stored_path", fr.storedPath.string()},
                    {"size_bytes", fr.sizeBytes},
                    {"success", fr.success},
                    {"http_status", fr.httpStatus ? json(*fr.httpStatus) : json(nullptr)},
                    {"etag", fr.etag ? json(*fr.etag) : json(nullptr)},
                    {"last_modified", fr.lastModified ? json(*fr.lastModified) : json(nullptr)},
                    {"checksum_ok", fr.checksumOk ? json(*fr.checksumOk) : json(nullptr)},
                    {"elapsed_ms", elapsed_ms}};
        if (jsonOutput_)
            std::cout << out.dump() << std::endl;
        else
            std::cout << out.dump(2) << std::endl;

        return Result<void>();
    }

    // Try daemon-first download for a single URL
    Result<void> tryDaemonDownload(const std::string& url) {
        try {
            yams::daemon::DownloadRequest dreq;
            dreq.url = url;
            dreq.outputPath = ""; // Let daemon determine the name
            dreq.quiet = quiet_;
            // Note: tags and metadata would need to be added here if we supported them in the CLI

            auto render = [&](const yams::daemon::DownloadResponse& resp) -> Result<void> {
                if (!resp.success) {
                    spdlog::error("Download failed: {}", resp.error);
                    return Error{ErrorCode::NetworkError, resp.error};
                }

                // Display results
                if (!quiet_) {
                    spdlog::info("Downloaded {} -> {} ({})", resp.url,
                                 resp.hash.substr(0, 16) + "...", resp.localPath);
                }

                if (jsonOutput_) {
                    json out = {{"url", resp.url},
                                {"hash", resp.hash},
                                {"local_path", resp.localPath},
                                {"size_bytes", resp.size},
                                {"success", resp.success}};
                    std::cout << out.dump() << std::endl;
                }

                return Result<void>();
            };

            auto fallback = [&]() -> Result<void> {
                spdlog::debug("Daemon unavailable, falling back to local download");
                return performLocalDownload(url);
            };

            if (auto d = daemon_first(dreq, fallback, render); d) {
                return Result<void>();
            } else {
                return d;
            }

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Daemon download failed: ") + e.what()};
        }
    }

    // CLI context
    YamsCLI* cli_{nullptr};

    // Inputs
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

    // Export policy (applies only if export is requested)
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