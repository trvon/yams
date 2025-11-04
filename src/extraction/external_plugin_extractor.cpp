// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 Trevon Sides

#include "yams/extraction/external_plugin_extractor.hpp"
#include "yams/extraction/jsonrpc_client.hpp"
#include "yams/extraction/plugin_process.hpp"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>

using json = nlohmann::json;

namespace yams::extraction {

//==============================================================================
// ExternalPluginExtractorConfig implementation
//==============================================================================

auto ExternalPluginExtractorConfig::with_executable(std::string path)
    -> ExternalPluginExtractorConfig& {
    executable_ = std::move(path);
    return *this;
}

auto ExternalPluginExtractorConfig::with_arg(std::string arg) -> ExternalPluginExtractorConfig& {
    args_.push_back(std::move(arg));
    return *this;
}

auto ExternalPluginExtractorConfig::with_args(std::vector<std::string> args)
    -> ExternalPluginExtractorConfig& {
    args_.insert(args_.end(), std::make_move_iterator(args.begin()),
                 std::make_move_iterator(args.end()));
    return *this;
}

auto ExternalPluginExtractorConfig::with_env(std::string key, std::string value)
    -> ExternalPluginExtractorConfig& {
    env_[std::move(key)] = std::move(value);
    return *this;
}

auto ExternalPluginExtractorConfig::in_directory(std::string dir)
    -> ExternalPluginExtractorConfig& {
    working_directory_ = std::move(dir);
    return *this;
}

auto ExternalPluginExtractorConfig::with_timeout(std::chrono::milliseconds timeout)
    -> ExternalPluginExtractorConfig& {
    timeout_ = timeout;
    return *this;
}

auto ExternalPluginExtractorConfig::with_startup_timeout(std::chrono::milliseconds timeout)
    -> ExternalPluginExtractorConfig& {
    startup_timeout_ = timeout;
    return *this;
}

auto ExternalPluginExtractorConfig::executable() const -> const std::string& {
    return executable_;
}

auto ExternalPluginExtractorConfig::args() const -> const std::vector<std::string>& {
    return args_;
}

auto ExternalPluginExtractorConfig::env() const
    -> const std::unordered_map<std::string, std::string>& {
    return env_;
}

auto ExternalPluginExtractorConfig::working_directory() const -> const std::string& {
    return working_directory_;
}

auto ExternalPluginExtractorConfig::timeout() const -> std::chrono::milliseconds {
    return timeout_;
}

auto ExternalPluginExtractorConfig::startup_timeout() const -> std::chrono::milliseconds {
    return startup_timeout_;
}

//==============================================================================
// ExternalPluginExtractor::Impl
//==============================================================================

class ExternalPluginExtractor::Impl {
public:
    explicit Impl(ExternalPluginExtractorConfig config) : config_(std::move(config)) {}

    auto init() -> Result<void> {
        if (is_initialized_) {
            return Result<void>();
        }

        try {
            // Build process config
            PluginProcessConfig proc_config{.executable = config_.executable(),
                                            .args = config_.args(),
                                            .env = config_.env(),
                                            .workdir =
                                                config_.working_directory().empty()
                                                    ? std::nullopt
                                                    : std::optional{config_.working_directory()},
                                            .init_timeout = config_.startup_timeout(),
                                            .rpc_timeout = config_.timeout()};

            // Create and spawn process
            process_ = std::make_unique<PluginProcess>(std::move(proc_config));

            // Create RPC client
            rpc_client_ = std::make_unique<JsonRpcClient>(*process_);

            // Handshake
            auto handshake_result = rpc_client_->call("handshake.manifest");
            if (!handshake_result) {
                return Result<void>(Error{ErrorCode::IOError, "Handshake failed"});
            }

            // Parse manifest
            auto manifest_json = handshake_result.value();
            manifest_.name = manifest_json.value("name", "unknown");
            manifest_.version = manifest_json.value("version", "0.0.0");
            manifest_.description = manifest_json.value("description", "");

            // Parse supported MIME types from capabilities (preferred) or direct list (legacy)
            if (manifest_json.contains("capabilities") &&
                manifest_json["capabilities"].contains("content_extraction") &&
                manifest_json["capabilities"]["content_extraction"].contains("formats")) {
                for (const auto& mime :
                     manifest_json["capabilities"]["content_extraction"]["formats"]) {
                    manifest_.supported_mime_types.push_back(mime.get<std::string>());
                }
            } else if (manifest_json.contains("supported_mime_types")) {
                for (const auto& mime : manifest_json["supported_mime_types"]) {
                    manifest_.supported_mime_types.push_back(mime.get<std::string>());
                }
            }

            // Call plugin.init
            auto init_result = rpc_client_->call("plugin.init");
            if (!init_result) {
                return Result<void>(Error{ErrorCode::IOError, "Plugin init failed"});
            }

            is_initialized_ = true;
            spdlog::info("External plugin '{}' v{} initialized", manifest_.name, manifest_.version);
            return Result<void>();

        } catch (const std::exception& ex) {
            return Result<void>(Error{ErrorCode::InternalError,
                                      std::string("Failed to initialize plugin: ") + ex.what()});
        }
    }

    auto health_check() -> Result<void> {
        if (!is_initialized_) {
            return Result<void>(Error{ErrorCode::InvalidState, "Plugin not initialized"});
        }

        auto result = rpc_client_->call("plugin.health");
        if (!result) {
            return Result<void>(Error{ErrorCode::IOError, "Health check failed"});
        }

        auto health_json = result.value();
        std::string status = health_json.value("status", "unknown");

        // Accept both "healthy" and "ok" as valid health statuses
        if (status != "healthy" && status != "ok") {
            return Result<void>(
                Error{ErrorCode::InvalidState, "Plugin reported unhealthy status: " + status});
        }

        return Result<void>();
    }

    auto shutdown() -> Result<void> {
        if (!is_initialized_) {
            return Result<void>();
        }

        // Try graceful shutdown
        if (rpc_client_) {
            auto result = rpc_client_->call("plugin.shutdown");
            if (!result) {
                spdlog::warn("Plugin shutdown RPC failed");
            }
        }

        // Process destructor will handle termination
        is_initialized_ = false;
        rpc_client_.reset();
        process_.reset();

        spdlog::info("External plugin '{}' shut down", manifest_.name);
        return Result<void>();
    }

    [[nodiscard]] auto manifest() const -> const PluginManifest& { return manifest_; }

    [[nodiscard]] auto is_ready() const -> bool {
        return is_initialized_ && process_ && process_->is_alive();
    }

    [[nodiscard]] auto supports(const std::string& mime, const std::string& /* extension */) const
        -> bool {
        if (!is_initialized_) {
            return false;
        }

        // Check cached manifest first
        if (std::find(manifest_.supported_mime_types.begin(), manifest_.supported_mime_types.end(),
                      mime) != manifest_.supported_mime_types.end()) {
            return true;
        }

        return false;
    }

    [[nodiscard]] auto extractText(const std::vector<std::byte>& bytes, const std::string& mime,
                                   const std::string& /* extension */)
        -> std::optional<std::string> {
        if (!is_initialized_) {
            spdlog::error("Plugin not initialized");
            return std::nullopt;
        }

        // Build request params
        json params;
        params["content"] = json::binary(
            std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(bytes.data()),
                                 reinterpret_cast<const uint8_t*>(bytes.data()) + bytes.size()));
        params["mime_type"] = mime;
        params["options"] = {{"extract_metadata", true}, {"max_text_size", 1000000}};

        // Call extractor.extract
        auto result = rpc_client_->call("extractor.extract", params);
        if (!result) {
            spdlog::error("Extraction failed");
            return std::nullopt;
        }

        auto extract_json = result.value();

        // Extract text field
        if (!extract_json.contains("text")) {
            spdlog::error("Extraction response missing 'text' field");
            return std::nullopt;
        }

        return extract_json["text"].get<std::string>();
    }

private:
    ExternalPluginExtractorConfig config_;
    std::unique_ptr<PluginProcess> process_;
    std::unique_ptr<JsonRpcClient> rpc_client_;
    PluginManifest manifest_;
    bool is_initialized_ = false;
};

//==============================================================================
// ExternalPluginExtractor implementation
//==============================================================================

ExternalPluginExtractor::ExternalPluginExtractor(ExternalPluginExtractorConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

ExternalPluginExtractor::~ExternalPluginExtractor() {
    if (impl_) {
        auto result = impl_->shutdown();
        if (!result.has_value()) {
            spdlog::warn("Failed to shutdown plugin in destructor: {}", result.error().message);
        }
    }
}

ExternalPluginExtractor::ExternalPluginExtractor(ExternalPluginExtractor&&) noexcept = default;
auto ExternalPluginExtractor::operator=(ExternalPluginExtractor&&) noexcept
    -> ExternalPluginExtractor& = default;

auto ExternalPluginExtractor::init() -> Result<void> {
    return impl_->init();
}

auto ExternalPluginExtractor::health_check() -> Result<void> {
    return impl_->health_check();
}

auto ExternalPluginExtractor::shutdown() -> Result<void> {
    return impl_->shutdown();
}

auto ExternalPluginExtractor::manifest() const -> const PluginManifest& {
    return impl_->manifest();
}

auto ExternalPluginExtractor::is_ready() const -> bool {
    return impl_->is_ready();
}

auto ExternalPluginExtractor::name() const -> std::string {
    return impl_->manifest().name;
}

auto ExternalPluginExtractor::version() const -> std::string {
    return impl_->manifest().version;
}

auto ExternalPluginExtractor::supports(const std::string& mime, const std::string& extension) const
    -> bool {
    return impl_->supports(mime, extension);
}

auto ExternalPluginExtractor::extractText(const std::vector<std::byte>& bytes,
                                          const std::string& mime, const std::string& extension)
    -> std::optional<std::string> {
    return impl_->extractText(bytes, mime, extension);
}

} // namespace yams::extraction
