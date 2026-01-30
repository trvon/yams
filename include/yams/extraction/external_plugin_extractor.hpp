// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 Trevon Sides

#pragma once

#include "yams/core/types.h"
#include "yams/extraction/content_extractor.h"
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::extraction {

// Forward declarations
class PluginProcess;
class JsonRpcClient;

/// Configuration for external plugin extractor
/// Uses builder pattern for fluent API
class ExternalPluginExtractorConfig {
public:
    ExternalPluginExtractorConfig() = default;

    /// Set plugin executable path
    auto with_executable(std::string path) -> ExternalPluginExtractorConfig&;

    /// Add command-line argument
    auto with_arg(std::string arg) -> ExternalPluginExtractorConfig&;

    /// Add multiple arguments
    auto with_args(std::vector<std::string> args) -> ExternalPluginExtractorConfig&;

    /// Set environment variable
    auto with_env(std::string key, std::string value) -> ExternalPluginExtractorConfig&;

    /// Set working directory
    auto in_directory(std::string dir) -> ExternalPluginExtractorConfig&;

    /// Set timeout for RPC calls
    auto with_timeout(std::chrono::milliseconds timeout) -> ExternalPluginExtractorConfig&;

    /// Set startup timeout
    auto with_startup_timeout(std::chrono::milliseconds timeout) -> ExternalPluginExtractorConfig&;

    // Getters
    [[nodiscard]] auto executable() const -> const std::string&;
    [[nodiscard]] auto args() const -> const std::vector<std::string>&;
    [[nodiscard]] auto env() const -> const std::unordered_map<std::string, std::string>&;
    [[nodiscard]] auto working_directory() const -> const std::string&;
    [[nodiscard]] auto timeout() const -> std::chrono::milliseconds;
    [[nodiscard]] auto startup_timeout() const -> std::chrono::milliseconds;

private:
    std::string executable_;
    std::vector<std::string> args_;
    std::unordered_map<std::string, std::string> env_;
    std::string working_directory_;
    std::chrono::milliseconds timeout_{3000};
    std::chrono::milliseconds startup_timeout_{5000};
};

/// Plugin manifest metadata
struct PluginManifest {
    std::string name;
    std::string version;
    std::string description;
    std::vector<std::string> supported_mime_types;
};

/// External plugin extractor
/// Implements IContentExtractor interface using external process
/// Lifecycle: construct → init() → [health_check, supports, extractText]* →
/// shutdown()
class ExternalPluginExtractor : public IContentExtractor {
public:
    /// Construct from configuration
    explicit ExternalPluginExtractor(ExternalPluginExtractorConfig config);

    /// RAII: shutdown on destruction
    ~ExternalPluginExtractor() override;

    // Non-copyable, movable
    ExternalPluginExtractor(const ExternalPluginExtractor&) = delete;
    auto operator=(const ExternalPluginExtractor&) -> ExternalPluginExtractor& = delete;
    ExternalPluginExtractor(ExternalPluginExtractor&&) noexcept;
    auto operator=(ExternalPluginExtractor&&) noexcept -> ExternalPluginExtractor&;

    /// Initialize plugin: spawn process, handshake, manifest, init
    /// Must be called before using the extractor
    [[nodiscard]] auto init() -> Result<void>;

    /// Perform health check
    [[nodiscard]] auto health_check() -> Result<void>;

    /// Graceful shutdown
    [[nodiscard]] auto shutdown() -> Result<void>;

    /// Get plugin manifest
    [[nodiscard]] auto manifest() const -> const PluginManifest&;

    /// Check if plugin is initialized and healthy
    [[nodiscard]] auto is_ready() const -> bool;

    /// Get plugin name
    [[nodiscard]] auto name() const -> std::string;

    /// Get plugin version
    [[nodiscard]] auto version() const -> std::string;

    // IContentExtractor interface
    [[nodiscard]] auto supports(const std::string& mime, const std::string& extension) const
        -> bool override;
    [[nodiscard]] auto extractText(const std::vector<std::byte>& bytes, const std::string& mime,
                                   const std::string& extension)
        -> std::optional<std::string> override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace yams::extraction
