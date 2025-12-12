// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0
//
// PBI-096: Unified content extractor adapter for ABI and external plugins

#pragma once

#include <yams/extraction/content_extractor.h>
#include <yams/plugins/abi.h>
#include <yams/plugins/content_extractor_v1.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace yams::daemon {

class ExternalPluginHost;

/**
 * @brief Unified content extractor adapter for both ABI and external plugins.
 *
 * This adapter provides a single implementation of IContentExtractor that can
 * work with either:
 *   - ABI plugins (native C++ via function table)
 *   - External plugins (Python/JS via JSON-RPC)
 *
 * The backend is selected at construction time based on which constructor is used.
 */
class PluginContentExtractorAdapter : public yams::extraction::IContentExtractor {
public:
    /// Construct adapter for an ABI (native) plugin
    explicit PluginContentExtractorAdapter(yams_content_extractor_v1* table, std::string name = {});

    /// Construct adapter for an external plugin
    /// Default timeout is 5 minutes to allow for heavy operations like Ghidra decompilation
    PluginContentExtractorAdapter(ExternalPluginHost* host, std::string pluginName,
                                  std::vector<std::string> supportedMimes,
                                  std::vector<std::string> supportedExtensions,
                                  std::chrono::milliseconds timeout = std::chrono::seconds{300});

    ~PluginContentExtractorAdapter() override = default;

    // Non-copyable (due to raw pointers), movable
    PluginContentExtractorAdapter(const PluginContentExtractorAdapter&) = delete;
    PluginContentExtractorAdapter& operator=(const PluginContentExtractorAdapter&) = delete;
    PluginContentExtractorAdapter(PluginContentExtractorAdapter&&) = default;
    PluginContentExtractorAdapter& operator=(PluginContentExtractorAdapter&&) = default;

    /// Check if this extractor supports the given MIME type or extension
    bool supports(const std::string& mime, const std::string& extension) const override;

    /// Extract text content from binary data
    std::optional<std::string> extractText(const std::vector<std::byte>& bytes,
                                           const std::string& mime,
                                           const std::string& extension) override;

    /// Get the plugin name
    const std::string& name() const { return name_; }

    /// Check if this is an external plugin adapter
    bool isExternal() const { return std::holds_alternative<ExternalBackend>(backend_); }

private:
    // ABI backend - direct function table calls
    struct AbiBackend {
        yams_content_extractor_v1* table{nullptr};
    };

    // External backend - JSON-RPC calls via ExternalPluginHost
    struct ExternalBackend {
        ExternalPluginHost* host{nullptr};
        std::vector<std::string> supportedMimes;
        std::vector<std::string> supportedExtensions;
        std::chrono::milliseconds timeout{std::chrono::seconds{300}};
    };

    std::string name_;
    std::variant<AbiBackend, ExternalBackend> backend_;

    // Implementation helpers
    bool supportsAbi(const std::string& mime, const std::string& extension) const;
    bool supportsExternal(const std::string& mime, const std::string& extension) const;
    std::optional<std::string> extractAbi(const std::vector<std::byte>& bytes);
    std::optional<std::string> extractExternal(const std::vector<std::byte>& bytes,
                                               const std::string& mime,
                                               const std::string& extension);
};

// Backward compatibility alias
using AbiContentExtractorAdapter = PluginContentExtractorAdapter;

} // namespace yams::daemon
