// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0
//
// PBI-096: Unified content extractor adapter implementation

#include <yams/daemon/resource/plugin_content_extractor_adapter.h>
#include <yams/daemon/resource/external_plugin_host.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>

namespace yams::daemon {

using json = nlohmann::json;

// ============================================================================
// Constructors
// ============================================================================

PluginContentExtractorAdapter::PluginContentExtractorAdapter(yams_content_extractor_v1* table,
                                                             std::string name)
    : name_(std::move(name)), backend_(AbiBackend{table}) {}

PluginContentExtractorAdapter::PluginContentExtractorAdapter(
    ExternalPluginHost* host, std::string pluginName, std::vector<std::string> supportedMimes,
    std::vector<std::string> supportedExtensions, std::chrono::milliseconds timeout)
    : name_(std::move(pluginName)),
      backend_(ExternalBackend{host, std::move(supportedMimes), std::move(supportedExtensions),
                               timeout}) {}

// ============================================================================
// IContentExtractor Interface
// ============================================================================

bool PluginContentExtractorAdapter::supports(const std::string& mime,
                                             const std::string& extension) const {
    return std::visit(
        [&](const auto& backend) -> bool {
            using T = std::decay_t<decltype(backend)>;
            if constexpr (std::is_same_v<T, AbiBackend>) {
                return supportsAbi(mime, extension);
            } else {
                return supportsExternal(mime, extension);
            }
        },
        backend_);
}

std::optional<std::string>
PluginContentExtractorAdapter::extractText(const std::vector<std::byte>& bytes,
                                           const std::string& mime, const std::string& extension) {
    return std::visit(
        [&](auto& backend) -> std::optional<std::string> {
            using T = std::decay_t<decltype(backend)>;
            if constexpr (std::is_same_v<T, AbiBackend>) {
                return extractAbi(bytes);
            } else {
                return extractExternal(bytes, mime, extension);
            }
        },
        backend_);
}

// ============================================================================
// ABI Backend Implementation
// ============================================================================

bool PluginContentExtractorAdapter::supportsAbi(const std::string& mime,
                                                const std::string& extension) const {
    const auto& abi = std::get<AbiBackend>(backend_);
    if (!abi.table || !abi.table->supports)
        return false;
    return abi.table->supports(mime.c_str(), extension.c_str());
}

std::optional<std::string>
PluginContentExtractorAdapter::extractAbi(const std::vector<std::byte>& bytes) {
    auto& abi = std::get<AbiBackend>(backend_);
    if (!abi.table || !abi.table->extract || !abi.table->free_result)
        return std::nullopt;

    yams_extraction_result_t* res = nullptr;
    int rc = abi.table->extract(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(), &res);
    if (rc != YAMS_PLUGIN_OK || !res)
        return std::nullopt;

    std::optional<std::string> out;
    if (res->text)
        out = std::string(res->text);
    abi.table->free_result(res);
    return out;
}

// ============================================================================
// External Backend Implementation
// ============================================================================

bool PluginContentExtractorAdapter::supportsExternal(const std::string& mime,
                                                     const std::string& extension) const {
    const auto& ext = std::get<ExternalBackend>(backend_);

    // Only use cached capabilities from manifest - no RPC fallback to avoid ID desync issues
    // Plugin manifests declare their supported MIME types and extensions at registration time
    if (!mime.empty()) {
        auto it = std::find(ext.supportedMimes.begin(), ext.supportedMimes.end(), mime);
        if (it != ext.supportedMimes.end())
            return true;
    }

    if (!extension.empty()) {
        // Normalize extension to include dot
        std::string ext_normalized = extension;
        if (!ext_normalized.empty() && ext_normalized[0] != '.')
            ext_normalized = "." + ext_normalized;

        auto it =
            std::find(ext.supportedExtensions.begin(), ext.supportedExtensions.end(), ext_normalized);
        if (it != ext.supportedExtensions.end())
            return true;

        // Also check without dot for flexibility
        auto it2 = std::find(ext.supportedExtensions.begin(), ext.supportedExtensions.end(), extension);
        if (it2 != ext.supportedExtensions.end())
            return true;
    }

    // Not in cached capabilities - return false
    // Note: RPC fallback was removed to prevent response ID desync when multiple
    // rapid supports() calls are made before extraction
    return false;
}

std::optional<std::string>
PluginContentExtractorAdapter::extractExternal(const std::vector<std::byte>& bytes,
                                               const std::string& mime,
                                               const std::string& extension) {
    auto& ext = std::get<ExternalBackend>(backend_);
    if (!ext.host)
        return std::nullopt;

    // Serialize access to the external plugin process (can only handle one request at a time)
    std::lock_guard<std::mutex> lock(externalMutex_);

    try {
        // Encode bytes as base64 for JSON transport
        // Use a simple base64 encoding
        static constexpr char base64_chars[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        std::string base64;
        base64.reserve(((bytes.size() + 2) / 3) * 4);

        const uint8_t* data = reinterpret_cast<const uint8_t*>(bytes.data());
        size_t len = bytes.size();

        for (size_t i = 0; i < len; i += 3) {
            uint32_t n = static_cast<uint32_t>(data[i]) << 16;
            if (i + 1 < len)
                n |= static_cast<uint32_t>(data[i + 1]) << 8;
            if (i + 2 < len)
                n |= static_cast<uint32_t>(data[i + 2]);

            base64 += base64_chars[(n >> 18) & 0x3F];
            base64 += base64_chars[(n >> 12) & 0x3F];
            base64 += (i + 1 < len) ? base64_chars[(n >> 6) & 0x3F] : '=';
            base64 += (i + 2 < len) ? base64_chars[n & 0x3F] : '=';
        }

        // Build RPC params
        json params = {{"source", {{"type", "bytes"}, {"data", base64}}},
                       {"options", {{"mime_type", mime}, {"extension", extension}}}};

        spdlog::info("PluginContentExtractorAdapter[{}]: calling extractor.extract ({} bytes)",
                     name_, bytes.size());

        auto result = ext.host->callRpc(name_, "extractor.extract", params, ext.timeout);
        if (!result) {
            spdlog::warn("PluginContentExtractorAdapter[{}]: extractor.extract RPC failed", name_);
            return std::nullopt;
        }

        const auto& resp = result.value();

        // Check for error in result (plugin-level error, not RPC error)
        if (resp.contains("error") && !resp["error"].is_null()) {
            std::string errMsg = resp["error"].is_string()
                ? resp["error"].get<std::string>()
                : resp["error"].dump();
            spdlog::warn("PluginContentExtractorAdapter[{}]: plugin returned error: {}", name_, errMsg);
            return std::nullopt;
        }

        // Extract text from response
        if (resp.contains("text") && resp["text"].is_string()) {
            auto text = resp["text"].get<std::string>();
            spdlog::info("PluginContentExtractorAdapter[{}]: extraction succeeded ({} chars)", name_,
                         text.size());
            return text;
        }

        // Some extractors return content instead of text
        if (resp.contains("content") && resp["content"].is_string()) {
            auto content = resp["content"].get<std::string>();
            spdlog::info("PluginContentExtractorAdapter[{}]: extraction succeeded ({} chars)", name_,
                         content.size());
            return content;
        }

        spdlog::debug("PluginContentExtractorAdapter[{}]: no text/content in response", name_);
        return std::nullopt;

    } catch (const std::exception& e) {
        spdlog::warn("PluginContentExtractorAdapter[{}]: extraction failed: {}", name_, e.what());
        return std::nullopt;
    }
}

bool PluginContentExtractorAdapter::isBusy() const {
    if (!isExternal())
        return false;
    if (externalMutex_.try_lock()) {
        externalMutex_.unlock();
        return false;
    }
    return true;
}

} // namespace yams::daemon
