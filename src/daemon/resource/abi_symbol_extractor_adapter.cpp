#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <iomanip>
#include <set>
#include <sstream>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>

namespace yams::daemon {

namespace {

std::string normalizeToken(std::string_view token) {
    std::string out;
    out.reserve(token.size());
    for (unsigned char c : token) {
        if (std::isalnum(c) != 0 || c == '_' || c == '-' || c == '.' || c == ':') {
            out.push_back(static_cast<char>(std::tolower(c)));
        }
    }
    return out;
}

std::string fnv1a64Hex(std::string_view input) {
    constexpr std::uint64_t kOffset = 1469598103934665603ull;
    constexpr std::uint64_t kPrime = 1099511628211ull;

    std::uint64_t hash = kOffset;
    for (unsigned char c : input) {
        hash ^= c;
        hash *= kPrime;
    }

    std::ostringstream oss;
    oss << std::hex << std::nouppercase << std::setw(16) << std::setfill('0') << hash;
    return oss.str();
}

std::string buildCapabilitiesFingerprint(const nlohmann::json& j) {
    std::vector<std::string> parts;

    if (j.contains("languages") && j["languages"].is_array()) {
        for (const auto& lang : j["languages"]) {
            if (lang.is_string()) {
                auto normalized = normalizeToken(lang.get<std::string>());
                if (!normalized.empty()) {
                    parts.push_back("lang=" + normalized);
                }
            } else if (lang.is_object() && lang.contains("id") && lang["id"].is_string()) {
                auto normalized = normalizeToken(lang["id"].get<std::string>());
                if (!normalized.empty()) {
                    parts.push_back("lang=" + normalized);
                }
            }
        }
    }

    if (j.contains("features") && j["features"].is_array()) {
        for (const auto& feature : j["features"]) {
            if (feature.is_string()) {
                auto normalized = normalizeToken(feature.get<std::string>());
                if (!normalized.empty()) {
                    parts.push_back("feature=" + normalized);
                }
            }
        }
    }

    if (j.contains("version") && j["version"].is_string()) {
        auto version = normalizeToken(j["version"].get<std::string>());
        if (!version.empty()) {
            parts.push_back("version=" + version);
        }
    }

    if (j.contains("name") && j["name"].is_string()) {
        auto name = normalizeToken(j["name"].get<std::string>());
        if (!name.empty()) {
            parts.push_back("name=" + name);
        }
    }

    if (parts.empty()) {
        return fnv1a64Hex(j.dump());
    }

    std::sort(parts.begin(), parts.end());
    parts.erase(std::unique(parts.begin(), parts.end()), parts.end());

    std::string joined;
    for (const auto& part : parts) {
        if (!joined.empty()) {
            joined.push_back('|');
        }
        joined += part;
    }
    return fnv1a64Hex(joined);
}

} // namespace

// Default extension-to-language mappings for common languages
static const std::unordered_map<std::string, std::string> kDefaultExtensionMap = {
    // C/C++
    {".c", "c"},
    {".h", "c"},
    {".cpp", "cpp"},
    {".cc", "cpp"},
    {".cxx", "cpp"},
    {".hpp", "cpp"},
    {".hxx", "cpp"},
    {".c++", "cpp"},
    {".h++", "cpp"},
    // Python
    {".py", "python"},
    {".pyw", "python"},
    {".pyi", "python"},
    // Rust
    {".rs", "rust"},
    // Go
    {".go", "go"},
    // JavaScript/TypeScript
    {".js", "javascript"},
    {".mjs", "javascript"},
    {".cjs", "javascript"},
    {".jsx", "javascript"},
    {".ts", "typescript"},
    {".tsx", "typescript"},
    {".mts", "typescript"},
    // Java
    {".java", "java"},
    // C#
    {".cs", "csharp"},
    // PHP
    {".php", "php"},
    // Kotlin
    {".kt", "kotlin"},
    {".kts", "kotlin"},
    // Perl
    {".pl", "perl"},
    {".pm", "perl"},
    // R
    {".r", "r"},
    {".R", "r"},
    // SQL
    {".sql", "sql"},
    // Solidity
    {".sol", "solidity"},
};

std::unordered_map<std::string, std::string>
AbiSymbolExtractorAdapter::getSupportedExtensions() const {
    std::unordered_map<std::string, std::string> result;

    if (!table_ || !table_->get_capabilities_json) {
        return result;
    }

    char* json_str = nullptr;
    int ret = table_->get_capabilities_json(table_->self, &json_str);
    if (ret != 0 || !json_str) {
        return result;
    }

    try {
        auto j = nlohmann::json::parse(json_str);

        // Format 1 (structured):
        // {
        //   "languages": [
        //     {"id": "cpp", "extensions": [".cpp", ".hpp", ".cc", ".h"]},
        //     {"id": "python", "extensions": [".py"]}
        //   ]
        // }
        //
        // Format 2 (simple array - used by treesitter plugin):
        // {
        //   "languages": ["cpp", "c++", "c", "python", "go", "rust", ...],
        //   "features": [...],
        //   "version": "..."
        // }
        if (j.contains("languages") && j["languages"].is_array()) {
            const auto& langs = j["languages"];
            if (!langs.empty()) {
                // Detect format by checking first element
                if (langs[0].is_object()) {
                    // Format 1: structured with id and extensions
                    for (const auto& lang : langs) {
                        if (lang.contains("id") && lang.contains("extensions")) {
                            std::string langId = lang["id"].get<std::string>();
                            for (const auto& ext : lang["extensions"]) {
                                std::string extension = ext.get<std::string>();
                                // Strip leading dot for consistency with callers
                                if (!extension.empty() && extension[0] == '.') {
                                    extension = extension.substr(1);
                                }
                                result[extension] = langId;
                            }
                        }
                    }
                } else if (langs[0].is_string()) {
                    // Format 2: simple array of language names
                    // Build extension map from supported languages using defaults
                    std::set<std::string> supportedLangs;
                    for (const auto& lang : langs) {
                        supportedLangs.insert(lang.get<std::string>());
                    }

                    // Map extensions to languages if the language is supported
                    for (const auto& [ext, lang] : kDefaultExtensionMap) {
                        // Check if plugin supports this language (including aliases)
                        bool supported = supportedLangs.count(lang) > 0;
                        // Also check common aliases
                        if (!supported && lang == "cpp") {
                            supported = supportedLangs.count("c++") > 0;
                        }
                        if (!supported && lang == "javascript") {
                            supported = supportedLangs.count("js") > 0;
                        }
                        if (!supported && lang == "typescript") {
                            supported = supportedLangs.count("ts") > 0;
                        }
                        if (!supported && lang == "csharp") {
                            supported =
                                supportedLangs.count("c#") > 0 || supportedLangs.count("cs") > 0;
                        }

                        if (supported) {
                            // Strip leading dot from extension for consistency with callers
                            std::string extKey = ext;
                            if (!extKey.empty() && extKey[0] == '.') {
                                extKey = extKey.substr(1);
                            }
                            result[extKey] = lang;
                        }
                    }

                    spdlog::debug("Symbol extractor supports {} languages, mapped {} extensions",
                                  supportedLangs.size(), result.size());
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Failed to parse symbol extractor capabilities: {}", e.what());
    } catch (...) {
        spdlog::debug("Failed to parse symbol extractor capabilities (unknown error)");
    }

    if (table_->free_string) {
        table_->free_string(table_->self, json_str);
    }

    return result;
}

std::string AbiSymbolExtractorAdapter::getExtractorId() const {
    if (!table_) {
        return "unknown";
    }

    std::string name = "symbol_extractor_v1";

    // Try to get name and version from capabilities JSON
    if (table_->get_capabilities_json) {
        char* json_str = nullptr;
        int ret = table_->get_capabilities_json(table_->self, &json_str);
        if (ret == 0 && json_str) {
            try {
                auto j = nlohmann::json::parse(json_str);
                std::string version;

                if (j.contains("name") && j["name"].is_string()) {
                    auto normalizedName = normalizeToken(j["name"].get<std::string>());
                    if (!normalizedName.empty()) {
                        name = std::move(normalizedName);
                    }
                } else if (j.contains("plugin") && j["plugin"].is_string()) {
                    auto normalizedName = normalizeToken(j["plugin"].get<std::string>());
                    if (!normalizedName.empty()) {
                        name = std::move(normalizedName);
                    }
                }
                if (j.contains("version") && j["version"].is_string()) {
                    version = normalizeToken(j["version"].get<std::string>());
                }

                if (table_->free_string) {
                    table_->free_string(table_->self, json_str);
                }

                if (!version.empty()) {
                    return name + ":" + version;
                }

                const std::string fingerprint = buildCapabilitiesFingerprint(j);
                if (!fingerprint.empty()) {
                    return name + ":cap-" + fingerprint;
                }
                return name + ":abi" + std::to_string(table_->abi_version);
            } catch (...) {
                // Fall through to default
            }

            if (table_->free_string) {
                table_->free_string(table_->self, json_str);
            }
        }
    }

    // Fallback: use ABI version
    return name + ":abi" + std::to_string(table_->abi_version);
}

} // namespace yams::daemon
