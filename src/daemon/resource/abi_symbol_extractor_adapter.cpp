#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <set>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>

namespace yams::daemon {

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
                            supported = supportedLangs.count("c#") > 0 || supportedLangs.count("cs") > 0;
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

} // namespace yams::daemon
