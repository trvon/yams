#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>

namespace yams::daemon {

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

        // Expected format:
        // {
        //   "languages": [
        //     {"id": "cpp", "extensions": [".cpp", ".hpp", ".cc", ".h"]},
        //     {"id": "python", "extensions": [".py"]}
        //   ]
        // }
        if (j.contains("languages") && j["languages"].is_array()) {
            for (const auto& lang : j["languages"]) {
                if (lang.contains("id") && lang.contains("extensions")) {
                    std::string langId = lang["id"].get<std::string>();
                    for (const auto& ext : lang["extensions"]) {
                        std::string extension = ext.get<std::string>();
                        result[extension] = langId;
                    }
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
