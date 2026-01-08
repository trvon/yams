#include <yams/daemon/resource/abi_entity_extractor_adapter.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

namespace yams::daemon {

std::string AbiEntityExtractorAdapter::getExtractorId() const {
    if (!table_ || !table_->get_capabilities_json)
        return "entity_extractor_unknown:v0";

    char* json = nullptr;
    int rc = table_->get_capabilities_json(table_->self, &json);
    if (rc != 0 || !json) {
        return "entity_extractor_unknown:v0";
    }

    std::string id = "entity_extractor_unknown:v0";
    try {
        auto caps = nlohmann::json::parse(json);
        std::string model = caps.value("model", "unknown");
        std::string version = caps.value("version", "0.0.0");
        id = "entity_extractor_" + model + ":v" + version;
    } catch (const std::exception& e) {
        spdlog::debug("AbiEntityExtractorAdapter: failed to parse capabilities: {}", e.what());
    }

    if (table_->free_string) {
        table_->free_string(table_->self, json);
    }

    return id;
}

} // namespace yams::daemon
