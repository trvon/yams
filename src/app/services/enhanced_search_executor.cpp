#define _CRT_SECURE_NO_WARNINGS
#include <yams/app/services/enhanced_search_executor.h>
#include <yams/config/config_helpers.h>
#include <yams/config/detail/config_parse_utils.h>

#include <initializer_list>
#include <optional>
#include <string_view>

namespace yams::app::services {

EnhancedConfig EnhancedSearchExecutor::loadConfigFromToml() {
    EnhancedConfig config{};
    bool invalidValue = false;
    const auto values = yams::config::parse_simple_toml(yams::config::get_config_path());

    const auto findValue =
        [&](std::initializer_list<std::string_view> keys) -> std::optional<std::string_view> {
        for (const auto key : keys) {
            if (const auto it = values.find(std::string(key)); it != values.end()) {
                return it->second;
            }
        }
        return std::nullopt;
    };
    const auto assignDouble = [&](std::initializer_list<std::string_view> keys, double& target) {
        if (const auto raw = findValue(keys)) {
            if (const auto parsed = yams::config::detail::parseDouble(*raw)) {
                target = *parsed;
            } else {
                invalidValue = true;
            }
        }
    };

    if (const auto raw =
            findValue({"search.enhanced.enable", "experimental.enhanced_search.enable"})) {
        if (const auto parsed = yams::config::detail::parseTomlBool(*raw)) {
            config.enable = *parsed;
        } else {
            invalidValue = true;
        }
    }
    assignDouble({"search.enhanced.classification_weight",
                  "experimental.enhanced_search.classification_weight"},
                 config.classification_weight);
    assignDouble(
        {"search.enhanced.kg_expansion_weight", "experimental.enhanced_search.kg_expansion_weight"},
        config.kg_expansion_weight);
    assignDouble({"search.enhanced.hotzone_weight", "experimental.enhanced_search.hotzone_weight"},
                 config.hotzone_weight);
    if (const auto raw = findValue({"search.enhanced.enhanced_search_timeout_ms",
                                    "experimental.enhanced_search.enhanced_search_timeout_ms"})) {
        if (const auto parsed = yams::config::detail::parseUnsignedIntegral<int>(*raw)) {
            config.enhanced_search_timeout_ms = *parsed;
        } else {
            invalidValue = true;
        }
    }

    assignDouble(
        {"search.hotzones.decay_interval_hours", "experimental.hotzones.decay_interval_hours"},
        config.hotzones.half_life_hours);
    assignDouble({"search.hotzones.max_boost_factor", "experimental.hotzones.max_boost_factor"},
                 config.hotzones.max_boost_factor);
    if (const auto raw = findValue(
            {"search.hotzones.enable_persistence", "experimental.hotzones.enable_persistence"})) {
        if (const auto parsed = yams::config::detail::parseTomlBool(*raw)) {
            config.hotzones.enable_persistence = *parsed;
        } else {
            invalidValue = true;
        }
    }
    if (const auto raw =
            findValue({"search.hotzones.data_file", "experimental.hotzones.data_file"})) {
        config.hotzones.data_file = *raw;
    }

    // Preserve the previous fail-closed section behavior: one malformed recognized setting
    // disables the enhancement instead of enabling a partially defaulted pipeline.
    return invalidValue ? EnhancedConfig{} : config;
}

void EnhancedSearchExecutor::apply(const AppContext& /*ctx*/, const EnhancedConfig& cfg,
                                   const std::string& /*query*/,
                                   std::vector<SearchItem>& items) const {
    if (!cfg.enable || items.empty())
        return;

    // Phase A: apply hotzone multiplier if we have a manager, otherwise leave scores as-is.
    if (hotzones_ && cfg.hotzone_weight > 0.0) {
        for (auto& it : items) {
            // Prefer stable key: path, then hash, then id.
            const std::string& key =
                !it.path.empty() ? it.path : (!it.hash.empty() ? it.hash : std::to_string(it.id));
            const double boost = hotzones_->getBoost(key);
            // Blend: score' = score * (1 + hotzone_weight * (boost - 1))
            const double blended = it.score * (1.0 + cfg.hotzone_weight * (boost - 1.0));
            it.score = blended;
        }
    }
}

} // namespace yams::app::services
