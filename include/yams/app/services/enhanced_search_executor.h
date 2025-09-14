#pragma once

#include <memory>
#include <string>
#include <vector>

#include <yams/app/services/services.hpp>
#include <yams/search/hotzone_manager.h>

namespace yams::app::services {

// Config for the enhanced pipeline (subset used in Phase A)
struct EnhancedConfig {
    bool enable{false};
    // Component weights (0..1). Only hotzone is applied in Phase A.
    double classification_weight{0.15};
    double kg_expansion_weight{0.10};
    double hotzone_weight{0.05};
    // Budget in ms for the whole pipeline (future use)
    int enhanced_search_timeout_ms{2000};

    // Hotzones config
    yams::search::HotzoneConfig hotzones;
};

// MVP executor: only applies hotzone boost to SearchItem scores when enabled.
class EnhancedSearchExecutor {
public:
    EnhancedSearchExecutor() = default;

    void setHotzoneManager(std::shared_ptr<yams::search::HotzoneManager> mgr) {
        hotzones_ = std::move(mgr);
    }

    // Apply enhancements in-place to the result set.
    void apply(const AppContext& ctx, const EnhancedConfig& cfg, const std::string& query,
               std::vector<SearchItem>& items) const;

    // Best-effort loader for config from ~/.config/yams/config.toml (no dependency on migrator).
    static EnhancedConfig loadConfigFromToml();

private:
    std::shared_ptr<yams::search::HotzoneManager> hotzones_;
};

} // namespace yams::app::services
