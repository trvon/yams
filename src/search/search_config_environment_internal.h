#pragma once

#include <yams/search/search_engine.h>
#include <yams/search/search_tuner.h>

#include <functional>
#include <optional>
#include <string>
#include <string_view>

namespace yams::search {

using SearchEnvironmentLookup = std::function<std::optional<std::string>(std::string_view name)>;

struct SearchEnvironmentPins {
    bool text = false;
    bool simeonText = false;
    bool vector = false;
    bool kg = false;
    bool similarityThreshold = false;
};

class LegacySearchConfigEnvironment {
public:
    explicit LegacySearchConfigEnvironment(SearchEnvironmentLookup lookup);

    static LegacySearchConfigEnvironment fromProcess();

    bool enabled() const;
    std::optional<TuningState> tuningStateOverride() const;
    SearchEnvironmentPins applyTo(SearchEngineConfig& config) const;

private:
    SearchEnvironmentLookup lookup_;
};

} // namespace yams::search
