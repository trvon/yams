#pragma once

namespace yams::daemon {

struct TitleEnrichmentPlan {
    bool dispatch = false;
    bool preserveTitle = false;
};

inline TitleEnrichmentPlan planTitleEnrichment(bool isCode, bool suppliedTitle,
                                               bool extractorAvailable, bool disabled) {
    return {!isCode && extractorAvailable && !disabled, suppliedTitle};
}

} // namespace yams::daemon
