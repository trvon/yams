#pragma once

#include <yams/app/services/services.hpp>
#include <yams/daemon/ipc/ipc_protocol_requests.h>

namespace yams::daemon::dispatch {

inline void mapSearchServiceOptions(const SearchRequest& source,
                                    app::services::SearchRequest& destination) noexcept {
    destination.vectorStageTimeoutMs = source.vectorStageTimeoutMs;
    destination.keywordStageTimeoutMs = source.keywordStageTimeoutMs;
    destination.snippetHydrationTimeoutMs = source.snippetHydrationTimeoutMs;
    destination.symbolRank = source.symbolRank;
}

} // namespace yams::daemon::dispatch
