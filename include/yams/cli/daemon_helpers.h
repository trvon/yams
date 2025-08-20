#pragma once

#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>

namespace yams::cli {

// Try daemon first, then fallback to local. Render on success.
// FallbackFn: Result<void>() -> executes local path
// RenderFn: Result<void>(const Res&) -> prints/handles response
template <class Req, class FallbackFn, class RenderFn>
Result<void> daemon_first(Req&& req, FallbackFn&& fallback, RenderFn&& render) {
    // using Res = yams::daemon::ResponseOfT<std::decay_t<Req>>; // Not used in this template
    try {
        yams::daemon::DaemonClient client;
        if (auto c = client.connect(); !c) {
            return std::forward<FallbackFn>(fallback)();
        }
        auto r = client.call<std::decay_t<Req>>(req);
        if (r) {
            return std::forward<RenderFn>(render)(r.value());
        }
        // Fast fallback for daemon not available/implemented
        if (r.error().code == ErrorCode::NetworkError ||
            r.error().code == ErrorCode::NotImplemented) {
            return std::forward<FallbackFn>(fallback)();
        }
        return r.error();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

} // namespace yams::cli
