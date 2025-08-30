#pragma once

#include <yams/daemon/ipc/connection_fsm.h>

namespace yams::daemon {

// Non-invasive adapter that derives scheduling hints from a ConnectionFsm.
// No ownership; call sites can compute hints each scheduling tick.
struct RepairSchedulingAdapter {
    struct SchedulingHints {
        bool streaming_high_load{false};
        bool maintenance_allowed{false};
        bool closing{false};
    };

    static SchedulingHints derive(const ConnectionFsm& fsm) noexcept {
        using S = ConnectionFsm::State;
        const auto st = fsm.state();
        SchedulingHints h{};
        // High load when actively streaming or reading large payloads
        h.streaming_high_load = (st == S::StreamingChunks || st == S::ReadingPayload);
        // Allow maintenance when connected but idle on IO boundaries
        h.maintenance_allowed =
            (st == S::Connected || st == S::ReadingHeader || st == S::WritingHeader);
        // Signal closing so repair can yield quickly
        h.closing = (st == S::Closing || st == S::Closed || st == S::Error);
        return h;
    }

    template <class H> static H to(const SchedulingHints& src) noexcept {
        H dst{};
        dst.streaming_high_load = src.streaming_high_load;
        dst.maintenance_allowed = src.maintenance_allowed;
        dst.closing = src.closing;
        return dst;
    }
};

} // namespace yams::daemon
