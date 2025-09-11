#pragma once

#include <yams/daemon/ipc/connection_fsm.h>

namespace yams::daemon::fsm_intents {

enum class LoadIntent { StreamingHighLoad, StreamingNormal, MaintenanceAllowed, Closing, Unknown };

inline LoadIntent derive(ConnectionFsm::State s) {
    switch (s) {
        case ConnectionFsm::State::StreamingChunks:
            return LoadIntent::StreamingHighLoad;
        case ConnectionFsm::State::WritingHeader:
        case ConnectionFsm::State::Connected:
        case ConnectionFsm::State::ReadingHeader:
        case ConnectionFsm::State::ReadingPayload:
            return LoadIntent::StreamingNormal;
        case ConnectionFsm::State::Closing:
        case ConnectionFsm::State::Closed:
            return LoadIntent::Closing;
        default:
            return LoadIntent::Unknown;
    }
}

} // namespace yams::daemon::fsm_intents
