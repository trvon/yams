#pragma once

#include <stdexcept>
#include <yams/daemon/ipc/connection_fsm.h>

namespace yams {
namespace daemon {
namespace fsm_helpers {

inline void require_can_read(const ConnectionFsm& fsm, const char* context = nullptr) {
    if (!fsm.can_read()) {
        if (context) {
            throw std::runtime_error(std::string("Invalid read in state (") + context + ")");
        }
        throw std::runtime_error("Invalid read in state");
    }
}

inline void require_can_write(const ConnectionFsm& fsm, const char* context = nullptr) {
    if (!fsm.can_write()) {
        if (context) {
            throw std::runtime_error(std::string("Invalid write in state (") + context + ")");
        }
        throw std::runtime_error("Invalid write in state");
    }
}

inline void validate_op(const ConnectionFsm& fsm, ConnectionFsm::Operation op) {
    fsm.validate_operation(op);
}

} // namespace fsm_helpers
} // namespace daemon
} // namespace yams
