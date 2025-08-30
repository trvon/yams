#pragma once

#include <memory>
#include <stdexcept>
#include <yams/daemon/ipc/connection_fsm.h>

namespace yams {
namespace core {

// Lightweight guard helpers to keep client code simple and consistent
inline void require_can_read(const std::shared_ptr<daemon::ConnectionFsm>& fsm) {
    if (fsm && !fsm->can_read()) {
        throw std::runtime_error("Invalid read in current state");
    }
}

inline void require_can_write(const std::shared_ptr<daemon::ConnectionFsm>& fsm) {
    if (fsm && !fsm->can_write()) {
        throw std::runtime_error("Invalid write in current state");
    }
}

inline void validate_op(const std::shared_ptr<daemon::ConnectionFsm>& fsm,
                        daemon::ConnectionFsm::Operation op) {
    if (!fsm)
        return;
    fsm->validate_operation(op);
}

} // namespace core
} // namespace yams
