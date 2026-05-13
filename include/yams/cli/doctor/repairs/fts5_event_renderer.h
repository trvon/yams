#pragma once

#include <yams/daemon/ipc/ipc_protocol.h>

#include <chrono>
#include <ostream>
#include <string>

namespace yams::cli::doctor {

/// Replaces the FTS5 `onEvent` lambda in runRepair's daemon RPC path.
class Fts5EventRenderer {
public:
    Fts5EventRenderer(std::ostream& os, bool tty, std::string& lastOperation,
                      uint64_t& lastProcessed, std::chrono::steady_clock::time_point& lastPrint)
        : os_(os), tty_(tty), lastOperation_(lastOperation), lastProcessed_(lastProcessed),
          lastPrint_(lastPrint) {}

    void onRepairEvent(const daemon::RepairEvent& ev);

private:
    std::ostream& os_;
    bool tty_;
    std::string& lastOperation_;
    uint64_t& lastProcessed_;
    std::chrono::steady_clock::time_point& lastPrint_;
};

} // namespace yams::cli::doctor
