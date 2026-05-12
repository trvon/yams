#pragma once

#include <chrono>
#include <ostream>

namespace yams::cli::doctor {

/// Replaces the FTS5 `printProgress` lambda in runRepair's local repair path.
class Fts5ProgressRenderer {
public:
    Fts5ProgressRenderer(std::ostream& os, bool tty, size_t& cur, size_t total,
                         const std::chrono::steady_clock::time_point& started,
                         std::chrono::steady_clock::time_point& lastPrint)
        : os_(os), tty_(tty), cur_(cur), total_(total), started_(started), lastPrint_(lastPrint) {}

    void render(bool forceNewline);

private:
    std::ostream& os_;
    bool tty_;
    size_t& cur_;
    size_t total_;
    const std::chrono::steady_clock::time_point& started_;
    std::chrono::steady_clock::time_point& lastPrint_;
};

} // namespace yams::cli::doctor
