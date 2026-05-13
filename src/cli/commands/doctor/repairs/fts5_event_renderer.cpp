#include <yams/cli/doctor/repairs/fts5_event_renderer.h>
#include <yams/cli/ui_helpers.hpp>

#include <sstream>

namespace yams::cli::doctor {

void Fts5EventRenderer::onRepairEvent(const daemon::RepairEvent& ev) {
    using namespace yams::cli::ui;
    if (ev.operation != lastOperation_) {
        if (!lastOperation_.empty())
            os_ << "\n";
        lastOperation_ = ev.operation;
        os_ << "  " << status_pending("Daemon: " + ev.operation) << "\n";
        lastProcessed_ = 0;
    }
    if (ev.phase == "repairing" && ev.total > 0) {
        auto now = std::chrono::steady_clock::now();
        if (tty_ && (now - lastPrint_) < std::chrono::milliseconds(200) &&
            ev.processed == lastProcessed_)
            return;
        lastProcessed_ = ev.processed;
        lastPrint_ = now;
        double frac = static_cast<double>(ev.processed) / static_cast<double>(ev.total);
        std::ostringstream oss;
        oss << status_pending("FTS5") << " "
            << progress_with_stats(
                   frac, 18, std::make_optional(std::make_pair(ev.processed, ev.total)), "docs");
        if (tty_)
            os_ << "\r\033[K" << oss.str() << std::flush;
    } else if (ev.phase == "error") {
        os_ << "  " << status_warning(ev.message) << "\n";
    } else if (ev.phase == "completed") {
        if (tty_)
            os_ << "\n";
        os_ << "  " << status_ok("Daemon FTS5 repair done") << "  processed=" << ev.processed
            << " ok=" << ev.succeeded << " failed=" << ev.failed << "\n";
    }
}

} // namespace yams::cli::doctor
