#include <yams/cli/doctor/repairs/fts5_progress.h>
#include <yams/cli/ui_helpers.hpp>

#include <iomanip>
#include <sstream>

namespace yams::cli::doctor {

void Fts5ProgressRenderer::render(bool forceNewline) {
    using namespace yams::cli::ui;
    auto now = std::chrono::steady_clock::now();
    double elapsed =
        std::chrono::duration_cast<std::chrono::duration<double>>(now - started_).count();
    double rate = (elapsed > 0.0) ? (static_cast<double>(cur_) / elapsed) : 0.0;
    uint64_t elapsed_s = static_cast<uint64_t>(elapsed);
    uint64_t eta_s = 0;
    if (rate > 0.0 && cur_ < total_)
        eta_s = static_cast<uint64_t>(static_cast<double>(total_ - cur_) / rate);
    double frac = (total_ > 0) ? (static_cast<double>(cur_) / static_cast<double>(total_)) : 1.0;
    std::ostringstream oss;
    oss << status_pending("FTS5") << " "
        << progress_with_stats(frac, 18,
                               std::make_optional(std::make_pair(static_cast<uint64_t>(cur_),
                                                                 static_cast<uint64_t>(total_))),
                               "docs")
        << " " << colorize("elapsed " + format_duration(elapsed_s), Ansi::DIM);
    if (rate > 0.0) {
        std::ostringstream r;
        r << std::fixed << std::setprecision(rate < 10.0 ? 1 : 0) << rate;
        oss << " " << colorize(r.str() + " docs/s", Ansi::DIM);
    }
    if (eta_s > 0)
        oss << " " << colorize("eta " + format_duration(eta_s), Ansi::DIM);
    const std::string line = oss.str();
    if (tty_ && !forceNewline)
        os_ << "\r\033[K" << line << std::flush;
    else
        os_ << "  " << line << "\n" << std::flush;
    lastPrint_ = now;
}

} // namespace yams::cli::doctor
