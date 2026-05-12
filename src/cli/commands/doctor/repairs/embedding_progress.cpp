#include <yams/cli/doctor/repairs/embedding_progress.h>
#include <yams/cli/ui_helpers.hpp>

#include <sstream>

namespace yams::cli::doctor {

void EmbeddingProgressRenderer::render(size_t current, size_t total, const std::string& details) {
    using namespace yams::cli::ui;
    auto now = std::chrono::steady_clock::now();
    if (!tty_ && (current % 200 != 0))
        return;
    if (tty_ && (now - lastPrint_) < std::chrono::milliseconds(200))
        return;
    uint64_t elapsed_s = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(now - progressStarted_).count());
    std::string detail = details;
    if (!detail.empty())
        detail = truncate_to_width(detail, 60);
    std::ostringstream oss;
    oss << status_pending("Embeddings") << " ";
    if (total > 0) {
        double frac = (static_cast<double>(current) / static_cast<double>(total));
        oss << progress_with_stats(frac, 18,
                                   std::make_optional(std::make_pair(static_cast<uint64_t>(current),
                                                                     static_cast<uint64_t>(total))),
                                   "docs");
    } else {
        oss << colorize("working", Ansi::DIM);
    }
    oss << " " << colorize("elapsed " + format_duration(elapsed_s), Ansi::DIM);
    if (!detail.empty())
        oss << " " << colorize(detail, Ansi::DIM);
    if (tty_)
        os_ << "\r\033[K" << oss.str() << std::flush;
    else
        os_ << "  " << oss.str() << "\n" << std::flush;
    lastPrint_ = now;
}

void EmbeddingProgressRenderer::finish() {
    if (tty_)
        os_ << "\n";
}

} // namespace yams::cli::doctor
