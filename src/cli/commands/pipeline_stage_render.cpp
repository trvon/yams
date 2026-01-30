#include <yams/cli/pipeline_stage_render.h>

#include <sstream>
#include <string>

namespace yams::cli::detail {

std::vector<ui::Row> renderPipelineStages(const StageInfo* stages, size_t count) {
    using namespace yams::cli::ui;

    std::vector<Row> rows;

    bool anyVisible = false;
    for (size_t i = 0; i < count; ++i) {
        const auto& st = stages[i];
        if (st.inflight > 0 || st.queueDepth > 0 || st.queued > 0)
            anyVisible = true;
    }
    if (!anyVisible)
        return rows;

    for (size_t i = 0; i < count; ++i) {
        const auto& st = stages[i];

        // Skip idle stages with no audit history
        if (st.inflight == 0 && st.queueDepth == 0 && st.queued == 0)
            continue;

        std::ostringstream val;
        bool completed =
            (st.queued > 0 && st.queued == st.consumed && st.inflight == 0 && st.queueDepth == 0);
        if (completed) {
            val << Ansi::GREEN << "\xe2\x9c\x93" << Ansi::RESET << " " << st.consumed << "/"
                << st.queued << " done";
            if (st.dropped > 0)
                val << " \xc2\xb7 " << st.dropped << " dropped";
        } else {
            double frac = static_cast<double>(st.inflight) / st.limit;
            val << progress_bar(frac, 8, "#", "\xe2\x96\x91", Ansi::GREEN, Ansi::YELLOW, Ansi::RED,
                                true)
                << " " << static_cast<int>(frac * 100) << "% (" << st.inflight << "/" << st.limit
                << " slots)";
            if (st.queueDepth > 0)
                val << " \xc2\xb7 queue: " << st.queueDepth;
            if (st.queued > 0)
                val << " \xc2\xb7 " << st.consumed << "/" << st.queued << " done";
            if (st.dropped > 0)
                val << " \xc2\xb7 " << st.dropped << " dropped";
        }

        rows.push_back({"  " + std::string(st.name), val.str(), ""});
    }

    return rows;
}

} // namespace yams::cli::detail
