#include <iomanip>
#include <sstream>
#include <yams/cli/progress_indicator.h>

namespace yams::cli {

// Define static members
constexpr const char* ProgressIndicator::SPINNER_CHARS[];
constexpr int ProgressIndicator::SPINNER_COUNT;

ProgressIndicator::ProgressIndicator(Style style, bool autoStart) : style_(style) {
    if (autoStart) {
        start("");
    }
}

ProgressIndicator::~ProgressIndicator() {
    if (active_) {
        stop();
    }
}

void ProgressIndicator::start(const std::string& message) {
    if (active_)
        return;

    message_ = message;
    active_ = true;
    current_ = 0;
    spinnerIndex_ = 0;
    lastUpdate_ = std::chrono::steady_clock::now();

    render();
}

void ProgressIndicator::update(size_t current, size_t total) {
    if (!active_)
        return;

    current_ = current;
    total_ = total;

    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastUpdate_).count();

    if (elapsed >= updateIntervalMs_) {
        spinnerIndex_ = (spinnerIndex_ + 1) % SPINNER_COUNT;
        lastUpdate_ = now;
        render();
    }
}

void ProgressIndicator::tick() {
    update(current_ + 1, total_);
}

void ProgressIndicator::stop() {
    if (!active_)
        return;

    // Clear the line
    std::cout << "\r\033[K" << std::flush;
    active_ = false;
}

void ProgressIndicator::render() {
    if (!active_)
        return;

    std::ostringstream oss;
    oss << "\r";

    switch (style_) {
        case Style::Spinner:
            oss << SPINNER_CHARS[spinnerIndex_] << " " << message_;
            if (showCount_ && current_ > 0) {
                oss << " (" << current_;
                if (total_ > 0) {
                    oss << "/" << total_;
                }
                oss << ")";
            }
            break;

        case Style::Percentage:
            if (total_ > 0) {
                int percent = static_cast<int>((current_ * 100) / total_);
                oss << "[" << std::setw(3) << percent << "%] " << message_;
                if (showCount_) {
                    oss << " (" << current_ << "/" << total_ << ")";
                }
            } else {
                // Fall back to spinner for indeterminate progress
                oss << SPINNER_CHARS[spinnerIndex_] << " " << message_;
                if (showCount_ && current_ > 0) {
                    oss << " (" << current_ << ")";
                }
            }
            break;

        case Style::Bar:
            if (total_ > 0) {
                const int barWidth = 20;
                int filled = static_cast<int>((current_ * barWidth) / total_);
                oss << "[";
                for (int i = 0; i < barWidth; ++i) {
                    oss << (i < filled ? "█" : "░");
                }
                oss << "] ";

                // Add percentage
                int percent = static_cast<int>((current_ * 100) / total_);
                oss << std::setw(3) << percent << "% " << message_;
            } else {
                // Fall back to spinner for indeterminate progress
                oss << SPINNER_CHARS[spinnerIndex_] << " " << message_;
                if (showCount_ && current_ > 0) {
                    oss << " (" << current_ << ")";
                }
            }
            break;

        case Style::Dots:
            oss << message_;
            for (int i = 0; i < ((spinnerIndex_ % 4) + 1); ++i) {
                oss << ".";
            }
            if (showCount_ && current_ > 0) {
                oss << " (" << current_;
                if (total_ > 0) {
                    oss << "/" << total_;
                }
                oss << ")";
            }
            break;
    }

    std::cout << oss.str() << std::flush;
}

} // namespace yams::cli