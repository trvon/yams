#include <iomanip>
#include <sstream>
#include <yams/cli/progress_indicator.h>
#include <yams/cli/ui_helpers.hpp>

namespace yams::cli {

// Define static members
constexpr int ProgressIndicator::SPINNER_COUNT;

ProgressIndicator::ProgressIndicator(Style style, bool autoStart) : style_(style) {
    // Nothing else
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

    const bool isTty = ui::stdout_is_tty();
    switch (style_) {
        case Style::Spinner:
            if (!isTty) {
                // Non-TTY: simple dots progression
                oss << message_;
                int dots = (spinnerIndex_ % 4) + 1;
                for (int i = 0; i < dots; ++i)
                    oss << ".";
            } else {
                oss << ui::Spinner::frame(static_cast<size_t>(spinnerIndex_)) << " " << message_;
            }
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
                oss << ui::Spinner::frame(static_cast<size_t>(spinnerIndex_)) << " " << message_;
                if (showCount_ && current_ > 0) {
                    oss << " (" << current_ << ")";
                }
            }
            break;

        case Style::Bar:
            if (total_ > 0) {
                const double fraction =
                    total_ > 0 ? (static_cast<double>(current_) / static_cast<double>(total_))
                               : 0.0;
                if (isTty) {
                    oss << ui::progress_bar(fraction, 20, true) << " " << message_;
                } else {
                    int percent = static_cast<int>((current_ * 100) / total_);
                    oss << "[" << std::setw(3) << percent << "%] " << message_;
                }
            } else {
                // Fall back to spinner for indeterminate progress
                oss << ui::Spinner::frame(static_cast<size_t>(spinnerIndex_)) << " " << message_;
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

// no extra methods

} // namespace yams::cli
