#pragma once

// Shared CLI UI helper utilities for YAMS
// - Terminal width detection
// - ANSI color enablement (TTY/NO_COLOR)
// - Section headers and title banners
// - Progress bar rendering
// - Visible-width-safe truncation and padding
//
// Header-only, no external dependencies. Portable with sensible fallbacks.

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(_WIN32)
#include <io.h>
#include <windows.h>
#define YAMS_UI_ISATTY _isatty
#define YAMS_UI_FILENO _fileno
#else
#include <unistd.h>
#define YAMS_UI_ISATTY isatty
#define YAMS_UI_FILENO fileno
#if defined(__unix__) || defined(__APPLE__)
#include <sys/ioctl.h>
#endif
#endif

namespace yams::cli::ui {

struct Ansi {
    static constexpr const char* RESET = "\x1b[0m";
    static constexpr const char* BOLD = "\x1b[1m";
    static constexpr const char* DIM = "\x1b[2m";
    static constexpr const char* ITALIC = "\x1b[3m";
    static constexpr const char* UNDERLINE = "\x1b[4m";

    // Standard colors
    static constexpr const char* BLACK = "\x1b[30m";
    static constexpr const char* RED = "\x1b[31m";
    static constexpr const char* GREEN = "\x1b[32m";
    static constexpr const char* YELLOW = "\x1b[33m";
    static constexpr const char* BLUE = "\x1b[34m";
    static constexpr const char* MAGENTA = "\x1b[35m";
    static constexpr const char* CYAN = "\x1b[36m";
    static constexpr const char* WHITE = "\x1b[37m";

    // Bright colors
    static constexpr const char* BRIGHT_BLACK = "\x1b[90m";
    static constexpr const char* BRIGHT_RED = "\x1b[91m";
    static constexpr const char* BRIGHT_GREEN = "\x1b[92m";
    static constexpr const char* BRIGHT_YELLOW = "\x1b[93m";
    static constexpr const char* BRIGHT_BLUE = "\x1b[94m";
    static constexpr const char* BRIGHT_MAGENTA = "\x1b[95m";
    static constexpr const char* BRIGHT_CYAN = "\x1b[96m";
    static constexpr const char* BRIGHT_WHITE = "\x1b[97m";

    // Background colors
    static constexpr const char* BG_BLACK = "\x1b[40m";
    static constexpr const char* BG_RED = "\x1b[41m";
    static constexpr const char* BG_GREEN = "\x1b[42m";
    static constexpr const char* BG_YELLOW = "\x1b[43m";
    static constexpr const char* BG_BLUE = "\x1b[44m";
    static constexpr const char* BG_MAGENTA = "\x1b[45m";
    static constexpr const char* BG_CYAN = "\x1b[46m";
    static constexpr const char* BG_WHITE = "\x1b[47m";
};

// Basic TTY detection on stdout
inline bool stdout_is_tty() {
#if defined(_WIN32) || defined(__unix__) || defined(__APPLE__)
    return YAMS_UI_ISATTY(YAMS_UI_FILENO(stdout));
#else
    // Fallback when platform API is unavailable
    return false;
#endif
}

inline bool unicode_supported() {
#if defined(_WIN32)
    if (GetConsoleOutputCP() == 65001) {
        return true;
    }
    if (std::getenv("WT_SESSION") != nullptr) {
        return true;
    }
#endif
    const char* lc = std::getenv("LC_ALL");
    if (!lc || !*lc)
        lc = std::getenv("LC_CTYPE");
    if (!lc || !*lc)
        lc = std::getenv("LANG");
    if (!lc)
        return false;
    std::string_view v{lc};
    return v.find("UTF-8") != std::string_view::npos || v.find("utf8") != std::string_view::npos ||
           v.find("utf-8") != std::string_view::npos;
}

// Colors enabled if:
// - NO_COLOR is not set (https://no-color.org/)
// - stdout is a TTY
// - TERM is not "dumb" (when available)
enum class ColorMode { Auto, ForceOn, ForceOff };

inline ColorMode& color_mode() {
    static ColorMode m = ColorMode::Auto;
    return m;
}

// Global color control:
// - set_color_mode(ColorMode::ForceOff) to disable colors
// - set_color_mode(ColorMode::ForceOn) to force-enable colors
// - set_color_mode(ColorMode::Auto) to restore auto-detection
inline void set_color_mode(ColorMode mode) {
    color_mode() = mode;
}

// Convenience setter used by CLI flags (true -> ForceOn, false -> ForceOff)
inline void set_colors_enabled_override(bool enabled) {
    color_mode() = enabled ? ColorMode::ForceOn : ColorMode::ForceOff;
}

// Clear override and return to auto mode
inline void clear_color_override() {
    color_mode() = ColorMode::Auto;
}

inline bool colors_enabled() {
    ColorMode m = color_mode();
    if (m == ColorMode::ForceOn)
        return true;
    if (m == ColorMode::ForceOff)
        return false;

    const char* no_color = std::getenv("NO_COLOR");
    if (no_color)
        return false;
    const char* term = std::getenv("TERM");
    if (term && std::string_view(term) == "dumb")
        return false;
    return stdout_is_tty();
}

inline std::string colorize(std::string_view s, const char* code) {
    if (!colors_enabled() || code == nullptr || *code == '\0') {
        return std::string(s);
    }
    std::string out;
    out.reserve(s.size() + 16);
    out.append(code);
    out.append(s.data(), s.size());
    out.append(Ansi::RESET);
    return out;
}

// Repeat character 'ch' n times
inline std::string repeat(char ch, size_t n) {
    return std::string(n, ch);
}

// Visible width of string treating ANSI CSI sequences as zero-width.
// Note: This is a practical approximation suitable for ASCII output with ANSI styles.
inline size_t visible_width(std::string_view s) {
    size_t w = 0;
    bool in_esc = false;

    for (size_t i = 0; i < s.size(); ++i) {
        unsigned char c = static_cast<unsigned char>(s[i]);

        if (!in_esc) {
            if (c == 0x1B) { // ESC
                in_esc = true;
                continue;
            }
            // Treat bytes as single-width; this is a simplification for ASCII content.
            ++w;
        } else {
            // In CSI, consume until we hit a letter terminator (commonly 'm', 'K', etc.)
            // CSI typically starts with ESC '[' then parameter bytes then a final byte 0x40..0x7E
            if ((c >= 0x40 && c <= 0x7E)) {
                in_esc = false;
            }
        }
    }
    return w;
}

// Truncate to max visible width while preserving ANSI sequences and appending ellipsis.
// If maxw == 0, returns empty string. Ellipsis defaults to "..." (ASCII).
inline std::string truncate_to_width(std::string_view s, size_t maxw,
                                     std::string_view ellipsis = "...") {
    if (maxw == 0)
        return std::string();
    size_t vis = visible_width(s);
    if (vis <= maxw)
        return std::string(s);

    size_t ell_w = visible_width(ellipsis);
    if (ell_w >= maxw) {
        // Ellipsis itself doesn't fit; return a hard truncation to maxw
        std::string out;
        out.reserve(maxw);
        size_t w = 0;
        bool in_esc = false;
        for (size_t i = 0; i < s.size() && w < maxw; ++i) {
            unsigned char c = static_cast<unsigned char>(s[i]);
            out.push_back(static_cast<char>(c));
            if (!in_esc) {
                if (c == 0x1B) {
                    in_esc = true;
                } else {
                    ++w;
                }
            } else {
                if (c >= 0x40 && c <= 0x7E) {
                    in_esc = false;
                }
            }
        }
        return out;
    }

    const size_t target = maxw - ell_w;
    std::string out;
    out.reserve(s.size());
    size_t w = 0;
    bool in_esc = false;

    for (size_t i = 0; i < s.size(); ++i) {
        unsigned char c = static_cast<unsigned char>(s[i]);

        if (!in_esc) {
            if (c == 0x1B) {
                in_esc = true;
                out.push_back(static_cast<char>(c));
                continue;
            }
            if (w + 1 > target)
                break;
            out.push_back(static_cast<char>(c));
            ++w;
        } else {
            out.push_back(static_cast<char>(c));
            if (c >= 0x40 && c <= 0x7E) {
                in_esc = false;
            }
        }
    }

    out.append(ellipsis.data(), ellipsis.size());
    // Ensure we end outside of an escape sequence; if not, close styles.
    if (in_esc) {
        out.append(Ansi::RESET);
    }
    return out;
}

// Pad right accounting for visible width
inline std::string pad_right(std::string_view s, size_t width, char fill = ' ') {
    size_t vw = visible_width(s);
    std::string out(s);
    if (vw < width)
        out.append(width - vw, fill);
    return out;
}

// Pad left accounting for visible width
inline std::string pad_left(std::string_view s, size_t width, char fill = ' ') {
    size_t vw = visible_width(s);
    if (vw >= width)
        return std::string(s);
    return std::string(width - vw, fill) + std::string(s);
}

// Terminal width detection with fallbacks:
// - POSIX ioctl(TIOCGWINSZ) when available
// - COLUMNS environment variable
// - Default 100
inline int terminal_width(int min_width = 60, int max_width = 200, int def_width = 100) {
    int width = def_width;

#if !defined(_WIN32) && (defined(__unix__) || defined(__APPLE__))
    if (stdout_is_tty()) {
        struct winsize w{};
        if (ioctl(YAMS_UI_FILENO(stdout), TIOCGWINSZ, &w) == 0 && w.ws_col > 0) {
            width = static_cast<int>(w.ws_col);
        }
    }
#endif

    if (const char* cols = std::getenv("COLUMNS")) {
        try {
            int env_cols = std::stoi(cols);
            if (env_cols > 0)
                width = env_cols;
        } catch (...) {
            // ignore parse failures
        }
    }

    width = std::max(min_width, std::min(max_width, width));
    return width;
}

// Render a simple progress bar with optional color thresholds.
// - fraction: [0, 1]
// - width: number of cells in the bar
// - filled: glyph(s) for filled segment (default "#")
// - empty: glyph(s) for empty segment (default "-")
// - good/warn/bad: color codes selected by fraction thresholds
inline std::string progress_bar(double fraction, size_t width, std::string_view filled = "#",
                                std::string_view empty = "-", const char* good = Ansi::GREEN,
                                const char* warn = Ansi::YELLOW, const char* bad = Ansi::RED,
                                bool colorize_fill = true) {
    if (width == 0)
        return std::string();

    double f = std::clamp(fraction, 0.0, 1.0);
    const size_t fill_cells = static_cast<size_t>(std::llround(f * static_cast<double>(width)));
    std::string fill_segment, empty_segment;

    // Build segments
    for (size_t i = 0; i < fill_cells; ++i)
        fill_segment.append(filled);
    for (size_t i = 0; i < width - fill_cells; ++i)
        empty_segment.append(empty);

    if (colorize_fill && colors_enabled()) {
        const char* code = good;
        if (f >= 0.66)
            code = bad;
        else if (f >= 0.33)
            code = warn;

        std::string out;
        out.reserve(fill_segment.size() + empty_segment.size() + 16);
        out.append(code);
        out.append(fill_segment);
        out.append(Ansi::RESET);
        out.append(empty_segment);
        return out;
    }

    return fill_segment + empty_segment;
}

// Title banner: == <title> ==
inline std::string title_banner(std::string_view title, int width = -1,
                                const char* color = Ansi::BOLD) {
    if (width <= 0)
        width = terminal_width();
    std::string t = " " + std::string(title) + " ";
    int sides = std::max(0, (width - static_cast<int>(t.size()) - 4) / 2);
    std::string left = repeat('=', static_cast<size_t>(sides));
    std::string right = repeat(
        '=', static_cast<size_t>(std::max(0, width - sides - static_cast<int>(t.size()) - 4)));
    std::string line = "==" + left + t + right + "==";
    return colors_enabled() ? colorize(line, color) : line;
}

// Section header: [ Title ] ------
inline std::string section_header(std::string_view title, int width = -1,
                                  const char* color = Ansi::CYAN) {
    if (width <= 0)
        width = terminal_width();
    std::string head = "[ " + std::string(title) + " ] ";
    int dashes = std::max(0, width - static_cast<int>(head.size()));
    std::string line = head + repeat('-', static_cast<size_t>(dashes));
    return colors_enabled() ? colorize(line, color) : line;
}

// Subsection header (lighter styling): ─── Title ───
inline std::string subsection_header(std::string_view title, int width = -1) {
    if (width <= 0)
        width = terminal_width();
    std::string t = " " + std::string(title) + " ";
    int sides = std::max(0, (width - static_cast<int>(t.size())) / 2);
    std::string left = repeat('-', std::min(static_cast<size_t>(sides), size_t(3)));
    std::string right = repeat('-', std::min(static_cast<size_t>(sides), size_t(3)));
    std::string line = left + t + right;
    return colors_enabled() ? colorize(line, Ansi::DIM) : line;
}

// Box drawing characters for tables
struct Box {
    static constexpr const char* TOP_LEFT = "┌";
    static constexpr const char* TOP_RIGHT = "┐";
    static constexpr const char* BOTTOM_LEFT = "└";
    static constexpr const char* BOTTOM_RIGHT = "┘";
    static constexpr const char* HORIZONTAL = "─";
    static constexpr const char* VERTICAL = "│";
    static constexpr const char* T_DOWN = "┬";
    static constexpr const char* T_UP = "┴";
    static constexpr const char* T_RIGHT = "├";
    static constexpr const char* T_LEFT = "┤";
    static constexpr const char* CROSS = "┼";
};

// Simple row layout support
struct Row {
    std::string label;
    std::string value;
    std::string extra;
};

// Helper for formatting byte sizes in human-readable form
inline std::string format_bytes(uint64_t bytes, int precision = 1) {
    if (bytes == 0)
        return "0 B";

    const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    double value = static_cast<double>(bytes);
    int unit_idx = 0;

    while (value >= 1024.0 && unit_idx < 5) {
        value /= 1024.0;
        ++unit_idx;
    }

    std::ostringstream oss;
    if (unit_idx == 0) {
        // Bytes: no decimal places
        oss << bytes << " B";
    } else {
        // Use dynamic precision: show more precision for small values
        int prec = (value < 10.0) ? precision : 0;
        oss << std::fixed << std::setprecision(prec) << value << " " << units[unit_idx];
    }
    return oss.str();
}

// Helper for formatting large numbers with thousand separators
inline std::string format_number(uint64_t value, char separator = ',') {
    if (value < 1000)
        return std::to_string(value);

    std::string num = std::to_string(value);
    std::string result;
    result.reserve(num.size() + num.size() / 3);

    int count = 0;
    for (auto it = num.rbegin(); it != num.rend(); ++it) {
        if (count > 0 && count % 3 == 0)
            result.insert(0, 1, separator);
        result.insert(0, 1, *it);
        ++count;
    }
    return result;
}

// Helper for formatting large numbers in compact "k/M/B" notation
inline std::string format_number_compact(uint64_t value, int precision = 1) {
    if (value < 1000)
        return std::to_string(value);

    const char* suffixes[] = {"", "k", "M", "B", "T"};
    double val = static_cast<double>(value);
    int suffix_idx = 0;

    while (val >= 1000.0 && suffix_idx < 4) {
        val /= 1000.0;
        ++suffix_idx;
    }

    std::ostringstream oss;
    int prec = (val < 10.0) ? precision : 0;
    oss << std::fixed << std::setprecision(prec) << val << suffixes[suffix_idx];
    return oss.str();
}

// Helper for formatting duration in human-readable form
inline std::string format_duration(uint64_t seconds) {
    if (seconds == 0)
        return "0s";

    std::ostringstream oss;

    if (seconds >= 86400) {
        uint64_t days = seconds / 86400;
        oss << days << "d";
        seconds %= 86400;
        if (seconds >= 3600) {
            oss << " " << (seconds / 3600) << "h";
        }
    } else if (seconds >= 3600) {
        uint64_t hours = seconds / 3600;
        oss << hours << "h";
        seconds %= 3600;
        if (seconds >= 60) {
            oss << " " << (seconds / 60) << "m";
        }
    } else if (seconds >= 60) {
        uint64_t minutes = seconds / 60;
        oss << minutes << "m";
        seconds %= 60;
        if (seconds > 0) {
            oss << " " << seconds << "s";
        }
    } else {
        oss << seconds << "s";
    }

    return oss.str();
}

// Helper for formatting percentage with color coding
inline std::string format_percentage(double value, double warn_threshold = 75.0,
                                     double critical_threshold = 90.0, int precision = 1) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(precision) << value << "%";
    std::string text = oss.str();

    if (!colors_enabled())
        return text;

    if (value >= critical_threshold)
        return colorize(text, Ansi::RED);
    if (value >= warn_threshold)
        return colorize(text, Ansi::YELLOW);
    return colorize(text, Ansi::GREEN);
}

// Status indicator helpers
inline std::string status_ok(std::string_view text) {
    if (!colors_enabled())
        return "✓ " + std::string(text);
    return colorize("✓ " + std::string(text), Ansi::GREEN);
}

inline std::string status_warning(std::string_view text) {
    if (!colors_enabled())
        return "⚠ " + std::string(text);
    return colorize("⚠ " + std::string(text), Ansi::YELLOW);
}

inline std::string status_error(std::string_view text) {
    if (!colors_enabled())
        return "✗ " + std::string(text);
    return colorize("✗ " + std::string(text), Ansi::RED);
}

inline std::string status_info(std::string_view text) {
    if (!colors_enabled())
        return "ℹ " + std::string(text);
    return colorize("ℹ " + std::string(text), Ansi::BLUE);
}

inline std::string status_pending(std::string_view text) {
    if (!colors_enabled())
        return "◷ " + std::string(text);
    return colorize("◷ " + std::string(text), Ansi::YELLOW);
}

// Spinner frames for progress indication
struct Spinner {
    static constexpr const char* FRAMES[] = {"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"};
    static constexpr size_t FRAME_COUNT = 10;

    static const char* frame(size_t index) { return FRAMES[index % FRAME_COUNT]; }
};


class SpinnerRunner {
public:
    SpinnerRunner() = default;
    ~SpinnerRunner() { stop(); }
    SpinnerRunner(const SpinnerRunner&) = delete;
    SpinnerRunner& operator=(const SpinnerRunner&) = delete;

    void start(const std::string& message) {
        if (!stdout_is_tty() || running_) {
            return;
        }
        message_ = message;
        running_ = true;
        worker_ = std::thread([this]() {
            size_t idx = 0;
            while (running_) {
                std::ostringstream oss;
                oss << "\r";
                oss << Spinner::frame(idx++) << " " << message_;
                std::cout << oss.str() << std::flush;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
    }

    void stop() {
        running_ = false;
        if (worker_.joinable()) {
            worker_.join();
        }
        if (stdout_is_tty()) {
            std::cout << "\r\033[K" << std::flush;
        }
    }

private:
    std::string message_;
    std::thread worker_;
    std::atomic<bool> running_{false};
};

// Bullet point helpers for lists
inline std::string bullet(std::string_view text, int indent = 0) {
    std::string prefix(indent, ' ');
    return prefix + "• " + std::string(text);
}

inline std::string numbered_item(std::string_view text, int number, int indent = 0) {
    std::string prefix(indent, ' ');
    return prefix + std::to_string(number) + ". " + std::string(text);
}

// Key-value pair formatter
inline std::string key_value(std::string_view key, std::string_view value,
                             const char* key_color = Ansi::CYAN, int key_width = 0) {
    std::string k = std::string(key);
    if (key_width > 0) {
        k = pad_right(k, static_cast<size_t>(key_width));
    }
    if (colors_enabled() && key_color) {
        k = colorize(k, key_color);
    }
    return k + ": " + std::string(value);
}

// Render rows with dynamic width allocation and visible-width-safe truncation.
// - padding controls the spaces between columns
// - min_col_width ensures labels/values don't shrink below this
inline void render_rows(std::ostream& os, const std::vector<Row>& rows, int term_width = -1,
                        int padding = 2, size_t min_col_width = 8) {
    if (rows.empty())
        return;
    if (term_width <= 0)
        term_width = terminal_width();

    // Compute max label/value widths across rows
    size_t max_label = min_col_width;
    size_t max_value = min_col_width;
    for (const auto& r : rows) {
        max_label = std::max(max_label, visible_width(r.label));
        max_value = std::max(max_value, visible_width(r.value));
    }

    // Fixed value column start to avoid ragged spacing:
    // indent (2) + label_col + padding
    const int value_col = 2 + static_cast<int>(max_label) + padding;
    for (const auto& r : rows) {
        std::string l = r.label;
        std::string v = r.value;
        std::string e = r.extra;

        // Compute total needed width for this row
        // Compute required width using actual visible lengths, but keep a fixed value column:
        int used_value = static_cast<int>(visible_width(v));
        int used_extra = static_cast<int>(visible_width(e));
        int base = value_col + used_value; // start of value column + value width
        int need = base + (e.empty() ? 0 : padding + used_extra);
        int overflow = need - term_width;

        if (overflow > 0) {
            // Squeeze extra first
            if (!e.empty()) {
                size_t ew = visible_width(e);
                size_t target =
                    (overflow >= static_cast<int>(ew)) ? 0 : (ew - static_cast<size_t>(overflow));
                e = truncate_to_width(e, target);
                used_extra = static_cast<int>(visible_width(e));
                need = value_col + used_value + (e.empty() ? 0 : padding + used_extra);
                overflow = need - term_width;
            }
            // Then value (truncate to fit remaining width)
            if (overflow > 0) {
                size_t target = static_cast<size_t>(std::max(0, used_value - overflow));
                v = truncate_to_width(v, target);
                used_value = static_cast<int>(visible_width(v));
                need = value_col + used_value + (e.empty() ? 0 : padding + used_extra);
                overflow = need - term_width;
            }
            // Finally, label (respect min_col_width)
            if (overflow > 0 && max_label > min_col_width) {
                size_t target = std::max(min_col_width, max_label - static_cast<size_t>(overflow));
                l = truncate_to_width(l, target);
                // Recompute value_col after label shrink
                int new_label_w = static_cast<int>(visible_width(l));
                // Keep at least min_col_width for label to avoid jitter
                int label_w = std::max(static_cast<int>(min_col_width), new_label_w);
                // Update value_col accordingly
                // Note: We can’t change previously computed truncations; this only helps future
                // rows. For current line, align using the updated label_w by padding the label.
                // (handled below by pad_right)
                (void)label_w;
            }
        }

        // Render: fixed label column width, then a single padding gap, then value, then optional
        // extra
        os << "  " << pad_right(l, max_label) << std::string(padding, ' ') << v;
        if (!e.empty()) {
            os << std::string(padding, ' ') << e;
        }
        os << '\n';
    }
}

// Simple table structure for multi-column data
struct Table {
    std::vector<std::string> headers;
    std::vector<std::vector<std::string>> rows;
    bool has_header = true;

    void add_row(const std::vector<std::string>& row) { rows.push_back(row); }
};

// Render a simple table with borders
inline void render_table(std::ostream& os, const Table& table, int term_width = -1) {
    if (table.rows.empty())
        return;
    if (term_width <= 0)
        term_width = terminal_width();

    size_t num_cols =
        table.has_header ? table.headers.size() : (!table.rows.empty() ? table.rows[0].size() : 0);
    if (num_cols == 0)
        return;

    // Calculate column widths
    std::vector<size_t> col_widths(num_cols, 0);

    if (table.has_header) {
        for (size_t i = 0; i < num_cols && i < table.headers.size(); ++i) {
            col_widths[i] = visible_width(table.headers[i]);
        }
    }

    for (const auto& row : table.rows) {
        for (size_t i = 0; i < num_cols && i < row.size(); ++i) {
            col_widths[i] = std::max(col_widths[i], visible_width(row[i]));
        }
    }

    // Render header
    if (table.has_header) {
        os << "  ";
        for (size_t i = 0; i < num_cols && i < table.headers.size(); ++i) {
            if (i > 0)
                os << "  ";
            os << pad_right(table.headers[i], col_widths[i]);
        }
        os << '\n';

        // Header separator
        os << "  ";
        for (size_t i = 0; i < num_cols; ++i) {
            if (i > 0)
                os << "  ";
            os << repeat('-', col_widths[i]);
        }
        os << '\n';
    }

    // Render rows
    for (const auto& row : table.rows) {
        os << "  ";
        for (size_t i = 0; i < num_cols; ++i) {
            if (i > 0)
                os << "  ";
            std::string cell = (i < row.size()) ? row[i] : "";
            os << pad_right(cell, col_widths[i]);
        }
        os << '\n';
    }
}

// Horizontal rule / separator
inline std::string horizontal_rule(int width = -1, char ch = '-') {
    if (width <= 0)
        width = terminal_width();
    return repeat(ch, static_cast<size_t>(width));
}

// Indent helper for nested content
inline std::string indent(std::string_view text, int spaces = 2) {
    std::string prefix(spaces, ' ');
    std::string result;
    result.reserve(text.size() + prefix.size() * 10); // Rough estimate

    bool at_line_start = true;
    for (char ch : text) {
        if (at_line_start && ch != '\n') {
            result += prefix;
            at_line_start = false;
        }
        result.push_back(ch);
        if (ch == '\n') {
            at_line_start = true;
        }
    }

    return result;
}

// Progress bar with percentage
inline std::string progress_bar(double fraction, int width = 30, bool show_percentage = true) {
    double clamped = std::clamp(fraction, 0.0, 1.0);
    int filled = static_cast<int>(std::llround(clamped * static_cast<double>(width)));

    std::string bar = "[";
    bar += repeat('=', static_cast<size_t>(filled));
    bar += repeat(' ', static_cast<size_t>(width - filled));
    bar += "]";

    if (show_percentage) {
        bar += " " + format_percentage(fraction);
    }

    // Color code based on progress
    if (colors_enabled()) {
        const char* color = Ansi::RED;
        if (clamped >= 0.9)
            color = Ansi::GREEN;
        else if (clamped >= 0.5)
            color = Ansi::YELLOW;
        return colorize(bar, color);
    }

    return bar;
}

// Word wrap text to fit within width
inline std::vector<std::string> wrap_text(std::string_view text, int width = -1) {
    if (width <= 0)
        width = terminal_width();

    std::vector<std::string> lines;
    std::string current_line;
    std::string current_word;

    for (char ch : text) {
        if (ch == '\n') {
            if (!current_word.empty()) {
                if (!current_line.empty())
                    current_line += ' ';
                current_line += current_word;
                current_word.clear();
            }
            lines.push_back(current_line);
            current_line.clear();
        } else if (ch == ' ' || ch == '\t') {
            if (!current_word.empty()) {
                if (!current_line.empty()) {
                    if (static_cast<int>(visible_width(current_line + ' ' + current_word)) <=
                        width) {
                        current_line += ' ' + current_word;
                    } else {
                        lines.push_back(current_line);
                        current_line = current_word;
                    }
                } else {
                    current_line = current_word;
                }
                current_word.clear();
            }
        } else {
            current_word.push_back(ch);
        }
    }

    // Handle remaining word
    if (!current_word.empty()) {
        if (!current_line.empty()) {
            if (static_cast<int>(visible_width(current_line + ' ' + current_word)) <= width) {
                current_line += ' ' + current_word;
            } else {
                lines.push_back(current_line);
                current_line = current_word;
            }
        } else {
            current_line = current_word;
        }
    }

    if (!current_line.empty()) {
        lines.push_back(current_line);
    }

    return lines;
}

// Center text within width
inline std::string center_text(std::string_view text, int width = -1) {
    if (width <= 0)
        width = terminal_width();

    int text_width = static_cast<int>(visible_width(text));
    if (text_width >= width)
        return std::string(text);

    int padding = (width - text_width) / 2;
    return repeat(' ', static_cast<size_t>(padding)) + std::string(text);
}

} // namespace yams::cli::ui
