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
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#if defined(_WIN32)
#include <io.h>
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
    static constexpr const char* RED = "\x1b[31m";
    static constexpr const char* GREEN = "\x1b[32m";
    static constexpr const char* YELLOW = "\x1b[33m";
    static constexpr const char* BLUE = "\x1b[34m";
    static constexpr const char* MAGENTA = "\x1b[35m";
    static constexpr const char* CYAN = "\x1b[36m";
    static constexpr const char* WHITE = "\x1b[37m";
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
inline std::string title_banner(std::string_view title, int width = -1) {
    if (width <= 0)
        width = terminal_width();
    std::string t = " " + std::string(title) + " ";
    int sides = std::max(0, (width - static_cast<int>(t.size()) - 4) / 2);
    std::string left = repeat('=', static_cast<size_t>(sides));
    std::string right = repeat(
        '=', static_cast<size_t>(std::max(0, width - sides - static_cast<int>(t.size()) - 4)));
    std::string line = "==" + left + t + right + "==";
    return colors_enabled() ? colorize(line, Ansi::MAGENTA) : line;
}

// Section header: [ Title ] ------
inline std::string section_header(std::string_view title, int width = -1) {
    if (width <= 0)
        width = terminal_width();
    std::string head = "[ " + std::string(title) + " ] ";
    int dashes = std::max(0, width - static_cast<int>(head.size()));
    std::string line = head + repeat('-', static_cast<size_t>(dashes));
    return colors_enabled() ? colorize(line, Ansi::CYAN) : line;
}

// Simple row layout support
struct Row {
    std::string label;
    std::string value;
    std::string extra;
};

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

} // namespace yams::cli::ui