#include <yams/extraction/format_handlers/format_handler.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <limits>
#include <string_view>

namespace yams::extraction::format {

// Helpers
static inline std::string trim(std::string_view sv) {
    auto l = sv.begin();
    auto r = sv.end();

    while (l != r && std::isspace(static_cast<unsigned char>(*l))) {
        ++l;
    }
    while (r != l) {
        auto prev = r;
        --prev;
        if (!std::isspace(static_cast<unsigned char>(*prev))) {
            break;
        }
        r = prev;
    }
    return std::string(l, r);
}

static inline bool parseInt(std::string_view sv, int& out) {
    sv = std::string_view(trim(sv));
    if (sv.empty())
        return false;

    int value = 0;
    auto* first = sv.data();
    auto* last = sv.data() + sv.size();
    auto res = std::from_chars(first, last, value);
    if (res.ec != std::errc{} || res.ptr != last)
        return false;
    out = value;
    return true;
}

// Parse list token like "N" or "A-B" into [start,end] (1-based, closed interval)
static Result<std::pair<int, int>> parseRangeToken(std::string_view tok) {
    auto s = trim(tok);
    if (s.empty()) {
        return Error{ErrorCode::InvalidArgument, "Empty range token"};
    }

    // Find hyphen, if any
    auto pos = s.find('-');
    if (pos == std::string_view::npos) {
        // Single number
        int v = 0;
        if (!parseInt(s, v) || v <= 0) {
            return Error{ErrorCode::InvalidArgument, "Invalid range value: " + std::string(s)};
        }
        return std::make_pair(v, v);
    }

    // A-B
    std::string_view sv{s};
    std::string_view a = sv.substr(0, pos);
    std::string_view b = sv.substr(pos + 1);

    int av = 0;
    int bv = 0;
    if (!parseInt(a, av) || av <= 0) {
        return Error{ErrorCode::InvalidArgument, "Invalid range start: " + std::string(a)};
    }
    if (!parseInt(b, bv) || bv <= 0) {
        return Error{ErrorCode::InvalidArgument, "Invalid range end: " + std::string(b)};
    }
    if (bv < av) {
        return Error{ErrorCode::InvalidArgument,
                     "Range end less than start: " + std::to_string(av) + "-" + std::to_string(bv)};
    }
    return std::make_pair(av, bv);
}

Result<std::vector<std::pair<int, int>>> parseClosedRanges(const std::string& expr) {
    std::vector<std::pair<int, int>> ranges;
    if (expr.empty()) {
        // Empty means "no restriction" — return empty set to signal caller
        return ranges;
    }

    std::string token;
    token.reserve(16);
    auto flushToken = [&](std::string_view t) -> Result<void> {
        if (t.empty())
            return Result<void>(); // skip empties (e.g., consecutive commas)
        auto pr = parseRangeToken(t);
        if (!pr) {
            return Error{ErrorCode::InvalidArgument, pr.error().message};
        }
        ranges.push_back(pr.value());
        return Result<void>();
    };

    // Split by commas
    std::string_view sv(expr);
    size_t start = 0;
    while (start <= sv.size()) {
        size_t comma = sv.find(',', start);
        if (comma == std::string_view::npos) {
            auto res = flushToken(sv.substr(start));
            if (!res)
                return Error{ErrorCode::InvalidArgument, res.error().message};
            break;
        } else {
            auto res = flushToken(sv.substr(start, comma - start));
            if (!res)
                return Error{ErrorCode::InvalidArgument, res.error().message};
            start = comma + 1;
        }
    }

    // Normalize: merge overlapping/adjacent intervals
    if (!ranges.empty()) {
        std::sort(ranges.begin(), ranges.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });
        std::vector<std::pair<int, int>> merged;
        merged.reserve(ranges.size());
        auto cur = ranges.front();
        for (size_t i = 1; i < ranges.size(); ++i) {
            const auto& nxt = ranges[i];
            if (nxt.first <= cur.second + 1) {
                // overlap or adjacent; merge
                cur.second = std::max(cur.second, nxt.second);
            } else {
                merged.push_back(cur);
                cur = nxt;
            }
        }
        merged.push_back(cur);
        ranges.swap(merged);
    }

    return ranges;
}

// Built-in handler stubs wiring
// In Phase 1, we rely on existing extractors to provide text.
// Specific handlers (text/markdown/html/code) can be registered here when implemented.
// For now, this function is a no-op placeholder to keep linkage and call sites stable.
void registerBuiltinHandlers(HandlerRegistry& registry) {
    (void)registry;
    // Example for future implementation:
    // registry.registerHandler(std::make_shared<TextLineRangeHandler>());
    // registry.registerHandler(std::make_shared<HtmlBasicHandler>());
    // #if YAMS_HAS_PDF_SUPPORT
    // registry.registerHandler(std::make_shared<PdfMvpHandler>());
    // #endif
}

} // namespace yams::extraction::format