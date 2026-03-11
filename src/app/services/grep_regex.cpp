#include <yams/app/services/grep_regex.hpp>

#include <spdlog/spdlog.h>

namespace yams::app::services {

// ---------------------------------------------------------------------------
// Pattern translation: ECMAScript → RE2-compatible
// ---------------------------------------------------------------------------

std::string GrepRegex::translateForRe2(std::string_view pattern) {
    // RE2 natively supports \d, \w, \s, \b, character classes, quantifiers,
    // alternation, and non-capturing groups (?:...).
    //
    // It does NOT support:
    //   - Backreferences (\1, \2, ...)
    //   - Lookahead (?=...) / (?!...)
    //   - Lookbehind (?<=...) / (?<!...)
    //   - Atomic groups (?>...)
    //   - Possessive quantifiers (++, *+, ?+)
    //
    // For the patterns grep typically handles, no translation is needed.
    // We return the pattern as-is and let RE2 reject unsupported constructs
    // (the caller will fall back to std::regex).
    return std::string(pattern);
}

// ---------------------------------------------------------------------------
// Compile
// ---------------------------------------------------------------------------

std::optional<GrepRegex> GrepRegex::compile(std::string_view pattern, bool ignoreCase) {
    GrepRegex result;

#if YAMS_HAS_RE2
    // Try RE2 first — 10-50x faster for most patterns.
    {
        re2::RE2::Options opts;
        opts.set_case_sensitive(!ignoreCase);
        opts.set_log_errors(false); // Don't spam stderr on unsupported patterns.
        opts.set_dot_nl(false);     // '.' does not match '\n' (grep convention).
        opts.set_one_line(false);   // ^ and $ match line boundaries.

        auto translated = translateForRe2(pattern);
        auto re = std::make_unique<re2::RE2>(translated, opts);

        if (re->ok()) {
            result.re2_ = std::move(re);
            result.usesRe2_ = true;
            spdlog::debug("[GrepRegex] Compiled with RE2: '{}'", pattern);
            return result;
        }

        // RE2 rejected the pattern — fall back to std::regex.
        result.fallbackReason_ = re->error();
        spdlog::debug("[GrepRegex] RE2 rejected '{}': {}; falling back to std::regex", pattern,
                      result.fallbackReason_);
    }
#endif

    // std::regex fallback.
    try {
        std::regex_constants::syntax_option_type flags = std::regex::ECMAScript;
        if (ignoreCase)
            flags |= std::regex::icase;
        result.stdRegex_.emplace(std::string(pattern), flags);
        result.usesRe2_ = false;
        return result;
    } catch (const std::regex_error& e) {
        spdlog::warn("[GrepRegex] Invalid regex '{}': {}", pattern, e.what());
        return std::nullopt;
    }
}

// ---------------------------------------------------------------------------
// findNext — raw char* buffer variant (hot inner loop)
// ---------------------------------------------------------------------------

std::optional<GrepRegexMatch> GrepRegex::findNext(const char* data, size_t len,
                                                  size_t offset) const {
    if (offset >= len)
        return std::nullopt;

#if YAMS_HAS_RE2
    if (usesRe2_ && re2_) {
        re2::StringPiece input(data + offset, len - offset);
        re2::StringPiece match;
        if (re2_->Match(input, 0, static_cast<int>(input.size()), re2::RE2::UNANCHORED, &match,
                        1)) {
            return GrepRegexMatch{
                .position = static_cast<size_t>(match.data() - data),
                .length = match.size(),
            };
        }
        return std::nullopt;
    }
#endif

    if (stdRegex_) {
        std::cmatch cm;
        if (std::regex_search(data + offset, data + len, cm, *stdRegex_)) {
            return GrepRegexMatch{
                .position = static_cast<size_t>(cm.position(0)) + offset,
                .length = static_cast<size_t>(cm.length(0)),
            };
        }
    }
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// findFirst — string_view convenience
// ---------------------------------------------------------------------------

std::optional<GrepRegexMatch> GrepRegex::findFirst(std::string_view text) const {
    return findNext(text.data(), text.size(), 0);
}

// ---------------------------------------------------------------------------
// countMatches — count all non-overlapping matches
// ---------------------------------------------------------------------------

size_t GrepRegex::countMatches(const char* data, size_t len) const {
    size_t count = 0;
    size_t offset = 0;
    while (offset < len) {
        auto m = findNext(data, len, offset);
        if (!m)
            break;
        ++count;
        // Advance past this match (at least 1 byte to avoid infinite loop on zero-width).
        offset = m->position + std::max<size_t>(m->length, 1);
    }
    return count;
}

// ---------------------------------------------------------------------------
// Move / Destructor
// ---------------------------------------------------------------------------

GrepRegex::GrepRegex(GrepRegex&& other) noexcept
    :
#if YAMS_HAS_RE2
      re2_(std::move(other.re2_)),
#endif
      stdRegex_(std::move(other.stdRegex_)), usesRe2_(other.usesRe2_),
      fallbackReason_(std::move(other.fallbackReason_)) {
    other.usesRe2_ = false;
}

GrepRegex& GrepRegex::operator=(GrepRegex&& other) noexcept {
    if (this != &other) {
#if YAMS_HAS_RE2
        re2_ = std::move(other.re2_);
#endif
        stdRegex_ = std::move(other.stdRegex_);
        usesRe2_ = other.usesRe2_;
        fallbackReason_ = std::move(other.fallbackReason_);
        other.usesRe2_ = false;
    }
    return *this;
}

GrepRegex::~GrepRegex() = default;

} // namespace yams::app::services
