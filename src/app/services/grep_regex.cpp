// Boost.Regex abandons a search after max(N·S², min(N², cap)) + 100000 states, where N is the
// input length and S the compiled pattern size. The default cap is 1e8; grep lines are short,
// so 1e7 keeps a pathological line to tens of milliseconds. No other translation unit in YAMS
// includes Boost.Regex, so this definition cannot disagree with another one.
#ifndef BOOST_REGEX_MAX_STATE_COUNT
#define BOOST_REGEX_MAX_STATE_COUNT 10000000
#endif

#include <yams/app/services/grep_regex.hpp>

#include <boost/regex.hpp>
#include <spdlog/spdlog.h>

#include <stdexcept>

namespace yams::app::services {

namespace {

#if defined(__has_feature)
#if __has_feature(address_sanitizer)
constexpr bool kDisableRe2UnderAsan = true;
#else
constexpr bool kDisableRe2UnderAsan = false;
#endif
#elif defined(__SANITIZE_ADDRESS__)
constexpr bool kDisableRe2UnderAsan = true;
#else
constexpr bool kDisableRe2UnderAsan = false;
#endif

} // namespace

struct GrepRegex::Backtracker {
    boost::regex re;
};

GrepRegex::GrepRegex() = default;
GrepRegex::~GrepRegex() = default;

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
    // (the caller will fall back to the backtracker).
    return std::string(pattern);
}

// ---------------------------------------------------------------------------
// Compile
// ---------------------------------------------------------------------------

std::optional<GrepRegex> GrepRegex::compile(std::string_view pattern, bool ignoreCase) {
    GrepRegex result;

#if YAMS_HAS_RE2
    // RE2 trips ASan container-overflow reports in libc++ vector relocation on this toolchain.
    // Keep sanitizer builds on the backtracker so the grep stack remains testable.
    if (!kDisableRe2UnderAsan) {
        // Try RE2 first: linear time, 10-50x faster for most patterns.
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

        // RE2 rejected the pattern: fall back to the backtracker.
        result.fallbackReason_ = re->error();
        spdlog::debug("[GrepRegex] RE2 rejected '{}': {}; falling back to Boost.Regex", pattern,
                      result.fallbackReason_);
    } else {
        result.fallbackReason_ = "RE2 disabled under AddressSanitizer";
        spdlog::debug("[GrepRegex] Skipping RE2 under ASan for '{}'", pattern);
    }
#endif

    // Bounded backtracker. ECMAScript is Perl syntax in Boost: a superset of std::regex
    // ECMAScript that also accepts lookbehind.
    try {
        boost::regex::flag_type flags = boost::regex::ECMAScript;
        if (ignoreCase)
            flags |= boost::regex::icase;
        result.backtracker_ =
            std::make_unique<Backtracker>(Backtracker{boost::regex(std::string(pattern), flags)});
        result.usesRe2_ = false;
        return result;
    } catch (const std::exception& e) {
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
        // Pass the whole buffer with a start position so ^ and \b see the preceding bytes.
        re2::StringPiece input(data, len);
        re2::StringPiece match;
        if (re2_->Match(input, offset, len, re2::RE2::UNANCHORED, &match, 1)) {
            return GrepRegexMatch{
                .position = static_cast<size_t>(match.data() - data),
                .length = match.size(),
            };
        }
        return std::nullopt;
    }
#endif

    if (backtracker_) {
        boost::match_flag_type flags = boost::match_default | boost::match_not_dot_newline;
        if (offset > 0) {
            flags |= boost::match_prev_avail;
        }
        boost::cmatch cm;
        try {
            if (boost::regex_search(data + offset, data + len, cm, backtracker_->re, flags)) {
                return GrepRegexMatch{
                    .position = static_cast<size_t>(cm[0].first - data),
                    .length = static_cast<size_t>(cm[0].second - cm[0].first),
                };
            }
        } catch (const std::runtime_error&) {
            // State budget or match stack exhausted. Report no match and count it.
            budgetExceeded_.fetch_add(1, std::memory_order_relaxed);
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
      backtracker_(std::move(other.backtracker_)), usesRe2_(other.usesRe2_),
      fallbackReason_(std::move(other.fallbackReason_)),
      budgetExceeded_(other.budgetExceeded_.load(std::memory_order_relaxed)) {
    other.usesRe2_ = false;
}

GrepRegex& GrepRegex::operator=(GrepRegex&& other) noexcept {
    if (this != &other) {
#if YAMS_HAS_RE2
        re2_ = std::move(other.re2_);
#endif
        backtracker_ = std::move(other.backtracker_);
        usesRe2_ = other.usesRe2_;
        fallbackReason_ = std::move(other.fallbackReason_);
        budgetExceeded_.store(other.budgetExceeded_.load(std::memory_order_relaxed),
                              std::memory_order_relaxed);
        other.usesRe2_ = false;
    }
    return *this;
}

} // namespace yams::app::services
