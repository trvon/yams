#pragma once
#include <algorithm>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::cli {

// Severity levels influence ordering & symbol choice.
enum class RecommendationSeverity {
    Critical = 0, // must address immediately
    Warning = 1,  // should address soon
    Info = 2,     // nice to have / optimization
};

struct Recommendation {
    RecommendationSeverity severity{RecommendationSeverity::Info};
    std::string message;                // Human readable line (no trailing punctuation enforced)
    std::string code;                   // Stable machine-readable code (e.g. "embeddings.missing")
    std::optional<std::string> details; // Optional extended context (one line)

    bool operator==(const Recommendation& other) const noexcept {
        return severity == other.severity && code == other.code && message == other.message;
    }
};

class RecommendationBuilder {
public:
    RecommendationBuilder& add(Recommendation rec) {
        // Deduplicate by code (if provided) else by full message
        if (!rec.code.empty()) {
            if (codes_.insert(rec.code).second) {
                recs_.push_back(std::move(rec));
            }
        } else {
            if (messages_.insert(rec.message).second) {
                recs_.push_back(std::move(rec));
            }
        }
        return *this;
    }

    RecommendationBuilder& critical(std::string code, std::string msg,
                                    std::optional<std::string> details = std::nullopt) {
        return add({RecommendationSeverity::Critical, std::move(msg), std::move(code),
                    std::move(details)});
    }
    RecommendationBuilder& warning(std::string code, std::string msg,
                                   std::optional<std::string> details = std::nullopt) {
        return add(
            {RecommendationSeverity::Warning, std::move(msg), std::move(code), std::move(details)});
    }
    RecommendationBuilder& info(std::string code, std::string msg,
                                std::optional<std::string> details = std::nullopt) {
        return add(
            {RecommendationSeverity::Info, std::move(msg), std::move(code), std::move(details)});
    }

    const std::vector<Recommendation>& items() const { return recs_; }

    std::vector<Recommendation> sorted() const {
        std::vector<Recommendation> out = recs_;
        std::stable_sort(out.begin(), out.end(),
                         [](const Recommendation& a, const Recommendation& b) {
                             if (a.severity != b.severity)
                                 return static_cast<int>(a.severity) < static_cast<int>(b.severity);
                             return a.code < b.code; // deterministic
                         });
        return out;
    }

    bool empty() const { return recs_.empty(); }
    size_t size() const { return recs_.size(); }

private:
    std::vector<Recommendation> recs_;
    std::unordered_set<std::string> codes_;
    std::unordered_set<std::string> messages_;
};

// Symbols per severity for text output.
inline const char* recommendationSymbol(RecommendationSeverity sev) {
    switch (sev) {
        case RecommendationSeverity::Critical:
            return "⚠"; // High urgency
        case RecommendationSeverity::Warning:
            return "□"; // Actionable
        case RecommendationSeverity::Info:
            return "→"; // Informational / nice-to-have
    }
    return "→";
}

// Forward declarations for helpers used below
inline std::string escape(const std::string& in);
inline std::string severityString(RecommendationSeverity sev);

// Helpers for severity string & JSON escaping
inline std::string severityString(RecommendationSeverity sev) {
    switch (sev) {
        case RecommendationSeverity::Critical:
            return "critical";
        case RecommendationSeverity::Warning:
            return "warning";
        case RecommendationSeverity::Info:
            return "info";
    }
    return "info";
}

// JSON escaping utility
inline std::string escape(const std::string& in) {
    std::string out;
    out.reserve(in.size() + 8);
    for (char c : in) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out += c;
                break;
        }
    }
    return out;
}

// Render a block of recommendations (text mode). Each line: <symbol> <message> [ — details]
inline void printRecommendationsText(const RecommendationBuilder& builder, std::ostream& os) {
    if (builder.empty())
        return;
    auto sorted = builder.sorted();
    os << "\nRecommendations:\n";
    for (const auto& r : sorted) {
        os << "  " << recommendationSymbol(r.severity) << ' ' << r.message;
        if (r.details && !r.details->empty()) {
            os << " — " << *r.details;
        }
        os << "\n";
    }
}

// Serialize recommendations to a compact JSON array (no external dependency assumed here).
inline std::string recommendationsToJson(const RecommendationBuilder& builder) {
    std::string json = "[";
    auto sorted = builder.sorted();
    for (size_t i = 0; i < sorted.size(); ++i) {
        const auto& r = sorted[i];
        if (i > 0)
            json += ",";
        json += "{\"code\":\"" + escape(r.code) + "\",\"severity\":\"" +
                severityString(r.severity) + "\",\"message\":\"" + escape(r.message) + "\"";
        if (r.details)
            json += ",\"details\":\"" + escape(*r.details) + "\"";
        json += "}";
    }
    json += "]";
    return json;
}

} // namespace yams::cli
