#pragma once

#include <yams/core/types.h>

#include <nlohmann/json.hpp>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace yams::search {

/**
 * @brief Per-result user relevance verdict.
 *
 * A session contains one label per retrieved result (up to top-K).
 */
enum class RelevanceLabel : std::uint8_t {
    Unknown = 0, // not labeled (e.g., user skipped or quit mid-query)
    Relevant = 1,
    NotRelevant = 2,
};

[[nodiscard]] constexpr const char* relevanceLabelToString(RelevanceLabel l) noexcept {
    switch (l) {
        case RelevanceLabel::Unknown:
            return "unknown";
        case RelevanceLabel::Relevant:
            return "relevant";
        case RelevanceLabel::NotRelevant:
            return "not_relevant";
    }
    return "unknown";
}

[[nodiscard]] constexpr RelevanceLabel relevanceLabelFromString(std::string_view s) noexcept {
    if (s == "relevant" || s == "y" || s == "yes") {
        return RelevanceLabel::Relevant;
    }
    if (s == "not_relevant" || s == "n" || s == "no") {
        return RelevanceLabel::NotRelevant;
    }
    return RelevanceLabel::Unknown;
}

/**
 * @brief A single labeled query within an interactive-tuner session.
 *
 * `rankedDocHashes[i]` is the result at rank i (0-based); `labels[i]` is
 * the user's verdict for that result. When the user skipped or quit before
 * reaching rank i, the label stays `Unknown`.
 *
 * `reward` is the position-discounted precision computed from labels
 * (sum_i rel_i / log2(i+2) normalized by the maximum possible for K).
 */
struct LabeledQuery {
    std::string queryText;
    std::vector<std::string> rankedDocHashes;
    std::vector<RelevanceLabel> labels;
    double reward = 0.0;
};

/**
 * @brief A single interactive-tuner session — one run of `yams tune`.
 *
 * Sessions are the persistence unit and the reward-aggregation unit for
 * the user-labeled tuning channel (`SearchTuner::observeRelevanceFeedback`).
 */
struct RelevanceSession {
    std::string timestamp; // ISO8601 of session start
    std::string configHash;
    std::optional<std::uint64_t> corpusEpoch;
    std::optional<std::uint64_t> topologyEpoch;
    std::string source = "interactive"; // "interactive" | "auto" | future
    std::size_t k = 10;                 // top-K labeled per query
    std::vector<LabeledQuery> queries;

    /**
     * @brief Mean per-query reward across all queries in this session.
     *
     * Returns 0.0 when the session has no queries.
     */
    [[nodiscard]] double meanReward() const noexcept;

    [[nodiscard]] nlohmann::json toJson() const;
    static RelevanceSession fromJson(const nlohmann::json& j);
};

/**
 * @brief Append-only JSONL store of user-labeled tuning sessions.
 *
 * Mirrors `BenchmarkHistoryStore` but writes one line per session (JSONL)
 * rather than a single JSON array. JSONL allows atomic O_APPEND writes and
 * keeps the file inspectable without rewriting on every append. Corrupt
 * lines are tolerated on read: a malformed line is logged and skipped.
 *
 * This store is explicitly non-production infrastructure: missing file or
 * corrupt lines yield empty results, never errors.
 */
class RelevanceLabelStore {
public:
    explicit RelevanceLabelStore(std::filesystem::path path);

    /**
     * @brief Append one session. Creates parent directory on first call.
     */
    [[nodiscard]] Result<void> append(const RelevanceSession& session);

    /**
     * @brief Read up to `limit` most recent sessions (newest last).
     *
     * Missing file returns an empty vector. Malformed lines are logged and
     * skipped. Returns an error only on filesystem I/O failure.
     */
    [[nodiscard]] Result<std::vector<RelevanceSession>> readRecent(std::size_t limit = 100) const;

    /**
     * @brief Read sessions matching `configHash` (newest last).
     */
    [[nodiscard]] Result<std::vector<RelevanceSession>>
    readByConfigHash(std::string_view configHash, std::size_t limit = 100) const;

    /**
     * @brief Remove the underlying file. Used by tests and `--reset`.
     */
    [[nodiscard]] Result<void> clear();

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
    mutable std::mutex mtx_;
};

} // namespace yams::search
