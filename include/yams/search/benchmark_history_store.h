#pragma once

#include <yams/core/types.h>
#include <yams/search/internal_benchmark.h>

#include <nlohmann/json.hpp>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace yams::search {

/**
 * @brief Append-only JSON-lines-style store of `BenchmarkResults` rows.
 *
 * Each call to `append()` writes a single JSON object serialized from
 * `BenchmarkResults::toJson()` plus extra keys captured at append time
 * (`config_hash`, optional corpus/topology epoch stamps). The file format is a
 * JSON array — reads load the whole array, appends rewrite via temp-file +
 * atomic rename so a crashed writer cannot leave a half-written row visible to
 * a future reader.
 *
 * The store is intentionally file-based (not SQLite) so that it has zero
 * schema migration cost and can be inspected with standard tools. Row count
 * is expected to stay <10k in practice.
 */
class BenchmarkHistoryStore {
public:
    struct Row {
        BenchmarkResults results;
        std::string configHash; // Opaque fingerprint of the benchmark+engine config used.
        std::optional<std::uint64_t> lexicalEpoch;  // From IndexFreshnessSnapshot, if available.
        std::optional<std::uint64_t> topologyEpoch; // From TopologyArtifactBatch, if available.

        [[nodiscard]] nlohmann::json toJson() const;
        static Row fromJson(const nlohmann::json& j);
    };

    /**
     * @brief Construct a store rooted at `path`. Parent directory is created
     * lazily on first append.
     */
    explicit BenchmarkHistoryStore(std::filesystem::path path);

    /**
     * @brief Append a new row. Returns an error if the file cannot be written.
     */
    [[nodiscard]] Result<void> append(const Row& row);

    /**
     * @brief Read up to `limit` most recent rows (newest last). Returns an
     * empty vector if the file does not exist; returns an error only on I/O
     * or parse failure.
     */
    [[nodiscard]] Result<std::vector<Row>> read(std::size_t limit = 100) const;

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
