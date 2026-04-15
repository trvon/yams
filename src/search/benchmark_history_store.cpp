#include <yams/search/benchmark_history_store.h>

#include <spdlog/spdlog.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <system_error>

namespace yams::search {

nlohmann::json BenchmarkHistoryStore::Row::toJson() const {
    auto j = results.toJson();
    j["config_hash"] = configHash;
    if (lexicalEpoch) {
        j["lexical_epoch"] = *lexicalEpoch;
    }
    if (topologyEpoch) {
        j["topology_epoch"] = *topologyEpoch;
    }
    return j;
}

BenchmarkHistoryStore::Row BenchmarkHistoryStore::Row::fromJson(const nlohmann::json& j) {
    Row row;
    row.results.mrr = j.value("mrr", 0.0f);
    row.results.recallAtK = j.value("recall_at_k", 0.0f);
    row.results.precisionAtK = j.value("precision_at_k", 0.0f);
    row.results.k = j.value("k", static_cast<std::size_t>(10));
    row.results.queriesRun = j.value("queries_run", static_cast<std::size_t>(0));
    row.results.queriesSucceeded = j.value("queries_succeeded", static_cast<std::size_t>(0));
    row.results.queriesFailed = j.value("queries_failed", static_cast<std::size_t>(0));
    row.results.totalTime = std::chrono::milliseconds{j.value("total_time_ms", std::int64_t{0})};
    row.results.timestamp = j.value("timestamp", std::string{});
    if (j.contains("latency")) {
        const auto& l = j["latency"];
        row.results.latency.meanMs = l.value("mean_ms", 0.0);
        row.results.latency.medianMs = l.value("median_ms", 0.0);
        row.results.latency.p95Ms = l.value("p95_ms", 0.0);
        row.results.latency.p99Ms = l.value("p99_ms", 0.0);
        row.results.latency.maxMs = l.value("max_ms", 0.0);
        row.results.latency.minMs = l.value("min_ms", 0.0);
    }
    if (j.contains("tuning_state")) {
        row.results.tuningState = j.at("tuning_state").get<std::string>();
    }
    if (j.contains("tuned_params")) {
        row.results.tunedParams = j.at("tuned_params");
    }
    row.configHash = j.value("config_hash", std::string{});
    if (j.contains("lexical_epoch")) {
        row.lexicalEpoch = j.at("lexical_epoch").get<std::uint64_t>();
    }
    if (j.contains("topology_epoch")) {
        row.topologyEpoch = j.at("topology_epoch").get<std::uint64_t>();
    }
    return row;
}

BenchmarkHistoryStore::BenchmarkHistoryStore(std::filesystem::path path) : path_(std::move(path)) {}

namespace {

Result<nlohmann::json> loadArray(const std::filesystem::path& p) {
    if (!std::filesystem::exists(p)) {
        return nlohmann::json::array();
    }
    std::ifstream in(p);
    if (!in) {
        return Error{ErrorCode::IOError, "Cannot open benchmark history: " + p.string()};
    }
    try {
        nlohmann::json j = nlohmann::json::parse(in, nullptr, /*allow_exceptions=*/true,
                                                 /*ignore_comments=*/true);
        if (!j.is_array()) {
            spdlog::warn("Benchmark history at {} is not a JSON array; starting fresh", p.string());
            return nlohmann::json::array();
        }
        return j;
    } catch (const std::exception& e) {
        spdlog::warn("Benchmark history at {} is corrupt ({}); starting fresh", p.string(),
                     e.what());
        return nlohmann::json::array();
    }
}

Result<void> writeAtomic(const std::filesystem::path& dst, const std::string& contents) {
    std::error_code ec;
    std::filesystem::create_directories(dst.parent_path(), ec);
    if (ec) {
        return Error{ErrorCode::IOError,
                     "Cannot create parent directory: " + dst.parent_path().string() + " (" +
                         ec.message() + ")"};
    }
    auto tmp = dst;
    tmp += ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) {
            return Error{ErrorCode::IOError, "Cannot open temp file for write: " + tmp.string()};
        }
        out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
        if (!out) {
            return Error{ErrorCode::IOError,
                         "Write failed for benchmark history temp: " + tmp.string()};
        }
    }
    std::filesystem::rename(tmp, dst, ec);
    if (ec) {
        return Error{ErrorCode::IOError, "Atomic rename failed: " + tmp.string() + " -> " +
                                             dst.string() + " (" + ec.message() + ")"};
    }
    return {};
}

} // namespace

Result<void> BenchmarkHistoryStore::append(const Row& row) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto arr = loadArray(path_);
    if (!arr) {
        return arr.error();
    }
    nlohmann::json updated = std::move(arr).value();
    updated.push_back(row.toJson());
    return writeAtomic(path_, updated.dump(2));
}

Result<std::vector<BenchmarkHistoryStore::Row>>
BenchmarkHistoryStore::read(std::size_t limit) const {
    std::lock_guard<std::mutex> lock(mtx_);
    auto arr = loadArray(path_);
    if (!arr) {
        return arr.error();
    }
    const auto& json = arr.value();
    std::vector<Row> rows;
    rows.reserve(std::min(limit, json.size()));
    std::size_t start = json.size() > limit ? json.size() - limit : 0;
    for (std::size_t i = start; i < json.size(); ++i) {
        try {
            rows.push_back(Row::fromJson(json[i]));
        } catch (const std::exception& e) {
            spdlog::warn("Skipping malformed benchmark history row {}: {}", i, e.what());
        }
    }
    return rows;
}

Result<void> BenchmarkHistoryStore::clear() {
    std::lock_guard<std::mutex> lock(mtx_);
    std::error_code ec;
    std::filesystem::remove(path_, ec);
    if (ec) {
        return Error{ErrorCode::IOError,
                     "Failed to remove: " + path_.string() + " (" + ec.message() + ")"};
    }
    return {};
}

} // namespace yams::search
