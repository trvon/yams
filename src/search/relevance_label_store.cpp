#include <yams/search/relevance_label_store.h>

#include <spdlog/spdlog.h>

#include <fstream>
#include <system_error>

namespace yams::search {

double RelevanceSession::meanReward() const noexcept {
    if (queries.empty()) {
        return 0.0;
    }
    double sum = 0.0;
    for (const auto& q : queries) {
        sum += q.reward;
    }
    return sum / static_cast<double>(queries.size());
}

nlohmann::json RelevanceSession::toJson() const {
    nlohmann::json queriesArr = nlohmann::json::array();
    for (const auto& q : queries) {
        nlohmann::json labels = nlohmann::json::array();
        for (auto l : q.labels) {
            labels.push_back(relevanceLabelToString(l));
        }
        nlohmann::json hashes = nlohmann::json::array();
        for (const auto& h : q.rankedDocHashes) {
            hashes.push_back(h);
        }
        queriesArr.push_back(nlohmann::json{{"query_text", q.queryText},
                                            {"ranked_doc_hashes", std::move(hashes)},
                                            {"labels", std::move(labels)},
                                            {"reward", q.reward}});
    }
    nlohmann::json j{{"timestamp", timestamp},
                     {"config_hash", configHash},
                     {"source", source},
                     {"k", k},
                     {"mean_reward", meanReward()},
                     {"queries", std::move(queriesArr)}};
    if (corpusEpoch) {
        j["corpus_epoch"] = *corpusEpoch;
    }
    if (topologyEpoch) {
        j["topology_epoch"] = *topologyEpoch;
    }
    return j;
}

RelevanceSession RelevanceSession::fromJson(const nlohmann::json& j) {
    RelevanceSession s;
    s.timestamp = j.value("timestamp", std::string{});
    s.configHash = j.value("config_hash", std::string{});
    s.source = j.value("source", std::string{"interactive"});
    s.k = j.value("k", static_cast<std::size_t>(10));
    if (j.contains("corpus_epoch") && j["corpus_epoch"].is_number_unsigned()) {
        s.corpusEpoch = j["corpus_epoch"].get<std::uint64_t>();
    }
    if (j.contains("topology_epoch") && j["topology_epoch"].is_number_unsigned()) {
        s.topologyEpoch = j["topology_epoch"].get<std::uint64_t>();
    }
    if (j.contains("queries") && j["queries"].is_array()) {
        for (const auto& qj : j["queries"]) {
            LabeledQuery q;
            q.queryText = qj.value("query_text", std::string{});
            q.reward = qj.value("reward", 0.0);
            if (qj.contains("ranked_doc_hashes") && qj["ranked_doc_hashes"].is_array()) {
                for (const auto& h : qj["ranked_doc_hashes"]) {
                    if (h.is_string()) {
                        q.rankedDocHashes.push_back(h.get<std::string>());
                    }
                }
            }
            if (qj.contains("labels") && qj["labels"].is_array()) {
                for (const auto& lv : qj["labels"]) {
                    if (lv.is_string()) {
                        q.labels.push_back(relevanceLabelFromString(lv.get<std::string>()));
                    }
                }
            }
            s.queries.push_back(std::move(q));
        }
    }
    return s;
}

RelevanceLabelStore::RelevanceLabelStore(std::filesystem::path path) : path_(std::move(path)) {}

Result<void> RelevanceLabelStore::append(const RelevanceSession& session) {
    std::lock_guard<std::mutex> lock(mtx_);
    std::error_code ec;
    if (path_.has_parent_path()) {
        std::filesystem::create_directories(path_.parent_path(), ec);
        if (ec) {
            return Error{ErrorCode::IOError,
                         "Cannot create parent directory: " + path_.parent_path().string() + " (" +
                             ec.message() + ")"};
        }
    }
    std::ofstream out(path_, std::ios::app | std::ios::binary);
    if (!out) {
        return Error{ErrorCode::IOError, "Cannot open relevance label store: " + path_.string()};
    }
    const auto line = session.toJson().dump();
    out.write(line.data(), static_cast<std::streamsize>(line.size()));
    out.put('\n');
    if (!out) {
        return Error{ErrorCode::IOError,
                     "Write failed for relevance label store: " + path_.string()};
    }
    return {};
}

namespace {

Result<std::vector<nlohmann::json>> loadLines(const std::filesystem::path& p) {
    std::vector<nlohmann::json> out;
    std::error_code ec;
    if (!std::filesystem::exists(p, ec)) {
        return out;
    }
    std::ifstream in(p);
    if (!in) {
        return Error{ErrorCode::IOError, "Cannot open relevance label store: " + p.string()};
    }
    std::string line;
    std::size_t idx = 0;
    while (std::getline(in, line)) {
        ++idx;
        if (line.empty()) {
            continue;
        }
        try {
            out.push_back(nlohmann::json::parse(line, nullptr, /*allow_exceptions=*/true,
                                                /*ignore_comments=*/true));
        } catch (const std::exception& e) {
            spdlog::warn("Skipping malformed relevance session line {} in {}: {}", idx, p.string(),
                         e.what());
        }
    }
    return out;
}

} // namespace

Result<std::vector<RelevanceSession>> RelevanceLabelStore::readRecent(std::size_t limit) const {
    std::lock_guard<std::mutex> lock(mtx_);
    auto lines = loadLines(path_);
    if (!lines) {
        return lines.error();
    }
    const auto& arr = lines.value();
    const std::size_t start = arr.size() > limit ? arr.size() - limit : 0;
    std::vector<RelevanceSession> sessions;
    sessions.reserve(arr.size() - start);
    for (std::size_t i = start; i < arr.size(); ++i) {
        try {
            sessions.push_back(RelevanceSession::fromJson(arr[i]));
        } catch (const std::exception& e) {
            spdlog::warn("Skipping malformed relevance session at index {}: {}", i, e.what());
        }
    }
    return sessions;
}

Result<std::vector<RelevanceSession>>
RelevanceLabelStore::readByConfigHash(std::string_view configHash, std::size_t limit) const {
    std::lock_guard<std::mutex> lock(mtx_);
    auto lines = loadLines(path_);
    if (!lines) {
        return lines.error();
    }
    std::vector<RelevanceSession> sessions;
    for (const auto& j : lines.value()) {
        try {
            auto s = RelevanceSession::fromJson(j);
            if (s.configHash == configHash) {
                sessions.push_back(std::move(s));
            }
        } catch (const std::exception& e) {
            spdlog::debug("Skipping malformed session during readByConfigHash: {}", e.what());
        }
    }
    if (sessions.size() > limit) {
        sessions.erase(sessions.begin(),
                       sessions.begin() + static_cast<std::ptrdiff_t>(sessions.size() - limit));
    }
    return sessions;
}

Result<void> RelevanceLabelStore::clear() {
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
