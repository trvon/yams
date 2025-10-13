#include <yams/search/chunk_coverage.h>

#include <algorithm>
#include <unordered_set>

namespace yams::search {

std::string baseIdFromChunkId(std::string_view chunk_id) {
    if (chunk_id.empty())
        return std::string{};
    const size_t pos = chunk_id.find('#');
    if (pos == std::string_view::npos) {
        return std::string(chunk_id);
    }
    return std::string(chunk_id.substr(0, pos));
}

std::vector<DocGroupStats> groupAndAggregate(
    const std::vector<yams::vector::SearchResult>& results, Pooling pooling,
    const std::function<std::optional<size_t>(std::string_view)>& total_chunk_resolver) {
    struct Accum {
        float sum = 0.0f;
        float maxv = 0.0f;
        size_t count = 0;
        std::unordered_set<std::string> uniq_chunks; // guard against duplicate ids
    };

    std::unordered_map<std::string, Accum> acc;
    acc.reserve(results.size());

    for (const auto& r : results) {
        const std::string base = baseIdFromChunkId(r.id);
        auto& a = acc[base];
        // de-duplicate by full chunk id
        if (a.uniq_chunks.insert(r.id).second) {
            a.sum += r.similarity;
            a.maxv = (a.count == 0) ? r.similarity : std::max(a.maxv, r.similarity);
            a.count += 1;
        }
    }

    std::vector<DocGroupStats> out;
    out.reserve(acc.size());
    for (auto& kv : acc) {
        DocGroupStats s;
        s.base_id = kv.first;
        s.contributing_chunks = kv.second.count;
        if (total_chunk_resolver) {
            s.total_chunks = total_chunk_resolver(s.base_id);
        }
        if (pooling == Pooling::MAX) {
            s.pooled_score = kv.second.maxv;
        } else {
            s.pooled_score = (kv.second.count > 0)
                                 ? (kv.second.sum / static_cast<float>(kv.second.count))
                                 : 0.0f;
        }
        out.push_back(std::move(s));
    }

    // Sort descending by pooled_score, stable for deterministic order
    std::ranges::stable_sort(out, [](const DocGroupStats& a, const DocGroupStats& b) {
        if (a.pooled_score != b.pooled_score)
            return a.pooled_score > b.pooled_score;
        return a.base_id < b.base_id;
    });

    return out;
}

} // namespace yams::search
