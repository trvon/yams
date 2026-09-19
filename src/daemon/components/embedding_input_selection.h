#pragma once

#include <string>
#include <unordered_set>
#include <vector>
#include <yams/daemon/components/InternalEventBus.h>

namespace yams::daemon::embed {

struct EmbeddingInputSelection {
    std::vector<std::size_t> preparedIndices;
    std::vector<std::string> gatherHashes;
};

// An unusable prepared payload must not suppress database gathering. Keep the
// first usable payload per revision and gather each remaining revision once.
inline EmbeddingInputSelection selectEmbeddingInputs(const InternalEventBus::EmbedJob& job) {
    EmbeddingInputSelection selected;
    std::unordered_set<std::string> handled;
    handled.reserve(job.hashes.size() + job.preparedDocs.size());
    for (std::size_t i = 0; i < job.preparedDocs.size(); ++i) {
        const auto& prepared = job.preparedDocs[i];
        if (!prepared.chunks.empty() && handled.insert(prepared.hash).second)
            selected.preparedIndices.push_back(i);
    }
    for (const auto& hash : job.hashes) {
        if (handled.insert(hash).second)
            selected.gatherHashes.push_back(hash);
    }
    return selected;
}

} // namespace yams::daemon::embed
