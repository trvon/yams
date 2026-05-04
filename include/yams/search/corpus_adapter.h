#pragma once

#include <yams/core/types.h>
#include <yams/search/search_models.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace yams::metadata {
class MetadataRepository;
}

namespace yams::search {

// Corpus adapters are lightweight, corpus-aware retrieval components that emit
// anchoring evidence into the normal YAMS fusion pipeline.  They are intended
// for domain structure that generic BM25/vector search cannot infer reliably:
// agent-memory metadata (pbi/task/phase/source), repository paths, issue IDs,
// benchmark IDs, language-specific fields, or externally curated qrel-backed
// corpora.  Adapters must be deterministic and side-effect free; expensive
// indexes should be built outside execute().
struct CorpusAdapterContext {
    std::string query;
    SearchParams params;
    const SearchEngineConfig* config{nullptr};
    std::shared_ptr<metadata::MetadataRepository> metadataRepo;
    std::size_t limit{100};
};

class CorpusAdapter {
public:
    virtual ~CorpusAdapter() = default;

    [[nodiscard]] virtual std::string name() const = 0;

    [[nodiscard]] virtual bool supports(const CorpusAdapterContext& context) const {
        return context.metadataRepo != nullptr && !context.query.empty();
    }

    [[nodiscard]] virtual Result<std::vector<ComponentResult>>
    execute(const CorpusAdapterContext& context) const = 0;
};

// Built-in adapter for YAMS itself: repository/path navigation plus structured
// agent-memory metadata.  This gives YAMS a first-class corpus hook immediately
// while keeping third-party/domain adapters pluggable.
class YamsNativeCorpusAdapter final : public CorpusAdapter {
public:
    [[nodiscard]] std::string name() const override { return "yams_native"; }
    [[nodiscard]] Result<std::vector<ComponentResult>>
    execute(const CorpusAdapterContext& context) const override;
};

} // namespace yams::search
