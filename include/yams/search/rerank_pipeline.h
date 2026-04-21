#pragma once

#include <yams/metadata/document_metadata.h>

#include <memory>
#include <string>
#include <vector>

namespace yams::metadata {
class MetadataRepository;
}

namespace yams::search {

struct SearchEngineConfig;

struct RerankInputs {
    std::vector<std::string> snippets;
    std::vector<size_t> passageDocIndices;
};

class RerankInputCollector {
public:
    RerankInputCollector(std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                         size_t snippetMaxChars);

    RerankInputs collect(const std::string& query,
                         const std::vector<metadata::SearchResult>& results,
                         size_t rerankWindow) const;

private:
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    size_t snippetMaxChars_;
};

class RerankScoreApplier {
public:
    explicit RerankScoreApplier(const SearchEngineConfig& config);

    void apply(std::vector<metadata::SearchResult>& results, size_t rerankWindow,
               const std::vector<float>& scores,
               const std::vector<size_t>& rerankPassageDocIndices) const;

private:
    double resolveEffectiveWeight(const std::vector<float>& scores) const;

    const SearchEngineConfig& config_;
};

} // namespace yams::search
