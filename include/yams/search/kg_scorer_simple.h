#pragma once

#include <memory>

namespace yams::app::services {
class IGraphQueryService;
}

namespace yams::metadata {
class KnowledgeGraphStore;
}

namespace yams::search {
class KGScorer;

std::shared_ptr<KGScorer>
makeSimpleKGScorer(std::shared_ptr<yams::metadata::KnowledgeGraphStore> store,
                   std::shared_ptr<yams::app::services::IGraphQueryService> graphService = nullptr);

} // namespace yams::search
