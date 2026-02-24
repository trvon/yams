#include <yams/daemon/components/repair/repair_health_probe.h>

#include <yams/daemon/components/GraphComponent.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>

#include <cstdlib>
#include <unordered_set>

namespace yams::daemon::repair {

RepairHealthProbe::RepairHealthProbe(std::shared_ptr<metadata::IMetadataRepository> meta,
                                     std::shared_ptr<vector::VectorDatabase> vectorDb,
                                     std::shared_ptr<GraphComponent> graphComponent,
                                     std::shared_ptr<metadata::KnowledgeGraphStore> kgStore)
    : meta_(std::move(meta)), vectorDb_(std::move(vectorDb)),
      graphComponent_(std::move(graphComponent)), kgStore_(std::move(kgStore)) {}

bool RepairHealthProbe::vectorsDisabledByEnv() {
    if (const char* env = std::getenv("YAMS_DISABLE_VECTORS"); env && *env)
        return true;
    if (const char* env = std::getenv("YAMS_DISABLE_VECTOR_DB"); env && *env)
        return true;
    return false;
}

RepairHealthSnapshot RepairHealthProbe::probe(const RepairHealthOptions& options) const {
    RepairHealthSnapshot snapshot;
    if (!meta_) {
        snapshot.issues.push_back("Metadata repository not available");
        return snapshot;
    }

    bool checkFts5 = options.checkFts5;
    bool checkEmbeddings = options.checkEmbeddings;
    bool checkGraph = options.checkGraph;

    std::unordered_set<int64_t> ftsRowIds;
    if (checkFts5) {
        auto ftsRes = meta_->getFts5IndexedRowIdSet();
        if (!ftsRes) {
            snapshot.issues.push_back("FTS5 index probe failed: " + ftsRes.error().message);
            checkFts5 = false;
        } else {
            ftsRowIds = std::move(ftsRes.value());
        }
    }

    std::unordered_set<std::string> embeddedHashes;
    if (checkEmbeddings) {
        if (vectorsDisabledByEnv()) {
            snapshot.issues.push_back("Embeddings disabled by environment");
            checkEmbeddings = false;
        } else if (!vectorDb_) {
            snapshot.issues.push_back("Vector database not available");
            checkEmbeddings = false;
        } else {
            embeddedHashes = vectorDb_->getEmbeddedDocumentHashes();
        }
    }

    if (checkGraph) {
        if (!graphComponent_) {
            snapshot.graphIntegrityOk = false;
            snapshot.issues.push_back("GraphComponent not available");
        } else {
            auto healthRes = graphComponent_->validateGraph();
            if (!healthRes) {
                snapshot.graphIntegrityOk = false;
                snapshot.issues.push_back("Graph health check failed: " +
                                          healthRes.error().message);
            } else if (!healthRes.value().issues.empty()) {
                snapshot.issues.push_back("Graph health issues: " +
                                          healthRes.value().issues.front());
            }
        }

        if (!kgStore_) {
            snapshot.issues.push_back("KnowledgeGraphStore not available");
        } else {
            auto countRes = kgStore_->countNodesByType("document");
            if (!countRes) {
                snapshot.issues.push_back("KG node count failed: " + countRes.error().message);
            } else {
                snapshot.graphDocNodes = static_cast<uint64_t>(countRes.value());
            }
        }
    }

    const bool needDocScan = checkFts5 || checkEmbeddings || (checkGraph && options.scanDocuments);
    if (!needDocScan) {
        return snapshot;
    }

    auto isEmbeddable = [](const std::string& m) -> bool {
        if (m.rfind("text/", 0) == 0)
            return true;
        if (m == "application/json" || m == "application/xml" || m == "application/x-yaml" ||
            m == "application/yaml")
            return true;
        return false;
    };

    constexpr std::size_t kBatchSize = 500;
    std::size_t offset = 0;
    while (true) {
        metadata::DocumentQueryOptions opts;
        opts.limit = static_cast<int>(kBatchSize);
        opts.offset = static_cast<int>(offset);

        auto docsRes = meta_->queryDocuments(opts);
        if (!docsRes) {
            snapshot.issues.push_back("Document scan failed: " + docsRes.error().message);
            break;
        }

        const auto& docs = docsRes.value();
        if (docs.empty()) {
            break;
        }

        for (const auto& d : docs) {
            ++snapshot.documentsScanned;

            if (checkFts5) {
                if (d.contentExtracted &&
                    d.extractionStatus == metadata::ExtractionStatus::Success) {
                    ++snapshot.fts5EligibleDocs;
                    if (ftsRowIds.count(d.id) == 0) {
                        ++snapshot.missingFts5;
                    }
                }
            }

            if (checkEmbeddings) {
                if (isEmbeddable(d.mimeType)) {
                    ++snapshot.embeddableDocs;
                    if (!d.sha256Hash.empty() && embeddedHashes.count(d.sha256Hash) == 0) {
                        ++snapshot.missingEmbeddings;
                    }
                }
            }
        }

        offset += docs.size();
        if (docs.size() < kBatchSize) {
            break;
        }
    }

    if (checkGraph && snapshot.graphDocNodes > 0 &&
        snapshot.documentsScanned > snapshot.graphDocNodes) {
        snapshot.graphDocNodeGap = snapshot.documentsScanned - snapshot.graphDocNodes;
    }

    return snapshot;
}

} // namespace yams::daemon::repair
