// Legacy vectors whose document lost its text: purged once, without touching replicated vectors.

#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"

#include <yams/daemon/components/LegacyVectorCleanup.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>

#include <memory>
#include <string>
#include <vector>

namespace {

using namespace yams;

struct CleanupHarness {
    yams::test::TempDirGuard tmp{"yams_legacy_vectors_"};
    yams::test::ScopedEnvVar vectorsOn{"YAMS_DISABLE_VECTORS", std::nullopt};
    yams::test::ScopedEnvVar vectorsOnAlias{"YAMS_DISABLE_VECTOR_DB", std::nullopt};
    std::unique_ptr<metadata::ConnectionPool> pool;
    std::shared_ptr<metadata::MetadataRepository> repo;
    std::unique_ptr<vector::VectorDatabase> vectors;

    CleanupHarness() {
        metadata::ConnectionPoolConfig pc;
        pc.minConnections = 1;
        pc.maxConnections = 2;
        pool = std::make_unique<metadata::ConnectionPool>((tmp.path() / "yams.db").string(), pc);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_shared<metadata::MetadataRepository>(*pool);
        vector::VectorDatabaseConfig vc;
        vc.database_path = ":memory:";
        vc.embedding_dim = 4;
        vc.create_if_missing = true;
        vc.use_in_memory = true;
        vectors = std::make_unique<vector::VectorDatabase>(vc);
        REQUIRE(vectors->initialize());
    }
    ~CleanupHarness() {
        vectors.reset();
        repo.reset();
        pool->shutdown();
    }

    void addDocument(const std::string& hash, bool withText, const std::string& modelId,
                     int chunks = 2) {
        metadata::DocumentInfo doc;
        doc.fileName = hash + ".txt";
        doc.filePath = (tmp.path() / doc.fileName).string();
        doc.fileExtension = ".txt";
        doc.fileSize = 4;
        doc.sha256Hash = hash;
        doc.mimeType = "text/plain";
        auto id = repo->insertDocument(doc);
        REQUIRE(id.has_value());
        if (withText) {
            metadata::DocumentContent text;
            text.documentId = id.value();
            text.contentText = "text of " + hash;
            text.contentLength = static_cast<int64_t>(text.contentText.size());
            REQUIRE(repo->insertContent(text).has_value());
        }
        for (int i = 0; i < chunks; ++i) {
            vector::VectorRecord rec;
            rec.document_hash = hash;
            rec.chunk_id = hash + "_chunk_" + std::to_string(i);
            rec.embedding = {0.5F, 0.5F, 0.5F, 0.5F};
            rec.content = "chunk";
            rec.model_id = modelId;
            REQUIRE(vectors->insertVector(rec));
        }
    }
};

} // namespace

TEST_CASE("Legacy vectors are purged only for documents without text",
          "[daemon][embeddings][legacy-vectors][catch2]") {
    CleanupHarness h;
    h.addDocument("legacy-orphan", /*withText=*/false, /*modelId=*/"");
    h.addDocument("replicated-early", /*withText=*/false, /*modelId=*/"simeon-v1");
    h.addDocument("legacy-with-text", /*withText=*/true, /*modelId=*/"");

    const auto embedded = h.vectors->getEmbeddedDocumentHashes();
    auto result = daemon::purgeLegacyVectorsWithoutText(*h.repo, *h.vectors, embedded);
    REQUIRE(result.has_value());
    CHECK(result.value().documentsPurged == 1);
    CHECK(result.value().documentsFailed == 0);

    CHECK(h.vectors->getVectorsByDocument("legacy-orphan").empty());
    // A peer's embedding can arrive before local extraction runs: keep it.
    CHECK(h.vectors->getVectorsByDocument("replicated-early").size() == 2);
    // The text still exists, so the vectors still describe it.
    CHECK(h.vectors->getVectorsByDocument("legacy-with-text").size() == 2);
}
