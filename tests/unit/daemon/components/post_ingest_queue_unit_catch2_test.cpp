// PostIngestQueue unit tests (Catch2)
// Tests LruCache template class and PostIngestQueue stage constants

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "src/daemon/components/post_ingest_nl_graph_builder.h"
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/metadata/path_utils.h>

using namespace yams::daemon;
using namespace std::chrono_literals;

// ============================================================================
// LruCache Basic Operations
// ============================================================================

TEST_CASE("LruCache put and get", "[daemon][post-ingest][cache][catch2]") {
    LruCache<std::string, int> cache(10, std::chrono::seconds(60));

    cache.put("key1", 42);
    auto result = cache.get("key1");

    REQUIRE(result.has_value());
    CHECK(result.value() == 42);
}

TEST_CASE("LruCache get returns nullopt for missing key", "[daemon][post-ingest][cache][catch2]") {
    LruCache<std::string, int> cache(10, std::chrono::seconds(60));

    auto result = cache.get("nonexistent");
    CHECK_FALSE(result.has_value());
}

TEST_CASE("LruCache evicts LRU on capacity", "[daemon][post-ingest][cache][catch2]") {
    LruCache<std::string, int> cache(3, std::chrono::seconds(60));

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);
    CHECK(cache.size() == 3);

    // Adding a 4th entry should evict the LRU ("a")
    cache.put("d", 4);
    CHECK(cache.size() == 3);

    // "a" should be evicted
    CHECK_FALSE(cache.get("a").has_value());
    // Others should still exist
    CHECK(cache.get("b").has_value());
    CHECK(cache.get("c").has_value());
    CHECK(cache.get("d").has_value());
}

TEST_CASE("LruCache TTL expiration", "[daemon][post-ingest][cache][catch2]") {
    // Use a very short TTL for testing
    LruCache<std::string, int> cache(10, std::chrono::seconds(1));

    cache.put("key", 100);
    auto result = cache.get("key");
    REQUIRE(result.has_value());
    CHECK(result.value() == 100);

    // Wait past TTL
    std::this_thread::sleep_for(1100ms);

    // Should be expired now
    auto expired = cache.get("key");
    CHECK_FALSE(expired.has_value());
}

TEST_CASE("LruCache clear empties cache", "[daemon][post-ingest][cache][catch2]") {
    LruCache<std::string, int> cache(10, std::chrono::seconds(60));

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);
    CHECK(cache.size() == 3);

    cache.clear();
    CHECK(cache.size() == 0);

    // All entries should be gone
    CHECK_FALSE(cache.get("a").has_value());
    CHECK_FALSE(cache.get("b").has_value());
    CHECK_FALSE(cache.get("c").has_value());
}

TEST_CASE("LruCache overwrite existing key", "[daemon][post-ingest][cache][catch2]") {
    LruCache<std::string, int> cache(10, std::chrono::seconds(60));

    cache.put("key", 1);
    CHECK(cache.get("key").value() == 1);

    cache.put("key", 2);
    CHECK(cache.get("key").value() == 2);

    // Size should not have increased
    CHECK(cache.size() == 1);
}

TEST_CASE("LruCache access refreshes position", "[daemon][post-ingest][cache][catch2]") {
    LruCache<std::string, int> cache(3, std::chrono::seconds(60));

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    // Access "a" to move it to front (most recently used)
    cache.get("a");

    // Add "d" — should evict "b" (now the LRU), not "a"
    cache.put("d", 4);

    CHECK(cache.get("a").has_value());       // refreshed, should survive
    CHECK_FALSE(cache.get("b").has_value()); // LRU, should be evicted
    CHECK(cache.get("c").has_value());
    CHECK(cache.get("d").has_value());
}

// ============================================================================
// PostIngestQueue Stage Constants
// ============================================================================

TEST_CASE("PostIngestQueue rejects and accounts KG admission after stop",
          "[daemon][post-ingest][lifecycle][catch2]") {
    PostIngestQueue queue{nullptr, nullptr, {}, nullptr, nullptr, nullptr, nullptr, 8};
    queue.stop();

    auto& bus = InternalEventBus::instance();
    const auto queuedBefore = bus.kgQueued();
    const auto droppedBefore = bus.kgDropped();
    InternalEventBus::KgJob job;
    job.hash = "after-stop";
    queue.testing_enqueueKgJob(std::move(job));

    CHECK(bus.kgQueued() == queuedBefore);
    CHECK(bus.kgDropped() == droppedBefore + 1);
    CHECK(queue.testing_pendingKgJobs() == 0);
}

TEST_CASE("PostIngestQueue stage constants", "[daemon][post-ingest][catch2]") {
    CHECK(PostIngestQueue::kStageCount == 5);
    CHECK(PostIngestQueue::kLimiterCount == 6);
}

TEST_CASE("PostIngestQueue stage names", "[daemon][post-ingest][catch2]") {
    CHECK(std::string(PostIngestQueue::kStageNames[0]) == "Extraction");
    CHECK(std::string(PostIngestQueue::kStageNames[1]) == "KnowledgeGraph");
    CHECK(std::string(PostIngestQueue::kStageNames[2]) == "Symbol");
    CHECK(std::string(PostIngestQueue::kStageNames[3]) == "Entity");
    CHECK(std::string(PostIngestQueue::kStageNames[4]) == "Title");
}

TEST_CASE("PostIngestQueue Stage enum values", "[daemon][post-ingest][catch2]") {
    CHECK(static_cast<uint8_t>(PostIngestQueue::Stage::Extraction) == 0);
    CHECK(static_cast<uint8_t>(PostIngestQueue::Stage::KnowledgeGraph) == 1);
    CHECK(static_cast<uint8_t>(PostIngestQueue::Stage::Symbol) == 2);
    CHECK(static_cast<uint8_t>(PostIngestQueue::Stage::Entity) == 3);
    CHECK(static_cast<uint8_t>(PostIngestQueue::Stage::Title) == 4);
}

TEST_CASE("SpscQueue failed rvalue push preserves the value for retry",
          "[daemon][post-ingest][queue][catch2]") {
    SpscQueue<std::string> queue(2);
    REQUIRE(queue.try_push(std::string{"occupied"}));

    std::string retry = "preserved";
    CHECK_FALSE(queue.try_push(std::move(retry)));
    CHECK(retry == "preserved");

    std::string popped;
    REQUIRE(queue.try_pop(popped));
    REQUIRE(queue.push_wait(std::move(retry), 1ms));
    REQUIRE(queue.try_pop(popped));
    CHECK(popped == "preserved");
}

// ============================================================================
// LruCache with integer keys
// ============================================================================

TEST_CASE("LruCache works with integer keys", "[daemon][post-ingest][cache][catch2]") {
    LruCache<int64_t, std::vector<std::string>> cache(5, std::chrono::seconds(60));

    std::vector<std::string> tags = {"tag1", "tag2", "tag3"};
    cache.put(42, tags);

    auto result = cache.get(42);
    REQUIRE(result.has_value());
    CHECK(result.value().size() == 3);
    CHECK(result.value()[0] == "tag1");
}

namespace {
yams::search::QueryConcept makeConcept(const std::string& text, const std::string& type,
                                       float confidence, const std::string& source) {
    const auto start = source.find(text);
    REQUIRE(start != std::string::npos);
    return {text, type, confidence, static_cast<std::uint32_t>(start),
            static_cast<std::uint32_t>(start + text.size())};
}

const yams::metadata::KGNode& findNode(const DeferredKGBatch& batch, const std::string& key) {
    const auto it = std::find_if(batch.nodes.begin(), batch.nodes.end(),
                                 [&](const auto& node) { return node.nodeKey == key; });
    REQUIRE(it != batch.nodes.end());
    return *it;
}

const DeferredEdge& findEdge(const DeferredKGBatch& batch, const std::string& source,
                             const std::string& target, const std::string& relation) {
    const auto it =
        std::find_if(batch.deferredEdges.begin(), batch.deferredEdges.end(), [&](const auto& edge) {
            return edge.srcNodeKey == source && edge.dstNodeKey == target &&
                   edge.relation == relation;
        });
    REQUIRE(it != batch.deferredEdges.end());
    return *it;
}

} // namespace

TEST_CASE("Post-ingest NL graph builder preserves graph schema and metric deltas",
          "[daemon][post-ingest][graph-builder][catch2]") {
    const std::string text =
        "TP53 response in lung cancer\nTP53 regulates apoptosis in epithelial cells. "
        "Aspirin treatment reduced lung cancer progression.";
    PostIngestNlGraphContext context{
        .hash = "abc123",
        .documentId = 42,
        .textSnippet = text,
        .fallbackTitle = "TP53 response in lung cancer",
        .filePath = "papers/study.txt",
        .language = std::string("e") + static_cast<char>(0xFF),
        .titleConfidence = 0.9f,
        .lastSeen = 123456,
    };
    std::vector<yams::search::QueryConcept> concepts{
        makeConcept("lung cancer", "disease", 0.82f, text),
        makeConcept("Aspirin", "drug", 0.88f, text),
        makeConcept("TP53", "gene", 0.95f, text),
    };

    auto built = buildPostIngestNlGraph(context, concepts);
    REQUIRE(built.batch);
    const auto& batch = *built.batch;
    const auto expectedPath =
        yams::metadata::computePathDerivedValues(context.filePath).normalizedPath;

    CHECK(batch.sourceFile == expectedPath);
    CHECK(batch.documentIdToDelete == 42);
    const auto& docNode = findNode(batch, "doc:abc123");
    CHECK(docNode.label == "papers/study.txt");
    const auto docProperties = nlohmann::json::parse(*docNode.properties);
    CHECK(docProperties["hash"] == "abc123");
    CHECK(docProperties["path"] == expectedPath);
    CHECK(docProperties["language"] == "e?");
    findNode(batch, "path:file:" + expectedPath);
    findNode(batch, "segment:title:abc123");
    findNode(batch, "segment:summary:abc123");
    findNode(batch, "segment:body:abc123:1");
    const auto& containsTitle =
        findEdge(batch, "doc:abc123", "segment:title:abc123", "contains_segment");
    CHECK(containsTitle.weight == 0.9f);
    const auto containsProperties = nlohmann::json::parse(*containsTitle.properties);
    CHECK(containsProperties["source"] == "gliner");
    CHECK(containsProperties["region"] == "title");
    CHECK(containsProperties["confidence"] == 0.9f);

    const auto& gene = findNode(batch, "nl_entity:gene:tp53");
    const auto geneProps = nlohmann::json::parse(*gene.properties);
    CHECK(geneProps["entity_text"] == "TP53");
    CHECK(geneProps["entity_type"] == "gene");
    CHECK(geneProps["last_seen"] == 123456);

    CHECK(std::any_of(batch.aliases.begin(), batch.aliases.end(), [](const auto& alias) {
        return alias.alias == "tp53" && alias.source == "gliner.surface|nl_entity:gene:tp53" &&
               alias.confidence == 0.95f;
    }));
    CHECK(std::any_of(
        batch.deferredDocEntities.begin(), batch.deferredDocEntities.end(), [](const auto& entity) {
            return entity.documentId == 42 && entity.entityText == "TP53" &&
                   entity.nodeKey == "nl_entity:gene:tp53" && entity.extractor == "gliner_title_nl";
        }));
    CHECK(std::any_of(batch.deferredEdges.begin(), batch.deferredEdges.end(), [](const auto& edge) {
        return edge.srcNodeKey == "nl_entity:gene:tp53" && edge.dstNodeKey == "doc:abc123" &&
               edge.relation == "title_mentions" && edge.weight == 1.0f;
    }));
    CHECK(std::any_of(batch.deferredEdges.begin(), batch.deferredEdges.end(), [](const auto& edge) {
        return edge.srcNodeKey == "nl_entity:drug:aspirin" &&
               edge.relation == "mentioned_in_segment" &&
               nlohmann::json::parse(*edge.properties)["region"] == "body_claim";
    }));
    const auto& primary = findEdge(batch, "nl_entity:gene:tp53", "doc:abc123", "primary_topic_of");
    CHECK(primary.weight == 1.0f);
    const auto primaryProperties = nlohmann::json::parse(*primary.properties);
    CHECK(primaryProperties["title_overlap"] == true);
    CHECK(primaryProperties["entity_type"] == "gene");
    const auto& coMention = findEdge(batch, "nl_entity:gene:tp53", "nl_entity:disease:lung cancer",
                                     "co_occurs_biomedical");
    CHECK(coMention.weight > 0.69f);
    CHECK(coMention.weight < 0.70f);
    const auto coMentionProperties = nlohmann::json::parse(*coMention.properties);
    CHECK(coMentionProperties["lhs_type"] == "gene");
    CHECK(coMentionProperties["rhs_type"] == "disease");
    CHECK(coMentionProperties["scope"] == "title");
    CHECK(coMentionProperties["provenance"]["source"] == "gliner");

    CHECK(built.metrics.segmentNodesCreated == 3);
    CHECK(built.metrics.bodySegmentNodesCreated == 1);
    CHECK(built.metrics.segmentEdgesCreated == 6);
    CHECK(built.metrics.entitySegmentEdgesCreated == 3);
    CHECK(built.metrics.bodyEntitySegmentEdgesCreated == 1);
    CHECK(built.metrics.deferredDocEntitiesQueued == 3);
    CHECK(built.metrics.entities == 3);
    CHECK(built.metrics.highValueEntities == 3);
    CHECK(built.metrics.primaryTopicEdges == 3);
    CHECK(built.metrics.coMentionEdges == 3);
}

TEST_CASE("Post-ingest NL graph builder bounds edges without prescribing equal-confidence order",
          "[daemon][post-ingest][graph-builder][catch2]") {
    const std::string text =
        "Topic overview\nGene0 Gene1 Gene2 Gene3 Gene4 Gene5 Gene6 Gene7 Gene8 Gene9 are linked "
        "in this sufficiently long body claim.";
    PostIngestNlGraphContext context{
        .hash = "stable",
        .documentId = 7,
        .textSnippet = text,
        .fallbackTitle = "Topic overview",
        .filePath = "/tmp/stable.txt",
        .language = "en",
        .titleConfidence = std::nullopt,
        .lastSeen = 99,
    };
    std::vector<yams::search::QueryConcept> concepts;
    for (int i = 9; i >= 0; --i) {
        concepts.push_back(makeConcept("Gene" + std::to_string(i), "gene", 0.9f, text));
    }
    auto reversed = concepts;
    std::reverse(reversed.begin(), reversed.end());

    auto first = buildPostIngestNlGraph(context, concepts);
    auto second = buildPostIngestNlGraph(context, reversed);

    for (const auto* built : {&first, &second}) {
        CHECK(built->metrics.primaryTopicEdges == 3);
        CHECK(built->metrics.coMentionEdges == 16);
        REQUIRE(built->batch);
        CHECK(std::count_if(
                  built->batch->nodes.begin(), built->batch->nodes.end(),
                  [](const auto& node) { return node.nodeKey.starts_with("nl_entity:"); }) == 10);
    }
}
