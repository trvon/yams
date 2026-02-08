// PostIngestQueue unit tests (Catch2)
// Tests LruCache template class and PostIngestQueue stage constants

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <thread>

#include <yams/daemon/components/PostIngestQueue.h>

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
