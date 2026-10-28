// Simeon PQ in-memory index maintenance under a mixed insert/search load.
//
// Every vector mutation bumps vector_index_generation, which marks the per-dim
// PQ index stale. Before this suite, the next search then discarded the index
// and retrained the codebook from scratch (k-means over up to train_limit rows,
// then a full re-encode) under the backend write lock. A steady add+search
// workload paid one full retrain per search. These cases pin down the cheaper
// contract: inserts append to the trained index, updates and deletes still
// retrain, and results stay correct either way.

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <sqlite3.h>
#include <yams/vector/sqlite_vec_backend.h>

using namespace yams::vector;

namespace {

struct SpqIncrementalFixture {
    static constexpr size_t kDim = 32;

    SpqIncrementalFixture() {
        for (const char* env : {"YAMS_SQLITE_VEC_SKIP_INIT", "YAMS_DISABLE_VECTORS"}) {
            if (const char* value = std::getenv(env)) {
                std::string v(value);
                if (v == "1" || v == "true") {
                    skipReason = std::string("Skipping (") + env + "=1)";
                }
            }
        }
    }

    ~SpqIncrementalFixture() {
        for (const auto& path : tempFiles) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
            std::filesystem::remove(path + "-wal", ec);
            std::filesystem::remove(path + "-shm", ec);
        }
    }

    void skipIfNeeded() const {
        if (!skipReason.empty()) {
            SKIP(skipReason);
        }
    }

    std::string createTempDbPath() {
        auto path = std::filesystem::temp_directory_path() /
                    ("sqlite_vec_spq_incr_" + std::to_string(++tempCounter) + "_" +
                     std::to_string(std::random_device{}()) + ".db");
        tempFiles.push_back(path.string());
        return path.string();
    }

    static SqliteVecBackend::Config smallPqConfig() {
        SqliteVecBackend::Config config;
        config.search_engine = VectorSearchEngine::SimeonPqAdc;
        config.embedding_dim = kDim;
        config.simeon_pq_subquantizers = 8;
        config.simeon_pq_centroids = 16;
        config.simeon_pq_train_limit = 512;
        config.simeon_pq_rerank_factor = 2;
        return config;
    }

    static std::vector<float> embedding(float seed) {
        std::vector<float> emb(kDim);
        std::mt19937 rng(static_cast<uint32_t>(seed * 1000));
        std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
        for (auto& v : emb) {
            v = dist(rng);
        }
        float norm = 0.0f;
        for (float v : emb) {
            norm += v * v;
        }
        norm = std::sqrt(norm);
        for (auto& v : emb) {
            v /= norm;
        }
        return emb;
    }

    static VectorRecord record(const std::string& id, const std::vector<float>& emb) {
        VectorRecord rec;
        rec.chunk_id = "chunk_" + id;
        rec.document_hash = "doc_" + id;
        rec.embedding = emb;
        rec.content = "content " + id;
        rec.start_offset = 0;
        rec.end_offset = rec.content.size();
        return rec;
    }

    static std::vector<VectorRecord> corpus(size_t count, size_t firstSeed = 1) {
        std::vector<VectorRecord> records;
        records.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            const auto seed = static_cast<float>(firstSeed + i);
            records.push_back(record(std::to_string(firstSeed + i), embedding(seed)));
        }
        return records;
    }

    static std::vector<std::string> topIds(SqliteVecBackend& backend, const std::vector<float>& q,
                                           size_t k) {
        auto result = backend.searchSimilar(q, k, -1.0f, std::nullopt, {});
        REQUIRE((result.has_value()));
        std::vector<std::string> ids;
        for (const auto& rec : result.value()) {
            ids.push_back(rec.chunk_id);
        }
        return ids;
    }

    std::string skipReason;
    std::vector<std::string> tempFiles;
    static inline int tempCounter = 0;
};

} // namespace

TEST_CASE_METHOD(SpqIncrementalFixture,
                 "SPQ: searches without vector changes never rebuild the index",
                 "[unit][vector][spq][incremental][catch2]") {
    skipIfNeeded();

    SqliteVecBackend backend(smallPqConfig());
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));
    REQUIRE((backend.insertVectorsBatch(corpus(300)).has_value()));

    for (size_t i = 0; i < 8; ++i) {
        auto ids = topIds(backend, embedding(static_cast<float>(1 + i)), 5);
        REQUIRE((!ids.empty()));
        CHECK((ids.front() == "chunk_" + std::to_string(1 + i)));
    }

    const auto stats = backend.simeonPqIndexStats();
    CHECK((stats.full_builds == 1));
    CHECK((stats.incremental_appends == 0));
}

TEST_CASE_METHOD(SpqIncrementalFixture, "SPQ: inserts between searches append to the trained index",
                 "[unit][vector][spq][incremental][catch2]") {
    skipIfNeeded();

    SqliteVecBackend backend(smallPqConfig());
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));
    REQUIRE((backend.insertVectorsBatch(corpus(300)).has_value()));

    // First search trains the codebook once.
    REQUIRE((topIds(backend, embedding(1.0f), 3).front() == "chunk_1"));
    REQUIRE((backend.simeonPqIndexStats().full_builds == 1));

    // Steady add+search: each new vector is searchable right away, no retrain.
    constexpr size_t kRounds = 12;
    for (size_t round = 0; round < kRounds; ++round) {
        const size_t id = 1000 + round;
        const auto emb = embedding(static_cast<float>(id));
        REQUIRE((backend.insertVector(record(std::to_string(id), emb)).has_value()));
        auto ids = topIds(backend, emb, 3);
        REQUIRE((!ids.empty()));
        CHECK((ids.front() == "chunk_" + std::to_string(id)));
    }

    const auto stats = backend.simeonPqIndexStats();
    INFO("full_builds=" << stats.full_builds
                        << " incremental_appends=" << stats.incremental_appends);
    CHECK((stats.full_builds == 1));
    CHECK((stats.incremental_appends == kRounds));
    CHECK((stats.appended_vectors == kRounds));

    // Older rows are still ranked by the shared codebook.
    CHECK((topIds(backend, embedding(7.0f), 3).front() == "chunk_7"));
}

TEST_CASE_METHOD(SpqIncrementalFixture,
                 "SPQ: batch inserts append and the appended rows are searchable",
                 "[unit][vector][spq][incremental][catch2]") {
    skipIfNeeded();

    SqliteVecBackend backend(smallPqConfig());
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));
    REQUIRE((backend.insertVectorsBatch(corpus(400)).has_value()));
    REQUIRE((topIds(backend, embedding(1.0f), 3).front() == "chunk_1"));

    REQUIRE((backend.insertVectorsBatch(corpus(40, 2000)).has_value()));
    for (size_t id = 2000; id < 2040; id += 7) {
        CHECK((topIds(backend, embedding(static_cast<float>(id)), 3).front() ==
               "chunk_" + std::to_string(id)));
    }

    const auto stats = backend.simeonPqIndexStats();
    CHECK((stats.full_builds == 1));
    CHECK((stats.incremental_appends == 1));
    CHECK((stats.appended_vectors == 40));
}

TEST_CASE_METHOD(SpqIncrementalFixture, "SPQ: updates and deletes retrain and results stay correct",
                 "[unit][vector][spq][incremental][catch2]") {
    skipIfNeeded();

    SqliteVecBackend backend(smallPqConfig());
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));
    REQUIRE((backend.insertVectorsBatch(corpus(300)).has_value()));
    REQUIRE((topIds(backend, embedding(1.0f), 3).front() == "chunk_1"));

    // Appended row, then moved in place: the stale code must not survive.
    const auto fresh = embedding(5000.0f);
    REQUIRE((backend.insertVector(record("5000", fresh)).has_value()));
    REQUIRE((topIds(backend, fresh, 3).front() == "chunk_5000"));
    REQUIRE((backend.simeonPqIndexStats().full_builds == 1));

    const auto moved = embedding(6000.0f);
    auto updated = record("5000", moved);
    REQUIRE((backend.updateVector("chunk_5000", updated).has_value()));
    auto afterUpdate = topIds(backend, moved, 3);
    REQUIRE((!afterUpdate.empty()));
    CHECK((afterUpdate.front() == "chunk_5000"));
    CHECK((backend.simeonPqIndexStats().full_builds == 2));

    REQUIRE((backend.deleteVector("chunk_5000").has_value()));
    auto afterDelete = topIds(backend, moved, 5);
    CHECK((std::find(afterDelete.begin(), afterDelete.end(), "chunk_5000") == afterDelete.end()));
    CHECK((backend.simeonPqIndexStats().full_builds == 3));

    REQUIRE((backend.deleteVectorsByDocument("doc_7").has_value()));
    auto afterDocDelete = topIds(backend, embedding(7.0f), 5);
    CHECK((std::find(afterDocDelete.begin(), afterDocDelete.end(), "chunk_7") ==
           afterDocDelete.end()));
    CHECK((backend.simeonPqIndexStats().full_builds == 4));
}

TEST_CASE_METHOD(SpqIncrementalFixture,
                 "SPQ: growth past the configured ratio retrains the codebook",
                 "[unit][vector][spq][incremental][catch2]") {
    skipIfNeeded();

    auto config = smallPqConfig();
    config.simeon_pq_incremental_growth_ratio = 0.10;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));
    REQUIRE((backend.insertVectorsBatch(corpus(200)).has_value()));
    REQUIRE((topIds(backend, embedding(1.0f), 3).front() == "chunk_1"));
    REQUIRE((backend.simeonPqIndexStats().full_builds == 1));

    // +5 % stays within the ratio: append.
    REQUIRE((backend.insertVectorsBatch(corpus(10, 3000)).has_value()));
    REQUIRE((topIds(backend, embedding(3000.0f), 3).front() == "chunk_3000"));
    CHECK((backend.simeonPqIndexStats().full_builds == 1));
    CHECK((backend.simeonPqIndexStats().incremental_appends == 1));

    // Another +10 % crosses it: retrain on the grown corpus.
    REQUIRE((backend.insertVectorsBatch(corpus(20, 4000)).has_value()));
    REQUIRE((topIds(backend, embedding(4010.0f), 3).front() == "chunk_4010"));
    CHECK((backend.simeonPqIndexStats().full_builds == 2));
    CHECK((backend.simeonPqIndexStats().incremental_appends == 1));
}

TEST_CASE_METHOD(SpqIncrementalFixture,
                 "SPQ: a loaded persisted index accepts appends without retraining",
                 "[unit][vector][spq][incremental][persistence][catch2]") {
    skipIfNeeded();

    const auto dbPath = createTempDbPath();
    {
        SqliteVecBackend backend(smallPqConfig());
        REQUIRE((backend.initialize(dbPath).has_value()));
        REQUIRE((backend.createTables(kDim).has_value()));
        REQUIRE((backend.insertVectorsBatch(corpus(300)).has_value()));
        REQUIRE((backend.buildIndex().has_value()));
        REQUIRE((backend.persistIndex().has_value()));
        backend.close();
    }

    SqliteVecBackend reopened(smallPqConfig());
    REQUIRE((reopened.initialize(dbPath).has_value()));
    REQUIRE((reopened.prepareSearchIndex().has_value()));
    REQUIRE((topIds(reopened, embedding(1.0f), 3).front() == "chunk_1"));
    REQUIRE((reopened.simeonPqIndexStats().full_builds == 0));

    const auto emb = embedding(7000.0f);
    REQUIRE((reopened.insertVector(record("7000", emb)).has_value()));
    CHECK((topIds(reopened, emb, 3).front() == "chunk_7000"));
    const auto stats = reopened.simeonPqIndexStats();
    CHECK((stats.full_builds == 0));
    CHECK((stats.incremental_appends == 1));

    // Persisting after an append writes the appended rows too.
    REQUIRE((reopened.persistIndex().has_value()));
    reopened.close();

    SqliteVecBackend third(smallPqConfig());
    REQUIRE((third.initialize(dbPath).has_value()));
    REQUIRE((third.prepareSearchIndex().has_value()));
    CHECK((topIds(third, emb, 3).front() == "chunk_7000"));
    CHECK((third.simeonPqIndexStats().full_builds == 0));
}

TEST_CASE_METHOD(SpqIncrementalFixture,
                 "SPQ: appended rows rank like a full rebuild of the same corpus",
                 "[unit][vector][spq][incremental][quality][catch2]") {
    skipIfNeeded();

    constexpr size_t kBase = 500;
    constexpr size_t kAppend = 150;
    constexpr size_t kQueries = 25;
    constexpr size_t kTopK = 10;

    const auto base = corpus(kBase);
    const auto extra = corpus(kAppend, 10000);
    std::vector<VectorRecord> all = base;
    all.insert(all.end(), extra.begin(), extra.end());

    auto cosine = [](const std::vector<float>& a, const std::vector<float>& b) {
        float dot = 0.0f;
        for (size_t i = 0; i < a.size(); ++i) {
            dot += a[i] * b[i];
        }
        return dot;
    };
    auto exactTop = [&](const std::vector<float>& q) {
        std::vector<std::pair<float, std::string>> scored;
        scored.reserve(all.size());
        for (const auto& rec : all) {
            scored.emplace_back(cosine(q, rec.embedding), rec.chunk_id);
        }
        std::partial_sort(scored.begin(), scored.begin() + kTopK, scored.end(),
                          [](const auto& a, const auto& b) { return a.first > b.first; });
        std::unordered_set<std::string> ids;
        for (size_t i = 0; i < kTopK; ++i) {
            ids.insert(scored[i].second);
        }
        return ids;
    };

    // Arm A: everything inserted, one build.
    SqliteVecBackend full(smallPqConfig());
    REQUIRE((full.initialize(":memory:").has_value()));
    REQUIRE((full.createTables(kDim).has_value()));
    REQUIRE((full.insertVectorsBatch(all).has_value()));

    // Arm B: base trained, extra appended.
    SqliteVecBackend incremental(smallPqConfig());
    REQUIRE((incremental.initialize(":memory:").has_value()));
    REQUIRE((incremental.createTables(kDim).has_value()));
    REQUIRE((incremental.insertVectorsBatch(base).has_value()));
    REQUIRE((topIds(incremental, embedding(1.0f), 3).front() == "chunk_1"));
    REQUIRE((incremental.insertVectorsBatch(extra).has_value()));

    size_t hitsFull = 0;
    size_t hitsIncremental = 0;
    for (size_t i = 0; i < kQueries; ++i) {
        const auto q = embedding(static_cast<float>(90000 + i));
        const auto expected = exactTop(q);
        for (const auto& id : topIds(full, q, kTopK)) {
            hitsFull += expected.contains(id) ? 1 : 0;
        }
        for (const auto& id : topIds(incremental, q, kTopK)) {
            hitsIncremental += expected.contains(id) ? 1 : 0;
        }
    }
    const double recallFull = static_cast<double>(hitsFull) / (kQueries * kTopK);
    const double recallIncremental = static_cast<double>(hitsIncremental) / (kQueries * kTopK);
    INFO("recall full=" << recallFull << " incremental=" << recallIncremental);
    CHECK((incremental.simeonPqIndexStats().full_builds == 1));
    CHECK((incremental.simeonPqIndexStats().incremental_appends == 1));
    CHECK((recallIncremental >= recallFull - 0.10));
    CHECK((recallIncremental >= 0.6));
}

TEST_CASE_METHOD(SpqIncrementalFixture,
                 "SPQ: structural generation column is created and maintained",
                 "[unit][vector][spq][incremental][persistence][catch2]") {
    skipIfNeeded();

    const auto dbPath = createTempDbPath();
    SqliteVecBackend backend(smallPqConfig());
    REQUIRE((backend.initialize(dbPath).has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));

    sqlite3* db = nullptr;
    REQUIRE((sqlite3_open(dbPath.c_str(), &db) == SQLITE_OK));
    auto readGenerations = [&]() {
        sqlite3_stmt* stmt = nullptr;
        REQUIRE((sqlite3_prepare_v2(db,
                                    "SELECT generation, structural_generation FROM "
                                    "vector_index_generation WHERE id = 1",
                                    -1, &stmt, nullptr) == SQLITE_OK));
        REQUIRE((sqlite3_step(stmt) == SQLITE_ROW));
        std::pair<long long, long long> out{sqlite3_column_int64(stmt, 0),
                                            sqlite3_column_int64(stmt, 1)};
        sqlite3_finalize(stmt);
        return out;
    };

    const auto g0 = readGenerations();
    REQUIRE((backend.insertVector(record("1", embedding(1.0f))).has_value()));
    const auto g1 = readGenerations();
    CHECK((g1.first == g0.first + 1));
    CHECK((g1.second == g0.second));

    REQUIRE((backend.updateVector("chunk_1", record("1", embedding(2.0f))).has_value()));
    const auto g2 = readGenerations();
    CHECK((g2.first > g1.first));
    CHECK((g2.second == g1.second + 1));

    REQUIRE((backend.deleteVector("chunk_1").has_value()));
    const auto g3 = readGenerations();
    CHECK((g3.first > g2.first));
    CHECK((g3.second == g2.second + 1));
    sqlite3_close(db);
}

TEST_CASE_METHOD(SpqIncrementalFixture,
                 "SPQ: concurrent searches during inserts see every appended row",
                 "[unit][vector][spq][incremental][concurrency][catch2]") {
    skipIfNeeded();

    const auto dbPath = createTempDbPath();
    SqliteVecBackend backend(smallPqConfig());
    REQUIRE((backend.initialize(dbPath).has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));
    REQUIRE((backend.insertVectorsBatch(corpus(300)).has_value()));
    REQUIRE((topIds(backend, embedding(1.0f), 3).front() == "chunk_1"));

    // Readers pause between searches: the backend's shared_mutex is reader-preferring on
    // glibc, and a tight reader loop under TSan can starve the inserting writer for minutes.
    constexpr size_t kReaders = 4;
    constexpr size_t kSearchesPerReader = 60;
    constexpr size_t kInserts = 40;
    std::atomic<size_t> searches{0};
    std::atomic<size_t> failures{0};
    std::vector<std::thread> readers;
    for (size_t r = 0; r < kReaders; ++r) {
        readers.emplace_back([&, r]() {
            for (size_t i = 0; i < kSearchesPerReader; ++i) {
                const auto q = embedding(static_cast<float>(1 + ((r * 97 + i) % 300)));
                auto result = backend.searchSimilar(q, 3, -1.0f, std::nullopt, {});
                if (!result.has_value() || result.value().empty()) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
                searches.fetch_add(1, std::memory_order_relaxed);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }
    for (size_t n = 0; n < kInserts; ++n) {
        const size_t id = 8000 + n;
        REQUIRE((backend.insertVector(record(std::to_string(id), embedding(static_cast<float>(id))))
                     .has_value()));
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    for (auto& t : readers) {
        t.join();
    }

    CHECK((failures.load() == 0));
    CHECK((searches.load() == kReaders * kSearchesPerReader));
    for (size_t n = 0; n < kInserts; n += 5) {
        const size_t id = 8000 + n;
        CHECK((topIds(backend, embedding(static_cast<float>(id)), 3).front() ==
               "chunk_" + std::to_string(id)));
    }
    const auto stats = backend.simeonPqIndexStats();
    INFO("searches=" << searches.load() << " full_builds=" << stats.full_builds
                     << " appends=" << stats.incremental_appends);
    CHECK((stats.full_builds == 1));
    CHECK((stats.appended_vectors == kInserts));
}
