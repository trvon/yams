#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <yams/metadata/metadata_repository.h>
#include <yams/search/simeon_lexical_backend.h>

#include <simeon/bm25.hpp>
#include <simeon/fusion.hpp>

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

struct TempCorpus {
    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repo;
    std::vector<std::int64_t> docIds;
};

TempCorpus makeCorpus(const std::vector<std::pair<std::string, std::string>>& docs) {
    TempCorpus corpus;
    corpus.dbPath = tempDbPath("simeon_lex_");
    ConnectionPoolConfig pcfg;
    corpus.pool = std::make_unique<ConnectionPool>(corpus.dbPath.string(), pcfg);
    REQUIRE(corpus.pool->initialize().has_value());
    corpus.repo = std::make_shared<MetadataRepository>(*corpus.pool);

    for (const auto& [hash, content] : docs) {
        DocumentInfo d;
        d.filePath = "/tmp/" + hash + ".txt";
        d.fileName = hash + ".txt";
        d.fileExtension = ".txt";
        d.fileSize = static_cast<int64_t>(content.size());
        d.sha256Hash = hash;
        d.mimeType = "text/plain";
        d.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        d.modifiedTime = d.createdTime;
        d.indexedTime = d.createdTime;
        auto did = corpus.repo->insertDocument(d);
        REQUIRE(did.has_value());
        corpus.docIds.push_back(did.value());

        REQUIRE(corpus.repo->indexDocumentContent(did.value(), hash, content, "text/plain")
                    .has_value());

        DocumentContent dc;
        dc.documentId = did.value();
        dc.contentText = content;
        dc.contentLength = static_cast<int64_t>(content.size());
        dc.extractionMethod = "test";
        dc.language = "en";
        REQUIRE(corpus.repo->insertContent(dc).has_value());
    }
    return corpus;
}

bool waitReady(const SimeonLexicalBackend& backend, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (backend.ready()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return backend.ready();
}

bool waitNotBuilding(const SimeonLexicalBackend& backend, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (!backend.building()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return !backend.building();
}

} // namespace

TEST_CASE("SimeonLexicalBackend default config uses SabSmooth", "[search][simeon][catch2]") {
    SimeonLexicalBackend::Config cfg;
    CHECK(cfg.variant == SimeonLexicalBackend::Variant::SabSmooth);
    CHECK(cfg.subword_gamma == 5.0f);
    CHECK(cfg.max_corpus_docs == 200'000u);
    CHECK(cfg.max_corpus_bytes == 256ULL * 1024ULL * 1024ULL);
    CHECK(cfg.max_index_bytes == 256ULL * 1024ULL * 1024ULL);
    CHECK_FALSE(cfg.index_budget_provider);
    CHECK(cfg.build_doc_chunk_bytes == 16ULL * 1024ULL);
    CHECK(cfg.build_doc_max_chunks == 4u);
    CHECK_FALSE(cfg.fragment_geometry_enabled); // retired by default; opt-in only
    CHECK(cfg.fragment_encoder_profile == SimeonLexicalBackend::FragmentEncoderProfile::CorpusPmi);
    CHECK(cfg.fragment_geometry_min_corpus_docs == 1000u);
    CHECK(cfg.fragment_geometry_max_docs == 20'000u);
    CHECK(cfg.fragment_geometry_max_corpus_bytes == 64ULL * 1024ULL * 1024ULL);
    CHECK(cfg.fragment_geometry_pmi_sample_docs == 8192u);
    CHECK(cfg.fragment_geometry_pmi_sample_bytes == 32ULL * 1024ULL * 1024ULL);
    CHECK(cfg.concept_weight == 0.5f);
}

TEST_CASE("Simeon query sessions preserve scores and isolate queries and recipes",
          "[search][simeon][request-scores][catch2]") {
    auto corpus =
        makeCorpus({{"a", "alpha alpha beta"}, {"b", "beta gamma"}, {"c", "gamma delta"}});
    SimeonLexicalBackend::Config config;
    config.strategy_router_enabled = GENERATE(false, true);
    config.router_enabled = !config.strategy_router_enabled;
    SimeonLexicalBackend backend(config);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));
    for (const auto& query : {"alpha", "gamma"}) {
        for (const auto& arm : {"", "sab_smooth", "atire"}) {
            const auto baseline = std::string_view(arm).empty()
                                      ? (backend.hasStrategyRouter()
                                             ? backend.scoreStrategyRouted(query, corpus.docIds)
                                             : backend.scoreRouted(query, corpus.docIds))
                                      : backend.scoreBanditRouted(query, arm, corpus.docIds);
            REQUIRE(baseline.has_value());
            const auto calls = backend.scoreCalls();
            SimeonLexicalBackend::QuerySession session(backend, query, arm);
            CHECK(backend.scoreCalls() == calls); // lazy, no work for unused sessions
            const auto first = session.score(corpus.docIds);
            REQUIRE(first.has_value());
            CHECK(first.value().scores == baseline.value().scores);
            CHECK(first.value().recipe_name == baseline.value().recipe_name);
            const std::vector<std::int64_t> subset{corpus.docIds.back(), -1, corpus.docIds.front()};
            const auto second = session.score(subset);
            REQUIRE(second.has_value());
            CHECK(second.value().scores == std::vector<float>{baseline.value().scores.back(), 0.0F,
                                                              baseline.value().scores.front()});
            REQUIRE(session.searchTop(2).has_value());
            CHECK(backend.scoreCalls() == calls + 1);
            // A second request reuses the thread-local bandit scratch. It must
            // not alter scores retained by the first request.
            SimeonLexicalBackend::QuerySession other(
                backend, std::string_view(query) == "alpha" ? "gamma" : "alpha", arm);
            REQUIRE(other.score(corpus.docIds).has_value());
            const auto retained = session.score(corpus.docIds);
            REQUIRE(retained.has_value());
            CHECK(retained.value().scores == baseline.value().scores);
            CHECK(backend.scoreCalls() == calls + 2);
        }
    }
}

TEST_CASE("SimeonLexicalBackend buildAsync flips ready on small corpus",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon"},
        {"hash_c", "omega sigma tau"},
    });

    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    CHECK_FALSE(backend.ready());

    auto build = backend.buildAsync(corpus.repo);
    REQUIRE(build.has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));
    CHECK(backend.doc_count() == 3u);
}

namespace {
// Holds the first getContent() call of a build until the test releases it, so a shutdown request
// can be issued while the build is provably mid-corpus.
class GatedContentRepository final : public MetadataRepository {
public:
    using MetadataRepository::MetadataRepository;

    Result<std::optional<DocumentContent>> getContent(int64_t documentId) override {
        if (calls_.fetch_add(1, std::memory_order_acq_rel) == 0) {
            entered_.set_value();
            releaseFuture_.wait();
        }
        return MetadataRepository::getContent(documentId);
    }

    bool waitEntered(std::chrono::milliseconds timeout) {
        return enteredFuture_.wait_for(timeout) == std::future_status::ready;
    }
    void release() { release_.set_value(); }
    std::size_t calls() const { return calls_.load(std::memory_order_acquire); }

private:
    std::atomic<std::size_t> calls_{0};
    std::promise<void> entered_;
    std::future<void> enteredFuture_{entered_.get_future()};
    std::promise<void> release_;
    std::shared_future<void> releaseFuture_{release_.get_future().share()};
};
} // namespace

TEST_CASE("SimeonLexicalBackend stops a running build when shutdown is requested",
          "[search][simeon][shutdown][catch2]") {
    std::vector<std::pair<std::string, std::string>> docs;
    for (int i = 0; i < 64; ++i) {
        docs.emplace_back("hash_stop_" + std::to_string(i),
                          "alpha beta gamma document " + std::to_string(i));
    }
    auto corpus = makeCorpus(docs);
    auto gated = std::make_shared<GatedContentRepository>(*corpus.pool);

    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    REQUIRE(backend.buildAsync(gated).has_value());
    REQUIRE(gated->waitEntered(std::chrono::seconds(5)));

    // Daemon shutdown asks the build to stop before the engine is destroyed, so the build thread
    // is already gone when SearchEngineManager::clearEngine() drops the engine.
    backend.requestStop();
    gated->release();

    REQUIRE(waitNotBuilding(backend, std::chrono::seconds(5)));
    CHECK_FALSE(backend.ready());
    CHECK((gated->calls() <= 2U));
}

TEST_CASE("SimeonLexicalBackend skips build when corpus text budget is exceeded",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon zeta"},
        {"hash_c", "omega sigma tau upsilon"},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.max_corpus_bytes = 16;
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitNotBuilding(backend, std::chrono::seconds(5)));
    CHECK_FALSE(backend.ready());
    CHECK(backend.doc_count() == 0u);
}

namespace {
std::vector<std::pair<std::string, std::string>> wordyCorpus(std::size_t docs) {
    std::vector<std::pair<std::string, std::string>> out;
    for (std::size_t d = 0; d < docs; ++d) {
        std::string text;
        for (std::size_t w = 0; w < 40; ++w) {
            text += "term" + std::to_string((d * 31 + w * 7) % 997) + " ";
        }
        out.emplace_back("hash_w" + std::to_string(d), std::move(text));
    }
    return out;
}
} // namespace

TEST_CASE("SimeonLexicalBackend stays on FTS5 when the index memory budget is exceeded",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus(wordyCorpus(64));

    SECTION("a generous grant builds") {
        SimeonLexicalBackend::Config cfg;
        cfg.max_index_bytes = 256ULL * 1024ULL * 1024ULL;
        SimeonLexicalBackend backend(cfg);
        REQUIRE(backend.buildAsync(corpus.repo).has_value());
        REQUIRE(waitNotBuilding(backend, std::chrono::seconds(10)));
        CHECK(backend.ready());
        CHECK(backend.doc_count() == 64u);
    }

    SECTION("a tiny fixed grant stops the build") {
        SimeonLexicalBackend::Config cfg;
        cfg.max_index_bytes = 4096;
        SimeonLexicalBackend backend(cfg);
        REQUIRE(backend.buildAsync(corpus.repo).has_value());
        REQUIRE(waitNotBuilding(backend, std::chrono::seconds(10)));
        CHECK_FALSE(backend.ready());
        CHECK(backend.doc_count() == 0u);
    }

    SECTION("the provider's grant replaces the fixed default") {
        SimeonLexicalBackend::Config cfg;
        cfg.max_index_bytes = 256ULL * 1024ULL * 1024ULL;
        std::atomic<int> calls{0};
        cfg.index_budget_provider = [&calls] {
            ++calls;
            return std::size_t{4096};
        };
        SimeonLexicalBackend backend(cfg);
        REQUIRE(backend.buildAsync(corpus.repo).has_value());
        REQUIRE(waitNotBuilding(backend, std::chrono::seconds(10)));
        CHECK_FALSE(backend.ready());
        CHECK(calls.load() >= 1);
    }
}

TEST_CASE("SimeonLexicalBackend re-reads the index grant during a build",
          "[search][simeon][catch2]") {
    // 600 docs crosses the 256-doc poll twice. The grant is generous at the
    // start and collapses at the first poll, as under a pressure spike.
    auto corpus = makeCorpus(wordyCorpus(600));
    SimeonLexicalBackend::Config cfg;
    std::atomic<int> calls{0};
    cfg.index_budget_provider = [&calls] {
        return ++calls == 1 ? std::size_t{256ULL * 1024ULL * 1024ULL} : std::size_t{4096};
    };
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitNotBuilding(backend, std::chrono::seconds(20)));
    CHECK_FALSE(backend.ready());
    CHECK(calls.load() >= 2);
}

TEST_CASE("SimeonLexicalBackend chunks oversized docs before applying corpus byte budget",
          "[search][simeon][catch2]") {
    std::string large(4096, 'x');
    auto corpus = makeCorpus({{{"hash_big", large}}});

    SimeonLexicalBackend::Config cfg;
    cfg.max_corpus_bytes = 512;
    cfg.build_doc_chunk_bytes = 128;
    cfg.build_doc_max_chunks = 2;
    cfg.fragment_geometry_enabled = false;
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));
    CHECK(backend.doc_count() == 1u);
}

namespace {

// 120 documents of ~4 KiB each. A strongly associated pair ("quantum entanglement") opens each
// document, followed by a wide filler vocabulary whose bigrams carry no association.
std::vector<std::pair<std::string, std::string>> conceptCorpusDocs() {
    std::vector<std::pair<std::string, std::string>> docs;
    docs.reserve(120);
    std::uint32_t lcg = 12345u;
    for (int i = 0; i < 120; ++i) {
        std::string text = "quantum entanglement quantum entanglement quantum entanglement ";
        while (text.size() < 4096) {
            lcg = lcg * 1664525u + 1013904223u;
            text += "w" + std::to_string(lcg % 397) + " ";
        }
        docs.emplace_back("hash_concept_" + std::to_string(i), std::move(text));
    }
    return docs;
}

} // namespace

TEST_CASE("SimeonLexicalBackend mines concepts from the text BM25 indexed",
          "[search][simeon][concepts][catch2]") {
    // Chunking reduces each ~4 KiB document to two 128-byte windows, so the indexed text is
    // about 31 KiB while the raw corpus is about 480 KiB. Concept mining used to re-read every
    // raw document into memory at once; it now reads the reduced text, so the corpus budget
    // that admits the index admits the concepts too.
    const auto docs = conceptCorpusDocs();
    std::size_t rawCorpusBytes = 0;
    for (const auto& doc : docs)
        rawCorpusBytes += doc.second.size();
    auto corpus = makeCorpus(docs);

    SimeonLexicalBackend::Config cfg;
    cfg.concept_mining_enabled = true;
    cfg.build_doc_chunk_bytes = 128;
    cfg.build_doc_max_chunks = 2;
    cfg.fragment_geometry_enabled = false;

    SECTION("a budget below the raw corpus but above the indexed text still mines concepts") {
        cfg.max_corpus_bytes = 64ULL * 1024ULL;
        REQUIRE(rawCorpusBytes > cfg.max_corpus_bytes);
        SimeonLexicalBackend backend(cfg);
        REQUIRE(backend.buildAsync(corpus.repo).has_value());
        REQUIRE(waitReady(backend, std::chrono::seconds(30)));
        CHECK(backend.doc_count() == 120u);
        CHECK(backend.concept_count() > 0u);
    }
    SECTION("a generous budget mines the same concepts") {
        cfg.max_corpus_bytes = 64ULL * 1024ULL;
        SimeonLexicalBackend tight(cfg);
        REQUIRE(tight.buildAsync(corpus.repo).has_value());
        REQUIRE(waitReady(tight, std::chrono::seconds(30)));

        cfg.max_corpus_bytes = 4ULL * 1024ULL * 1024ULL;
        SimeonLexicalBackend generous(cfg);
        REQUIRE(generous.buildAsync(corpus.repo).has_value());
        REQUIRE(waitReady(generous, std::chrono::seconds(30)));

        CHECK(generous.concept_count() > 0u);
        CHECK(generous.concept_count() == tight.concept_count());
    }
    SECTION("fewer than 100 documents mine nothing") {
        auto smallCorpus = makeCorpus(
            std::vector<std::pair<std::string, std::string>>(docs.begin(), docs.begin() + 99));
        SimeonLexicalBackend backend(cfg);
        REQUIRE(backend.buildAsync(smallCorpus.repo).has_value());
        REQUIRE(waitReady(backend, std::chrono::seconds(30)));
        CHECK(backend.doc_count() == 99u);
        CHECK(backend.concept_count() == 0u);
    }
}

TEST_CASE("SimeonLexicalBackend concept scoring survives a document with no content",
          "[search][simeon][concepts][catch2]") {
    // A document that is FTS5-indexed but has no content row is skipped by the index. Concepts
    // used to be numbered over every listed document, so one such document left the concept
    // index a different size from the BM25 index and concept scoring was silently dropped.
    auto corpus = makeCorpus(conceptCorpusDocs());
    {
        DocumentInfo d;
        d.filePath = "/tmp/hash_contentless.txt";
        d.fileName = "hash_contentless.txt";
        d.fileExtension = ".txt";
        d.fileSize = 0;
        d.sha256Hash = "hash_contentless";
        d.mimeType = "text/plain";
        d.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        d.modifiedTime = d.createdTime;
        d.indexedTime = d.createdTime;
        auto did = corpus.repo->insertDocument(d);
        REQUIRE(did.has_value());
        REQUIRE(corpus.repo
                    ->indexDocumentContent(did.value(), "hash_contentless", "placeholder words",
                                           "text/plain")
                    .has_value());
    }

    SimeonLexicalBackend::Config cfg;
    cfg.concept_mining_enabled = true;
    cfg.fragment_geometry_enabled = false;

    cfg.concept_weight = 0.0f;
    SimeonLexicalBackend plain(cfg);
    REQUIRE(plain.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(plain, std::chrono::seconds(30)));
    REQUIRE(plain.doc_count() == 120u);

    cfg.concept_weight = 0.5f;
    SimeonLexicalBackend blended(cfg);
    REQUIRE(blended.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(blended, std::chrono::seconds(30)));
    REQUIRE(blended.doc_count() == 120u);
    REQUIRE(blended.concept_count() > 0u);

    const auto plainScores = plain.score("quantum entanglement", corpus.docIds);
    const auto blendedScores = blended.score("quantum entanglement", corpus.docIds);
    REQUIRE(plainScores.has_value());
    REQUIRE(blendedScores.has_value());
    REQUIRE(plainScores.value().size() == blendedScores.value().size());
    std::size_t raised = 0;
    for (std::size_t i = 0; i < plainScores.value().size(); ++i) {
        if (blendedScores.value()[i] > plainScores.value()[i]) {
            ++raised;
        }
    }
    CHECK(raised > 0u);
}

TEST_CASE("SimeonLexicalBackend score returns one float per candidate",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon"},
        {"hash_c", "omega sigma tau"},
    });

    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    auto scores = backend.score("beta", corpus.docIds);
    REQUIRE(scores.has_value());
    CHECK(scores.value().size() == corpus.docIds.size());
}

TEST_CASE("SimeonLexicalBackend scoring folds ASCII case consistently",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "Beta beta gamma"},
        {"hash_b", "omega sigma tau"},
    });

    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    auto lower = backend.score("beta", corpus.docIds);
    auto upper = backend.score("BETA", corpus.docIds);
    REQUIRE(lower.has_value());
    REQUIRE(upper.has_value());
    REQUIRE(lower.value().size() == upper.value().size());
    CHECK(lower.value()[0] > 0.0f);
    CHECK(upper.value()[0] == Catch::Approx(lower.value()[0]));
}

TEST_CASE("Simeon BM25 variant RRF does not assign priors to zero-score docs",
          "[search][simeon][catch2]") {
    simeon::Bm25Config sabCfg;
    sabCfg.variant = simeon::Bm25Variant::SubwordAwareBackoff;
    simeon::Bm25Index sab(sabCfg);

    simeon::Bm25Config atireCfg;
    atireCfg.variant = simeon::Bm25Variant::Atire;
    simeon::Bm25Index atire(atireCfg);

    for (auto* idx : {&sab, &atire}) {
        idx->add_doc("alpha beta");
        idx->add_doc("gamma delta");
        idx->finalize();
    }

    std::vector<float> scores(2, -1.0f);
    const simeon::Bm25Index* variants[2] = {&sab, &atire};
    simeon::score_bm25_variants_rrf(std::span<const simeon::Bm25Index* const>(variants, 2),
                                    "absent", std::span<float>{scores}, 60.0f);

    CHECK(scores[0] == 0.0f);
    CHECK(scores[1] == 0.0f);
}

TEST_CASE("Simeon lead-field arm uses indexed auxiliary lead text", "[search][simeon][catch2]") {
    auto makeLongDoc = [](bool needleInLead) {
        std::string text;
        if (needleInLead) {
            text = "needle";
        }
        for (int i = 0; i < 80; ++i) {
            if (!text.empty()) {
                text.push_back(' ');
            }
            text += "filler";
        }
        if (!needleInLead) {
            text += " needle";
        }
        return text;
    };

    auto corpus = makeCorpus({
        {"hash_lead", makeLongDoc(true)},
        {"hash_tail", makeLongDoc(false)},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.strategy_router_enabled = true;
    cfg.fragment_geometry_enabled = false;
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    auto base = backend.scoreBanditRouted("needle", "sab_smooth", corpus.docIds);
    auto lead = backend.scoreBanditRouted("needle", "lead_field", corpus.docIds);
    REQUIRE(base.has_value());
    REQUIRE(lead.has_value());
    REQUIRE(base.value().scores.size() == 2U);
    REQUIRE(lead.value().scores.size() == 2U);

    const float baseGap = base.value().scores[0] - base.value().scores[1];
    const float leadGap = lead.value().scores[0] - lead.value().scores[1];
    CHECK(leadGap > baseGap);
}

TEST_CASE("SimeonLexicalBackend skips fragment geometry below the default corpus threshold",
          "[search][simeon][catch2]") {
    std::vector<std::pair<std::string, std::string>> docs;
    docs.reserve(80);
    for (int i = 0; i < 80; ++i) {
        std::string content;
        for (int j = 0; j < 20; ++j) {
            if (j > 0) {
                content.push_back(' ');
            }
            content += "token" + std::to_string((i + j) % 80);
        }
        docs.emplace_back("hash_" + std::to_string(i), std::move(content));
    }
    auto corpus = makeCorpus(docs);

    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(10)));
    CHECK_FALSE(backend.fragmentGeometryReady());
}

TEST_CASE("SimeonLexicalBackend fixed-hash fragments do not require a PMI vocabulary",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta. alpha beta."},
        {"hash_b", "beta gamma epsilon zeta. beta epsilon."},
        {"hash_c", "omega sigma tau upsilon. omega tau."},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.fragment_geometry_enabled = true;
    cfg.fragment_geometry_min_corpus_docs = 1;
    cfg.fragment_encoder_profile = SimeonLexicalBackend::FragmentEncoderProfile::FixedHash384;
    SimeonLexicalBackend backend(cfg);

    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(10)));
    CHECK(backend.fragmentGeometryReady());
    CHECK(backend.fragmentEmbeddingSpaceIdentity() == "simeon-v1-384");

    auto scores = backend.score("alpha beta", corpus.docIds);
    REQUIRE(scores.has_value());
    REQUIRE(scores.value().size() == corpus.docIds.size());
    CHECK(std::isfinite(scores.value().front()));
}

TEST_CASE("SimeonLexicalBackend explicit fragment geometry does not quality-route to BM25",
          "[search][simeon][fragment-geometry][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta. alpha beta."},
        {"hash_b", "beta gamma epsilon zeta. beta epsilon."},
        {"hash_c", "omega sigma tau upsilon. omega tau."},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.fragment_geometry_enabled = true;
    cfg.fragment_geometry_min_corpus_docs = 1;
    cfg.fragment_encoder_profile = SimeonLexicalBackend::FragmentEncoderProfile::FixedHash384;
    SimeonLexicalBackend backend(cfg);

    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(10)));

    auto decision = backend.scoreFragmentGeometry("alpha beta", corpus.docIds);
    REQUIRE(decision.has_value());
    CHECK(std::string(decision.value().recipe_name) == "FragmentRichCovPhssApprox");
    REQUIRE(decision.value().scores.size() == corpus.docIds.size());
    for (const float score : decision.value().scores) {
        CHECK(std::isfinite(score));
    }
}

TEST_CASE("SimeonLexicalBackend score returns 0 for unknown doc_ids", "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma"},
        {"hash_b", "beta gamma epsilon"},
    });

    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    const std::int64_t missingId = 999'999;
    std::vector<std::int64_t> ids = {corpus.docIds.front(), missingId};
    auto scores = backend.score("beta", ids);
    REQUIRE(scores.has_value());
    REQUIRE(scores.value().size() == ids.size());
    CHECK(scores.value()[1] == 0.0f);
}

TEST_CASE("SimeonLexicalBackend score before ready returns NotInitialized",
          "[search][simeon][catch2]") {
    SimeonLexicalBackend backend(SimeonLexicalBackend::Config{});
    REQUIRE_FALSE(backend.ready());
    std::vector<std::int64_t> ids = {1};
    auto scores = backend.score("anything", ids);
    REQUIRE_FALSE(scores.has_value());
    CHECK(scores.error().code == ErrorCode::NotInitialized);
}

TEST_CASE("SimeonLexicalBackend Atire variant also builds", "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma"},
        {"hash_b", "beta gamma epsilon"},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.variant = SimeonLexicalBackend::Variant::Atire;
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    auto scores = backend.score("beta", corpus.docIds);
    REQUIRE(scores.has_value());
    CHECK(scores.value().size() == corpus.docIds.size());
}

TEST_CASE("SimeonLexicalBackend scoreRouted passthrough when router disabled",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon"},
        {"hash_c", "omega sigma tau"},
    });

    SimeonLexicalBackend::Config cfg; // router_enabled=false by default
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    auto decision = backend.scoreRouted("beta", corpus.docIds);
    REQUIRE(decision.has_value());
    CHECK(decision.value().scores.size() == corpus.docIds.size());
    // router=off → recipe label falls back to the configured variant.
    CHECK(std::string(decision.value().recipe_name) == "SabSmooth");
}

TEST_CASE("SimeonLexicalBackend scoreRouted picks a recipe with router on",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon zeta"},
        {"hash_c", "omega sigma tau upsilon"},
        {"hash_d", "beta gamma theta iota"},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.router_enabled = true; // passE preset defaults
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    auto decision = backend.scoreRouted("beta gamma", corpus.docIds);
    REQUIRE(decision.has_value());
    CHECK(decision.value().scores.size() == corpus.docIds.size());
    const std::string name = decision.value().recipe_name;
    CHECK((name == "Bm25Atire" || name == "Bm25SabSmooth" || name == "CascadeLinearAlpha"));
}

TEST_CASE("SimeonLexicalBackend scoreRouted routes OOV query to SabSmooth",
          "[search][simeon][catch2]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon zeta"},
        {"hash_c", "omega sigma tau upsilon"},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.router_enabled = true; // passE: oov_threshold=0 → any OOV → SAB
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    // "xyzneverpresent" does not appear in any document → oov_rate = 1.0 > 0.
    auto decision = backend.scoreRouted("xyzneverpresent", corpus.docIds);
    REQUIRE(decision.has_value());
    CHECK(std::string(decision.value().recipe_name) == "Bm25SabSmooth");
}

TEST_CASE("SimeonLexicalBackend score timing counters accumulate",
          "[search][simeon][catch2][instrumentation]") {
    auto corpus = makeCorpus({
        {"hash_a", "alpha beta gamma delta"},
        {"hash_b", "beta gamma epsilon"},
    });

    SimeonLexicalBackend::Config cfg;
    cfg.score_cache_entries = 8;
    SimeonLexicalBackend backend(cfg);
    REQUIRE(backend.buildAsync(corpus.repo).has_value());
    REQUIRE(waitReady(backend, std::chrono::seconds(5)));

    CHECK(backend.scoreCalls() == 0);
    CHECK(backend.scoreCacheHits() == 0);

    REQUIRE(backend.score("beta", corpus.docIds).has_value());
    CHECK(backend.scoreCalls() == 1);
    CHECK(backend.scoreCacheHits() == 0);

    REQUIRE(backend.score("beta", corpus.docIds).has_value());
    CHECK(backend.scoreCalls() == 2);
    CHECK(backend.scoreCacheHits() == 1);

    REQUIRE(backend.scoreRouted("gamma", corpus.docIds).has_value());
    CHECK(backend.scoreCalls() == 3);
    CHECK(backend.scoreMicrosTotal() > 0);
}
