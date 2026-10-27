// SPDX-License-Identifier: GPL-3.0-or-later
// Isolated text-only SearchEngineBuilder publication vs asynchronous Simeon lexical readiness.
// This is not a daemon startup benchmark: memory-sync, vector PQ, KG and worker scheduling
// are absent. Seed time is excluded from both measured intervals.
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/search/search_engine_builder.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <sys/resource.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {
using namespace yams::metadata;
using Clock = std::chrono::steady_clock;

struct Options {
    std::size_t docs{10};
    std::chrono::milliseconds deadline{30'000};
};

bool parse(int argc, char** argv, Options& options) {
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string_view arg{argv[i]};
            if (arg == "--help") {
                std::cout << "usage: search_build_phase_bench [--docs N] [--deadline-ms N]\n";
                return false;
            }
            if ((arg == "--docs" || arg == "--deadline-ms") && i + 1 < argc) {
                const auto value = std::stoull(argv[++i]);
                if (arg == "--docs") {
                    options.docs = static_cast<std::size_t>(value);
                } else {
                    options.deadline = std::chrono::milliseconds(value);
                }
            } else {
                std::cerr << "invalid option: " << arg << '\n';
                return false;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "invalid benchmark argument: " << e.what() << '\n';
        return false;
    }
    return options.docs > 0 && options.docs <= 10'000 && options.deadline.count() > 0;
}

std::uint64_t micros(Clock::duration elapsed) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count());
}

std::uint64_t cpuMicros(const timeval& value) {
    return static_cast<std::uint64_t>(value.tv_sec) * 1'000'000ULL +
           static_cast<std::uint64_t>(value.tv_usec);
}

struct Fixture {
    std::filesystem::path dir;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repo;
    ~Fixture() {
        repo.reset();
        if (pool) {
            pool->shutdown();
        }
        pool.reset();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
};

bool seed(Fixture& fixture, std::size_t docs) {
    const auto dbPath = fixture.dir / "search.db";
    ConnectionPoolConfig config;
    config.minConnections = 1;
    config.maxConnections = 2;
    fixture.pool = std::make_unique<ConnectionPool>(dbPath.string(), config);
    if (auto initialized = fixture.pool->initialize(); !initialized) {
        std::cerr << "pool init failed: " << initialized.error().message << '\n';
        return false;
    }
    fixture.repo = std::make_shared<MetadataRepository>(*fixture.pool);
    for (std::size_t i = 0; i < docs; ++i) {
        DocumentInfo doc;
        const auto path = "/bench/search/doc_" + std::to_string(i) + ".md";
        const auto derived = computePathDerivedValues(path);
        doc.filePath = derived.normalizedPath;
        doc.fileName = "doc_" + std::to_string(i) + ".md";
        doc.fileExtension = ".md";
        doc.pathPrefix = derived.pathPrefix;
        doc.reversePath = derived.reversePath;
        doc.pathHash = derived.pathHash;
        doc.parentHash = derived.parentHash;
        doc.pathDepth = derived.pathDepth;
        doc.sha256Hash = std::string(64, 'a');
        doc.sha256Hash.replace(0, std::to_string(i).size(), std::to_string(i));
        doc.mimeType = "text/markdown";
        doc.fileSize = 128;
        const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.createdTime = doc.modifiedTime = doc.indexedTime = now;
        doc.contentExtracted = true;
        doc.extractionStatus = ExtractionStatus::Success;
        auto inserted = fixture.repo->insertDocument(doc);
        if (!inserted) {
            std::cerr << "insert failed: " << inserted.error().message << '\n';
            return false;
        }
        const auto content = "alpha beta search benchmark document " + std::to_string(i);
        DocumentContent extracted{inserted.value(), content,
                                  static_cast<std::int64_t>(content.size()), "benchmark", "en"};
        if (auto stored = fixture.repo->insertContent(extracted); !stored) {
            std::cerr << "content insert failed: " << stored.error().message << '\n';
            return false;
        }
        if (auto indexed = fixture.repo->indexDocumentContentTrusted(inserted.value(), doc.fileName,
                                                                     content, doc.mimeType);
            !indexed) {
            std::cerr << "FTS index failed: " << indexed.error().message << '\n';
            return false;
        }
    }
    auto indexedIds = fixture.repo->getAllFts5IndexedDocumentIds();
    if (!indexedIds || indexedIds.value().size() != docs) {
        std::cerr << "fixture FTS document count differs from seeded records\n";
        return false;
    }
    for (const auto id : indexedIds.value()) {
        const auto content = fixture.repo->getContent(id);
        if (!content || !content.value()) {
            std::cerr << "fixture has an indexed document without extractable content\n";
            return false;
        }
    }
    return true;
}
} // namespace

int main(int argc, char** argv) {
    Options options;
    if (!parse(argc, argv, options)) {
        return 2;
    }
    spdlog::set_level(spdlog::level::warn);
    Fixture fixture;
    fixture.dir = std::filesystem::temp_directory_path() /
                  ("yams-search-build-" + std::to_string(Clock::now().time_since_epoch().count()));
    if (!std::filesystem::create_directory(fixture.dir) || !seed(fixture, options.docs)) {
        return 3;
    }

    yams::search::SearchEngineBuilder builder;
    builder.withMetadataRepo(fixture.repo);
    auto buildOptions = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
    buildOptions.config.vectorWeight = 0.0f;
    buildOptions.config.vectorMaxResults = 0;
    buildOptions.compatibilityEnvironmentSnapshotted = true;
    buildOptions.simeonLexicalConfig = yams::search::SimeonLexicalBackend::Config{};
    buildOptions.simeonLexicalConfig->max_corpus_docs = options.docs;

    rusage before{};
    rusage after{};
    getrusage(RUSAGE_SELF, &before);
    const auto started = Clock::now();
    auto built = builder.buildEmbedded(buildOptions);
    const auto published = Clock::now();
    if (!built) {
        std::cerr << "engine build failed: " << built.error().message << '\n';
        return 4;
    }
    auto engine = std::move(built.value());
    // The backend has no completion notification; bounded observation avoids confusing
    // engine publication with lexical readiness. A 2ms sample bounds measurement skew.
    auto status = engine->getSimeonLexicalStatus();
    while (!status.ready && status.building && Clock::now() - started < options.deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        status = engine->getSimeonLexicalStatus();
    }
    const auto lexical = Clock::now();
    getrusage(RUSAGE_SELF, &after);
    if (!status.configured || !status.ready || status.docCount != options.docs) {
        engine->requestShutdown();
        std::cerr << "lexical build did not become ready with seeded count: configured="
                  << status.configured << " ready=" << status.ready
                  << " building=" << status.building << " docs=" << status.docCount
                  << " expected=" << options.docs << '\n';
        return 5;
    }
    nlohmann::json result{{"scope", "builder_text_only"},
                          {"docs", options.docs},
                          {"engine_published_us", micros(published - started)},
                          {"lexical_ready_us", micros(lexical - started)},
                          {"lexical_docs", status.docCount},
                          {"user_us", cpuMicros(after.ru_utime) - cpuMicros(before.ru_utime)},
                          {"system_us", cpuMicros(after.ru_stime) - cpuMicros(before.ru_stime)},
#ifdef __APPLE__
                          {"peak_rss_bytes", static_cast<std::uint64_t>(after.ru_maxrss)}};
#else
                          {"peak_rss_bytes", static_cast<std::uint64_t>(after.ru_maxrss) * 1024}};
#endif
    std::cout << result.dump() << '\n';
    engine->requestShutdown();
    engine.reset();
    return 0;
}
