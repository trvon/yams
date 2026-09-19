// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <algorithm>
#include <bit>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "../../include/yams/vector/sqlite_vec_backend.h"

namespace {

using Clock = std::chrono::steady_clock;
using yams::vector::SqliteVecBackend;
using yams::vector::VectorRecord;
using yams::vector::VectorSearchEngine;

struct Config {
    std::filesystem::path dbPath;
    std::size_t dim{1024};
    std::size_t vectors{10000};
    std::size_t batchSize{512};
    bool reuse{false};
    bool dirtyCheck{false};
};

struct FixtureMetrics {
    double insertMs{0.0};
    double buildMs{0.0};
    double persistMs{0.0};
};

struct ReaderMetrics {
    double initializeMs{0.0};
    double prepareMs{0.0};
    bool reusable{false};
    double queryMs{0.0};
    std::size_t resultCount{0};
    std::uint64_t resultFingerprint{0};
};

struct DirtyRestartMetrics {
    bool reusableBeforeMutation{false};
    bool reusableAfterMutation{false};
    double insertMs{0.0};
    double staleInitializeMs{0.0};
    double staleProbeMs{0.0};
    bool staleSnapshotRejected{false};
    double rebuildPrepareMs{0.0};
    bool reusableAfterRebuild{false};
    double rebuildQueryMs{0.0};
    std::uint64_t rebuildFingerprint{0};
    double reloadInitializeMs{0.0};
    double reloadProbeMs{0.0};
    bool reloadReusable{false};
    double reloadQueryMs{0.0};
    std::uint64_t reloadFingerprint{0};
};

template <typename Integer> bool parseInteger(std::string_view text, Integer& value) {
    const char* begin = text.data();
    const char* end = begin + text.size();
    const auto [parsedEnd, error] = std::from_chars(begin, end, value);
    return error == std::errc{} && parsedEnd == end;
}

bool pathWithin(const std::filesystem::path& candidate, const std::filesystem::path& root) {
    const auto [rootEnd, _] =
        std::mismatch(root.begin(), root.end(), candidate.begin(), candidate.end());
    return rootEnd == root.end();
}

Config parseArgs(int argc, char** argv) {
    Config config;
    const auto stamp = Clock::now().time_since_epoch().count();
    config.dbPath = std::filesystem::temp_directory_path() /
                    ("yams-vector-warm-start-" + std::to_string(stamp)) / "vectors.db";

    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        const auto requireValue = [&](std::string_view option) -> std::string_view {
            if (i + 1 >= argc) {
                throw std::runtime_error("missing value for " + std::string(option));
            }
            return argv[++i];
        };
        if (arg == "--path") {
            config.dbPath = requireValue(arg);
        } else if (arg == "--dim") {
            if (!parseInteger(requireValue(arg), config.dim) || config.dim == 0) {
                throw std::runtime_error("invalid --dim");
            }
        } else if (arg == "--vectors") {
            if (!parseInteger(requireValue(arg), config.vectors) || config.vectors == 0) {
                throw std::runtime_error("invalid --vectors");
            }
        } else if (arg == "--batch-size") {
            if (!parseInteger(requireValue(arg), config.batchSize) || config.batchSize == 0) {
                throw std::runtime_error("invalid --batch-size");
            }
        } else if (arg == "--reuse") {
            config.reuse = true;
        } else if (arg == "--dirty-check") {
            config.dirtyCheck = true;
        } else {
            throw std::runtime_error("unknown argument: " + std::string(arg));
        }
    }

    const auto candidate = std::filesystem::weakly_canonical(config.dbPath);
    const auto systemTemp =
        std::filesystem::weakly_canonical(std::filesystem::temp_directory_path());
    bool isolated = pathWithin(candidate, systemTemp);
#ifndef _WIN32
    isolated = isolated || pathWithin(candidate, std::filesystem::weakly_canonical("/tmp"));
#endif
    if (!isolated) {
        throw std::runtime_error("--path must remain inside an isolated temporary directory");
    }
    config.dbPath = candidate;
    return config;
}

std::size_t chooseSubquantizers(std::size_t dim) {
    for (std::size_t candidate = std::min<std::size_t>(32, dim); candidate > 0; --candidate) {
        if (dim % candidate == 0) {
            return candidate;
        }
    }
    return 1;
}

SqliteVecBackend::Config backendConfig(const Config& config) {
    SqliteVecBackend::Config backend;
    backend.embedding_dim = config.dim;
    backend.search_engine = VectorSearchEngine::SimeonPqAdc;
    backend.simeon_pq_subquantizers = chooseSubquantizers(config.dim);
    backend.simeon_pq_centroids = 256;
    backend.simeon_pq_train_limit = 4096;
    backend.simeon_pq_rerank_factor = 2;
    return backend;
}

std::vector<float> makeVector(std::size_t index, std::size_t dim) {
    std::vector<float> vector(dim);
    std::uint64_t state = 0x9E3779B97F4A7C15ULL ^ (index + 1);
    double squaredNorm = 0.0;
    for (auto& value : vector) {
        state ^= state >> 12U;
        state ^= state << 25U;
        state ^= state >> 27U;
        const auto sample = state * 0x2545F4914F6CDD1DULL;
        const auto mantissa = static_cast<std::uint32_t>(sample >> 40U);
        value = static_cast<float>(mantissa) / static_cast<float>(1U << 23U) - 1.0F;
        squaredNorm += static_cast<double>(value) * static_cast<double>(value);
    }
    const float inverseNorm = 1.0F / static_cast<float>(std::sqrt(squaredNorm));
    for (auto& value : vector) {
        value *= inverseNorm;
    }
    return vector;
}

VectorRecord makeRecord(std::size_t index, std::size_t dim) {
    VectorRecord record;
    record.chunk_id = "warm_chunk_" + std::to_string(index);
    record.document_hash = "warm_doc_" + std::to_string(index / 4);
    record.embedding = makeVector(index, dim);
    record.embedding_dim = dim;
    record.content = "warm-start benchmark " + std::to_string(index);
    return record;
}

double elapsedMs(Clock::time_point start) {
    return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}

std::uint64_t resultFingerprint(const std::vector<VectorRecord>& records) {
    std::uint64_t hash = 1469598103934665603ULL;
    const auto mix = [&](std::uint8_t byte) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    };
    for (const auto& record : records) {
        for (const unsigned char byte : record.chunk_id) {
            mix(byte);
        }
        const auto scoreBits = std::bit_cast<std::uint32_t>(record.relevance_score);
        for (unsigned shift = 0; shift < 32; shift += 8) {
            mix(static_cast<std::uint8_t>(scoreBits >> shift));
        }
    }
    return hash;
}

FixtureMetrics createFixture(const Config& config) {
    if (std::filesystem::exists(config.dbPath)) {
        throw std::runtime_error("fixture already exists; pass --reuse or choose another --path");
    }
    if (!config.dbPath.parent_path().empty()) {
        std::filesystem::create_directories(config.dbPath.parent_path());
    }

    SqliteVecBackend backend(backendConfig(config));
    auto initialized = backend.initialize(config.dbPath.string());
    if (!initialized) {
        throw std::runtime_error("initialize failed: " + initialized.error().message);
    }
    auto tables = backend.createTables(config.dim);
    if (!tables) {
        throw std::runtime_error("createTables failed: " + tables.error().message);
    }

    const auto insertStart = Clock::now();
    for (std::size_t offset = 0; offset < config.vectors; offset += config.batchSize) {
        const auto count = std::min(config.batchSize, config.vectors - offset);
        std::vector<VectorRecord> batch;
        batch.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            batch.push_back(makeRecord(offset + i, config.dim));
        }
        auto inserted = backend.insertVectorsBatch(batch);
        if (!inserted) {
            throw std::runtime_error("insertVectorsBatch failed: " + inserted.error().message);
        }
    }
    const double insertMs = elapsedMs(insertStart);

    const auto buildStart = Clock::now();
    auto built = backend.buildIndex();
    if (!built) {
        throw std::runtime_error("buildIndex failed: " + built.error().message);
    }
    const double buildMs = elapsedMs(buildStart);

    const auto persistStart = Clock::now();
    auto persisted = backend.persistIndex();
    if (!persisted) {
        throw std::runtime_error("persistIndex failed: " + persisted.error().message);
    }
    const double persistMs = elapsedMs(persistStart);
    backend.close();

    return {.insertMs = insertMs, .buildMs = buildMs, .persistMs = persistMs};
}

ReaderMetrics measureReader(const Config& config, bool probeFirst) {
    SqliteVecBackend backend(backendConfig(config));
    const auto initializeStart = Clock::now();
    auto initialized = backend.initialize(config.dbPath.string());
    if (!initialized) {
        throw std::runtime_error("reader initialize failed: " + initialized.error().message);
    }
    const double initializeMs = elapsedMs(initializeStart);

    bool reusable = false;
    double prepareMs = 0.0;
    const auto prepareStart = Clock::now();
    if (probeFirst) {
        auto probe = backend.hasReusablePersistedSearchIndex();
        if (!probe) {
            throw std::runtime_error("persisted-index probe failed: " + probe.error().message);
        }
        reusable = probe.value();
    } else {
        auto prepared = backend.prepareSearchIndex();
        if (!prepared) {
            throw std::runtime_error("prepareSearchIndex failed: " + prepared.error().message);
        }
    }
    prepareMs = elapsedMs(prepareStart);
    if (!probeFirst) {
        auto probe = backend.hasReusablePersistedSearchIndex();
        if (!probe) {
            throw std::runtime_error("post-prepare persisted-index probe failed: " +
                                     probe.error().message);
        }
        reusable = probe.value();
    }

    const auto query = makeVector(0, config.dim);
    const auto queryStart = Clock::now();
    auto results = backend.searchSimilar(query, 10, -1.0F);
    if (!results) {
        throw std::runtime_error("search failed: " + results.error().message);
    }
    const double queryMs = elapsedMs(queryStart);
    const auto fingerprint = resultFingerprint(results.value());
    const auto resultCount = results.value().size();
    backend.close();

    return {.initializeMs = initializeMs,
            .prepareMs = prepareMs,
            .reusable = reusable,
            .queryMs = queryMs,
            .resultCount = resultCount,
            .resultFingerprint = fingerprint};
}

DirtyRestartMetrics measureDirtyRestart(const Config& config) {
    DirtyRestartMetrics metrics;
    const auto dirtyRecord = makeRecord(config.vectors, config.dim);
    const auto dirtyQuery = dirtyRecord.embedding;

    {
        SqliteVecBackend mutator(backendConfig(config));
        auto initialized = mutator.initialize(config.dbPath.string());
        if (!initialized) {
            throw std::runtime_error("dirty mutator initialize failed: " +
                                     initialized.error().message);
        }
        auto reusableBefore = mutator.hasReusablePersistedSearchIndex();
        if (!reusableBefore) {
            throw std::runtime_error("pre-mutation persisted-index probe failed: " +
                                     reusableBefore.error().message);
        }
        metrics.reusableBeforeMutation = reusableBefore.value();

        const auto insertStart = Clock::now();
        auto inserted = mutator.insertVector(dirtyRecord);
        if (!inserted) {
            throw std::runtime_error("dirty mutation insert failed: " + inserted.error().message);
        }
        metrics.insertMs = elapsedMs(insertStart);

        auto reusableAfter = mutator.hasReusablePersistedSearchIndex();
        if (!reusableAfter) {
            throw std::runtime_error("post-mutation persisted-index probe failed: " +
                                     reusableAfter.error().message);
        }
        metrics.reusableAfterMutation = reusableAfter.value();
        mutator.close();
    }

    {
        SqliteVecBackend rebuilder(backendConfig(config));
        const auto initializeStart = Clock::now();
        auto initialized = rebuilder.initialize(config.dbPath.string());
        if (!initialized) {
            throw std::runtime_error("dirty rebuilder initialize failed: " +
                                     initialized.error().message);
        }
        metrics.staleInitializeMs = elapsedMs(initializeStart);

        const auto probeStart = Clock::now();
        auto staleProbe = rebuilder.hasReusablePersistedSearchIndex();
        if (!staleProbe) {
            throw std::runtime_error("stale persisted-index probe failed: " +
                                     staleProbe.error().message);
        }
        metrics.staleProbeMs = elapsedMs(probeStart);
        metrics.staleSnapshotRejected = !staleProbe.value();

        const auto prepareStart = Clock::now();
        auto prepared = rebuilder.prepareSearchIndex();
        if (!prepared) {
            throw std::runtime_error("dirty rebuild prepare failed: " + prepared.error().message);
        }
        metrics.rebuildPrepareMs = elapsedMs(prepareStart);

        auto reusableAfterRebuild = rebuilder.hasReusablePersistedSearchIndex();
        if (!reusableAfterRebuild) {
            throw std::runtime_error("post-rebuild persisted-index probe failed: " +
                                     reusableAfterRebuild.error().message);
        }
        metrics.reusableAfterRebuild = reusableAfterRebuild.value();

        const auto queryStart = Clock::now();
        auto results = rebuilder.searchSimilar(dirtyQuery, 10, -1.0F);
        if (!results) {
            throw std::runtime_error("post-rebuild search failed: " + results.error().message);
        }
        metrics.rebuildQueryMs = elapsedMs(queryStart);
        metrics.rebuildFingerprint = resultFingerprint(results.value());
        rebuilder.close();
    }

    {
        SqliteVecBackend reloader(backendConfig(config));
        const auto initializeStart = Clock::now();
        auto initialized = reloader.initialize(config.dbPath.string());
        if (!initialized) {
            throw std::runtime_error("dirty reload initialize failed: " +
                                     initialized.error().message);
        }
        metrics.reloadInitializeMs = elapsedMs(initializeStart);

        const auto probeStart = Clock::now();
        auto reusable = reloader.hasReusablePersistedSearchIndex();
        if (!reusable) {
            throw std::runtime_error("dirty reload persisted-index probe failed: " +
                                     reusable.error().message);
        }
        metrics.reloadProbeMs = elapsedMs(probeStart);
        metrics.reloadReusable = reusable.value();

        const auto queryStart = Clock::now();
        auto results = reloader.searchSimilar(dirtyQuery, 10, -1.0F);
        if (!results) {
            throw std::runtime_error("dirty reload search failed: " + results.error().message);
        }
        metrics.reloadQueryMs = elapsedMs(queryStart);
        metrics.reloadFingerprint = resultFingerprint(results.value());
        reloader.close();
    }

    return metrics;
}

} // namespace

int main(int argc, char** argv) {
    try {
        const Config config = parseArgs(argc, argv);
        FixtureMetrics fixture;
        if (!config.reuse) {
            fixture = createFixture(config);
        }
        const auto probeReader = measureReader(config, true);
        const auto prepareReader = measureReader(config, false);
        const bool fingerprintsEqual =
            probeReader.resultFingerprint == prepareReader.resultFingerprint;
        DirtyRestartMetrics dirty;
        if (config.dirtyCheck) {
            dirty = measureDirtyRestart(config);
        }
        const bool dirtyCheckPassed =
            !config.dirtyCheck ||
            (dirty.reusableBeforeMutation && !dirty.reusableAfterMutation &&
             dirty.staleSnapshotRejected && dirty.reusableAfterRebuild && dirty.reloadReusable &&
             dirty.rebuildFingerprint == dirty.reloadFingerprint);

        std::cout
            << std::fixed << std::setprecision(3) << '{'
            << "\"path\":" << std::quoted(config.dbPath.string()) << ",\"dim\":" << config.dim
            << ",\"vectors\":" << config.vectors << ",\"batch_size\":" << config.batchSize
            << ",\"fixture_created\":" << (config.reuse ? "false" : "true") << ",\"fixture\":{"
            << "\"insert_ms\":" << fixture.insertMs << ",\"build_ms\":" << fixture.buildMs
            << ",\"persist_ms\":" << fixture.persistMs << "},\"probe_reader\":{"
            << "\"initialize_ms\":" << probeReader.initializeMs
            << ",\"probe_ms\":" << probeReader.prepareMs
            << ",\"reusable\":" << (probeReader.reusable ? "true" : "false")
            << ",\"query_ms\":" << probeReader.queryMs
            << ",\"result_count\":" << probeReader.resultCount
            << ",\"result_fingerprint\":" << probeReader.resultFingerprint
            << "},\"prepare_reader\":{"
            << "\"initialize_ms\":" << prepareReader.initializeMs
            << ",\"prepare_ms\":" << prepareReader.prepareMs
            << ",\"reusable\":" << (prepareReader.reusable ? "true" : "false")
            << ",\"query_ms\":" << prepareReader.queryMs
            << ",\"result_count\":" << prepareReader.resultCount
            << ",\"result_fingerprint\":" << prepareReader.resultFingerprint
            << "},\"fingerprints_equal\":" << (fingerprintsEqual ? "true" : "false")
            << ",\"dirty_check_requested\":" << (config.dirtyCheck ? "true" : "false")
            << ",\"dirty_check_passed\":" << (dirtyCheckPassed ? "true" : "false")
            << ",\"dirty_check\":{"
            << "\"reusable_before_mutation\":" << (dirty.reusableBeforeMutation ? "true" : "false")
            << ",\"reusable_after_mutation\":" << (dirty.reusableAfterMutation ? "true" : "false")
            << ",\"insert_ms\":" << dirty.insertMs
            << ",\"stale_initialize_ms\":" << dirty.staleInitializeMs
            << ",\"stale_probe_ms\":" << dirty.staleProbeMs
            << ",\"stale_snapshot_rejected\":" << (dirty.staleSnapshotRejected ? "true" : "false")
            << ",\"rebuild_prepare_ms\":" << dirty.rebuildPrepareMs
            << ",\"reusable_after_rebuild\":" << (dirty.reusableAfterRebuild ? "true" : "false")
            << ",\"rebuild_query_ms\":" << dirty.rebuildQueryMs
            << ",\"rebuild_fingerprint\":" << dirty.rebuildFingerprint
            << ",\"reload_initialize_ms\":" << dirty.reloadInitializeMs
            << ",\"reload_probe_ms\":" << dirty.reloadProbeMs
            << ",\"reload_reusable\":" << (dirty.reloadReusable ? "true" : "false")
            << ",\"reload_query_ms\":" << dirty.reloadQueryMs
            << ",\"reload_fingerprint\":" << dirty.reloadFingerprint << "}}\n";
        return fingerprintsEqual && dirtyCheckPassed ? 0 : 2;
    } catch (const std::exception& error) {
        std::cerr << "vector persistence warm-start benchmark failed: " << error.what() << '\n';
        return 1;
    }
}
