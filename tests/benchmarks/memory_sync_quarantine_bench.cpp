// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// Public-API microbenchmark for direct writer quarantine, not a daemon/startup benchmark.
// Usage: memory_sync_quarantine_bench EMPTY_DIR RECORDS IDENTITY_LIMIT ITERATIONS accept|reject
// Seeding and recovery are outside the per-call wall/CPU measurements. Process peak RSS
// includes them. Accepted calls use distinct reader checkpoints; rejected calls reuse the
// unchanged reader. Never compare those two paths as equivalent successful work.

#include <yams/crypto/hasher.h>
#include <yams/memory_sync/memory_sync.h>
#include <yams/storage/storage_backend.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <sys/resource.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
using namespace yams::memory_sync;

std::size_t count(std::string_view input, std::size_t maximum) {
    std::size_t consumed = 0;
    const auto value = std::stoull(std::string(input), &consumed);
    if (consumed != input.size() || value == 0 || value > maximum) {
        throw std::invalid_argument("count is outside benchmark bounds");
    }
    return static_cast<std::size_t>(value);
}

std::uint64_t micros(const timeval& value) {
    return static_cast<std::uint64_t>(value.tv_sec) * 1'000'000ULL + value.tv_usec;
}

rusage usage() {
    rusage result{};
    if (getrusage(RUSAGE_SELF, &result) != 0) {
        throw std::runtime_error("getrusage failed");
    }
    return result;
}

void seed(yams::storage::FilesystemBackend& backend, std::size_t records) {
    for (std::size_t i = 0; i < records; ++i) {
        MemoryIndexRecord record;
        record.ts = {100, 0};
        record.origin = "bench-writer";
        record.vv.observe(record.origin, i + 1);
        record.corpusId = "local-test-corpus";
        record.corpusEpoch = 1;
        record.operationId = makeOperationId(record.origin, i + 1);
        record.logicalKey = "seed/" + std::to_string(10'000 + i);
        record.recordKind = std::string(kMemoryTombstoneRecordKind);
        record.tombstonePayload = record.logicalKey;
        const auto json = nlohmann::json(record).dump();
        std::vector<std::byte> encoded(json.size());
        std::memcpy(encoded.data(), json.data(), json.size());
        yams::crypto::SHA256Hasher hasher;
        hasher.init();
        hasher.update(encoded);
        const auto key = "index/" + record.logicalKey + "/" + hasher.finalize();
        if (const auto stored = backend.store(key, encoded); !stored) {
            throw std::runtime_error("seed: " + stored.error().message);
        }
    }
}
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 6) {
            throw std::invalid_argument(
                "expected EMPTY_DIR RECORDS LIMIT ITERATIONS accept|reject");
        }
        const std::filesystem::path dir{argv[1]};
        const auto records = count(argv[2], 4096);
        MemorySyncLimits limits;
        limits.maxTrackedIdentities = count(argv[3], 8192);
        const auto iterations = count(argv[4], 1'000'000);
        const std::string_view outcome{argv[5]};
        if (outcome != "accept" && outcome != "reject") {
            throw std::invalid_argument("expected outcome must be accept or reject");
        }
        const bool expectAccepted = outcome == "accept";
        std::filesystem::create_directories(dir);
        if (!std::filesystem::is_empty(dir)) {
            throw std::invalid_argument("benchmark requires an empty directory");
        }
        spdlog::set_level(spdlog::level::off);
        yams::storage::FilesystemBackend backend;
        yams::storage::BackendConfig config;
        config.type = "filesystem";
        config.localPath = dir;
        if (const auto initialized = backend.initialize(config); !initialized) {
            throw std::runtime_error("initialize: " + initialized.error().message);
        }
        seed(backend, records);
        MemorySyncControl control;
        control.exclusiveBackend = true;
        std::unique_ptr<MemorySyncLoop> reader;
        std::vector<double> wallMicros;
        std::uint64_t userMicros = 0;
        std::uint64_t systemMicros = 0;
        wallMicros.reserve(iterations);
        for (std::size_t i = 0; i < iterations; ++i) {
            const auto readerId = "bench-reader-" + std::to_string(expectAccepted ? i : 0);
            if (!reader || expectAccepted) {
                reader = std::make_unique<MemorySyncLoop>(backend, readerId, "local-test-corpus", 1,
                                                          false, limits, control);
                if (const auto recovered = reader->syncFully(); !recovered) {
                    throw std::runtime_error("recovery: " + recovered.error().message);
                }
                if (reader->mergedRecordCount() != records) {
                    throw std::runtime_error("recovery did not retain every seeded winner");
                }
            }
            const auto generation = reader->durableQuarantineGeneration();
            const auto before = usage();
            const auto start = std::chrono::steady_clock::now();
            const auto quarantined =
                reader->quarantineWriter("bench-writer", readerId, "benchmark");
            const auto end = std::chrono::steady_clock::now();
            const auto after = usage();
            wallMicros.push_back(std::chrono::duration<double, std::micro>(end - start).count());
            userMicros += micros(after.ru_utime) - micros(before.ru_utime);
            systemMicros += micros(after.ru_stime) - micros(before.ru_stime);
            if (expectAccepted) {
                if (!quarantined || !quarantined.value() ||
                    !reader->writerQuarantined("bench-writer") ||
                    reader->mergedRecordCount() != 0) {
                    throw std::runtime_error("expected complete accepted quarantine");
                }
            } else if (quarantined ||
                       quarantined.error().code != yams::ErrorCode::ResourceExhausted ||
                       reader->writerQuarantined("bench-writer") ||
                       reader->durableQuarantineGeneration() != generation ||
                       reader->mergedRecordCount() != records) {
                throw std::runtime_error("expected unchanged resource-exhausted quarantine");
            }
        }
        auto rss = static_cast<std::uint64_t>(usage().ru_maxrss);
#ifndef __APPLE__
        rss *= 1024;
#endif
        std::cout << nlohmann::json{{"records", records},
                                    {"identity_limit", limits.maxTrackedIdentities},
                                    {"iterations", iterations},
                                    {"expected_outcome", outcome},
                                    {"wall_us", wallMicros},
                                    {"user_us", userMicros},
                                    {"system_us", systemMicros},
                                    {"process_peak_rss_bytes", rss}}
                         .dump()
                  << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "quarantine benchmark: " << error.what() << '\n';
        return 1;
    }
}
