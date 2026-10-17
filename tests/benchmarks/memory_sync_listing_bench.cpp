// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
//
// Isolated listing-cost benchmark for memory-sync backfill.
//
// Shadow's daemon profile showed one core saturated with 87.9% system time in
// MemorySyncLoop::sync / FilesystemBackend::listPage. The mechanism is repeated
// complete op-store enumeration: a write invalidates the completed-scan stamp, so a
// publish followed by syncFully walks the whole tree once per 512-record page.
//
// This harness uses production APIs only, so the identical source builds against
// both arms of the A/B and no measurement seam has to be ported into the baseline.
// Enumeration counts are established separately by the unit suite; this binary
// reports the cost those counts produce.
//
// One scenario per process: the driver spawns a fresh process per repeat so no cache
// state leaks between repeats or arms.
//
// Scenarios:
//   cold    first sweep over a pre-seeded store (no cached listing)
//   warm    second sweep with no intervening write (idle reuse path)
//   active  warm sweep, then --publishes publish+syncFully cycles
//
// Reported: wall/user/system microseconds and peak RSS. Timings are comparable only
// within one build type, machine and filesystem.

#include <yams/crypto/hasher.h>
#include <yams/memory_sync/memory_sync.h>
#include <yams/memory_sync/version_vector.h>
#include <yams/storage/storage_backend.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <time.h>
#include <sys/resource.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace yams::memory_sync;

struct Options {
    std::size_t records{1025};
    std::size_t publishes{2};
    std::string scenario{"active"};
    std::string label{"unlabeled"};
    std::optional<std::filesystem::path> dir;
    std::optional<std::filesystem::path> json;
    bool seedOnly{false};
    bool reuse{false};
};

std::vector<std::byte> bytesOf(std::string_view text) {
    std::vector<std::byte> out(text.size());
    std::memcpy(out.data(), text.data(), text.size());
    return out;
}

std::string digest(std::span<const std::byte> data) {
    yams::crypto::SHA256Hasher hasher;
    hasher.init();
    hasher.update(data);
    return hasher.finalize();
}

std::uint64_t nowMicros(clockid_t clock) {
    timespec ts{};
    clock_gettime(clock, &ts);
    return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000ULL +
           static_cast<std::uint64_t>(ts.tv_nsec) / 1'000ULL;
}

std::uint64_t userMicros(const rusage& usage) {
    return static_cast<std::uint64_t>(usage.ru_utime.tv_sec) * 1'000'000ULL +
           static_cast<std::uint64_t>(usage.ru_utime.tv_usec);
}

std::uint64_t systemMicros(const rusage& usage) {
    return static_cast<std::uint64_t>(usage.ru_stime.tv_sec) * 1'000'000ULL +
           static_cast<std::uint64_t>(usage.ru_stime.tv_usec);
}

std::uint64_t peakRssBytes(const rusage& usage) {
#ifdef __APPLE__
    return static_cast<std::uint64_t>(usage.ru_maxrss);
#else
    return static_cast<std::uint64_t>(usage.ru_maxrss) * 1024ULL;
#endif
}

bool parse(int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg{argv[i]};
        const auto takeValue = [&](std::string_view name) -> std::optional<std::string_view> {
            if (i + 1 >= argc) {
                std::cerr << "missing value for " << name << '\n';
                return std::nullopt;
            }
            return std::string_view{argv[++i]};
        };
        if (arg == "--records") {
            const auto value = takeValue(arg);
            if (!value) {
                return false;
            }
            options.records = static_cast<std::size_t>(std::stoull(std::string(*value)));
        } else if (arg == "--publishes") {
            const auto value = takeValue(arg);
            if (!value) {
                return false;
            }
            options.publishes = static_cast<std::size_t>(std::stoull(std::string(*value)));
        } else if (arg == "--scenario") {
            const auto value = takeValue(arg);
            if (!value) {
                return false;
            }
            options.scenario = std::string(*value);
        } else if (arg == "--label") {
            const auto value = takeValue(arg);
            if (!value) {
                return false;
            }
            options.label = std::string(*value);
        } else if (arg == "--dir") {
            const auto value = takeValue(arg);
            if (!value) {
                return false;
            }
            options.dir = std::filesystem::path{std::string(*value)};
        } else if (arg == "--json") {
            const auto value = takeValue(arg);
            if (!value) {
                return false;
            }
            options.json = std::filesystem::path{std::string(*value)};
        } else if (arg == "--seed-only") {
            options.seedOnly = true;
        } else if (arg == "--reuse") {
            options.reuse = true;
        } else if (arg == "--help") {
            std::cout << "usage: memory_sync_listing_bench --records N [--publishes K]\n"
                         "       --scenario cold|warm|active --dir DIR [--reuse]\n"
                         "       [--seed-only] [--label ARM] [--json OUT]\n";
            return false;
        } else {
            std::cerr << "unknown argument: " << arg << '\n';
            return false;
        }
    }
    if (!options.dir) {
        options.dir = std::filesystem::temp_directory_path() / "yams-memory-sync-listing-bench";
    }
    return options.scenario == "cold" || options.scenario == "warm" || options.scenario == "active";
}

// Seed the store with N well-formed bound envelopes under index/<logical>/<digest>.
// The shape matches a real op store: one blob record plus one envelope per key.
bool seedCorpus(const std::filesystem::path& dir, std::size_t records) {
    yams::storage::FilesystemBackend backend;
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = dir;
    if (auto initialized = backend.initialize(config); !initialized) {
        std::cerr << "backend init failed: " << initialized.error().message << '\n';
        return false;
    }
    const auto payload = bytesOf("seed payload for listing-cost benchmark");
    const auto payloadHash = digest(payload);
    if (auto stored = backend.store("blob/" + payloadHash, payload); !stored) {
        std::cerr << "blob seed failed: " << stored.error().message << '\n';
        return false;
    }
    for (std::size_t i = 0; i < records; ++i) {
        MemoryIndexRecord record;
        record.entryHash = payloadHash;
        record.ts = {100, 0};
        record.origin = "seed";
        record.vv.observe(record.origin, i + 1);
        record.corpusId = "local-test-corpus";
        record.corpusEpoch = 1;
        record.operationId = makeOperationId(record.origin, i + 1);
        record.logicalKey = "seed/" + std::to_string(10'000 + i);
        record.recordKind = std::string(kMemoryValueRecordKind);
        const auto encoded = bytesOf(nlohmann::json(record).dump());
        const auto key = "index/" + record.logicalKey + "/" + digest(encoded);
        if (auto stored = backend.store(key, encoded); !stored) {
            std::cerr << "envelope seed failed at " << i << ": " << stored.error().message << '\n';
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

    const auto dir = *options.dir;
    std::filesystem::create_directories(dir);
    const auto marker = dir / "bench-seeded.count";
    std::size_t seededRecords = 0;
    if (std::filesystem::exists(marker)) {
        std::ifstream in{marker};
        in >> seededRecords;
    }
    if (!std::filesystem::exists(marker) || seededRecords != options.records) {
        if (!seedCorpus(dir, options.records)) {
            return 3;
        }
        std::ofstream{marker} << options.records << '\n';
    }
    if (options.seedOnly) {
        nlohmann::json seededJson{{"label", options.label},
                                  {"records", options.records},
                                  {"seeded", true},
                                  {"dir", dir.string()}};
        std::cout << seededJson.dump() << '\n';
        return 0;
    }

    yams::storage::FilesystemBackend backend;
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = dir;
    if (auto initialized = backend.initialize(config); !initialized) {
        std::cerr << "backend init failed: " << initialized.error().message << '\n';
        return 4;
    }

    MemorySyncLoop loop{backend,
                        "bench-reader",
                        "local-test-corpus",
                        1,
                        false,
                        {},
                        MemorySyncControl{.exclusiveBackend = true}};

    std::size_t reconciled = 0;
    std::uint64_t wall = 0;
    std::uint64_t user = 0;
    std::uint64_t sys = 0;
    rusage after{};

    if (options.scenario == "cold") {
        const auto wallStart = nowMicros(CLOCK_MONOTONIC);
        rusage before{};
        [[maybe_unused]] const auto beforeStatus = getrusage(RUSAGE_SELF, &before);
        const auto synced = loop.syncFully();
        if (!synced) {
            std::cerr << "syncFully failed: " << synced.error().message << '\n';
            return 5;
        }
        const auto wallEnd = nowMicros(CLOCK_MONOTONIC);
        [[maybe_unused]] const auto afterStatus = getrusage(RUSAGE_SELF, &after);
        reconciled = synced.value().size();
        wall = wallEnd - wallStart;
        user = userMicros(after) - userMicros(before);
        sys = systemMicros(after) - systemMicros(before);
    } else {
        const auto warm = loop.syncFully();
        if (!warm) {
            std::cerr << "warm syncFully failed: " << warm.error().message << '\n';
            return 5;
        }
        const auto wallStart = nowMicros(CLOCK_MONOTONIC);
        rusage before{};
        [[maybe_unused]] const auto beforeStatus = getrusage(RUSAGE_SELF, &before);

        if (options.scenario == "active") {
            for (std::size_t i = 0; i < options.publishes; ++i) {
                const auto key = "active/" + std::to_string(i);
                if (auto published = loop.publish(key, bytesOf("live value")); !published) {
                    std::cerr << "publish failed: " << published.error().message << '\n';
                    return 6;
                }
                const auto reconciledRun = loop.syncFully();
                if (!reconciledRun) {
                    std::cerr << "syncFully failed: " << reconciledRun.error().message << '\n';
                    return 5;
                }
                reconciled = reconciledRun.value().size();
            }
        } else {
            const auto synced = loop.syncFully();
            if (!synced) {
                std::cerr << "syncFully failed: " << synced.error().message << '\n';
                return 5;
            }
            reconciled = synced.value().size();
        }

        const auto wallEnd = nowMicros(CLOCK_MONOTONIC);
        [[maybe_unused]] const auto afterStatus = getrusage(RUSAGE_SELF, &after);
        wall = wallEnd - wallStart;
        user = userMicros(after) - userMicros(before);
        sys = systemMicros(after) - systemMicros(before);
    }

    nlohmann::json result{{"label", options.label},
                          {"scenario", options.scenario},
                          {"records", options.records},
                          {"publishes", options.publishes},
                          {"dir", dir.string()},
                          {"reconciled", reconciled},
                          {"wall_us", wall},
                          {"user_us", user},
                          {"system_us", sys},
                          {"cpu_us", user + sys},
                          {"peak_rss_bytes", peakRssBytes(after)}};
    std::cout << result.dump() << '\n';
    if (options.json) {
        std::ofstream out{*options.json};
        out << result.dump(2) << '\n';
    }
    return 0;
}
