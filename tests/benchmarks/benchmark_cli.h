#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace yams::benchmark {

struct BenchmarkCliConfig {
    // Core execution controls
    std::size_t warmupIterations{3};
    std::size_t iterations{10};
    bool verbose{true};
    bool trackMemory{true};

    // Selection
    std::vector<std::string> filters;

    // Output
    std::filesystem::path outDir{"bench_results"};
    std::optional<std::filesystem::path> outputFile;

    // Archiving
    bool archive{true};
    std::filesystem::path archiveDir{"bench_results/archive"};

    // RNG seeding (benchmarks may opt-in)
    std::optional<std::uint64_t> seed;
};

inline void printBenchmarkUsage(std::string_view exeName) {
    // Keep this minimal; CLI output is consumed by humans.
    std::cout << "Usage: " << exeName << " [options]\n\n"
              << "Options:\n"
              << "  --warmup N            Warmup iterations (default: 3)\n"
              << "  --iterations N        Benchmark iterations (default: 10)\n"
              << "  --quiet               Disable per-benchmark progress output\n"
              << "  --verbose             Enable per-benchmark progress output (default)\n"
              << "  --no-memory           Disable memory tracking\n"
              << "  --filter PATTERN       Run only benchmarks whose name contains PATTERN\n"
              << "                         (repeatable)\n"
              << "  --out-dir DIR          Output directory (default: bench_results)\n"
              << "  --output FILE          Append JSONL results to FILE\n"
              << "  --seed N               Seed for RNG (optional; benchmark-dependent)\n"
              << "  --archive              Archive results to bench_results/archive (default)\n"
              << "  --no-archive           Disable archiving\n"
              << "  --archive-dir DIR      Archive directory\n"
              << "  --help                 Show this help\n";
}

inline BenchmarkCliConfig parseBenchmarkArgs(int argc, char** argv) {
    BenchmarkCliConfig cfg;

    auto toSizeT = [](const char* s) -> std::size_t {
        return static_cast<std::size_t>(std::stoull(std::string(s)));
    };

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i] ? std::string(argv[i]) : std::string();
        if (arg == "--help" || arg == "-h") {
            printBenchmarkUsage(argv[0] ? argv[0] : "benchmark");
            std::exit(0);
        }
        if (arg == "--quiet") {
            cfg.verbose = false;
            continue;
        }
        if (arg == "--verbose") {
            cfg.verbose = true;
            continue;
        }
        if (arg == "--no-memory") {
            cfg.trackMemory = false;
            continue;
        }
        if (arg == "--archive") {
            cfg.archive = true;
            continue;
        }
        if (arg == "--no-archive") {
            cfg.archive = false;
            continue;
        }

        auto needValue = [&](const char* flag) -> const char* {
            if (i + 1 >= argc || !argv[i + 1]) {
                throw std::runtime_error(std::string("Missing value for ") + flag);
            }
            return argv[++i];
        };

        if (arg == "--warmup") {
            cfg.warmupIterations = toSizeT(needValue("--warmup"));
        } else if (arg == "--iterations") {
            cfg.iterations = toSizeT(needValue("--iterations"));
        } else if (arg == "--filter") {
            cfg.filters.emplace_back(needValue("--filter"));
        } else if (arg == "--out-dir") {
            cfg.outDir = std::filesystem::path(needValue("--out-dir"));
        } else if (arg == "--output") {
            cfg.outputFile = std::filesystem::path(needValue("--output"));
        } else if (arg == "--archive-dir") {
            cfg.archiveDir = std::filesystem::path(needValue("--archive-dir"));
        } else if (arg == "--seed") {
            cfg.seed = static_cast<std::uint64_t>(std::stoull(std::string(needValue("--seed"))));
        } else {
            // Unknown args are ignored for forward-compat across multiple benchmark binaries.
        }
    }

    return cfg;
}

inline bool matchesAnyFilter(std::string_view name, const std::vector<std::string>& filters) {
    if (filters.empty()) {
        return true;
    }
    for (const auto& f : filters) {
        if (!f.empty() && name.find(f) != std::string_view::npos) {
            return true;
        }
    }
    return false;
}

inline std::string iso8601UtcNow() {
    using clock = std::chrono::system_clock;
    const auto now = clock::now();
    const auto t = clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H-%M-%SZ", &tm);
    return std::string(buf);
}

inline std::optional<std::string> tryReadTextFile(const std::filesystem::path& p) {
    std::ifstream in(p);
    if (!in) {
        return std::nullopt;
    }
    std::string s;
    std::getline(in, s);
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) {
        s.pop_back();
    }
    return s;
}

inline std::optional<std::filesystem::path>
archiveJsonFileBestEffort(const std::filesystem::path& src,
                          const std::filesystem::path& archiveRoot, std::string_view suiteName) {
    std::error_code ec;
    if (!std::filesystem::exists(src)) {
        return std::nullopt;
    }

    const std::string stamp = iso8601UtcNow();
    std::filesystem::path dstDir = archiveRoot / std::string(suiteName) / stamp;
    std::filesystem::create_directories(dstDir, ec);

    std::filesystem::path dst = dstDir / src.filename();
    std::filesystem::copy_file(src, dst, std::filesystem::copy_options::overwrite_existing, ec);

    return dstDir;
}

} // namespace yams::benchmark
