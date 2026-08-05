#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace yams::benchmark {

struct BenchConfigDefaults {
    std::size_t warmupIterations{3};
    std::size_t iterations{10};
};

struct BenchConfig {
    std::string suiteName;

    // Core execution controls.
    std::size_t warmupIterations{3};
    std::size_t iterations{10};
    bool verbose{true};
    bool trackMemory{true};

    // Selection.
    std::vector<std::string> filters;
    std::vector<std::string> exactFilters;

    // Output and archiving.
    std::filesystem::path outDir;
    std::optional<std::filesystem::path> outputFile;
    bool archive{true};
    std::filesystem::path archiveDir;

    // RNG seeding (benchmarks may opt in).
    std::optional<std::uint64_t> seed;

    // Stable identity and field-level authority.
    std::string configHash;
    std::string generatedAtUtc;
    std::string executable;
    std::map<std::string, std::string> sources;
};

inline void printBenchmarkUsage(std::string_view exeName,
                                const BenchConfigDefaults& defaults = {}) {
    std::cout << "Usage: " << exeName << " [options]\n\n"
              << "Options:\n"
              << "  --warmup N            Warmup iterations (default: " << defaults.warmupIterations
              << ")\n"
              << "  --iterations N        Benchmark iterations (default: " << defaults.iterations
              << ")\n"
              << "  --quiet               Disable per-benchmark progress output\n"
              << "  --verbose             Enable per-benchmark progress output (default)\n"
              << "  --no-memory           Disable memory tracking\n"
              << "  --filter PATTERN       Run benchmarks containing PATTERN (repeatable)\n"
              << "  --exact-filter NAME    Run an exact benchmark NAME (repeatable)\n"
              << "  --out-dir DIR          Exact output directory override\n"
              << "  --output FILE          Append JSONL results to FILE\n"
              << "  --seed N               Seed for RNG (optional; benchmark-dependent)\n"
              << "  --archive              Archive results (default)\n"
              << "  --no-archive           Disable archiving\n"
              << "  --archive-dir DIR      Exact archive directory override\n"
              << "  --help                 Show this help\n\n"
              << "Default output: build/benchmarks/<suite>/<UTC timestamp>-<config hash>/\n";
}

inline std::string benchmarkUtcStamp() {
    using clock = std::chrono::system_clock;
    const auto now = clock::now();
    const auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(now);
    const auto micros =
        std::chrono::duration_cast<std::chrono::microseconds>(now - seconds).count();
    const auto time = clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &time);
#else
    gmtime_r(&time, &tm);
#endif
    std::ostringstream output;
    output << std::put_time(&tm, "%Y%m%dT%H%M%S") << std::setw(6) << std::setfill('0') << micros
           << 'Z';
    return output.str();
}

inline std::optional<std::filesystem::path> benchmarkRepositoryRoot(std::string_view executable) {
    std::error_code error;
    std::vector<std::filesystem::path> starts;
    if (auto current = std::filesystem::current_path(error); !error) {
        starts.push_back(current);
    }
    error.clear();
    if (auto executablePath = std::filesystem::absolute(std::filesystem::path{executable}, error);
        !error) {
        starts.push_back(executablePath.parent_path());
    }

    for (const auto& start : starts) {
        for (auto candidate = start; !candidate.empty(); candidate = candidate.parent_path()) {
            error.clear();
            if (std::filesystem::is_regular_file(candidate / "meson.build", error) && !error &&
                std::filesystem::is_directory(candidate / "tests" / "benchmarks", error) &&
                !error) {
                return candidate;
            }
            if (candidate == candidate.root_path()) {
                break;
            }
        }
    }
    return std::nullopt;
}

inline std::string benchmarkConfigHash(const BenchConfig& config) {
    std::ostringstream identity;
    const auto append = [&](std::string_view name, std::string_view value) {
        identity << name.size() << ':' << name << '=' << value.size() << ':' << value << ';';
    };
    append("suite", config.suiteName);
    append("warmup", std::to_string(config.warmupIterations));
    append("iterations", std::to_string(config.iterations));
    append("verbose", config.verbose ? "true" : "false");
    append("track_memory", config.trackMemory ? "true" : "false");
    append("archive", config.archive ? "true" : "false");
    append("seed", config.seed ? std::to_string(*config.seed) : "unset");
    for (const auto& filter : config.filters) {
        append("filter", filter);
    }
    for (const auto& filter : config.exactFilters) {
        append("exact_filter", filter);
    }
    if (config.sources.contains("out_dir") && config.sources.at("out_dir") == "cli:--out-dir") {
        append("out_dir", config.outDir.generic_string());
    }
    if (config.outputFile) {
        append("output_file", config.outputFile->generic_string());
    }
    if (config.sources.contains("archive_dir") &&
        config.sources.at("archive_dir") == "cli:--archive-dir") {
        append("archive_dir", config.archiveDir.generic_string());
    }

    std::uint64_t hash = 1469598103934665603ULL;
    for (const unsigned char byte : identity.str()) {
        hash ^= static_cast<std::uint64_t>(byte);
        hash *= 1099511628211ULL;
    }
    std::ostringstream encoded;
    encoded << std::hex << std::setw(16) << std::setfill('0') << hash;
    return encoded.str();
}

inline BenchConfig parseBenchConfig(int argc, char** argv, std::string_view suiteName,
                                    const BenchConfigDefaults& defaults = {}) {
    BenchConfig config;
    config.suiteName = std::string{suiteName};
    config.warmupIterations = defaults.warmupIterations;
    config.iterations = defaults.iterations;
    config.executable = argc > 0 && argv[0] ? argv[0] : "benchmark";
    config.sources = {{"warmup_iterations", "default:" + std::to_string(defaults.warmupIterations)},
                      {"iterations", "default:" + std::to_string(defaults.iterations)},
                      {"verbose", "default:true"},
                      {"track_memory", "default:true"},
                      {"filters", "default:all"},
                      {"exact_filters", "default:all"},
                      {"out_dir", "default:timestamp-config-hash"},
                      {"output_file", "default:run-dir"},
                      {"archive", "default:true"},
                      {"archive_dir", "default:run-dir/archive"},
                      {"seed", "default:unset"}};

    const auto toSize = [](const char* value) {
        return static_cast<std::size_t>(std::stoull(std::string{value}));
    };
    for (int index = 1; index < argc; ++index) {
        const std::string argument = argv[index] ? argv[index] : "";
        if (argument == "--help" || argument == "-h") {
            printBenchmarkUsage(config.executable, defaults);
            std::exit(0);
        }
        const auto needValue = [&](const char* flag) -> const char* {
            if (index + 1 >= argc || !argv[index + 1]) {
                throw std::runtime_error(std::string{"Missing value for "} + flag);
            }
            return argv[++index];
        };

        if (argument == "--quiet") {
            config.verbose = false;
            config.sources["verbose"] = "cli:--quiet";
        } else if (argument == "--verbose") {
            config.verbose = true;
            config.sources["verbose"] = "cli:--verbose";
        } else if (argument == "--no-memory") {
            config.trackMemory = false;
            config.sources["track_memory"] = "cli:--no-memory";
        } else if (argument == "--archive") {
            config.archive = true;
            config.sources["archive"] = "cli:--archive";
        } else if (argument == "--no-archive") {
            config.archive = false;
            config.sources["archive"] = "cli:--no-archive";
        } else if (argument == "--warmup") {
            config.warmupIterations = toSize(needValue("--warmup"));
            config.sources["warmup_iterations"] = "cli:--warmup";
        } else if (argument == "--iterations") {
            config.iterations = toSize(needValue("--iterations"));
            config.sources["iterations"] = "cli:--iterations";
        } else if (argument == "--filter") {
            config.filters.emplace_back(needValue("--filter"));
            config.sources["filters"] = "cli:--filter";
        } else if (argument == "--exact-filter") {
            config.exactFilters.emplace_back(needValue("--exact-filter"));
            config.sources["exact_filters"] = "cli:--exact-filter";
        } else if (argument == "--out-dir") {
            config.outDir = std::filesystem::path{needValue("--out-dir")};
            config.sources["out_dir"] = "cli:--out-dir";
        } else if (argument == "--output") {
            config.outputFile = std::filesystem::path{needValue("--output")};
            config.sources["output_file"] = "cli:--output";
        } else if (argument == "--archive-dir") {
            config.archiveDir = std::filesystem::path{needValue("--archive-dir")};
            config.sources["archive_dir"] = "cli:--archive-dir";
        } else if (argument == "--seed") {
            config.seed = std::stoull(std::string{needValue("--seed")});
            config.sources["seed"] = "cli:--seed";
        }
    }

    config.configHash = benchmarkConfigHash(config);
    config.generatedAtUtc = benchmarkUtcStamp();
    if (config.sources.at("out_dir") != "cli:--out-dir") {
        const auto repositoryRoot = benchmarkRepositoryRoot(config.executable);
        if (!repositoryRoot) {
            throw std::runtime_error(
                "Unable to locate the YAMS repository for benchmark output; use --out-dir");
        }
        config.outDir = *repositoryRoot / "build" / "benchmarks" / config.suiteName /
                        (config.generatedAtUtc + '-' + config.configHash);
    }
    if (config.sources.at("archive_dir") != "cli:--archive-dir") {
        config.archiveDir = config.outDir / "archive";
    }
    return config;
}

inline std::string benchmarkJsonEscape(std::string_view value) {
    std::ostringstream escaped;
    for (const unsigned char byte : value) {
        switch (byte) {
            case '\\':
                escaped << "\\\\";
                break;
            case '"':
                escaped << "\\\"";
                break;
            case '\n':
                escaped << "\\n";
                break;
            case '\r':
                escaped << "\\r";
                break;
            case '\t':
                escaped << "\\t";
                break;
            default:
                if (byte < 0x20) {
                    escaped << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                            << static_cast<unsigned int>(byte) << std::dec;
                } else {
                    escaped << static_cast<char>(byte);
                }
        }
    }
    return escaped.str();
}

inline bool prepareBenchmarkRun(BenchConfig& config) {
    std::error_code error;
    if (config.sources.at("out_dir") == "default:timestamp-config-hash") {
        std::filesystem::create_directories(config.outDir.parent_path(), error);
        if (error) {
            return false;
        }
        const auto base = config.outDir;
        bool created = false;
        for (std::uint32_t collision = 0; collision < 1000 && !created; ++collision) {
            error.clear();
            const auto candidate =
                collision == 0
                    ? base
                    : std::filesystem::path{base.string() + '-' + std::to_string(collision)};
            created = std::filesystem::create_directory(candidate, error);
            if (error) {
                return false;
            }
            if (created) {
                const auto outputFilename =
                    config.outputFile ? config.outputFile->filename() : std::filesystem::path{};
                config.outDir = candidate;
                if (config.sources.at("output_file") == "default:run-dir" &&
                    !outputFilename.empty()) {
                    config.outputFile = config.outDir / outputFilename;
                }
                if (config.sources.at("archive_dir") == "default:run-dir/archive") {
                    config.archiveDir = config.outDir / "archive";
                }
            }
        }
        if (!created) {
            return false;
        }
    } else {
        std::filesystem::create_directories(config.outDir, error);
        if (error) {
            return false;
        }
    }
    std::ofstream manifest{config.outDir / "run_manifest.json", std::ios::trunc};
    if (!manifest) {
        return false;
    }
    const auto quote = [](std::string_view value) {
        return '"' + benchmarkJsonEscape(value) + '"';
    };
    manifest << "{\n"
             << "  \"schema_version\": 1,\n"
             << "  \"config_hash\": " << quote(config.configHash) << ",\n"
             << "  \"effective\": {\n"
             << "    \"suite\": " << quote(config.suiteName) << ",\n"
             << "    \"warmup_iterations\": " << config.warmupIterations << ",\n"
             << "    \"iterations\": " << config.iterations << ",\n"
             << "    \"verbose\": " << (config.verbose ? "true" : "false") << ",\n"
             << "    \"track_memory\": " << (config.trackMemory ? "true" : "false") << ",\n"
             << "    \"filters\": [";
    for (std::size_t index = 0; index < config.filters.size(); ++index) {
        manifest << (index == 0 ? "" : ", ") << quote(config.filters[index]);
    }
    manifest << "],\n    \"exact_filters\": [";
    for (std::size_t index = 0; index < config.exactFilters.size(); ++index) {
        manifest << (index == 0 ? "" : ", ") << quote(config.exactFilters[index]);
    }
    manifest << "],\n"
             << "    \"out_dir\": " << quote(config.outDir.generic_string()) << ",\n"
             << "    \"output_file\": "
             << quote(config.outputFile ? config.outputFile->generic_string() : "") << ",\n"
             << "    \"archive\": " << (config.archive ? "true" : "false") << ",\n"
             << "    \"archive_dir\": " << quote(config.archiveDir.generic_string()) << ",\n"
             << "    \"seed\": ";
    if (config.seed) {
        manifest << *config.seed;
    } else {
        manifest << "null";
    }
    manifest << "\n  },\n  \"sources\": {\n";
    for (auto iterator = config.sources.begin(); iterator != config.sources.end(); ++iterator) {
        manifest << "    " << quote(iterator->first) << ": {\"source\": " << quote(iterator->second)
                 << "}";
        manifest << (std::next(iterator) == config.sources.end() ? "\n" : ",\n");
    }
    manifest << "  },\n"
             << "  \"provenance\": {\n"
             << "    \"generated_at_utc\": " << quote(config.generatedAtUtc) << ",\n"
             << "    \"executable\": " << quote(config.executable) << ",\n"
             << "    \"working_directory\": "
             << quote(std::filesystem::current_path(error).generic_string()) << "\n"
             << "  }\n}\n";
    return static_cast<bool>(manifest);
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

inline bool matchesAnyFilter(std::string_view name, const std::vector<std::string>& filters,
                             const std::vector<std::string>& exactFilters) {
    if (!exactFilters.empty()) {
        for (const auto& f : exactFilters) {
            if (!f.empty() && name == f) {
                return true;
            }
        }
    }

    if (!filters.empty()) {
        return matchesAnyFilter(name, filters);
    }

    return exactFilters.empty();
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
