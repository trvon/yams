#include <spdlog/spdlog.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include "../common/env_compat.h"
#include <yams/compat/unistd.h>
#include <yams/daemon/daemon.h>

namespace fs = std::filesystem;
using steady_clock_t = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace {

struct TimingSample {
    long long totalMs = 0;
    long long constructMs = 0;
    long long startMs = 0;
    long long startupMs = 0;
    long long holdMs = 0;
    long long stopMs = 0;
};

bool envIsOne(const char* name) {
    const char* value = std::getenv(name);
    return value && std::string_view(value) == "1";
}

int readEnvInt(const char* name, int fallback, int minValue, int maxValue) {
    const char* value = std::getenv(name);
    if (!value || !*value) {
        return fallback;
    }
    char* end = nullptr;
    errno = 0;
    long parsed = std::strtol(value, &end, 10);
    if (errno != 0 || end == value || (end && *end != '\0')) {
        return fallback;
    }
    if (parsed < static_cast<long>(minValue)) {
        return minValue;
    }
    if (parsed > static_cast<long>(maxValue)) {
        return maxValue;
    }
    return static_cast<int>(parsed);
}

std::optional<spdlog::level::level_enum> parseLogLevel(const std::string_view level) {
    if (level == "trace") {
        return spdlog::level::trace;
    }
    if (level == "debug") {
        return spdlog::level::debug;
    }
    if (level == "info") {
        return spdlog::level::info;
    }
    if (level == "warn") {
        return spdlog::level::warn;
    }
    if (level == "error") {
        return spdlog::level::err;
    }
    if (level == "critical") {
        return spdlog::level::critical;
    }
    if (level == "off") {
        return spdlog::level::off;
    }
    return std::nullopt;
}

double meanMs(const std::vector<long long>& values) {
    if (values.empty()) {
        return 0.0;
    }
    long long sum = 0;
    for (const long long value : values) {
        sum += value;
    }
    return static_cast<double>(sum) / static_cast<double>(values.size());
}

long long percentileMs(std::vector<long long> values, int p) {
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    const size_t idx = static_cast<size_t>((static_cast<double>(p) / 100.0) *
                                           static_cast<double>(values.size() - 1));
    return values[idx];
}

} // namespace

static bool isSocketPermissionDenied(const yams::Error& error) {
    const std::string_view message{error.message};
    return message.find("Operation not permitted") != std::string_view::npos ||
           message.find("Permission denied") != std::string_view::npos;
}

int main() {
    const char* enable = std::getenv("YAMS_ENABLE_DAEMON_BENCH");
    if (!enable || std::string_view(enable) != "1") {
        std::cout << "Skipping daemon warm latency bench (set YAMS_ENABLE_DAEMON_BENCH=1)\n";
        return 0; // treat skip as pass for Meson runner
    }

    const bool profileLoop = envIsOne("YAMS_DAEMON_BENCH_PROFILE_LOOP");
    const bool minThreads = envIsOne("YAMS_DAEMON_BENCH_MIN_THREADS");
    const int iterations =
        readEnvInt("YAMS_DAEMON_BENCH_ITERATIONS", profileLoop ? 200 : 1, 1, 10000);
    const int holdMs = readEnvInt("YAMS_DAEMON_BENCH_SLEEP_MS", profileLoop ? 0 : 50, 0, 5000);
    const int maxTotalMs = readEnvInt("YAMS_DAEMON_BENCH_MAX_TOTAL_MS", 5000, 100, 300000);

    if (const char* ll = std::getenv("YAMS_DAEMON_BENCH_LOG_LEVEL"); ll && *ll) {
        if (auto parsedLevel = parseLogLevel(ll); parsedLevel.has_value()) {
            spdlog::set_level(*parsedLevel);
        }
    } else {
        spdlog::set_level(spdlog::level::off);
    }

    if (minThreads) {
        if (const char* workThreads = std::getenv("YAMS_WORK_COORDINATOR_THREADS");
            !workThreads || !*workThreads) {
            ::setenv("YAMS_WORK_COORDINATOR_THREADS", "1", 1);
        }
        if (const char* ioThreads = std::getenv("YAMS_IO_THREADS"); !ioThreads || !*ioThreads) {
            ::setenv("YAMS_IO_THREADS", "1", 1);
        }
    }

    std::cout << "Daemon bench config: iterations=" << iterations << " hold_ms=" << holdMs
              << " max_total_ms=" << maxTotalMs;
    if (profileLoop) {
        std::cout << " profile_loop=1";
    }
    if (minThreads) {
        std::cout << " min_threads=1";
    }
    std::cout << "\n";

    ::setenv("YAMS_DB_OPEN_TIMEOUT_MS", "1000", 1);
    ::setenv("YAMS_DB_MIGRATE_TIMEOUT_MS", "1500", 1);
    ::setenv("YAMS_SEARCH_BUILD_TIMEOUT_MS", "1000", 1);
    ::setenv("YAMS_DISABLE_VECTORS", "1", 1);

    std::vector<TimingSample> samples;
    samples.reserve(static_cast<size_t>(iterations));

    for (int i = 0; i < iterations; ++i) {
        const auto nowTicks =
            static_cast<unsigned long>(steady_clock_t::now().time_since_epoch().count() & 0xfffff);
        const fs::path runtime_root =
            fs::temp_directory_path() / ("ydbench_" + std::to_string(::getpid()) + "_" +
                                         std::to_string(i) + "_" + std::to_string(nowTicks));

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = runtime_root / "data";
        cfg.socketPath = runtime_root / "sock";
        cfg.pidFile = runtime_root / "daemon.pid";
        cfg.logFile = runtime_root / "daemon.log";
        cfg.maxMemoryGb = 1;
        cfg.enableModelProvider = false;
        cfg.autoLoadPlugins = false;

        std::error_code se;
        fs::create_directories(cfg.dataDir, se);

        const auto totalStart = steady_clock_t::now();
        auto daemon = std::make_unique<yams::daemon::YamsDaemon>(cfg);
        const auto constructEnd = steady_clock_t::now();

        const auto startBegin = constructEnd;
        auto start = daemon->start();
        const auto startEnd = steady_clock_t::now();
        if (!start) {
            if (isSocketPermissionDenied(start.error())) {
                std::cout << "Skipping: UNIX domain sockets not permitted: "
                          << start.error().message << "\n";
                return 0;
            }
            std::cerr << "Failed to start daemon: " << start.error().message << "\n";
            return 1;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(holdMs));

        const auto stopBegin = steady_clock_t::now();
        daemon->stop();
        const auto stopEnd = steady_clock_t::now();

        const TimingSample sample{
            .totalMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(stopEnd - totalStart).count(),
            .constructMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(constructEnd - totalStart)
                    .count(),
            .startMs = std::chrono::duration_cast<std::chrono::milliseconds>(startEnd - startBegin)
                           .count(),
            .startupMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(startEnd - totalStart)
                    .count(),
            .holdMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(stopBegin - startEnd).count(),
            .stopMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(stopEnd - stopBegin).count(),
        };
        samples.push_back(sample);

        if (iterations == 1) {
            std::cout << "Warm start/stop: " << sample.totalMs << " ms\n";
        } else {
            std::cout << "Warm start/stop: " << sample.totalMs << " ms"
                      << " (iter " << (i + 1) << "/" << iterations << ")\n";
        }
        std::cout << "Split ms: construct=" << sample.constructMs << " start=" << sample.startMs
                  << " startup=" << sample.startupMs << " hold=" << sample.holdMs
                  << " stop=" << sample.stopMs << "\n";

        std::error_code ec;
        fs::remove_all(runtime_root, ec);

        if (sample.totalMs >= maxTotalMs) {
            std::cerr << "Warm start/stop exceeded " << maxTotalMs << "ms: " << sample.totalMs
                      << " ms\n";
            return 2;
        }
    }

    std::vector<long long> totals;
    std::vector<long long> constructs;
    std::vector<long long> starts;
    std::vector<long long> startups;
    std::vector<long long> holds;
    std::vector<long long> stops;
    totals.reserve(samples.size());
    constructs.reserve(samples.size());
    starts.reserve(samples.size());
    startups.reserve(samples.size());
    holds.reserve(samples.size());
    stops.reserve(samples.size());
    for (const TimingSample& sample : samples) {
        totals.push_back(sample.totalMs);
        constructs.push_back(sample.constructMs);
        starts.push_back(sample.startMs);
        startups.push_back(sample.startupMs);
        holds.push_back(sample.holdMs);
        stops.push_back(sample.stopMs);
    }

    std::cout << "Summary total_ms: mean=" << meanMs(totals) << " p50=" << percentileMs(totals, 50)
              << " p90=" << percentileMs(totals, 90) << " min=" << percentileMs(totals, 0)
              << " max=" << percentileMs(totals, 100) << "\n";
    std::cout << "Summary startup_ms: mean=" << meanMs(startups)
              << " p50=" << percentileMs(startups, 50) << " p90=" << percentileMs(startups, 90)
              << "\n";
    std::cout << "Summary construct_ms: mean=" << meanMs(constructs)
              << " p50=" << percentileMs(constructs, 50) << " p90=" << percentileMs(constructs, 90)
              << "\n";
    std::cout << "Summary start_ms: mean=" << meanMs(starts) << " p50=" << percentileMs(starts, 50)
              << " p90=" << percentileMs(starts, 90) << "\n";
    std::cout << "Summary stop_ms:  mean=" << meanMs(stops) << " p50=" << percentileMs(stops, 50)
              << " p90=" << percentileMs(stops, 90) << "\n";

    return 0;
}
