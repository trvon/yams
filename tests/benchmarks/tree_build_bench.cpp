#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <yams/metadata/tree_builder.h>
#include <yams/storage/storage_engine.h>

namespace fs = std::filesystem;
using namespace yams;

int main() {
    spdlog::set_level(spdlog::level::warn);
    auto dataDir = fs::temp_directory_path() / "yams_tree_bench";
    fs::remove_all(dataDir);
    fs::create_directories(dataDir);

    const int runs[] = {100, 500, 1000, 5000, 10000};

    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int> dist('a', 'z');

    auto makeContent = [&](int bytes) {
        std::string s(bytes, '\0');
        for (int i = 0; i < bytes; ++i)
            s[i] = static_cast<char>(dist(rng));
        return s;
    };

    nlohmann::json results = nlohmann::json::array();

    for (int numFiles : runs) {
        // Clean and create
        auto workDir = dataDir / ("n" + std::to_string(numFiles));
        fs::create_directories(workDir);

        // Create files
        for (int i = 0; i < numFiles; ++i) {
            char buf[32];
            snprintf(buf, sizeof(buf), "file_%05d.txt", i);
            auto fp = workDir / buf;
            auto content = makeContent(512);
            std::ofstream ofs(fp);
            ofs.write(content.data(), static_cast<std::streamsize>(content.size()));
        }

        // Build tree
        auto casDir = dataDir / ("cas" + std::to_string(numFiles));
        storage::StorageConfig scfg;
        scfg.basePath = casDir;
        auto storagePtr = storage::createStorageEngine(scfg);
        metadata::TreeBuilder builder(
            std::shared_ptr<storage::IStorageEngine>(std::move(storagePtr)));

        auto t0 = std::chrono::steady_clock::now();
        auto result = builder.buildFromDirectory(workDir.string());
        auto t1 = std::chrono::steady_clock::now();

        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        const double throughputFilesPerSec = ms > 0.0 ? numFiles / (ms / 1000.0) : 0.0;

        nlohmann::json entry;
        entry["files"] = numFiles;
        entry["latency_ms"] = ms;
        entry["throughput_files_per_sec"] = throughputFilesPerSec;
        results.push_back(entry);

        std::cout << "n=" << numFiles << "  " << ms << " ms  "
                  << static_cast<int>(throughputFilesPerSec) << " files/sec\n";
    }

    std::ofstream out("/tmp/tree_build_results.json");
    out << results.dump(2) << "\n";
    out.close();

    fs::remove_all(dataDir);
    return 0;
}
