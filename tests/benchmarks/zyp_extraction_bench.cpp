// zyp PDF extraction benchmark — measures sequential vs parallel throughput
//
// Usage: zyp_extraction_bench <pdf_path> [repeat_count]
//
// Reports extraction speed (pages/s, MB/s) for 1, 2, 4, and N threads.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../../plugins/zyp/zpdf_wrapper.h"

namespace {

struct BenchResult {
    std::string label;
    int numThreads;
    int totalPages;
    double totalMb;
    double elapsedSec;
    double pagesPerSec;
    double mbPerSec;
};

BenchResult runExtraction(const std::vector<uint8_t>& pdfData, int numThreads, int totalPages,
                          const std::string& label) {
    auto start = std::chrono::steady_clock::now();
    auto text = yams::zyp::Document::extractAllParallelized(pdfData, numThreads);
    auto end = std::chrono::steady_clock::now();

    double elapsed =
        static_cast<double>(
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()) /
        1'000'000.0;

    double totalMb = 0.0;
    if (text) {
        totalMb = static_cast<double>(text.size()) / (1024.0 * 1024.0);
    }

    BenchResult r;
    r.label = label;
    r.numThreads = numThreads;
    r.totalPages = totalPages;
    r.totalMb = totalMb;
    r.elapsedSec = elapsed;
    r.pagesPerSec = elapsed > 0.0 ? static_cast<double>(totalPages) / elapsed : 0.0;
    r.mbPerSec = elapsed > 0.0 ? totalMb / elapsed : 0.0;
    return r;
}

std::vector<uint8_t> loadPdf(const std::string& path) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file)
        return {};
    auto size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::vector<uint8_t> data(static_cast<size_t>(size));
    file.read(reinterpret_cast<char*>(data.data()), size);
    return data;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: zyp_extraction_bench <pdf_path> [repeat_count]\n";
        std::cerr << "  pdf_path    : Path to a multi-page PDF file\n";
        std::cerr << "  repeat_count: Number of measurement runs per config (default: 3)\n";
        return 1;
    }

    std::string pdfPath = argv[1];
    int repeat = 3;
    if (argc >= 3)
        repeat = std::max(1, std::atoi(argv[2]));

    auto pdfData = loadPdf(pdfPath);
    if (pdfData.empty()) {
        std::cerr << "Error: Could not read PDF: " << pdfPath << "\n";
        return 1;
    }

    auto probeDoc = yams::zyp::Document::openMemory(pdfData);
    if (!probeDoc) {
        std::cerr << "Error: Could not open PDF: " << pdfPath << "\n";
        return 1;
    }
    int totalPages = probeDoc->pageCount();
    probeDoc.reset();

    if (totalPages <= 0) {
        std::cerr << "Error: PDF has no pages\n";
        return 1;
    }

    int hwThreads = static_cast<int>(std::thread::hardware_concurrency());
    if (hwThreads < 1)
        hwThreads = 4;

    std::cout << "\nPDF: " << pdfPath << "  pages=" << totalPages
              << "  size=" << (pdfData.size() / 1024) << " KB"
              << "  hw_threads=" << hwThreads << "  repeat=" << repeat << "\n"
              << std::endl;

    std::vector<std::pair<int, std::string>> configs = {{1, "sequential"}};
    if (hwThreads >= 2 && totalPages >= 2)
        configs.push_back({2, "2-threads"});
    if (hwThreads >= 4 && totalPages >= 4)
        configs.push_back({4, "4-threads"});
    if (hwThreads > 4 && totalPages >= hwThreads)
        configs.push_back({hwThreads, std::to_string(hwThreads) + "-threads"});

    std::vector<BenchResult> allResults;
    allResults.reserve(static_cast<size_t>(repeat) * configs.size());

    // Warmup
    std::cout << "Warming up..." << std::flush;
    runExtraction(pdfData, 1, totalPages, "warmup");
    std::cout << " done.\n" << std::endl;

    for (int r = 0; r < repeat; ++r) {
        for (const auto& [threads, label] : configs) {
            allResults.push_back(runExtraction(pdfData, threads, totalPages, label));
        }
    }

    std::cout << std::left << std::setw(18) << "config" << std::right << std::setw(10)
              << "elapsed_s" << std::setw(12) << "pages/s" << std::setw(12) << "MB/s"
              << std::setw(10) << "MB_out" << "\n";
    std::cout << std::string(62, '-') << "\n";

    for (const auto& [threads, label] : configs) {
        double avgElapsed = 0, avgPagesPerSec = 0, avgMbPerSec = 0, avgMb = 0;
        int count = 0;
        for (const auto& r : allResults) {
            if (r.numThreads == threads) {
                avgElapsed += r.elapsedSec;
                avgPagesPerSec += r.pagesPerSec;
                avgMbPerSec += r.mbPerSec;
                avgMb += r.totalMb;
                ++count;
            }
        }
        if (count == 0)
            continue;
        avgElapsed /= static_cast<double>(count);
        avgPagesPerSec /= static_cast<double>(count);
        avgMbPerSec /= static_cast<double>(count);
        avgMb /= static_cast<double>(count);

        std::cout << std::left << std::setw(18) << label << std::right << std::fixed
                  << std::setprecision(3) << std::setw(10) << avgElapsed << std::setw(12)
                  << static_cast<int>(avgPagesPerSec) << std::setprecision(2) << std::setw(12)
                  << avgMbPerSec << std::setprecision(2) << std::setw(10) << avgMb << "\n";
    }

    if (configs.size() >= 2) {
        auto seqIt = std::find_if(allResults.begin(), allResults.end(), [](const auto& r) {
            return r.numThreads == 1 && r.elapsedSec > 0;
        });
        auto parIt = std::find_if(allResults.begin(), allResults.end(), [](const auto& r) {
            return r.numThreads > 1 && r.elapsedSec > 0;
        });
        if (seqIt != allResults.end() && parIt != allResults.end()) {
            double speedup = seqIt->elapsedSec / parIt->elapsedSec;
            std::cout << "\nParallel speedup (sequential / " << parIt->label << "): " << std::fixed
                      << std::setprecision(2) << speedup << "x\n";
            if (speedup < 1.1) {
                std::cout << "WARNING: No significant parallel speedup detected. "
                          << "Extraction may not be using multiple threads effectively.\n";
            } else {
                std::cout << "OK: Parallel speedup confirmed (>= 1.1x)\n";
            }
        }
    }

    std::cout << std::endl;
    return 0;
}
