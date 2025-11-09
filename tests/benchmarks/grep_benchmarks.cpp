#include <algorithm>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "benchmark_base.h"
#include <yams/app/services/literal_extractor.hpp>
#include <yams/app/services/simd_newline_scanner.hpp>

namespace yams::benchmark {

/**
 * Benchmark literal extraction from regex patterns
 */
class LiteralExtractionBenchmark : public BenchmarkBase {
public:
    LiteralExtractionBenchmark(const std::string& name, std::vector<std::string> patterns,
                               const Config& config = Config())
        : BenchmarkBase("Grep_LiteralExtraction_" + name, config),
          patterns_(std::move(patterns)) {}

protected:
    size_t runIteration() override {
        size_t extracted = 0;
        for (const auto& pattern : patterns_) {
            auto result = yams::app::services::LiteralExtractor::extract(pattern, false);
            if (result.longestLength > 0) {
                ++extracted;
            }
        }
        return extracted;
    }

    std::vector<std::string> patterns_;
};

/**
 * Benchmark Boyer-Moore-Horspool vs std::string::find
 */
class BMHSearchBenchmark : public BenchmarkBase {
public:
    BMHSearchBenchmark(const std::string& name, std::string needle, size_t textSize,
                       const Config& config = Config())
        : BenchmarkBase("Grep_BMH_" + name, config), needle_(std::move(needle)) {
        // Generate haystack with a few needle occurrences
        text_.reserve(textSize);
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> charDist('a', 'z');
        
        for (size_t i = 0; i < textSize; ++i) {
            // Insert needle every ~1000 chars
            if (i > 0 && i % 1000 == 0 && i + needle_.size() < textSize) {
                text_ += needle_;
                i += needle_.size();
            }
            text_ += static_cast<char>(charDist(rng));
        }
        
        bmhSearcher_ =
            std::make_unique<yams::app::services::BMHSearcher>(needle_, false);
    }

protected:
    size_t runIteration() override {
        size_t count = 0;
        size_t pos = 0;
        while ((pos = bmhSearcher_->find(text_, pos)) != std::string::npos) {
            ++count;
            ++pos;
        }
        return count;
    }

    std::string needle_;
    std::string text_;
    std::unique_ptr<yams::app::services::BMHSearcher> bmhSearcher_;
};

/**
 * Benchmark std::string::find baseline
 */
class StdFindBenchmark : public BenchmarkBase {
public:
    StdFindBenchmark(const std::string& name, std::string needle, size_t textSize,
                     const Config& config = Config())
        : BenchmarkBase("Grep_StdFind_" + name, config), needle_(std::move(needle)) {
        text_.reserve(textSize);
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> charDist('a', 'z');
        
        for (size_t i = 0; i < textSize; ++i) {
            if (i > 0 && i % 1000 == 0 && i + needle_.size() < textSize) {
                text_ += needle_;
                i += needle_.size();
            }
            text_ += static_cast<char>(charDist(rng));
        }
    }

protected:
    size_t runIteration() override {
        size_t count = 0;
        size_t pos = 0;
        while ((pos = text_.find(needle_, pos)) != std::string::npos) {
            ++count;
            ++pos;
        }
        return count;
    }

    std::string needle_;
    std::string text_;
};

/**
 * Benchmark SIMD newline scanning
 */
class SimdNewlineBenchmark : public BenchmarkBase {
public:
    SimdNewlineBenchmark(const std::string& name, size_t textSize, size_t linesCount,
                         const Config& config = Config())
        : BenchmarkBase("Grep_SimdNewline_" + name, config) {
        // Generate text with known number of newlines
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> charDist('a', 'z');
        size_t charsPerLine = textSize / linesCount;
        
        text_.reserve(textSize);
        for (size_t i = 0; i < linesCount; ++i) {
            for (size_t j = 0; j < charsPerLine; ++j) {
                text_ += static_cast<char>(charDist(rng));
            }
            text_ += '\n';
        }
    }

protected:
    size_t runIteration() override {
        size_t lineCount = 0;
        size_t pos = 0;
        while (pos < text_.size()) {
            size_t remaining = text_.size() - pos;
            size_t nlOffset =
                yams::app::services::SimdNewlineScanner::findNewline(text_.data() + pos, remaining);
            if (nlOffset >= remaining) {
                break;
            }
            ++lineCount;
            pos += nlOffset + 1;
        }
        return lineCount;
    }

    std::string text_;
};

/**
 * Benchmark memchr baseline for newline scanning
 */
class MemchrNewlineBenchmark : public BenchmarkBase {
public:
    MemchrNewlineBenchmark(const std::string& name, size_t textSize, size_t linesCount,
                           const Config& config = Config())
        : BenchmarkBase("Grep_MemchrNewline_" + name, config) {
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> charDist('a', 'z');
        size_t charsPerLine = textSize / linesCount;
        
        text_.reserve(textSize);
        for (size_t i = 0; i < linesCount; ++i) {
            for (size_t j = 0; j < charsPerLine; ++j) {
                text_ += static_cast<char>(charDist(rng));
            }
            text_ += '\n';
        }
    }

protected:
    size_t runIteration() override {
        size_t lineCount = 0;
        const char* p = text_.data();
        const char* end = p + text_.size();
        while (p < end) {
            const char* nl = static_cast<const char*>(
                memchr(p, '\n', static_cast<size_t>(end - p)));
            if (!nl) {
                break;
            }
            ++lineCount;
            p = nl + 1;
        }
        return lineCount;
    }

    std::string text_;
};

} // namespace yams::benchmark

int main() {
    using namespace yams::benchmark;

    std::cout << "=== YAMS Grep Service Algorithmic Benchmarks ===" << std::endl;
    std::cout << "Measuring: Literal Extraction, BMH vs std::find, SIMD vs memchr" << std::endl;
    std::cout << std::endl;

    // Literal Extraction Benchmark
    {
        std::vector<std::string> patterns = {
            R"(class\s+(\w+))",        // class definition
            R"(function\s+\w+\s*\()",  // function declaration
            R"(import\s+.*from)",      // import statement
            R"(\b\d{3}-\d{2}-\d{4}\b)", // SSN pattern
            R"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", // email
        };
        
        BenchmarkBase::Config config;
        config.benchmark_iterations = 10000;
        config.warmup_iterations = 100;
        config.verbose = true;
        
        LiteralExtractionBenchmark bench("Common_Patterns", patterns, config);
        bench.run();
    }

    // BMH vs std::find - Short Pattern (3 chars)
    {
        BenchmarkBase::Config config;
        config.benchmark_iterations = 1000;
        config.warmup_iterations = 10;
        
        std::cout << std::endl;
        std::cout << "--- Short Pattern (3 chars, 1MB text) ---" << std::endl;
        BMHSearchBenchmark bmhBench("ShortPattern", "foo", 1024 * 1024, config);
        bmhBench.run();
        
        StdFindBenchmark stdBench("ShortPattern", "foo", 1024 * 1024, config);
        stdBench.run();
    }

    // BMH vs std::find - Medium Pattern (8 chars)
    {
        BenchmarkBase::Config config;
        config.benchmark_iterations = 1000;
        config.warmup_iterations = 10;
        
        std::cout << std::endl;
        std::cout << "--- Medium Pattern (8 chars, 1MB text) ---" << std::endl;
        BMHSearchBenchmark bmhBench("MediumPattern", "function", 1024 * 1024, config);
        bmhBench.run();
        
        StdFindBenchmark stdBench("MediumPattern", "function", 1024 * 1024, config);
        stdBench.run();
    }

    // BMH vs std::find - Long Pattern (16 chars)
    {
        BenchmarkBase::Config config;
        config.benchmark_iterations = 1000;
        config.warmup_iterations = 10;
        
        std::cout << std::endl;
        std::cout << "--- Long Pattern (16 chars, 1MB text) ---" << std::endl;
        BMHSearchBenchmark bmhBench("LongPattern", "class_definition", 1024 * 1024, config);
        bmhBench.run();
        
        StdFindBenchmark stdBench("LongPattern", "class_definition", 1024 * 1024, config);
        stdBench.run();
    }

    // SIMD vs memchr - Small File (100KB, 1000 lines)
    {
        BenchmarkBase::Config config;
        config.benchmark_iterations = 5000;
        config.warmup_iterations = 50;
        
        std::cout << std::endl;
        std::cout << "--- Newline Scanning: Small File (100KB, 1000 lines) ---" << std::endl;
        SimdNewlineBenchmark simdBench("SmallFile", 100 * 1024, 1000, config);
        simdBench.run();
        
        MemchrNewlineBenchmark memchrBench("SmallFile", 100 * 1024, 1000, config);
        memchrBench.run();
    }

    // SIMD vs memchr - Large File (10MB, 100K lines)
    {
        BenchmarkBase::Config config;
        config.benchmark_iterations = 100;
        config.warmup_iterations = 5;
        config.verbose = true;
        
        std::cout << std::endl;
        std::cout << "--- Newline Scanning: Large File (10MB, 100K lines) ---" << std::endl;
        SimdNewlineBenchmark simdBench("LargeFile", 10 * 1024 * 1024, 100000, config);
        simdBench.run();
        
        MemchrNewlineBenchmark memchrBench("LargeFile", 10 * 1024 * 1024, 100000, config);
        memchrBench.run();
    }

    std::cout << std::endl;
    std::cout << "=== Benchmark Complete ===" << std::endl;
    return 0;
}
