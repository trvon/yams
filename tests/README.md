# YAMS Test Suite Documentation

## Overview

The YAMS test suite provides comprehensive testing coverage including unit tests, integration tests, performance benchmarks, and stress tests. This document explains how to write, run, and maintain tests for YAMS.

## Quick Start

### Running Tests (Meson)

```bash
# Configure with tests enabled
conan install . -of build/debug -s build_type=Debug -b missing
meson setup build/debug --native-file build/debug/build/Debug/generators/conan_meson_native.ini --buildtype=debug -Dbuild-tests=true
meson compile -C build/debug

# Run suites
meson test -C build/debug                 # all
meson test -C build/debug -t unit         # unit only
meson test -C build/debug -t integration  # integration only
meson test -C build/debug -t stress       # stress (long-running)

# List tests
meson test -C build/debug -l
```

### Legacy CMake/CTest (for reference)
These commands are retained for contributors still using the legacy build. Prefer Meson going forward.

```bash
cmake -B build -DBUILD_TESTS=ON -DENABLE_COVERAGE=ON
cmake --build build
cd build && ctest -V
```

### Running Benchmarks

The benchmark suite is now consolidated into a few executables (`api_benchmarks`, `core_benchmarks`, `search_benchmarks`).

```bash
# Run all benchmarks
cd build/tests/benchmarks
./run_all_benchmarks

# Run a specific benchmark suite
./core_benchmarks

# Pass arguments to the benchmark runner
./api_benchmarks --iterations 100 --output my_results.json
```

## Test Organization

```
tests/
├── common/                 # Shared test utilities
│   ├── test_helpers.h     # Basic test helpers
│   ├── test_data_generator.h  # Test data generation
│   ├── fixture_manager.h  # Test fixture management
│   └── benchmark_tracker.h # Benchmark tracking
├── unit/                   # Unit tests for each component
│   ├── crypto/
│   ├── storage/
│   └── ...
├── integration/           # End-to-end and multi-component tests
│   ├── daemon/
│   └── ...
├── benchmarks/           # Performance benchmarks
│   ├── api_benchmarks.cpp
│   ├── core_benchmarks.cpp
│   └── search_benchmarks.cpp
├── stress/              # Stress tests
├── data/                # Test data files
├── fixtures/            # Test fixtures
└── scripts/             # Test automation scripts
```

## Writing Tests

### Unit Tests

Unit tests verify individual components in isolation. Place unit tests in `tests/unit/<module>/`.

```cpp
#include <gtest/gtest.h>
#include <yams/extraction/pdf_extractor.h>
#include "tests/common/fixture_manager.h"

using namespace yams;
using namespace yams::test;

class PDFExtractorTest : public ::testing::Test {
protected:
    void SetUp() override {
        extractor_ = std::make_unique<extraction::PDFExtractor>();
    }
    
    std::unique_ptr<extraction::PDFExtractor> extractor_;
};

TEST_F(PDFExtractorTest, ExtractTextFromSimplePDF) {
    // Arrange
    auto fixture = FixtureManager::getSimplePDF();
    
    // Act
    auto result = extractor_->extract(fixture.path);
    
    // Assert
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->text.empty());
    EXPECT_EQ(result->metadata["pages"], "1");
}

TEST_F(PDFExtractorTest, HandleCorruptedPDF) {
    // Arrange
    auto fixture = FixtureManager::getCorruptedPDF();
    
    // Act
    auto result = extractor_->extract(fixture.path);
    
    // Assert
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::InvalidFormat);
}
```

### Integration Tests

Integration tests verify multiple components working together. Place in `tests/integration/`.

```cpp
#include <gtest/gtest.h>
#include "tests/common/test_helpers.h"

class DocumentLifecycleTest : public yams::test::YamsTest {
protected:
    void SetUp() override {
        YamsTest::SetUp();
        // Initialize YAMS in test directory
        initializeYAMS(testDir);
    }
};

TEST_F(DocumentLifecycleTest, CompleteDocumentWorkflow) {
    // Add document
    auto addResult = runCommand("yams add test.txt --tags test");
    ASSERT_TRUE(addResult.isOk());
    
    std::string hash = extractHash(addResult.value());
    
    // Search for document
    auto searchResult = runCommand("yams search test");
    ASSERT_TRUE(searchResult.value().contains(hash));
    
    // Update metadata
    auto updateResult = runCommand(
        "yams update --hash " + hash + " --metadata status=processed");
    ASSERT_TRUE(updateResult.isOk());
    
    // Retrieve document
    auto getResult = runCommand("yams get " + hash);
    ASSERT_EQ(getResult.value(), readFile("test.txt"));
}
```

### Performance Benchmarks

Benchmarks use a custom `BenchmarkBase` class for consistency. Place new benchmarks in the appropriate consolidated file (`core_benchmarks.cpp`, `search_benchmarks.cpp`, etc.).

```cpp
#include "benchmark_base.h"
#include <yams/crypto/hasher.h>

using namespace yams::benchmark;

// Define a benchmark class that inherits from a suitable base or BenchmarkBase itself
class HashingBenchmark : public BenchmarkBase {
public:
    HashingBenchmark(const std::string& name, size_t dataSize) 
        : BenchmarkBase("Hashing_" + name), dataSize_(dataSize) { 
        // Setup code here
        hasher_ = yams::crypto::createSHA256Hasher();
        data_.resize(dataSize_);
    }

protected:
    // The core work of the benchmark goes here
    size_t runIteration() override {
        hasher_->hash(data_);
        return 1; // Number of operations in this iteration
    }

private:
    std::unique_ptr<yams::crypto::IContentHasher> hasher_;
    std::vector<std::byte> data_;
    size_t dataSize_;
};

// In main(), instantiate and run the benchmark
int main() {
    BenchmarkBase::Config config;
    // ... configure ...
    auto bench = std::make_unique<HashingBenchmark>("SHA256_1MB", 1024 * 1024);
    bench->run();
    return 0;
}

```

## Test Data Management

### Using Test Data Generator

```cpp
#include "tests/common/test_data_generator.h"

TestDataGenerator generator;

// Generate text document
std::string text = generator.generateTextDocument(1024);  // 1KB

// Generate PDF
auto pdfData = generator.generatePDF(5);  // 5 pages

// Generate corpus
auto corpus = generator.generateCorpus(100, 4096, 0.1);  // 100 docs, 4KB avg, 10% dups

// Generate metadata
auto metadata = generator.generateMetadata(10, {"string", "number", "boolean"});
```

### Using Fixture Manager

```cpp
#include "tests/common/fixture_manager.h"
#include "tests/common/test_data_generator.h"

using yams::test::FixtureManager;
using yams::test::TestDataGenerator;

// Get predefined fixtures
auto simplePDF = FixtureManager::getSimplePDF();
auto complexPDF = FixtureManager::getComplexPDF();
auto largeText = FixtureManager::getLargeTextFile();

// Create custom fixtures rooted in a temporary directory
FixtureManager manager;
TestDataGenerator generator;
auto textFixture = manager.createTextFixture("custom.txt", "content");
auto binaryFixture = manager.createBinaryFixture("data.bin", generator.generateRandomBytes(512));

// Create a synthetic corpus for search/metadata tests
auto fixtures = manager.createCorpus(50, 1024);  // 50 docs, 1KB each
```

#### Search Corpus Presets

Search-heavy suites (daemon search, CLI regressions, benchmarking) reuse a deterministic preset corpus so they can share documents, tags, and query workloads.

```cpp
#include "tests/common/fixture_manager.h"
#include "tests/common/search_corpus_presets.h"

yams::test::FixtureManager fixtures(tmpDir);
auto spec = yams::test::defaultSearchCorpusSpec();
spec.commonTags.push_back("regression");
auto corpus = fixtures.createSearchCorpus(spec);

yams::app::services::DocumentIngestionService ingiter;
yams::app::services::AddOptions opts;
opts.socketPath = sock;
opts.explicitDataDir = storageDir;
opts.path = (fixtures.root() / spec.rootDirectory).string();
opts.recursive = true;
opts.noEmbeddings = false; // ensure vectors are generated
auto addRes = ingiter.addViaDaemon(opts);
ASSERT_TRUE(addRes);
```

`createSearchCorpus` returns both a flat list (`corpus.fixtures`) and a topic map (`corpus.fixturesByTopic`) so callers can pick targeted subsets (for example `topic:python`). All artifacts live under `FixtureManager::root()` and are removed when the manager is destroyed.

> See `docs/delivery/028/artifacts/search-fixture-benchmark.md` for a command-line flow that uses the same presets to benchmark `yams search` end to end.

> **Tip:** Integration smoke tests (daemon ingestion) and metadata-heavy unit suites (search service) are wired to `FixtureManager`; prefer extending those helpers over ad-hoc `std::ofstream` usage when adding new file-backed scenarios.

## Code Coverage

### Generating Coverage Reports

```bash
# Build with coverage enabled
cmake -B build -DENABLE_COVERAGE=ON
cmake --build build

# Run tests
cd build
ctest

# Generate coverage report
make coverage

# View HTML report
open coverage_report/index.html

# Generate LCOV report
lcov --capture --directory . --output-file coverage.info
lcov --remove coverage.info '/usr/*' '*/tests/*' --output-file coverage.info
genhtml coverage.info --output-directory coverage_html
```

### Coverage Goals

- Unit tests: >80% line coverage
- Integration tests: >70% scenario coverage
- New features: Must include tests before merge
- Critical paths: 100% coverage required

## Continuous Integration

### GitHub Actions Workflow

The CI pipeline runs on every push and pull request:

1. **Build Matrix**: Tests on Ubuntu, macOS, Windows
2. **Test Execution**: All test categories
3. **Coverage Upload**: Results sent to Codecov
4. **Benchmark Regression**: Compares with baseline
5. **Report Generation**: Creates test artifacts

### Pre-commit Hooks

Install pre-commit hooks to run tests locally:

```bash
# Install hook
cp tests/scripts/pre-commit.sh .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit

# Hook runs:
# - Quick unit tests
# - Code coverage check
# - Benchmark regression check
```

## Best Practices

### General Guidelines

1. **Test Naming**: Use descriptive names that explain what is being tested
   - Good: `ExtractTextFromMultiPagePDF`
   - Bad: `Test1`

2. **Test Independence**: Tests should not depend on execution order
   - Use `SetUp()` and `TearDown()` for initialization/cleanup
   - Don't share state between tests

3. **Assertions**: Use appropriate assertion macros
   - `ASSERT_*`: Fatal failures that abort the test
   - `EXPECT_*`: Non-fatal failures that continue the test

4. **Test Data**: Use generators and fixtures instead of hardcoded data
   - Ensures reproducibility
   - Simplifies maintenance

5. **Error Testing**: Always test error conditions
   - Invalid input
   - Resource failures
   - Boundary conditions

### Performance Testing

1. **Benchmark Isolation**: Minimize external factors
   - Use `benchmark::DoNotOptimize()` to prevent optimization
   - Run multiple iterations for stability

2. **Regression Detection**: Set appropriate thresholds
   - Time-based: 10% increase is regression
   - Throughput: 10% decrease is regression

3. **Environment Recording**: Capture system information
   - Platform, compiler, build type
   - Hardware specifications
   - Resource availability

### Integration Testing

1. **Realistic Scenarios**: Test actual user workflows
   - Complete operations end-to-end
   - Include error recovery paths

2. **Concurrency Testing**: Verify thread safety
   - Multiple readers/writers
   - Race condition detection
   - Deadlock prevention

3. **Data Integrity**: Verify consistency
   - Transaction atomicity
   - Rollback behavior
   - Data persistence

## Troubleshooting

### Common Issues

**Tests fail with "fixture not found"**
- Ensure test data directory exists
- Check file permissions
- Verify fixture manager initialization

**Benchmarks show high variance**
- Increase iteration count
- Close other applications
- Use performance CPU governor
- Run in release mode

**Coverage reports missing files**
- Check CMake coverage flags
- Ensure gcov/llvm-cov installed
- Verify source file paths

**Integration tests timeout**
- Check for deadlocks
- Increase timeout limits
- Verify resource cleanup

### Debug Techniques

```bash
# Run single test with debugging
gdb ./tests/unit/extraction/extraction_tests
(gdb) break PDFExtractorTest_ExtractText_Test::TestBody
(gdb) run --gtest_filter="PDFExtractorTest.ExtractText"

# Run with sanitizers
cmake -B build -DSANITIZE_ADDRESS=ON
./build/tests/unit/all_unit_tests

# Verbose test output
ctest -V --output-on-failure

# Test with valgrind
valgrind --leak-check=full ./tests/unit/all_unit_tests
```

## Contributing Tests

When adding new features:

1. **Write tests first** (TDD approach)
2. **Cover happy path** and error cases
3. **Add benchmarks** for performance-critical code
4. **Document test purpose** in comments
5. **Update this README** if adding new patterns

### Checklist for New Tests

- [ ] Unit tests for new components
- [ ] Integration tests for workflows
- [ ] Benchmarks for performance-critical paths
- [ ] Error handling tests
- [ ] Documentation in test files
- [ ] Coverage meets requirements
- [ ] CI passes on all platforms

## Performance Baselines

Current performance baselines (as of v0.0.7):

| Operation | Baseline | Unit | Notes |
|-----------|----------|------|-------|
| SHA256 (1MB) | 3.5 | ms | Single-threaded |
| PDF Extract (10 pages) | 50 | ms | With PDFium |
| Search (10K docs) | 25 | ms | Full-text search |
| Metadata Update | 2 | ms | Single field |
| Document Ingestion | 100 | docs/s | 1KB documents |

## Contact

For test-related questions:
- Check existing test examples
- Consult this documentation
- Review CI logs for patterns
- Open an issue with test label
