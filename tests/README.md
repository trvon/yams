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

Integration timeouts and stress iters (Meson options)

You can tune integration-suite timeouts without editing sources by passing Meson options at setup/configure time:

```bash
# Examples (defaults: services=900s, ui=600s, smoke=300s)
meson configure build/debug -Dintegration-timeout=900 -Dintegration-ui-timeout=600 -Dintegration-smoke-timeout=300 -Dstress-iters=100

# Or set at setup:
meson setup build/debug --buildtype=debug -Dbuild-tests=true \
  -Dintegration-timeout=900 -Dintegration-ui-timeout=600 -Dintegration-smoke-timeout=300 -Dstress-iters=100
```

Profiles via native files (no scripts)

You can also use Meson native files to version preset profiles:

```bash
# quicker local runs
meson setup build/fast --buildtype=debug -Dbuild-tests=true --native-file meson/profiles/ci-fast.ini
meson test -C build/fast integration_services -t 1

# nightly soak
meson setup build/soak --buildtype=debug -Dbuild-tests=true --native-file meson/profiles/nightly-soak.ini
meson test -C build/soak integration_services
```

### Services Integration Suite (isolated)

The Services suite exercises add/list/get/grep via a live daemon in a temporary sandbox.

```bash
# Run only the services integration executable (isolated; serial)
meson test -C build/debug integration_services -v

# Notes
# - Requires AF_UNIX socket availability (macOS/Linux). If your environment forbids AF_UNIX
#   binds, affected tests will SKIP with an explanatory message.
# - Logs: build/debug/meson-logs/testlog.txt
```

### UI/CLI Integration (services layer)

We mirror CLI expectations at the services layer to keep tests fast and
deterministic (no external shell). The custom shard `integration_ui` runs these
checks serially against a temporary daemon sandbox.

```bash
# Run only the UI/CLI expectations shard
meson test -C build/debug integration_ui -v

# What it includes
# - UiCliExpectationsIT.* (search/list/get structure and options)
# - GrepServiceExpectationsIT.* (regex pathsOnly, count mode, negatives)

# Notes
# - Tests bring up their own daemon with mock embeddings
# - AF_UNIX is required; tests skip with a helpful message if unavailable
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
All tests use Catch2 v3; see `tests/CATCH2_PATTERNS.md` for canonical patterns.

```cpp
#include <catch2/catch_test_macros.hpp>
#include <yams/extraction/pdf_extractor.h>
#include "tests/common/fixture_manager.h"

using namespace yams;
using namespace yams::test;

namespace {
struct PDFExtractorFixture {
    PDFExtractorFixture() { extractor_ = std::make_unique<extraction::PDFExtractor>(); }
    std::unique_ptr<extraction::PDFExtractor> extractor_;
};
} // namespace

TEST_CASE_METHOD(PDFExtractorFixture, "ExtractTextFromSimplePDF", "[pdf]") {
    auto fixture = FixtureManager::getSimplePDF();
    auto result = extractor_->extract(fixture.path);
    REQUIRE(result.has_value());
    CHECK_FALSE(result->text.empty());
    CHECK(result->metadata["pages"] == "1");
}

TEST_CASE_METHOD(PDFExtractorFixture, "HandleCorruptedPDF", "[pdf]") {
    auto fixture = FixtureManager::getCorruptedPDF();
    auto result = extractor_->extract(fixture.path);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == ErrorCode::InvalidFormat);
}
```

### Integration Tests

Integration tests verify multiple components working together. Place in `tests/integration/`.

```cpp
#include <catch2/catch_test_macros.hpp>
#include "tests/common/test_helpers_catch2.h"

namespace {
struct DocumentLifecycleFixture {
    DocumentLifecycleFixture() { initializeYAMS(testDir); }
    std::filesystem::path testDir = yams::test::make_temp_dir("yams_doc_lifecycle");
};
} // namespace

TEST_CASE_METHOD(DocumentLifecycleFixture, "CompleteDocumentWorkflow", "[integration]") {
    auto addResult = runCommand("yams add test.txt --tags test");
    REQUIRE(addResult.isOk());

    std::string hash = extractHash(addResult.value());

    auto searchResult = runCommand("yams search test");
    REQUIRE(searchResult.value().contains(hash));

    auto updateResult = runCommand(
        "yams update --hash " + hash + " --metadata status=processed");
    REQUIRE(updateResult.isOk());

    auto getResult = runCommand("yams get " + hash);
    CHECK(getResult.value() == readFile("test.txt"));
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

3. **Assertions**: Use appropriate Catch2 macros
   - `REQUIRE(...)`: Fatal failures that abort the test
   - `CHECK(...)`: Non-fatal failures that continue the test
   - `SKIP("reason")`: Skip a test case at runtime
   - Filter by tag: `./test_binary "[tag]"` (replaces `--gtest_filter`)

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
# Run single test case by tag (Catch2 filter)
./build/debug/tests/<test_binary> "[pdf]"

# Run under gdb and break inside a Catch2 TEST_CASE
gdb ./build/debug/tests/<test_binary>
(gdb) break Catch::RunContext::runCurrentTest
(gdb) run "[pdf]"

# Verbose test output
meson test -C build/debug -v --print-errorlogs

# Run with sanitizers (configure once)
meson setup build/asan --buildtype=debug -Dbuild-tests=true -Db_sanitize=address
meson test -C build/asan
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
