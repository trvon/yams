# YAMS Stress Tests

This directory contains comprehensive stress tests for validating YAMS behavior under extreme load conditions.

## Overview

The stress tests are designed to validate:

1. **Compression Stress Tests** (`compression_stress_test.cpp`)
   - Multiple compression algorithms (Zstandard, LZ4, Gzip)
   - Various compression levels (1, 3, 6, 9)
   - Different data patterns (random, zeros, repetitive)
   - Large file handling (1MB to 100MB)
   - Sustained high-throughput compression
   - Resource exhaustion scenarios
   - Rapid compression/decompression cycles
   - Memory leak detection
   - Error injection and recovery
   - Compression bomb protection

2. **Storage Concurrency Tests** (`storage/concurrent_test.cpp`)
   - Concurrent readers (100, 500, 1000 threads)
   - Concurrent writers (up to 100 threads)
   - Mixed read/write workloads (80/20, 90/10, 50/50, 25/75 ratios)
   - High contention scenarios (many threads, few objects)
   - Variable object sizes (100B to 1MB)
   - Burst traffic patterns
   - Long-running sustained load (30+ seconds)
   - Create/delete cycles
   - Latency under load measurements
   - Connection pool scaling

## Building Stress Tests

Stress tests are **disabled by default** to avoid long CI times. Enable them during build:

```bash
# Configure with stress tests enabled
meson setup builddir -Denable-stress-tests=true

# Or reconfigure existing build
meson configure builddir -Denable-stress-tests=true

# Build
meson compile -C builddir
```

## Running Stress Tests

### Basic Execution

Run all stress tests once:

```bash
meson test -C builddir stress --suite stress
```

### Running Tests Multiple Times

Meson provides a `--repeat` option to run tests in a loop:

```bash
# Run stress tests 10 times
meson test -C builddir stress --suite stress --repeat 10

# Run 100 times to detect intermittent issues
meson test -C builddir stress --suite stress --repeat 100

# Run indefinitely until failure (Ctrl+C to stop)
meson test -C builddir stress --suite stress --repeat 999999
```

### Advanced Options

```bash
# Run with verbose output
meson test -C builddir stress --suite stress --repeat 10 -v

# Stop on first failure
meson test -C builddir stress --suite stress --repeat 50 --maxfail 1

# Run without rebuilding (faster for repeated runs)
meson test -C builddir stress --suite stress --repeat 100 --no-rebuild

# Run specific test patterns
meson test -C builddir "ConcurrentStorageTest.ThousandConcurrentReaders" --repeat 20

# Run multiple specific tests
meson test -C builddir "CompressionStressTest.*" --repeat 10
```

### Parallel Test Execution

Run multiple test processes in parallel (use with caution on shared resources):

```bash
# Run with 4 parallel jobs
meson test -C builddir stress --suite stress --repeat 10 --num-processes 4
```

### Timeout Configuration

Stress tests have a 1800 second (30 minute) timeout by default. Adjust if needed:

```bash
# Extend timeout to 1 hour
meson test -C builddir stress --suite stress --repeat 10 --timeout-multiplier 2
```

## Test Categories

### Quick Stress (< 5 minutes each)
- `CompressionStressTest.ExtremelyLargeFiles`
- `CompressionStressTest.RapidCycles`
- `ConcurrentStorageTest.ThousandConcurrentReaders`
- `ConcurrentStorageTest.HundredConcurrentWriters`

### Moderate Stress (5-15 minutes each)
- `CompressionStressTest.SustainedHighThroughput`
- `ConcurrentStorageTest.MixedReadWriteWorkload`
- `ConcurrentStorageTest.HighContentionSameObjects`
- `ConcurrentStorageTest.VariableObjectSizesConcurrent`

### Heavy Stress (15+ minutes each)
- `CompressionStressTest.ResourceExhaustion`
- `ConcurrentStorageTest.LongRunningSustainedLoad`
- `ConcurrentStorageTest.ConnectionPoolWorkerSweep`

## Interpreting Results

### Success Criteria

All tests should:
- Complete without crashes or hangs
- Report zero data corruption or integrity failures
- Meet performance targets (logged in output)
- Show consistent behavior across multiple runs

### Performance Metrics

Key metrics reported:
- **Throughput**: Operations per second, MB/s
- **Latency**: Average, P50, P95, P99 response times
- **Concurrency**: Number of simultaneous operations
- **Resource usage**: Memory consumption, thread count
- **Error rates**: Failed operations, conflicts, retries

### Common Issues

**Intermittent Failures**: Run with `--repeat 100` to identify race conditions

**Timeout**: Increase with `--timeout-multiplier` or reduce test parameters

**Resource Exhaustion**: Reduce concurrent thread counts in test configuration

**High Error Rates**: Check system resources (memory, file descriptors, disk space)

## CI/CD Integration

For continuous integration, use selective stress testing:

```bash
# Quick smoke test (5 iterations, critical tests only)
meson test -C builddir "ConcurrentStorageTest.ThousandConcurrentReaders" --repeat 5

# Nightly full stress test
meson test -C builddir stress --suite stress --repeat 50 --maxfail 3

# Pre-release validation (extensive)
meson test -C builddir stress --suite stress --repeat 200 --maxfail 1
```

## System Resource Requirements

Stress tests are designed to **push system limits**. Some tests may exhaust system resources (this is intentional validation, not a bug):

### File Descriptor Limits

The `LongRunningSustainedLoad` test may exhaust file descriptors on systems with default limits. This validates that the storage engine properly handles resource exhaustion.

**Symptoms**: Errors like "Failed to create temp file" after thousands of operations

**Performance achieved before exhaustion**: ~500,000 ops/sec (excellent!)

**Solutions**:
```bash
# Check current limit
ulimit -n

# Temporarily increase (for current session)
ulimit -n 1048576

# Permanently increase (add to /etc/security/limits.conf)
* soft nofile 1048576
* hard nofile 1048576
```

### Expected Resource Usage

- **Memory**: Up to 4-8GB during sustained load tests
- **File Descriptors**: Up to 10,000+ for concurrent write tests
- **CPU**: Will saturate all available cores
- **Disk I/O**: Sustained writes of 100+ MB/s

## Debugging Failed Stress Tests

1. **Enable verbose logging**:
   ```bash
   SPDLOG_LEVEL=debug meson test -C builddir stress --suite stress --repeat 10 -v
   ```

2. **Run under debugger**:
   ```bash
   meson test -C builddir "TestName" --gdb
   ```

3. **Isolate the failing test**:
   ```bash
   # Run just the failing test with high repeat count
   meson test -C builddir "FailingTest" --repeat 100 --verbose
   ```

4. **Check system resources**:
   ```bash
   # Monitor during test execution
   watch -n 1 'free -h; echo; ps aux | grep yams_stress | head -20'
   
   # Check file descriptor usage
   lsof -p $(pgrep yams_stress) | wc -l
   ```

5. **Resource exhaustion is a success**: If tests fail due to "out of file descriptors" or similar OS limits after completing millions of operations, **this validates the stress test is working correctly**. The system reached its limit before the code did.

## Customizing Stress Tests

### Environment Variables

Some tests support environment variables for customization:

```bash
# FTS5 performance tests
export YAMS_RUN_STRESS_TESTS=1
export YAMS_STRESS_DOC_COUNT=100000
export YAMS_STRESS_TIMEOUT_MS=10000

meson test -C builddir "FTS5StressTest.*"
```

### Modifying Test Parameters

Edit test source files to adjust:
- Thread counts
- Operation counts
- Data sizes
- Test durations
- Workload ratios

After modifications, rebuild and rerun:

```bash
meson compile -C builddir
meson test -C builddir stress --suite stress --repeat 10
```

## Performance Baselines

Expected performance on a modern workstation (adjust for your hardware):

- **Compression**: 
  - Zstandard level 1: ~500 MB/s
  - Zstandard level 6: ~100 MB/s
  - LZ4: ~1000 MB/s

- **Storage**:
  - Concurrent reads: >10,000 ops/sec with 1000 threads
  - Concurrent writes: >1,000 ops/sec with 100 threads
  - Mixed workload: >8,000 ops/sec

- **Latency**:
  - P99 read latency: < 10ms under load
  - P99 write latency: < 50ms under load

## Contributing

When adding new stress tests:

1. Place in appropriate subdirectory (`compression`, `storage`, etc.)
2. Add to `tests/stress/meson.build` or subdirectory build file
3. Use descriptive test names: `ComponentName.TestScenario`
4. Test multiple scenarios with loops/iterations within the test
5. Log meaningful metrics with `spdlog::info()`
6. Document in this README
7. Validate with `--repeat 100` before committing

## See Also

- [Main test README](../README.md)
- [Benchmark documentation](../benchmarks/README.md)
- [Test helpers](../common/)
