# Performance Profiling & Analysis

YAMS supports multiple profiling and analysis tools to help identify performance bottlenecks and optimize critical paths.

## Profiling Build

The `Profiling` build type enables instrumentation for various profiling tools:

```bash
# Build with profiling support
./setup.sh Profiling

# Compile
meson compile -C build/profiling
```

This build:
- Uses Debug as base (symbols + assertions enabled)
- Enables Tracy profiler integration
- Includes test binaries for benchmarking
- Outputs to `build/profiling/` directory

## Tracy Profiler

Tracy provides real-time, low-overhead performance profiling with frame-by-frame analysis.

### Setup

1. **Download Tracy**: Get the profiler from https://github.com/wolfpld/tracy/releases
2. **Build with Tracy**: `./setup.sh Profiling`
3. **Start Tracy Server**: Run the Tracy GUI application
4. **Run YAMS**: Tracy automatically connects and captures data

### Usage Example

```bash
# Build with profiling
./setup.sh Profiling
meson compile -C build/profiling

# Run commands (Tracy server should be running)
build/profiling/tools/yams-cli/yams search "query" --limit 20
build/profiling/tools/yams-cli/yams add large_corpus/ --recursive

# Observe in Tracy:
# - Function execution times
# - Call stack flamegraphs
# - Memory allocations
# - Lock contention
```

### Key Areas to Profile

For **PBI-080 Hierarchical Search**:
- `twoStageVectorSearch()` total overhead
- Document grouping (hash map operations)
- Hierarchical boosting calculation
- Final result sorting

For **General Performance**:
- Search query execution paths
- Document ingestion throughput
- Vector index operations
- Metadata repository queries

## Valgrind

Valgrind provides detailed memory profiling, leak detection, and cache analysis.

### Memory Leak Detection

```bash
# Build profiling binary
./setup.sh Profiling
meson compile -C build/profiling

# Run with Valgrind
valgrind --leak-check=full \
         --show-leak-kinds=all \
         --track-origins=yes \
         --verbose \
         --log-file=valgrind-out.txt \
         build/profiling/tools/yams-cli/yams search "test query"

# Check results
less valgrind-out.txt
```

### Callgrind (CPU Profiling)

```bash
# Run with callgrind
valgrind --tool=callgrind \
         --dump-instr=yes \
         --collect-jumps=yes \
         build/profiling/tools/yams-cli/yams search "query"

# Visualize with kcachegrind
kcachegrind callgrind.out.*
```

### Massif (Heap Profiling)

```bash
# Track heap usage over time
valgrind --tool=massif \
         build/profiling/tools/yams-cli/yams add large_directory/ --recursive

# Visualize with massif-visualizer
massif-visualizer massif.out.*
```

#### Daemon under Massif (startup + ingest)

1. Build the profiling daemon: `./setup.sh Profiling && meson compile -C build/profiling`
2. Run the daemon under Massif and give it a few minutes to bring up plugins, repair, and model loads:
   ```bash
   timeout 600s valgrind --tool=massif --stacks=yes --time-unit=ms --detailed-freq=10 \
           --max-snapshots=400 --massif-out-file=/tmp/massif.yams-daemon.out \
           build/profiling/src/daemon/yams-daemon --foreground --log-level=info
   ```
3. While Massif runs, drive a workload from another shell (e.g., ingest a tree):  
   `build/profiling/tools/yams-cli/yams add /path/to/data --recursive`
4. Inspect results: `ms_print /tmp/massif.yams-daemon.out | less` or open in `massif-visualizer`.

Tips:
- Increase `timeout` and `--max-snapshots` for longer captures.
- `--time-unit=ms` helps correlate snapshots with daemon logs.
- If the profiling build is not available, you can substitute the installed daemon binary: replace the last line with `/usr/local/bin/yams-daemon --foreground --log-level=info` (features like TSAN may be disabled).

### Cachegrind (Cache Analysis)

```bash
# Analyze cache behavior
valgrind --tool=cachegrind \
         build/profiling/tools/yams-cli/yams search "query"

# View cache statistics
cg_annotate cachegrind.out.*
```

## Perf (Linux Profiling)

For system-wide profiling on Linux:

```bash
# Record performance data
perf record -g build/profiling/tools/yams-cli/yams search "query"

# Generate report
perf report

# Generate flamegraph (requires flamegraph tools)
perf script | stackcollapse-perf.pl | flamegraph.pl > flamegraph.svg
```

## Fuzzing Build (Stub)

The `Fuzzing` build type is reserved for future AFL++/libFuzzer integration:

```bash
# TODO: Not yet implemented
# ./setup.sh Fuzzing
```

When implemented, this will enable:
- AFL++ instrumentation for coverage-guided fuzzing
- libFuzzer support for targeted fuzzing
- Sanitizer integration (ASan, UBSan, MSan)
- Corpus generation and minimization

Target areas for fuzzing:
- Query parser (malformed queries)
- Content handlers (malicious file formats)
- IPC protocol (invalid protobuf messages)
- Metadata repository (SQL injection vectors)

## Build Comparison

| Build Type | Use Case | Overhead | Tools |
|------------|----------|----------|-------|
| `Debug` | Development | Low | GDB, basic profiling |
| `Release` | Production | None | Minimal instrumentation |
| `Profiling` | Performance analysis | Low-Medium | Tracy, Valgrind, Perf |
| `Fuzzing` | Security testing | High | AFL++, libFuzzer (TODO) |

## Best Practices

1. **Start with Tracy** for interactive, real-time profiling
2. **Use Valgrind** for deep memory analysis and leak detection
3. **Use Perf** for system-wide CPU profiling on Linux
4. **Profile realistic workloads** - synthetic benchmarks may not represent production
5. **Focus on hot paths** - optimize where it matters most
6. **Measure before and after** - validate optimizations with data

## Disabling Profiling

Regular builds have no profiling overhead:

```bash
./setup.sh Debug    # Standard debug build
./setup.sh Release  # Optimized production build
```

## References

- **Tracy**: https://github.com/wolfpld/tracy
- **Valgrind**: https://valgrind.org/docs/manual/
- **Perf**: https://perf.wiki.kernel.org/index.php/Tutorial
- **AFL++**: https://github.com/AFLplusplus/AFLplusplus
- **Flamegraphs**: https://www.brendangregg.com/flamegraphs.html
