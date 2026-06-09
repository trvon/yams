# Performance Profiling & Analysis

YAMS supports multiple profiling and analysis tools to help identify performance bottlenecks and optimize critical paths.

## Measurement Loop

Use profiling as an evidence loop, not as a rewrite trigger:

1. State the question in falsifiable terms: p95/p99 latency, throughput, allocation volume, lock wait, startup time, or stage time.
2. Choose or build a representative workload/benchmark that can be rerun with low noise.
3. Build the right binary for the question: production-like optimization, symbols/frame pointers when needed, and Tracy zones only where they clarify the path.
4. Capture the baseline before code changes.
5. Change one important thing, rerun the same workload, and record the delta plus any trade-off.
6. Document non-fixes: suspicious paths measured and ruled out.

Prefer broad tools first: sampling for CPU, tracing for waiting/stage interaction, heap tools for allocation churn, and hardware counters only after the hot path is proven.

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

For **hierarchical search**:
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

## Fuzzing (AFL++)

YAMS includes AFL++ fuzz harnesses and a Docker-based runner:

```bash
./tools/fuzzing/fuzz.sh build
./tools/fuzzing/fuzz.sh fuzz ipc_roundtrip
```

This setup provides:
- AFL++ instrumentation for coverage-guided fuzzing
- libFuzzer-compat mode harnesses (`-fsanitize=fuzzer`)
- Sanitizer integration via `AFL_USE_ASAN=1` (ASan)
- Corpus + findings directories under `data/fuzz/`

Details and additional targets live in `tools/fuzzing/README.md`.

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
| `Fuzzing` | Security testing | High | AFL++ (Docker-based harnesses) |

## Best Practices

1. **Start with a question and baseline** before opening a profiler.
2. **Start with Tracy** when you need timeline/stage/lock interaction and existing `YAMS_*` zones can answer the question.
3. **Use Perf** for broad CPU sampling on Linux, especially when you need flamegraphs or system-wide context.
4. **Use Heaptrack/Massif** when allocation volume, peak memory, or transient churn is the suspected bottleneck.
5. **Profile realistic workloads** - synthetic benchmarks may not represent production.
6. **Focus on causal hot paths** - optimize where it matters most; do not rewrite code only because it is visible in a profile.
7. **Measure before and after** - validate optimizations with data and keep the same workload.

## Disabling Profiling

Regular builds have no profiling overhead:

```bash
./setup.sh Debug    # Standard debug build
./setup.sh Release  # Optimized production build
```

## References

- KDAB, "C and C++ Profiling Tools: What You Need to Know" — measurement loop and tool selection: https://www.kdab.com/c-cpp-profiling-tools/
- "The Art of Profiling C++ Applications" — ask precise profiling questions, separate CPU work from waiting, and keep representative workloads: https://stofu.io/blog/art-of-profiling-cpp-applications.html
- **Tracy**: https://github.com/wolfpld/tracy
- **Valgrind**: https://valgrind.org/docs/manual/
- **Perf**: https://perf.wiki.kernel.org/index.php/Tutorial
- **AFL++**: https://github.com/AFLplusplus/AFLplusplus
- **Flamegraphs**: https://www.brendangregg.com/flamegraphs.html
