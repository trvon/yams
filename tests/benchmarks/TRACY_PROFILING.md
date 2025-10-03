# Tracy Profiler Integration for YAMS Benchmarks

## Overview

YAMS benchmarks are instrumented with [Tracy Profiler](https://github.com/wolfpld/tracy) to provide real-time, nanosecond-resolution profiling of service layer operations. This integration helps identify performance bottlenecks that benchmarks alone might miss.

## Prerequisites

### 1. Build Tracy Profiler Server

Clone and build the Tracy profiler GUI:

```bash
git clone https://github.com/wolfpld/tracy.git
cd tracy/profiler/build/unix
make release
```

The profiler executable will be at `tracy/profiler/build/unix/Tracy-release`.

### 2. Enable Tracy in YAMS Build

Tracy integration is **optional** and controlled by the presence of the `tracy` dependency:

```bash
# Option 1: Use system Tracy (if installed)
meson setup build --buildtype=debugoptimized

# Option 2: Use Tracy as a subproject (if wrap file exists)
meson setup build --buildtype=debugoptimized --wrap-mode=forcefallback

# Option 3: Disable Tracy (benchmarks run without profiling)
# Tracy is automatically disabled if dependency not found
meson setup build --buildtype=release
```

**Recommended build type:** `debugoptimized` (optimized code with debug symbols for profiling).

## Running Benchmarks with Tracy

### 1. Start Tracy Server First

Launch the Tracy GUI on port 8086 (default):

```bash
./Tracy-release
```

Click **"Connect"** in the Tracy window and wait for client connection.

### 2. Run Benchmarks

Execute benchmarks **after** starting Tracy server:

```bash
# Run retrieval service benchmarks with Tracy profiling
cd build
./tests/benchmarks/yams_retrieval_service_benchmarks

# Run other benchmarks
./tests/benchmarks/yams_api_benchmarks
./tests/benchmarks/yams_search_benchmarks
```

### 3. Analyze Results

In the Tracy GUI:

- **Zones View**: See hierarchical call stacks with timing (`ZoneScoped` macros)
- **Statistics**: Aggregated zone durations, P95/P99 latencies
- **Find Zone**: Search for specific functions (e.g., `getByNameSmart_call`)
- **Compare**: Diff traces before/after optimizations

## Instrumented Code Paths

YAMS benchmarks instrument critical service layer operations:

### Retrieval Service Benchmarks
- `GetByName_FTS5Ready`: Tests nominal case (index ready)
  - Zone: `getByNameSmart_call` - Direct retrieval timing
- `GetByName_FTS5Backlogged`: Tests fast-fail path (index not ready)
  - Zone: `getByNameSmart_backlogged` - Queue backlog handling
- `GetByName_P95Latency`: 95th percentile latency under varied load
- `GrepAfterAdd_Latency`: grep immediately after document add
- `GetByName_Concurrent`: Multi-threaded stress test

### API Benchmarks
- (Future) Document add operations
- (Future) Batch ingestion pipelines

### Search Benchmarks
- (Future) FTS5 query performance
- (Future) Hybrid search (semantic + keyword)

## Adding Tracy Instrumentation to New Code

### Basic Zone Profiling

```cpp
#ifdef TRACY_ENABLE
#include <tracy/Tracy.hpp>
#endif

void myFunction() {
#ifdef TRACY_ENABLE
    ZoneScoped;  // Automatically profiles this function
#endif
    // Your code here
}
```

### Named Zones

```cpp
void complexOperation() {
#ifdef TRACY_ENABLE
    ZoneScopedN("ComplexOperation");  // Named zone for clarity
#endif
    
    // Critical section
#ifdef TRACY_ENABLE
    {
        ZoneScopedN("CriticalPath");
        // Hot path code
    }
#endif
}
```

### Color-Coded Zones

```cpp
#ifdef TRACY_ENABLE
    ZoneScopedC(tracy::Color::Red);  // Red = performance-critical
#endif
```

## Performance Targets (from PBI-040)

| Benchmark | Target Latency | Threshold |
|-----------|----------------|-----------|
| GetByName_FTS5Ready | < 500ms | Single-op retrieval |
| GetByName_FTS5Backlogged | < 1000ms | Fast error path |
| GetByName_P95Latency | < 2000ms | 95th percentile |
| GrepAfterAdd_Latency | < 500ms | With sync indexing (040-4) |

Tracy profiling helps validate these targets by:
1. **Identifying hot paths** (which functions dominate latency)
2. **Detecting contention** (lock waits, queue stalls)
3. **Validating async behavior** (PostIngestQueue processing)
4. **Tracking regressions** (compare traces across commits)

## Troubleshooting

### Tracy Not Found

If Meson reports `tracy` dependency not found:

```bash
# Check available dependencies
meson introspect build --dependencies

# Install Tracy system-wide (optional)
# Ubuntu/Debian:
sudo apt install tracy-profiler

# macOS (via Homebrew):
brew install tracy
```

Alternatively, create a Meson wrap file in `subprojects/tracy.wrap`:

```ini
[wrap-git]
url = https://github.com/wolfpld/tracy.git
revision = v0.10
depth = 1

[provide]
tracy = tracy_dep
```

### Benchmarks Hang Waiting for Tracy

If Tracy server is not running, benchmarks will **not** hang - Tracy uses on-demand connection:

```cpp
#ifdef TRACY_ON_DEMAND
    // Only profiles when Tracy server is connected
#endif
```

To disable on-demand mode (always profile, even without server):

```bash
meson configure build -Dtracy:on_demand=false
```

### No Zones Visible in Tracy GUI

1. **Check `TRACY_ENABLE` is defined**:
   ```bash
   grep -r "TRACY_ENABLE" build/compile_commands.json
   ```

2. **Verify Tracy client is linked**:
   ```bash
   ldd build/tests/benchmarks/yams_retrieval_service_benchmarks | grep tracy
   ```

3. **Ensure zones are instrumented** (search for `ZoneScoped` in code)

## References

- [Tracy Manual](https://github.com/wolfpld/tracy/releases/latest/download/tracy.pdf)
- [Tracy GitHub](https://github.com/wolfpld/tracy)
- [PBI-040: Post-Ingest Performance & Query Responsiveness](../../docs/delivery/040/prd.md)
- [Task 040-8: Add service layer performance benchmarks](../../docs/delivery/040/tasks.md)

## Related PBIs

- **PBI-040**: Performance improvements (FTS5 readiness, sync indexing)
- **PBI-001**: Core functionality (UTF-8 grep fix)

Last updated: 2025-09-30
