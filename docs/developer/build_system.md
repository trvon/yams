# Build System (Developer Reference)

Compact overview of how to build, test, and extend YAMS. Start with the **Quick Loop**, then dive deeper.

## 1. Stack
| Layer | Tool |
|-------|------|
| Build | Meson ≥ 1.2 |
| Dependencies | Conan 2.x |
| Generator | Ninja (preferred) |
| Compilers | Clang ≥14 / GCC ≥11 |
| Optional | ccache, lld, clang-tidy |

## 2. Directory Layout
Out-of-source builds under `builddir/`:
- Debug: `builddir/` (default with Meson)
- Release: `build-release/`

## 3. One-Time: Profile
```bash
conan profile detect --force
```

## 4. Quick Loop

**Using setup script (recommended):**
```bash
# Debug build (includes tests)
./setup.sh Debug
meson compile -C builddir

# Test
meson test -C builddir
```

**Manual (Conan + Meson):**
```bash
# Debug dependencies
conan install . -of build/debug -s build_type=Debug -b missing

# Initial configure
meson setup builddir \
  --prefix /usr/local \
  --native-file build/debug/build-debug/conan/conan_meson_native.ini

# Build
ninja -C builddir

# Test
meson test -C builddir
```

See [setup.sh](../../setup.sh) for environment variables and advanced options (coverage, cross-compilation, compiler selection).

## 5. ONNX / GenAI Paths
Options (Conan scope):
- `enable_onnx` (default True) — toggles embedding / GenAI integration
- `use_conan_onnx` (default False) — when True pull packaged ORT; when False allow system ORT

Meson options:
- `enable-onnx` — mirrors `enable_onnx`
- `onnx-runtime-path` — optional path to system ONNX Runtime

## 6. Configure & Build
```bash
# Using Meson (preferred)
meson setup builddir
ninja -C builddir

# Install
sudo meson install -C builddir
```

## 7. Tests

### Meson Test System (Primary)

YAMS uses Meson for test organization with hierarchical suite structure:

```bash
# Quick smoke tests (< 2min)
meson test -C builddir --suite unit:smoke

# Fast unit tests (sharded for parallel execution)
meson test -C builddir --suite unit

# Database-heavy tests
meson test -C builddir --suite unit:db

# Integration tests
meson test -C builddir --suite integration:smoke

# Performance benchmarks
meson test -C builddir --suite bench:perf

# List all available tests
meson test -C builddir --list
```

#### Test Categories

| Category | Suite | Description | Typical Runtime |
|----------|-------|-------------|-----------------|
| **Unit** | `unit:fast` | Fast, isolated unit tests | < 30s per shard |
| | `unit:slow` | Heavy unit tests (DB, lifecycle) | 2-5 min |
| | `unit:db` | Database-intensive tests | 1-3 min |
| | `unit:vector` | Vector database tests | 1-2 min |
| | `unit:smoke` | Quick validation subset | < 2 min |
| | `unit:stable` | CI-safe gate tests | < 5 min |
| **Integration** | `integration:smoke` | Basic integration workflows | 2-5 min |
| | `integration:services` | Service layer integration | 5-10 min |
| | `integration:daemon` | Daemon lifecycle tests | 5-10 min |
| | `integration:cli` | CLI command tests | 3-5 min |
| **Bench** | `bench:perf` | Performance benchmarks | 5-15 min |
| | `bench:stress` | Load/stress tests | 10-30 min |
| | `bench:isolated` | Heavy single-process tests | 5-10 min |
| **Acceptance** | `acceptance` | End-to-end validation | 10-20 min |

#### Common Test Workflows

```bash
# Developer iteration (fastest feedback)
meson test -C builddir --suite unit:smoke

# Pre-commit validation (stable subset)
meson test -C builddir --suite unit:stable

# Full unit test suite (sharded)
meson test -C builddir --suite unit

# Integration smoke (quick integration check)
meson test -C builddir --suite integration:smoke

# Run ALL benchmarks (unified suite)
meson test -C builddir --suite bench

# Run only isolated benchmarks
meson test -C builddir --suite bench:isolated

# Run only performance benchmarks (exclude isolated)
meson test -C builddir --suite bench:perf

# Specific test by name
meson test -C builddir repair_coordinator_lifecycle

# Pattern matching
meson test -C builddir --suite unit --gtest_filter="*RepairCoordinator*"

# Verbose output
meson test -C builddir --suite unit:smoke -v

# Parallel execution with custom jobs
meson test -C builddir --suite unit -j 8
```

#### Test Naming Conventions

Tests follow the pattern: `<component>_<aspect>_<variant>`

Examples:
- `content_store_basic` - Basic content store operations
- `repair_coordinator_lifecycle` - Full repair coordinator lifecycle
- `metadata_repository_cache` - Metadata repository caching
- `daemon_warm_latency` - Daemon warm start latency benchmark

**Note**: See `docs/delivery/064/` (PBI-064) for test organization standardization in progress.

## 8. Key Build Options

| Meson Option | Purpose |
|--------------|---------|
| `build-cli` | Build command-line interface |
| `build-mcp-server` | Build MCP server |
| `enable-tests` / `enable-benchmarks` | Test/benchmark builds |
| `enable-onnx` | Enable ONNX vector/GenAI paths |
| `enable-pdf` | PDF extraction support |
| `enable-sanitizers` | Debug ASan/UBSan toggle |

Configure options:
```bash
meson setup builddir -Denable-onnx=true -Dbuild-cli=true
```

## 9. Dependency Management Notes

Edit `conanfile.py` then re-run `conan install` for each configuration. Prefer pinned versions. Use `--build=missing` to compile absent binaries.

## 10. Common Scenarios

| Scenario | Commands |
|---------|----------|
| Fresh debug loop | Conan install (Debug) → meson setup → ninja → meson test |
| System-only (no Conan) | Provide all libs, configure manually with flags |
| Release packaging | Conan install (Release) → meson setup → ninja → package |

## 11. CI Flow

```bash
conan profile detect --force
conan install . -of build/release -s build_type=Release --build=missing
meson setup build/release --native-file build/release/conan_meson_native.ini
ninja -C build/release
meson test -C build/release
```

Cache `~/.conan` + optionally `build/`.

## 12. Troubleshooting Quick Table

| Symptom | Fix |
|---------|-----|
| Missing native file | Re-run Conan install (regenerates native file) |
| ONNX disabled | Check `enable-onnx` option + provide system ORT path if needed |
| Link errors after dep change | Delete `builddir` and reconfigure from scratch |
| Tests undiscovered | Use `meson test -C builddir --list`; ensure `enable-tests=true` |
| Slow incremental builds | Enable ccache or check Unity build settings |

## 13. Conventions

Out-of-source only. Keep Release for perf/benchmarks; iterate in Debug. Avoid committing generated build artifacts.

## 14. Test Organization (Active Improvement)

**Current State**: Test names and suites have some inconsistencies (e.g., `unit_isolated_*` tests in bench suite, PBI numbers in names).

**In Progress**: PBI-064 is standardizing test organization with:
- Semantic naming: `<component>_<aspect>_<variant>`
- Hierarchical suites: `<category>[+<tag>]`
- Clear categorization: unit/integration/bench/acceptance
- Removal of temporal coupling (no PBI numbers in test names)

See `docs/delivery/064/prd.md` for the full plan.

### Current Test Listing

```bash
# View all tests with current names
meson test -C builddir --list

# Example output shows current organization:
# yams:unit / unit_shard0
# yams:unit / unit_shard1
# yams:bench+isolated / unit_isolated_repair_coordinator
# yams:unit+smoke / unit_smoke
# yams:integration+smoke / integration_smoke
```

**Migration in Progress**: Test names will be gradually updated to follow the new conventions while maintaining backward compatibility.

---

**Revision notes**:
- Migrated from CMake to Meson as primary build system
- Added comprehensive test organization documentation
- Linked to PBI-064 for ongoing test standardization
- Simplified ONNX configuration
