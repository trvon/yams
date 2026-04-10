# Build System (Developer Reference)

Detailed reference for the YAMS build infrastructure, test organization, and CI flow.

For step-by-step build instructions, see [Developer Setup](setup.md). For compiler requirements and platform support, see [BUILD.md](../BUILD.md).

## 1. Stack

| Layer | Tool |
|-------|------|
| Build | Meson ≥ 1.2 |
| Bootstrap | CMake wrapper (`CMakeLists.txt`) |
| Dependencies | Conan 2.x or system/pkg-config/CMake prefixes |
| Generator | Ninja (preferred) |
| Optional | ccache, lld, clang-tidy |

## 2. Directory Layout
Out-of-source builds under `builddir/`:
- Debug: `builddir/` (default with Meson)
- Release: `build-release/`

## 3. ONNX / GenAI Paths
Options (Conan scope):
- `enable_onnx` (default True) — toggles embedding / GenAI integration
- `use_conan_onnx` (default False) — when True pull packaged ORT; when False allow system ORT

Meson options:
- `enable-onnx` — mirrors `enable_onnx`
- `onnx-runtime-path` — optional path to system ONNX Runtime

## 4. Configure & Build
```bash
# Using Meson (preferred)
meson setup builddir --buildtype=debugoptimized -Db_ndebug=true
ninja -C builddir

# Install
sudo meson install -C builddir
```

For integrator-style builds, prefer one of these entrypoints:

```bash
# Default bootstrap path (Conan + Meson)
./setup.sh Release

# Cache-only/offline Conan path
./setup.sh Release --offline

# System dependency path (no Conan, no network)
YAMS_USE_SYSTEM_DEPS=true \
YAMS_OFFLINE=true \
YAMS_PKG_CONFIG_PATH=/opt/yams-deps/lib/pkgconfig \
YAMS_CMAKE_PREFIX_PATH=/opt/yams-deps \
./setup.sh Release --system-deps --offline
```

## 5. Tests

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
meson test -C builddir repair_scheduling_adapter

# Verbose output
meson test -C builddir --suite unit:smoke -v

# Parallel execution with custom jobs
meson test -C builddir --suite unit -j 8
```

#### Test Naming Conventions

Tests follow the pattern: `<component>_<aspect>_<variant>`

Examples:
- `content_store_basic` - Basic content store operations
- `repair_scheduling_adapter` - Repair scheduling adapter coverage
- `metadata_repository_cache` - Metadata repository caching
- `daemon_warm_latency` - Daemon warm start latency benchmark

**Note**: Test organization and suite naming are being standardized; treat suite labels as best-effort until this work completes.

## 6. Key Build Options

| Meson Option | Purpose |
|--------------|---------|
| `build-cli` | Build command-line interface |
| `build-mcp-server` | Build MCP server |
| `enable-tests` / `enable-benchmarks` | Test/benchmark builds |
| `enable-onnx` | Enable ONNX vector/GenAI paths |
| `plugin-symbols` | Tree-sitter symbol extraction plugin |
| `enable-sanitizers` | Debug ASan/UBSan toggle |

Configure options:
```bash
meson setup builddir -Denable-onnx=true -Dbuild-cli=true
```

## 7. Dependency Management Notes

Edit `conanfile.py` then re-run `conan install` for each configuration. Prefer pinned versions. For sandboxed or distro builds, skip Conan entirely and use `--system-deps` with explicit pkg-config/CMake prefixes.

## 8. Common Scenarios

| Scenario | Commands |
|---------|----------|
| Fresh debug loop | Conan install (Debug) → meson setup → ninja → meson test |
| System-only (no Conan) | Provide all libs, `--wrap-mode=nofallback`, explicit pkg-config/CMake prefixes |
| Release packaging | Conan install (Release) → meson setup → ninja → package |

## 9. CI Flow

```bash
conan profile detect --force
conan install . -of build/release -s build_type=Release --build=missing
meson setup build/release --native-file build/release/conan_meson_native.ini --buildtype=debugoptimized -Db_ndebug=true
ninja -C build/release
meson test -C build/release
```

Cache `~/.conan` + optionally `build/`.

## 10. Troubleshooting Quick Table

| Symptom | Fix |
|---------|-----|
| Missing native file | Re-run Conan install (regenerates native file) |
| ONNX disabled | Check `enable-onnx` option + provide system ORT path if needed |
| Link errors after dep change | Delete `builddir` and reconfigure from scratch |
| Tests undiscovered | Use `meson test -C builddir --list`; ensure `enable-tests=true` |
| Slow incremental builds | Enable ccache or check Unity build settings |
| qpdf: "recompile with -fPIC" | `conan remove 'qpdf/*' -c` then re-setup |
| Clang: "cannot find -lstdc++" | Install libstdc++ or use `YAMS_COMPILER=gcc` |
| Windows: Boost build failures | Install v143 toolset via VS Installer, clean Boost cache |
| Windows: Missing recipes | Export qpdf and onnxruntime recipes (see [Developer Setup](setup.md)) |

## 11. Conventions

Out-of-source only. Keep portable optimized builds on `debugoptimized` + `b_ndebug=true`; reserve more aggressive tuning for explicit local benchmarking. Avoid committing generated build artifacts.

## 12. Test Organization (Active Improvement)

**Current State**: Test names and suites have some inconsistencies (e.g., `unit_isolated_*` tests in bench suite, PBI numbers in names).

**In Progress**: PBI-064 is standardizing test organization with:
- Semantic naming: `<component>_<aspect>_<variant>`
- Hierarchical suites: `<category>[+<tag>]`
- Clear categorization: unit/integration/bench/acceptance
- Removal of temporal coupling (no PBI numbers in test names)

See the repository issues/PRs for the current plan and status.

### Current Test Listing

```bash
# View all tests with current names
meson test -C builddir --list

# Example output shows current organization:
# yams:unit / unit_shard0
# yams:unit / unit_shard1
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
