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

YAMS uses out-of-source builds.

Common repo conventions:

- Debug via `./setup.sh Debug`: `builddir/`
- Release via `./setup.sh Release`: `build/release/`
- Other scripted profiles: `build/<profile>/` (for example `build/profiling/`, `build/fuzzing/`)

Manual Meson setup can use a different build directory, but the commands in this repo most often
assume `builddir/` for debug and `build/release/` for release.

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

YAMS uses Meson test **tags** and explicit test names. The current tree does **not** expose a fully
normalized `unit:smoke` / `bench:isolated` taxonomy, so prefer `meson test --list` before relying
on older suite labels from historical docs.

Reliable selectors today:

```bash
# List current tests and tags
meson test -C builddir --list

# Broad tag groups
meson test -C builddir --suite unit
meson test -C builddir --suite integration
meson test -C builddir --suite plugins
meson test -C builddir --suite bench
meson test -C builddir --suite stress

# Common component tags
meson test -C builddir --suite daemon
meson test -C builddir --suite storage
meson test -C builddir --suite search
meson test -C builddir --suite metadata

# Useful named entrypoints
meson test -C builddir integration_smoke
meson test -C builddir storage_submodule
meson test -C builddir storage_submodule_slow
```

#### Current high-signal tags and entrypoints

| Selector | Kind | Meaning |
|----------|------|---------|
| `unit` | tag | Broad unit-test coverage across subsystems |
| `integration` | tag | Integration tests built under `tests/integration/*` |
| `plugins` | tag | Plugin-focused tests, including dedicated plugin binaries |
| `bench` | tag | Benchmarks registered through Meson |
| `stress` | tag/name | Long-running stress coverage |
| `slow` | tag | Explicit slow-path tests kept out of fast defaults |
| `daemon`, `storage`, `search`, `metadata`, `cli`, `extraction`, `integrity` | tag | Component-oriented filtering |
| `integration_smoke` | test name | Small isolated integration smoke binary |
| `storage_submodule` | test name | Fast storage correctness path |
| `storage_submodule_slow` | test name | Explicit slow storage soak path |

Historical docs may still mention labels such as `unit:smoke`, `unit:stable`, `unit:db`,
`unit:vector`, `integration:smoke`, `bench:perf`, or `bench:isolated`. Treat those as
aspirational or stale unless `meson test --list` shows matching current tags/names.

#### Common Test Workflows

```bash
# Developer iteration
meson test -C builddir --suite unit

# Focused subsystem pass
meson test -C builddir --suite daemon
meson test -C builddir --suite storage

# Quick integration sanity check
meson test -C builddir integration_smoke

# Storage fast/slow split
meson test -C builddir storage_submodule
meson test -C builddir storage_submodule_slow

# Plugin-focused coverage
meson test -C builddir --suite plugins

# Benchmarks and stress
meson test -C builddir --suite bench
meson test -C builddir --suite stress

# Specific test by name
meson test -C builddir repair_scheduling_adapter

# Verbose output
meson test -C builddir --suite unit -v

# Parallel execution with custom jobs
meson test -C builddir --suite unit -j 8
```

#### Test Naming Conventions

Preferred naming for new tests is still:

- test names: `<component>_<aspect>_<variant>` where practical
- Meson tags: broad behavior or subsystem labels (`unit`, `daemon`, `storage`, `slow`, ...)

The current tree still mixes:

- focused descriptive names such as `integration_smoke` and `storage_submodule`
- aggregate binaries such as `plugins_catch2` and `integrity_catch2`
- migration-era tags such as `yams-3s4`, `yams-8b9`, and `pbi-009`

So use `meson test --list` as the source of truth when discovering what to run.

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
| Clang: "cannot find -lstdc++" | Install libstdc++ or use `YAMS_COMPILER=gcc` |
| Windows: Boost build failures | Install v143 toolset via VS Installer, clean Boost cache |
| Windows: Missing ONNX Runtime recipe | Export the local onnxruntime recipe (see [Developer Setup](setup.md)) |

## 11. Conventions

Out-of-source only. Keep portable optimized builds on `debugoptimized` + `b_ndebug=true`; reserve more aggressive tuning for explicit local benchmarking. Avoid committing generated build artifacts.

## 12. Test Organization (Active Improvement)

**Current State**: Test registration is broad and valuable, but naming is still mixed. The tree uses
component tags, aggregate executables, explicit slow paths, and some migration-era milestone labels.
Examples visible in `tests/meson.build` today include `plugins_catch2`, `integrity_catch2`,
`integration_smoke`, `storage_submodule`, `storage_submodule_slow`, plus suite tags like
`yams-3s4`, `yams-8b9`, and `pbi-009`.

**In Progress**: test organization standardization is moving toward:

- semantic test names where practical: `<component>_<aspect>_<variant>`
- stable Meson tags for behavior/subsystem filtering
- explicit separation of fast, slow, stress, and benchmark paths
- removal of temporal/milestone coupling from user-facing test discovery

See the repository issues/PRs for the current plan and status.

### Current Test Listing

```bash
# View all tests with current names
meson test -C builddir --list

# Example output will include test names plus suite tags, for example:
# integration_smoke
# plugins_catch2
# storage_submodule
# storage_submodule_slow
```

**Migration in Progress**: test names and tags will keep evolving, so treat `meson test --list` as
canonical and prefer updating docs when registration changes.

---

**Revision notes**:

- Migrated from CMake to Meson as primary build system
- Added comprehensive test organization documentation
- Linked to ongoing test standardization
- Simplified ONNX configuration
