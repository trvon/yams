# Developer Build System

Concise overview of YAMS build and dependency tooling for contributors.

## Stack

- Build system: CMake (3.25+)
- Dependency manager: Conan 2.x
- Generators: Ninja (recommended), Unix Makefiles supported
- Compilers: Clang ≥14 or GCC ≥11 (macOS/Linux)
- Optional: ccache for faster rebuilds

## Build directories and presets

Conan generates CMakePresets and an out-of-source build tree per configuration:

- Release:
  - Conan output: build/conan-release
  - Configure preset: conan-release
  - Inner CMake build dir: build/conan-release/build/Release (multi-config) or build/conan-release/build (single-config generators)
- Debug:
  - Conan output: build/conan-debug
  - Configure preset: conan-debug
  - Inner build dir: build/conan-debug/build/Debug (multi-config) or build/conan-debug/build

Notes:
- Paths may differ slightly depending on the generator; use the preset’s inner build path for ctest and artifacts.
- Keep build/* out of version control.

## One-time setup

Detect a Conan profile for your host toolchain:

```bash
conan profile detect --force
```

## Install dependencies

Conan resolves and fetches third-party libraries and generates CMake presets.

Release:

```bash
conan install . --output-folder=build/conan-release -s build_type=Release --build=missing
```

Debug:

```bash
conan install . --output-folder=build/conan-debug -s build_type=Debug --build=missing
```

Tips:
- Append --build=missing to build packages not available precompiled.
- Use a custom profile via -pr:h=<host> -pr:b=<build> as needed.

## Configure and build

CMake configure (from repo root):

```bash
cmake --preset conan-release
# or
cmake --preset conan-debug
```

Build:

```bash
cmake --build --preset conan-release -j
# or
cmake --build --preset conan-debug -j
```

Install (optional):

```bash
sudo cmake --install build/conan-release/build/Release
# set a custom prefix with -DCMAKE_INSTALL_PREFIX=/opt/yams when configuring
```

## Tests

Run tests with ctest from the inner build directory:

```bash
# Debug
ctest --output-on-failure --test-dir build/conan-debug/build/Debug -j

# Release
ctest --output-on-failure --test-dir build/conan-release/build/Release -j
```

If your generator is single-config, omit the trailing /Debug or /Release.

## Build options and toolchain notes

- Generator selection: set CMAKE_GENERATOR (e.g., Ninja) in your environment or profile.
- Install prefix: set CMAKE_INSTALL_PREFIX at configure time.
- Werror/code style: adhere to the project’s CMake defaults; do not relax warnings locally unless troubleshooting.
- Sanitizers: enable via your toolchain or add flags in a local cache:
  - Example (single-config): -DCMAKE_C_FLAGS_DEBUG="-fsanitize=address,undefined" -DCMAKE_CXX_FLAGS_DEBUG="-fsanitize=address,undefined"
  - For multi-config generators, use CMAKE_<LANG>_FLAGS_<CONFIG>.
- ccache: export CC="ccache clang" and CXX="ccache clang++" (or set via toolchain/profile) to speed up rebuilds.

## Dependency management (Conan)

- Dependencies are declared in the project’s conanfile (e.g., conanfile.py/conanfile.txt).
- Change dependencies by editing the conanfile; then re-run conan install for each configuration output-folder.
- Pinning: prefer exact versions and revisions where feasible; consider lockfiles for reproducible CI.
- Editable packages: use conan editable add for local development of dependent packages.

Example round-trip after editing dependencies:

```bash
# Release
conan install . --output-folder=build/conan-release -s build_type=Release --build=missing
cmake --preset conan-release
cmake --build --preset conan-release -j
```

## CI-friendly flow

```bash
conan profile detect --force
conan install . --output-folder=build/conan-release -s build_type=Release --build=missing
cmake --preset conan-release
cmake --build --preset conan-release -j$(nproc)
ctest --output-on-failure --test-dir build/conan-release/build/Release -j$(nproc)
```

Cache:
- Cache ~/.conan and optionally the build/ directory (if your CI preserves workspaces) to reduce cold start time.

## Docker (optional)

Run the published image for CLI checks:

```bash
docker run --rm -it ghcr.io/trvon/yams:latest --version
```

Persist data with a bind mount:

```bash
mkdir -p $HOME/yams-data
docker run --rm -it -v $HOME/yams-data:/var/lib/yams ghcr.io/trvon/yams:latest yams init --non-interactive
```

For containerized builds, use a dev image with toolchains or mount your build cache and run the Conan/CMake flow inside.

## Troubleshooting

- Missing compiler/CMake/Ninja:
  - Ensure they’re on PATH; re-run conan profile detect.
- Preset not found:
  - Re-run conan install to regenerate presets.
- Link errors after dependency changes:
  - Clean or re-generate: delete the build/conan-*/CMakeCache.txt or start with a fresh output-folder.
- Tests not discovered:
  - Run ctest from the correct inner build directory; confirm enable_testing/add_test in the codebase.
- ABI or stdlib mismatches:
  - Align compiler versions and libc++/libstdc++ across your profile and environment.

## Conventions

- Out-of-source builds only (build/*).
- Do not commit local cache or build artifacts.
- Keep Release builds for benchmarks and distribution; Debug for development.
- Prefer presets over ad-hoc build commands for repeatability.