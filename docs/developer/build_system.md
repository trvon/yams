# Build System Guide: Conan 2 + CMake on Linux (Best Practices)

This guide explains how to build YAMS robustly on Linux using Conan 2 with CMake, and how to fall back to a pure CMake + system/FETCHContent flow. It consolidates best practices for reproducibility, CI, and developer efficiency.

Goals
- Reproducible and cacheable builds on Linux with Conan 2 and CMake
- Portable configuration via CMakePresets.json and CMakeToolchain/CMakeDeps
- Clear options mapping between Conan and CMake
- Guidance for CI, lockfiles, and common troubleshooting
- Optional API docs via Doxygen (YAMS_BUILD_API_DOCS); off by default; generate with the docs-api target
- CI disables Pandoc-based docs by default (YAMS_BUILD_DOCS=OFF) to avoid extra dependencies on runners

Prerequisites (Linux)
- CMake 3.20+ and Ninja (recommended) or GNU Make
- GCC 11+ or Clang 14+ with C++20
- Conan 2.0+ (pipx or pip)
- Git, pkg-config

Install toolchain
```/dev/null/setup.sh#L1-20
# CMake + Ninja (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build pkg-config git curl

# Conan 2
python3 -m pip install --user pipx
python3 -m pipx ensurepath
pipx install conan

# Verify
cmake --version
ninja --version
conan --version
```

Conan + CMake integration (project conventions)
- The recipe `conanfile.py` uses:
  - CMakeDeps to generate find_package config for dependencies
  - CMakeToolchain to generate `conan_toolchain.cmake` (Do not hardcode a generator in the recipe; use presets)
- Top-level `CMakeLists.txt`:
  - Auto-detects Conan usage if `CMAKE_TOOLCHAIN_FILE` contains `conan_toolchain.cmake`
  - Supports both dependency flows: Conan-backed and FetchContent/system
- `CMakePresets.json`:
  - Includes Ninja/Unix Makefiles presets for Debug/Release
  - Dedicated “conan-” presets expect the Conan-generated toolchain file in the build directory

Quick start: Conan-managed build
1) Detect and initialize a profile
```/dev/null/conan-profile.sh#L1-20
# Create/refresh default profile and prefer GNU libstdc++11 ABI (Linux)
conan profile detect --force

# Optionally set default libc++ (GCC/Clang using libstdc++)
conan profile path default
# If needed: conan profile update 'settings.compiler.libcxx=libstdc++11' default
```

2) Install dependencies into a build dir that matches a CMake preset
```/dev/null/conan-install.sh#L1-20
# Debug (Ninja)
conan install . \
  --output-folder=build/conan-ninja-debug \
  -s build_type=Debug \
  --build=missing

# Release (Ninja)
conan install . \
  --output-folder=build/conan-ninja-release \
  -s build_type=Release \
  --build=missing
```

3) Configure, build, and test using CMake presets
```/dev/null/cmake-with-presets.sh#L1-40
# Debug
cmake --preset conan-ninja-debug
cmake --build --preset build-conan-ninja-debug -j
ctest --preset test-conan-ninja-debug

# Release
cmake --preset conan-ninja-release
cmake --build --preset build-conan-ninja-release -j
ctest --preset test-conan-ninja-release
```

Notes
- The “conan-” presets set `CMAKE_TOOLCHAIN_FILE` to `${buildDir}/conan_toolchain.cmake` and flip `YAMS_USE_CONAN=ON`.
- Conan’s CMakeDeps + CMakeToolchain simplifies CMake to standard `find_package(...)` + `target_link_libraries(...)`.

API Docs (Doxygen) and CI behavior
- API docs (optional): enable with `-DYAMS_BUILD_API_DOCS=ON` (requires `doxygen` in PATH).
- Build target: `docs-api` (HTML output under `build/docs/api/html`).
- CI defaults: `-DYAMS_BUILD_DOCS=OFF` to skip Pandoc-based CLI/manpage generation on runners.
- If you want CLI/manpage docs in CI, enable `YAMS_BUILD_DOCS=ON` and preinstall `pandoc`, or keep it OFF to reduce flakiness.

Building without Conan (FetchContent/system)
This path is intended for environments without Conan or for quick system-package builds.

Install minimal system headers (Ubuntu/Debian)
```/dev/null/system-deps.sh#L1-20
sudo apt-get update
sudo apt-get install -y \
  build-essential cmake ninja-build pkg-config git \
  libssl-dev libsqlite3-dev protobuf-compiler
# ncurses is auto-detected when building the CLI on Linux:
sudo apt-get install -y libncurses-dev
```

Configure, build, test
```/dev/null/cmake-standalone.sh#L1-20
# Ninja generator is faster and recommended
cmake --preset ninja-release
cmake --build --preset build-ninja-release -j
ctest --preset test-ninja-release
```

Feature and options matrix
The project accepts options via either Conan (preferred when using Conan flow) or CMake variables.

- CMake options
  - YAMS_BUILD_CLI: ON/OFF
  - YAMS_BUILD_MCP_SERVER: ON/OFF
  - YAMS_BUILD_TESTS: ON/OFF
  - YAMS_BUILD_BENCHMARKS: ON/OFF
  - YAMS_ENABLE_PDF: ON/OFF
  - YAMS_USE_CONAN: ON/OFF (auto-set when a Conan toolchain is detected)

- Conan options (examples)
```/dev/null/conan-options.sh#L1-40
# Enable tests and benchmarks for a Conan build
conan install . \
  --output-folder=build/conan-ninja-debug \
  -s build_type=Debug \
  -o yams/*:build_tests=True \
  -o yams/*:build_benchmarks=True \
  --build=missing

# Disable CLI and enable MCP server
conan install . \
  --output-folder=build/conan-ninja-release \
  -s build_type=Release \
  -o yams/*:build_cli=False \
  -o yams/*:build_mcp_server=True \
  --build=missing
```

Packaging and installation
- Install artifacts into a prefix
```/dev/null/cmake-install.sh#L1-20
cmake --build build/conan-ninja-release -j
cmake --install build/conan-ninja-release --prefix ./prefix
```
- Consumers can find YAMS with:
```/dev/null/consumer-config.sh#L1-10
# In the consumer project
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH=/path/to/yams/prefix
cmake --build build -j
```
- CPack is configured in the project; you can produce archives after building:
```/dev/null/cpack.sh#L1-10
# From the same build directory (e.g., build/conan-ninja-release)
cpack
```

Reproducibility with lockfiles (Conan)
Commit lockfiles to stabilize dependency versions across CI and dev machines.

```/dev/null/locks.sh#L1-40
# Create a lockfile per configuration
conan lock create . -s build_type=Release --lockfile-out=conan.lock --build=missing
# For Debug similarly:
conan lock create . -s build_type=Debug --lockfile-out=conan-debug.lock --build=missing

# Use the lockfile during installs
conan install . \
  --lockfile=conan.lock \
  --output-folder=build/conan-ninja-release \
  -s build_type=Release \
  --build=missing
```

CI recommendations (Linux)
- Prefer Ninja generator for speed and consistency.
- Cache Conan’s package cache directory (typically ~/.conan2/p) between CI runs.
- Use presets end-to-end for configure/build/test.
- Use lockfiles to pin versions per configuration.
- Fail fast on missing packages except when explicitly allowing `--build=missing`.

Example GH Actions steps (excerpt)
```/dev/null/ci-example.yml#L1-60
- name: Set up dependencies
  run: |
    sudo apt-get update
    sudo apt-get install -y cmake ninja-build pkg-config git
    python3 -m pip install --user pipx
    python3 -m pipx ensurepath
    pipx install conan
    conan profile detect --force

- name: Conan install (Release)
  run: |
    conan install . \
      --output-folder=build/conan-ninja-release \
      -s build_type=Release \
      --build=missing

- name: Configure/Build/Test
  run: |
    cmake --preset conan-ninja-release
    cmake --build --preset build-conan-ninja-release -j
    ctest --preset test-conan-ninja-release --output-on-failure

- name: Install + Package
  run: |
    cmake --install build/conan-ninja-release --prefix ./prefix
    (cd build/conan-ninja-release && cpack)
```

Linux-specific notes and best practices
- Compiler and standard library
  - GCC 11+ is required (C++20); set profile `settings.compiler.libcxx=libstdc++11`.
  - If using Clang on Linux and the project adds `-stdlib=libc++` for Clang, install libc++/libc++abi development packages or switch to libstdc++ by overriding flags in your toolchain/profile as needed.
- RPATH and runtime
  - The project sets sensible install RPATH on Linux; installed binaries should find libs under `lib/`. When running from the build tree, prefer `ctest` to avoid LD_LIBRARY_PATH issues.
- PDF support
  - When `YAMS_ENABLE_PDF=ON`, prebuilt PDFium binaries are fetched for your platform/arch. If network is restricted, disable PDF or pre-provision artifacts.
- Ncurses (CLI)
  - On Linux without Conan, the build will search `ncursesw` (or `ncurses`) and fail with a clear message if missing; install `libncurses-dev`.

Dependency strategy
- Prefer Conan on Linux for:
  - Deterministic versions (lockfiles)
  - Faster incremental builds via cache
  - Avoiding system package drift
- Non-Conan mode (FetchContent/system) is supported for:
  - Rapid prototyping and minimal environments
  - Air-gapped or policy-constrained environments (with local mirrors)

Troubleshooting
- CMake cannot find conan-generated packages
  - Ensure you ran `conan install` to the same build directory used by the “conan-” preset so `conan_toolchain.cmake` exists.
- Clang link errors about libc++/libstdc++
  - Either install libc++/libc++abi or override the standard library flags for Clang to use libstdc++ consistently with your profile.
- ncurses not found (Linux, non-Conan build)
  - Install `libncurses-dev` (Ubuntu/Debian) or the equivalent for your distro.
- OpenSSL not found
  - Install `libssl-dev` (Ubuntu/Debian) or specify `OPENSSL_ROOT_DIR` if non-standard.

Quality checklist (internal)
- [ ] Use CMakePresets.json for local dev and CI
- [ ] Use Conan 2 with CMakeDeps + CMakeToolchain
- [ ] Commit lockfiles for each configuration used in CI
- [ ] Avoid forcing CMake generator in recipe; use presets
- [ ] Pin FetchContent tags for all external dependencies
- [ ] Prefer Ninja on Linux for speed and reliable parallelism
- [ ] Use `-DCMAKE_BUILD_TYPE=Release` for perf-sensitive targets
- [ ] Enable sanitizers only in Debug; keep tooling flags out of Release
- [ ] Ensure `compiler.libcxx=libstdc++11` on Linux (GCC/Clang)

Appendix: Handy one-liners
```/dev/null/one-liners.sh#L1-60
# Full clean (local)
rm -rf build prefix

# Conan Release in one shot
conan install . --output-folder=build/conan-ninja-release -s build_type=Release --build=missing && \
cmake --preset conan-ninja-release && \
cmake --build --preset build-conan-ninja-release -j && \
ctest --preset test-conan-ninja-release --output-on-failure

# Install to prefix and run a downstream consumer
cmake --install build/conan-ninja-release --prefix ./prefix && \
cmake -S test/consumer -B consumer_build -G Ninja -DCMAKE_PREFIX_PATH=$PWD/prefix && \
cmake --build consumer_build -j && \
ctest --test-dir consumer_build --output-on-failure
```

References
- Conan 2: CMakeDeps and CMakeToolchain
- CMake: CMAKE_TOOLCHAIN_FILE, Presets best practices
