# Building YAMS with GCC/G++

YAMS prefers Clang + LLD when present; these instructions cover the fully supported GCC flow. If LLVM is not detected the presets transparently fall back to GCC.

Use this file as a quick reference. Progresses from "10‑second build" to deeper detail.

## 1. Prerequisites (Minimal)

Compiler:
- GCC 11+ (coroutines / C++20)
- GCC 13+ recommended (full `std::format`)

Quick check:
```bash

```

Fast build helpers (optional but recommended):
```bash
# Ubuntu/Debian
sudo apt-get install -y ninja-build ccache lld clang-tidy
# Fedora
sudo dnf install -y ninja-build ccache lld clang-tools-extra
# Arch
sudo pacman -S --needed ninja ccache lld clang
```
If `lld` or `clang-tidy` are missing the presets still work (they silently drop related flags).

Install a newer GCC (examples):
* Ubuntu: `sudo apt-get install gcc-13 g++-13` (enable via update-alternatives if desired)
* Fedora / Arch: distro packages are recent
* RHEL/CentOS/Rocky: enable devtoolset (`gcc-toolset-13`)
## 2. System Packages

Core dev libs (names by distro): OpenSSL, libcurl, sqlite3, ncurses (for TUI), protobuf compiler + dev headers, zlib.

Ubuntu example:
```bash
sudo apt-get install -y build-essential cmake pkg-config libssl-dev libcurl4-openssl-dev libsqlite3-dev libncurses-dev protobuf-compiler libprotobuf-dev
```
Other distros: use analogous `*-devel` / package names.

## 3. Quick Start (Conan + Presets)

```bash
# Debug (Unity build, sanitizers, tests optional)
conan install . -of build/yams-debug -s build_type=Debug -b missing \
  -o yams/*:enable_onnx=True -o yams/*:use_conan_onnx=True
cmake --preset yams-debug
cmake --build --preset yams-debug -j

# Release (LTO/IPO)
conan install . -of build/yams-release -s build_type=Release -b missing \
  -o yams/*:enable_onnx=True -o yams/*:use_conan_onnx=True
cmake --preset yams-release
cmake --build --preset yams-release -j

# Tests (Debug)
ctest --preset yams-debug --output-on-failure
```

Set parallelism: `export CMAKE_BUILD_PARALLEL_LEVEL=$(nproc)` (Linux) or pass `-j` to build step.

GenAI headers:
If the packaged ONNX Runtime lacks GenAI C++ headers, YAMS automatically
downloads the lightweight `onnxruntime-genai` headers (v0.9.1) at configure
time (no additional flags). You will see a provider log entry
`onnxruntime-genai headers provided (v0.9.1)` on success.

### Plugin & Targeted Tests

Explicitly build plugin/unit test targets first (ctest does not auto-build):
```bash
cmake --build --preset yams-debug --target s3_signer_tests object_storage_adapter_tests
ctest --preset yams-debug -R S3SignerUnitTests --output-on-failure
ctest --preset yams-debug -R ObjectStorageAdapterUnitTests --output-on-failure
```
S3 smoke (networked, optional):
```bash
cmake --build --preset yams-debug --target s3_plugin_smoke_test
ctest --preset yams-debug -R s3_plugin_smoke_test --output-on-failure
```
Enable integration smoke: `-DYAMS_TEST_S3_PLUGIN_INTEGRATION=ON` at configure time.

Integration tests (S3 / ONNX) are disabled by default to keep local loops fast.

### No-Conan (System Dependencies Only)
Presets: `yams-debug-no-conan`, `yams-release-no-conan` (assumes you supply all libs and set `YAMS_ENABLE_ONNX=ON` + provide ORT via `CMAKE_PREFIX_PATH` or internal build flag).
```bash
cmake --preset yams-debug-no-conan
cmake --build --preset yams-debug-no-conan -j
```

## 4. Defaults & Tooling (Debug)

Debug presets enable:
- Unity builds (faster iterative compile)
- Address/UB sanitizers (`-DYAMS_ENABLE_SANITIZERS=OFF` to disable)
- clang-tidy (disable per-config: `-D CMAKE_CXX_CLANG_TIDY=`)

If `ccache` present it is leveraged via environment/toolchain; if absent nothing breaks.

## 5. Manual (Without Presets)

If you prefer manual configuration without presets:

### Basic Build

```bash
# Clone the repository
git clone https://github.com/trvon/yams.git
cd yams

# Create build directory
mkdir build && cd build

# Configure with GCC
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DYAMS_BUILD_PROFILE=release

# Build
make -j$(nproc)

# Install (optional)
sudo make install
```

### Development Build (Debug + tests)

For development with tests and debugging:

```bash
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Debug \
    -DYAMS_BUILD_PROFILE=dev \
    -DYAMS_BUILD_TESTS=ON \
    -DYAMS_ENABLE_SANITIZERS=ON

make -j$(nproc)

# Run tests
ctest --output-on-failure
```

### Disable CURL

If you don't need HTTP download features, you can build without CURL:

```bash
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DYAMS_REQUIRE_CURL=OFF

make -j$(nproc)
```

Note: When YAMS_REQUIRE_CURL=OFF, HTTP downloader features will be unavailable.

### Coverage

For code coverage analysis:

```bash
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Debug \
    -DYAMS_BUILD_PROFILE=dev \
    -DYAMS_BUILD_TESTS=ON \
    -DYAMS_ENABLE_COVERAGE=ON

make -j$(nproc)

# Run tests to generate coverage data
ctest

# Generate coverage report
gcovr --root .. \
    --exclude '_deps/*' \
    --exclude 'tests/*' \
    --html --html-details \
    --output coverage.html
```

## 6. Compiler Notes

### std::format

YAMS automatically detects std::format availability:

- GCC 13+: Full std::format support
- GCC 11–12: Falls back to fmt library
- The build system will automatically configure the appropriate option

### Coroutines

GCC has full C++20 coroutine support starting from version 11.0. The build system automatically adds the `-fcoroutines` flag when using GCC.

### LTO

For optimal release builds with LTO:

```bash
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-flto" \
    -DCMAKE_EXE_LINKER_FLAGS="-flto"
```

## 7. Troubleshooting

### "std::format not found"

Solution: This is expected with GCC < 13. The build will automatically use the fmt library as a fallback.

### "ncurses library not found"

Solution: Install ncurses development package:
```bash
# Ubuntu/Debian
sudo apt-get install libncurses-dev

# RHEL/CentOS
sudo yum install ncurses-devel
```

### Coroutine compilation errors

Solution: Ensure you're using GCC 11 or later:
```bash
g++ --version
```

If using an older version, upgrade GCC or use a developer toolset.

### Undefined references during linking

Solution: Ensure all dependencies are installed and pkg-config can find them:
```bash
pkg-config --list-all | grep -E "(sqlite|openssl|protobuf|libcurl)"
```

### "Could NOT find CURL"

Solution: Either install libcurl development package:
```bash
# Ubuntu/Debian
sudo apt-get install libcurl4-openssl-dev

# RHEL/CentOS
sudo yum install libcurl-devel
```

Or build without CURL support:
```bash
cmake .. -DYAMS_REQUIRE_CURL=OFF
```

### ONNX / GenAI disabled

Causes and fixes:
- ONNX disabled at configure time. Enable explicitly:
  - Conan: add `-o yams/*:enable_onnx=True` to your `conan install` (default in bundled profiles).
  - Plain CMake: pass `-DYAMS_ENABLE_ONNX=ON`.
- `onnxruntime` not found. Provide it to CMake:
  - Install dev packages/libraries, or
  - Set `-DCMAKE_PREFIX_PATH=/path/to/onnxruntime` so `find_package(onnxruntime REQUIRED)` succeeds.
- Validate your configure log shows: `ONNX Runtime found - enabling local embedding generation`.

## 8. Performance Tips

For best performance with GCC:

```bash
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-O3 -march=native -mtune=native -flto" \
    -DCMAKE_EXE_LINKER_FLAGS="-flto -Wl,-O1"
```

Note: `-march=native` optimizes for your specific CPU but makes binaries non-portable.

## 9. Quick Verification

After building, verify the installation:

```bash
# Check the binary
./tools/yams-cli/yams --version

# Run a simple test
echo "Hello, YAMS!" | ./tools/yams-cli/yams add -

# If installed system-wide
yams --version
```

## 10. CI Snapshot

YAMS CI/CD pipeline tests GCC builds on Ubuntu. The configuration used in CI can be found in `.github/workflows/ci.yml` under the "traditional" build matrix entry.

## 11. References

- GCC Documentation: https://gcc.gnu.org/onlinedocs/
- GCC C++20 Status: https://gcc.gnu.org/projects/cxx-status.html#cxx20
- CMake Presets: https://cmake.org/cmake/help/latest/manual/cmake-presets.7.html
- YAMS Build Options: BUILD.md

## 12. Sanitizer Example (Standalone)
```bash
cmake -S . -B build/asan -G Ninja \
  -DCMAKE_TOOLCHAIN_FILE=build/asan/generators/conan_toolchain.cmake \
  -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=ON
cmake --build build/asan -j
ctest --test-dir build/asan --output-on-failure
```

---
Revision highlights in this doc:
- Added internal ONNX Runtime path (`use_conan_onnx=False` + `YAMS_BUILD_INTERNAL_ONNXRUNTIME=ON`).
- Reordered for progressive disclosure (quick start → detail → troubleshooting).
- Condensed distro package lists & redundant wording.
