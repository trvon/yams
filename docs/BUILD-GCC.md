# Building YAMS with GCC/G++ or Clang/LLVM

YAMS supports both GCC and Clang/LLVM toolchains with automatic fallback. When using Clang, the build system automatically prefers the LLD linker for faster linking. These instructions cover both flows.
Use this file as a quick reference. Progresses from "10‑second build" to deeper detail.

## 1. Prerequisites (Minimal)

Compiler (choose one or both):yams c
- **GCC**: GCC 11+ (coroutines / C++20), GCC 13+ recommended (full `std::format`)
- **Clang/LLVM**: Clang 14+ (C++20), Clang 16+ recommended

Quick check:
```bash
# GCC
gcc --version
g++ --version

# Clang/LLVM
clang --version
clang++ --version
```

Fast build helpers (optional but recommended):
```bash
# Ubuntu/Debian
sudo apt-get install -y ninja-build ccache lld clang clang-tidy
# Fedora
sudo dnf install -y ninja-build ccache lld clang clang-tools-extra
# Arch
sudo pacman -S --needed ninja ccache lld clang
```
If `lld`, `clang`, or `clang-tidy` are missing, the build will fall back to available tools.

Install a newer GCC (if needed):
* Ubuntu: `sudo apt-get install gcc-13 g++-13` (enable via update-alternatives if desired)
* Fedora / Arch: distro packages are recent
* RHEL/CentOS/Rocky: enable devtoolset (`gcc-toolset-13`)
That 
Install Clang/LLVM (if not present):
* Ubuntu: `sudo apt-get install clang lld`
* Fedora: `sudo dnf install clang lld`
* Arch: `sudo pacman -S clang lld`
## 2. System Packages

Core dev libs (names by distro): OpenSSL, libcurl, sqlite3, protobuf compiler + dev headers, zlib.

Ubuntu example:
```bash
sudo apt-get install -y build-essential cmake pkg-config libssl-dev libcurl4-openssl-dev libsqlite3-dev protobuf-compiler libprotobuf-dev
```
Other distros: use analogous `*-devel` / package names.

## 3. Quick Start (Conan + Meson)

### Option A: Build with GCC (default)

```bash
# Debug
conan install . -of build/debug -s build_type=Debug -b missing
meson setup build/debug \
  --prefix /usr/local \
  --native-file build/debug/build-debug/conan/conan_meson_native.ini \
  --buildtype=debug
meson compile -C build/debug

# Reconfigure later (Debug)
meson setup build/debug --reconfigure \
  --prefix /usr/local \
  --native-file build/debug/build-debug/conan/conan_meson_native.ini

# Release
conan install . -of build/release -s build_type=Release -b missing
meson setup build/release \
  --prefix /usr/local \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --buildtype=release
meson compile -C build/release
```

### Option B: Build with Clang + LLD (recommended for faster builds)

```bash
# Debug
conan install . -of build/clang-debug -s build_type=Debug \
  -s compiler=clang -s compiler.version=19 -s compiler.libcxx=libstdc++11 \
  -b missing
CC=clang CXX=clang++ meson setup build/clang-debug \
  --prefix /usr/local \
  --native-file build/clang-debug/build-debug/conan/conan_meson_native.ini \
  --buildtype=debug
meson compile -C build/clang-debug

# Release
conan install . -of build/clang-release -s build_type=Release \
  -s compiler=clang -s compiler.version=19 -s compiler.libcxx=libstdc++11 \
  -b missing
CC=clang CXX=clang++ meson setup build/clang-release \
  --prefix /usr/local \
  --native-file build/clang-release/build-release/conan/conan_meson_native.ini \
  --buildtype=release
meson compile -C build/clang-release
```

**Note**: The Meson build automatically detects when Clang is used and enables LLD linker if available. You'll see "Using LLD linker (auto with Clang)" in the configure output.

### Option C: Force LLD with GCC (optional)

You can also use LLD with GCC for faster linking:

```bash
# Configure with LLD forced
meson setup build/gcc-lld \
  --prefix /usr/local \
  --native-file build/debug/build-debug/conan/conan_meson_native.ini \
  --buildtype=debug \
  -Duse-lld=enabled
meson compile -C build/gcc-lld
```

### Disable LLD

To explicitly disable LLD (use default linker):

```bash
meson setup build/debug -Duse-lld=disabled
```

Set parallelism: `export CMAKE_BUILD_PARALLEL_LEVEL=$(nproc)` (Linux) or pass `-j` to build step.

GenAI headers:
If the packaged ONNX Runtime lacks GenAI C++ headers, YAMS automatically
downloads the lightweight `onnxruntime-genai` headers (v0.9.1) at configure
time (no additional flags). You will see a provider log entry
`onnxruntime-genai headers provided (v0.9.1)` on success.

### Tests

By default tests are disabled to keep loops fast. Enable them via Meson:
```bash
meson configure build/debug -Dbuild-tests=true
meson compile -C build/debug
meson test -C build/debug -t unit
meson test -C build/debug -t integration
```

### No-Conan (System Dependencies Only)
Presets: `yams-debug-no-conan`, `yams-release-no-conan` (assumes you supply all libs and set `YAMS_ENABLE_ONNX=ON` + provide ORT via `CMAKE_PREFIX_PATH` or internal build flag).
```bash
cmake --preset yams-debug-no-conan
cmake --build --preset yams-debug-no-conan -j
```

## 4. libc++ Hardening

When building with Clang and libc++, enable runtime hardening to detect bugs:

```bash
# Fast mode (recommended for Debug, ~5% overhead)
YAMS_LIBCXX_HARDENING=fast ./setup.sh Debug

# Extensive mode (thorough testing, ~10-20% overhead)
YAMS_LIBCXX_HARDENING=extensive ./setup.sh Debug

# Debug mode (maximum validation)
YAMS_LIBCXX_HARDENING=debug ./setup.sh Debug
```

Hardening detects:
- Out-of-bounds container access
- Iterator invalidation
- Use of moved-from objects
- String bound violations

**Platform support:**
- macOS: Works automatically (always uses libc++)
- Linux: Only when using Clang with `-stdlib=libc++`
- GCC: Ignored (use `-D_GLIBCXX_DEBUG` instead)

Can be combined with sanitizers:
```bash
YAMS_LIBCXX_HARDENING=extensive \
CXXFLAGS="-fsanitize=address" \
LDFLAGS="-fsanitize=address" \
./setup.sh Debug
```

See [libc++ Hardening Documentation](https://libcxx.llvm.org/Hardening.html) for details.

## 5. Defaults & Tooling (Debug)

Debug presets enable:
- Unity builds (faster iterative compile)
- Address/UB sanitizers (`-DYAMS_ENABLE_SANITIZERS=OFF` to disable)
- clang-tidy (disable per-config: `-D CMAKE_CXX_CLANG_TIDY=`)

If `ccache` present it is leveraged via environment/toolchain; if absent nothing breaks.

## 6. Manual (Without Presets)

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

```bash
conan install . -of build/debug -s build_type=Debug -b missing
meson setup build/debug \
  --prefix /usr/local \
  --native-file build/debug/build-debug/conan/conan_meson_native.ini \
  --buildtype=debug -Dbuild-tests=true
meson compile -C build/debug
meson test -C build/debug --print-errorlogs
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

For code coverage analysis (example):

```bash
conan install . -of build/debug -s build_type=Debug -b missing
meson setup build/debug \
  --prefix /usr/local \
  --native-file build/debug/build-debug/conan/conan_meson_native.ini \
  --buildtype=debug -Dbuild-tests=true
meson compile -C build/debug
meson test -C build/debug
gcovr --root . --exclude '_deps/*' --exclude 'tests/*' --html --html-details --output build/debug/coverage.html
```

## 7. Compiler Notes

### Clang vs GCC

YAMS supports both compilers equally:
- **Clang**: Faster linking with LLD, better diagnostics, integrated sanitizers
- **GCC**: Mature ecosystem, sometimes better optimizations for specific targets

The build system automatically:
- Detects which compiler is being used
- Enables LLD when using Clang (if available)
- Falls back gracefully when tools are missing

### std::format

YAMS automatically detects std::format availability:

- **Clang 16+**: Full std::format support
- **GCC 13+**: Full std::format support
- **GCC 11–12**: Falls back to fmt library
- **Clang 14-15**: Falls back to fmt library
- The build system will automatically configure the appropriate option

### Coroutines

Both GCC and Clang have full C++20 coroutine support:
- **GCC**: Starting from version 11.0
- **Clang**: Starting from version 14.0

The build system automatically adds the necessary flags when using either compiler.

### LLD Linker

When using Clang, YAMS automatically prefers the LLD linker:
- Significantly faster linking times compared to GNU ld
- Lower memory usage during linking
- Automatically detected and enabled via `-fuse-ld=lld`
- Falls back to default linker if LLD is not available

You can verify LLD is being used by checking the Meson configuration summary.

### LTO

For optimal release builds with LTO:

**GCC:**
```bash
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-flto" \
    -DCMAKE_EXE_LINKER_FLAGS="-flto"
```

**Clang + LLD (with ThinLTO):**
```bash
CC=clang CXX=clang++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-flto=thin" \
    -DCMAKE_EXE_LINKER_FLAGS="-flto=thin -fuse-ld=lld"
```

Note: ThinLTO with Clang+LLD provides faster incremental builds while maintaining most of the performance benefits of full LTO.

## 8. Troubleshooting

### "std::format not found"

Solution: This is expected with GCC < 13. The build will automatically use the fmt library as a fallback.

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

## 9. Performance Tips

For best performance with GCC:

```bash
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-O3 -march=native -mtune=native -flto" \
    -DCMAKE_EXE_LINKER_FLAGS="-flto -Wl,-O1"
```

For best performance with Clang + LLD:

```bash
CC=clang CXX=clang++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-O3 -march=native -mtune=native -flto=thin" \
    -DCMAKE_EXE_LINKER_FLAGS="-flto=thin -fuse-ld=lld -Wl,-O2"
```

Note: `-march=native` optimizes for your specific CPU but makes binaries non-portable.

## 10. Quick Verification

After building, verify the installation:

```bash
# Check the binary
./tools/yams-cli/yams --version

# Run a simple test
echo "Hello, YAMS!" | ./tools/yams-cli/yams add -

# If installed system-wide
yams --version
```

## 11. CI Snapshot

YAMS CI/CD pipeline tests both GCC and Clang builds on Ubuntu:
- **GCC builds**: Compatibility and fallback testing
- **Clang + LLD builds**: Primary recommended configuration
- **Sanitizer builds**: Clang with ASan/UBSan for debug validation

The configuration used in CI can be found in `.github/workflows/ci.yml`.

## 12. References

- GCC Documentation: https://gcc.gnu.org/onlinedocs/
- GCC C++20 Status: https://gcc.gnu.org/projects/cxx-status.html#cxx20
- CMake Presets: https://cmake.org/cmake/help/latest/manual/cmake-presets.7.html
- YAMS Build Options: BUILD.md

## 13. Sanitizer Example (Standalone)
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
