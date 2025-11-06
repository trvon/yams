# Building YAMS with GCC or Clang

Quick reference for building YAMS with either GCC or Clang toolchains.

## Quick Start

**Recommended:** Use the `setup.sh` script (auto-detects compiler, handles Conan + Meson):

```bash
# Release build (default: Clang if available, else GCC)
./setup.sh Release

# Debug build with tests
./setup.sh Debug

# Force specific compiler
YAMS_COMPILER=gcc ./setup.sh Release
YAMS_COMPILER=clang ./setup.sh Release
```

Then compile:
```bash
meson compile -C build/release  # or builddir for Debug
```

## Prerequisites

**Compiler** (choose one):
- GCC 13+ (recommended) or GCC 11+ (C++20 minimum)
- Clang 16+ (recommended) or Clang 14+ (C++20 minimum)

**Build tools:**
```bash
# Ubuntu/Debian
sudo apt-get install -y build-essential cmake meson ninja-build pkg-config

# Optional but recommended for faster builds
sudo apt-get install -y ccache lld clang clang-tidy
```

**System dependencies:**
```bash
# Ubuntu/Debian
sudo apt-get install -y libssl-dev libcurl4-openssl-dev libsqlite3-dev \
    protobuf-compiler libprotobuf-dev zlib1g-dev

# Fedora
sudo dnf install -y openssl-devel libcurl-devel sqlite-devel protobuf-devel zlib-devel

# Arch
sudo pacman -S openssl curl sqlite protobuf zlib
```

**Conan:**
```bash
pip install conan
conan profile detect --force
```

## Manual Build (without setup.sh)

### GCC

```bash
conan install . -of build/release -s build_type=Release -b missing
meson setup build/release \
  --prefix /usr/local \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --buildtype=release
meson compile -C build/release
```

### Clang

```bash
conan install . -of build/release -s build_type=Release \
  -s compiler=clang -s compiler.version=18 -s compiler.libcxx=libstdc++11 \
  -b missing
CC=clang CXX=clang++ meson setup build/release \
  --prefix /usr/local \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --buildtype=release
meson compile -C build/release
```

**Note:** Clang automatically uses LLD linker if available (faster linking).

## Common Options

**Enable tests:**
```bash
./setup.sh Debug
meson configure builddir -Dbuild-tests=true
meson test -C builddir
```

**Disable optional features:**
```bash
YAMS_DISABLE_ONNX=true ./setup.sh Release
YAMS_DISABLE_PDF=true ./setup.sh Release
YAMS_DISABLE_SYMBOL_EXTRACTION=true ./setup.sh Release
```

**Code coverage:**
```bash
./setup.sh Debug --coverage
meson test -C builddir
gcovr --html --html-details -o builddir/coverage.html
```

**libc++ hardening (Clang only):**
```bash
YAMS_LIBCXX_HARDENING=fast ./setup.sh Debug    # ~5% overhead, recommended
YAMS_LIBCXX_HARDENING=extensive ./setup.sh Debug  # ~10-20% overhead
```

## Troubleshooting

### Clang: "cannot find -lstdc++"

**Error:**
```
/usr/bin/ld: cannot find -lstdc++: No such file or directory
```

**Cause:** Clang trying to use libstdc++ but missing development files.

**Fix (choose one):**

1. Switch to GCC:
   ```bash
   YAMS_COMPILER=gcc ./setup.sh Release
   ```

2. Use Clang's libc++ instead:
   ```bash
   sudo apt-get install libc++-dev libc++abi-dev
   # Edit ~/.conan2/profiles/default:
   # Change: compiler.libcxx=libstdc++11 → compiler.libcxx=libc++
   ```

3. Install libstdc++ dev files:
   ```bash
   sudo apt-get install libstdc++-13-dev
   ```

See: [Conan Profiles](https://docs.conan.io/2/reference/config_files/profiles.html)

### Missing dependencies

**pkg-config errors:**
```bash
pkg-config --list-all | grep -E "(sqlite|openssl|protobuf|libcurl)"
```

If missing, install development packages (see Prerequisites).

### Old compiler version

**GCC < 11 or Clang < 14:**

Upgrade your compiler:
```bash
# Ubuntu
sudo apt-get install gcc-13 g++-13 clang-18

# Update alternatives (optional)
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 100
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 100
```

### ONNX disabled

Check configure output for "ONNX Runtime found". If disabled:

```bash
# Enable in build
./setup.sh Release  # ONNX enabled by default

# Or explicitly
conan install . -of build/release -s build_type=Release \
  -o yams/*:enable_onnx=True -b missing
```

## Compiler Comparison

| Feature | GCC 13+ | Clang 16+ |
|---------|---------|-----------|
| C++20 coroutines | ✓ | ✓ |
| std::format | ✓ | ✓ |
| Default linker | GNU ld | LLD (faster) |
| Hardening | `-D_GLIBCXX_DEBUG` | libc++ modes |
| Diagnostics | Good | Better |

Both work equally well. Clang + LLD gives faster link times.

## Advanced

**Custom C++ standard:**
```bash
YAMS_CPPSTD=20 ./setup.sh Release  # C++20 instead of C++23
```

**Custom install prefix:**
```bash
YAMS_INSTALL_PREFIX=/opt/yams ./setup.sh Release
```

**LTO (Link Time Optimization):**
```bash
# GCC
CC=gcc CXX=g++ CXXFLAGS="-flto" LDFLAGS="-flto" ./setup.sh Release

# Clang (ThinLTO)
CC=clang CXX=clang++ CXXFLAGS="-flto=thin" LDFLAGS="-flto=thin" ./setup.sh Release
```

**Machine-specific optimizations:**
```bash
CXXFLAGS="-march=native -mtune=native" ./setup.sh Release
```
⚠️ Binary won't be portable to other CPUs.

## Verification

```bash
# Check version
./build/release/tools/yams-cli/yams --version

# Quick test
echo "Hello, YAMS!" | ./build/release/tools/yams-cli/yams add -

# After install
yams --version
```

## References

- [Main Build Guide](BUILD.md) - Detailed build options
- [Conan Documentation](https://docs.conan.io/2/)
- [Meson Documentation](https://mesonbuild.com/)
- [GCC C++20 Status](https://gcc.gnu.org/projects/cxx-status.html#cxx20)
- [Clang C++ Status](https://clang.llvm.org/cxx_status.html)
