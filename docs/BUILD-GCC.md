# Building YAMS with GCC/G++

This document provides instructions for building YAMS with the GNU Compiler Collection (GCC/G++).

## Prerequisites

### Compiler Requirements

- **GCC/G++ 11.0 or later** (for C++20 and coroutine support)
- **GCC/G++ 13.0 or later** (recommended for std::format support)

To check your GCC version:
```bash
g++ --version
```

### Installing GCC

#### Ubuntu/Debian
```bash
# For GCC 11
sudo apt-get update
sudo apt-get install gcc-11 g++-11

# For GCC 13 (recommended)
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt-get update
sudo apt-get install gcc-13 g++-13

# Set as default (optional)
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 100
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 100
```

#### RHEL/CentOS/Rocky Linux
```bash
# Enable developer toolset for newer GCC
sudo yum install gcc-toolset-11
scl enable gcc-toolset-11 bash

# Or for GCC 13
sudo yum install gcc-toolset-13
scl enable gcc-toolset-13 bash
```

#### Fedora
```bash
sudo dnf install gcc g++
```

#### Arch Linux
```bash
sudo pacman -S gcc
```

## System Dependencies

Install required system libraries:

### Ubuntu/Debian
```bash
sudo apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    libsqlite3-dev \
    libncurses-dev \
    protobuf-compiler \
    libprotobuf-dev
```

### RHEL/CentOS/Rocky Linux
```bash
sudo yum install -y \
    cmake3 \
    openssl-devel \
    libcurl-devel \
    sqlite-devel \
    ncurses-devel \
    protobuf-compiler \
    protobuf-devel
```

### Fedora
```bash
sudo dnf install -y \
    cmake \
    openssl-devel \
    libcurl-devel \
    sqlite-devel \
    ncurses-devel \
    protobuf-compiler \
    protobuf-devel
```

### Arch Linux
```bash
sudo pacman -S \
    cmake \
    openssl \
    curl \
    sqlite \
    ncurses \
    protobuf
```

## Building YAMS

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

### Development Build

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

### Building Without CURL

If you don't need HTTP download features, you can build without CURL:

```bash
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DYAMS_REQUIRE_CURL=OFF

make -j$(nproc)
```

**Note**: When YAMS_REQUIRE_CURL=OFF, HTTP downloader features will be unavailable.

### Build with Coverage

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

## Compiler-Specific Considerations

### std::format Support

YAMS automatically detects std::format availability:

- **GCC 13+**: Full std::format support
- **GCC 11-12**: Falls back to fmt library
- The build system will automatically configure the appropriate option

### Coroutine Support

GCC has full C++20 coroutine support starting from version 11.0. The build system automatically adds the `-fcoroutines` flag when using GCC.

### Link-Time Optimization (LTO)

For optimal release builds with LTO:

```bash
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-flto" \
    -DCMAKE_EXE_LINKER_FLAGS="-flto"
```

## Troubleshooting

### Issue: "std::format not found"

**Solution**: This is expected with GCC < 13. The build will automatically use the fmt library as a fallback.

### Issue: "ncurses library not found"

**Solution**: Install ncurses development package:
```bash
# Ubuntu/Debian
sudo apt-get install libncurses-dev

# RHEL/CentOS
sudo yum install ncurses-devel
```

### Issue: Compilation errors with coroutines

**Solution**: Ensure you're using GCC 11 or later:
```bash
g++ --version
```

If using an older version, upgrade GCC or use a developer toolset.

### Issue: Undefined references during linking

**Solution**: Ensure all dependencies are installed and pkg-config can find them:
```bash
pkg-config --list-all | grep -E "(sqlite|openssl|protobuf|libcurl)"
```

### Issue: "Could NOT find CURL" during configuration

**Solution**: Either install libcurl development package:
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

## Performance Optimization

For best performance with GCC:

```bash
CC=gcc CXX=g++ cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-O3 -march=native -mtune=native -flto" \
    -DCMAKE_EXE_LINKER_FLAGS="-flto -Wl,-O1"
```

**Note**: `-march=native` optimizes for your specific CPU but makes binaries non-portable.

## Verification

After building, verify the installation:

```bash
# Check the binary
./tools/yams-cli/yams --version

# Run a simple test
echo "Hello, YAMS!" | ./tools/yams-cli/yams add -

# If installed system-wide
yams --version
```

## Continuous Integration

YAMS CI/CD pipeline tests GCC builds on Ubuntu. The configuration used in CI can be found in `.github/workflows/ci.yml` under the "traditional" build matrix entry.

## Additional Resources

- [GCC Documentation](https://gcc.gnu.org/onlinedocs/)
- [GCC C++20 Status](https://gcc.gnu.org/projects/cxx-status.html#cxx20)
- [YAMS Build Options](BUILD.md)
