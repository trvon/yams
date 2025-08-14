# YAMS Build Compatibility Guide

YAMS supports both modern C++20 and legacy C++17 systems for maximum compatibility across different environments.

## Quick Start

### C++20 Mode (Default)
```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

### C++17 Compatibility Mode
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DYAMS_CXX17_MODE=ON ..
make -j$(nproc)
```

## Compiler Support Matrix

| Compiler | C++20 Mode | C++17 Mode | Notes |
|----------|------------|------------|-------|
| **GCC** | | | |
| GCC 11+ | ✅ Full support | ✅ Full support | Recommended for C++20 |
| GCC 9-10 | ❌ | ✅ Full support | Use C++17 mode |
| GCC 8 | ❌ | ❌ | Not supported |
| **Clang** | | | |
| Clang 12+ | ✅ Full support | ✅ Full support | Recommended for C++20 |
| Clang 9-11 | ❌ | ✅ Full support | Use C++17 mode |
| Clang 8 | ❌ | ❌ | Not supported |
| **MSVC** | | | |
| MSVC 2022 | ✅ Full support | ✅ Full support | Visual Studio 2022 |
| MSVC 2019 | ⚠️ Partial | ✅ Full support | Use C++17 mode |

## Operating System Support

### Linux Distributions

| Distribution | Default Support | C++17 Mode | Package Requirements |
|--------------|----------------|------------|---------------------|
| **Ubuntu** | | | |
| 22.04 LTS (Jammy) | ✅ (GCC 11) | ✅ | `build-essential cmake` |
| 20.04 LTS (Focal) | ❌ | ✅ (GCC 9) | `build-essential cmake` |
| 18.04 LTS (Bionic) | ❌ | ❌ | Upgrade required |
| **RHEL/CentOS** | | | |
| RHEL 9 | ✅ (GCC 11) | ✅ | `gcc-toolset-11` |
| RHEL 8 | ❌ | ✅ (GCC 9) | `gcc-toolset-9` |
| CentOS 7 | ❌ | ❌ | Upgrade required |
| **Fedora** | | | |
| 35+ | ✅ (GCC 11+) | ✅ | `gcc-c++ cmake` |
| 32-34 | ❌ | ✅ (GCC 9-10) | `gcc-c++ cmake` |

### macOS Support

| macOS Version | Xcode | Default Support | C++17 Mode |
|---------------|-------|----------------|------------|
| macOS 13+ (Ventura) | 14+ | ✅ | ✅ |
| macOS 12 (Monterey) | 13+ | ✅ | ✅ |
| macOS 11 (Big Sur) | 12+ | ⚠️ | ✅ |
| macOS 10.15 (Catalina) | 11+ | ❌ | ✅ |

## Feature Compatibility

### C++20 Features Used

| Feature | C++20 Mode | C++17 Fallback | Implementation |
|---------|------------|----------------|----------------|
| `std::span` | ✅ Native | ✅ `boost::span` | `yams::span` alias |
| `std::format` | ✅ Native | ✅ `fmt` library | `yams::format` alias |
| Concepts | ✅ Native | ❌ Disabled | Conditional compilation |
| Ranges | ✅ Native | ❌ Manual loops | Conditional compilation |
| Coroutines | ✅ Native | ❌ Disabled | Conditional compilation |

### Dependencies

#### C++20 Mode
- All dependencies use standard library features
- Optimal performance and memory usage
- Latest language features available

#### C++17 Mode  
- `boost::span` for span functionality
- `fmt` library for formatting
- Some advanced features disabled
- Excellent compatibility with older systems

## Performance Considerations

### C++20 Mode
- **Recommended** for new deployments
- Best performance due to native implementations
- Full feature set available
- More efficient compilation

### C++17 Mode
- Minimal performance overhead (< 2%)
- Slightly larger binary size due to additional libraries
- Some optimizations may be disabled
- Excellent for legacy system compatibility

## Build Configuration

### CMake Options

```cmake
# C++ standard selection
YAMS_CXX17_MODE          # Enable C++17 compatibility mode (default: OFF)

# Component selection
YAMS_BUILD_CLI           # Build CLI tool (default: ON)
YAMS_BUILD_MCP_SERVER    # Build MCP server (default: ON)
YAMS_BUILD_TESTS         # Build unit tests (default: OFF)
YAMS_BUILD_BENCHMARKS    # Build benchmarks (default: OFF)

# Feature toggles
YAMS_ENABLE_PDF          # Enable PDF support (default: ON)
YAMS_ENABLE_NATIVE_OPTIMIZATIONS  # Enable -march=native (default: OFF)
```

### Example Configurations

#### Production Deployment (C++20)
```bash
cmake -DCMAKE_BUILD_TYPE=Release \
      -DYAMS_BUILD_PROFILE=release \
      -DYAMS_ENABLE_NATIVE_OPTIMIZATIONS=ON \
      ..
```

#### Legacy System (C++17)
```bash
cmake -DCMAKE_BUILD_TYPE=Release \
      -DYAMS_CXX17_MODE=ON \
      -DYAMS_BUILD_PROFILE=release \
      ..
```

#### Development Build
```bash
cmake -DCMAKE_BUILD_TYPE=Debug \
      -DYAMS_BUILD_PROFILE=dev \
      -DYAMS_BUILD_TESTS=ON \
      -DYAMS_BUILD_BENCHMARKS=ON \
      ..
```

## Troubleshooting

### Common Issues

#### "C++20 features not available"
**Solution:** Use C++17 mode:
```bash
cmake -DYAMS_CXX17_MODE=ON ..
```

#### "std::format not found"
**Cause:** Compiler doesn't support `std::format`
**Solution:** Automatic fallback to `fmt` library (no action needed)

#### "boost::span not found" 
**Cause:** Missing Boost dependency in C++17 mode
**Solution:** Install Boost development headers:
```bash
# Ubuntu/Debian
sudo apt-get install libboost-dev

# RHEL/CentOS
sudo yum install boost-devel

# macOS
brew install boost
```

#### Link errors with fmt library
**Cause:** Missing fmt dependency
**Solution:** 
- Conan builds: fmt is automatically provided
- Manual builds: Install fmt library or let CMake fetch it

### Performance Debugging

Enable verbose build output:
```bash
cmake -DCMAKE_VERBOSE_MAKEFILE=ON ..
make VERBOSE=1
```

Check feature detection:
```bash
# Look for these messages in cmake output:
# "std::format is available" or "using fmt fallback"  
# "C++20 concepts available" or "concepts not available"
# "Using C++17 compatibility mode" or "Using C++20 mode"
```

## Migration Guide

### From C++17 to C++20

1. Upgrade compiler to supported version
2. Remove `-DYAMS_CXX17_MODE=ON` from cmake command
3. Rebuild project
4. Verify all features work as expected

### From C++20 to C++17 (Downgrade)

1. Add `-DYAMS_CXX17_MODE=ON` to cmake command
2. Install Boost and fmt if not using Conan
3. Rebuild project
4. Some advanced features may be disabled

## Support Policy

- **C++20 Mode:** Full support, recommended for all new projects
- **C++17 Mode:** Maintenance support, recommended for legacy systems
- **C++14 and older:** Not supported

For questions or issues, please file a GitHub issue with:
- Operating system and version
- Compiler type and version  
- CMake version
- Full cmake configuration command used
- Complete error output