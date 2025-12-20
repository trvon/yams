# YAMS Installation Guide

YAMS provides multiple installation methods to suit different users and environments.

**Supported platforms:** Linux x86_64/ARM64, macOS x86_64/ARM64, Windows x86_64

## Package Manager Installation

### Homebrew (macOS/Linux)

```bash
# Stable release (recommended)
brew install trvon/yams/yams

# Or get nightly builds for latest features
brew install trvon/yams/yams@nightly

# If linking fails due to conflicts, force link
brew link --overwrite yams

# Verify installation
yams --version

# Run as a service (optional)
brew services start yams
```

### Update

```bash
brew upgrade yams
```

## Manual Installation from GitHub Releases

1. Visit the [releases page](https://github.com/trvon/yams/releases)
2. Download the appropriate binary for your platform:
   - `yams-VERSION-linux-x86_64.tar.gz` for Linux x86_64
   - `yams-VERSION-linux-arm64.tar.gz` for Linux ARM64
   - `yams-VERSION-macos-x86_64.zip` for macOS Intel
   - `yams-VERSION-macos-arm64.zip` for macOS Apple Silicon
   - `yams-VERSION-windows-x86_64.zip` for Windows
3. Extract the archive
4. Move the `yams` binary to a directory in your PATH

## Docker

```bash
# Pull the latest image
docker pull ghcr.io/trvon/yams:latest

# Run YAMS
docker run --rm -v yams-data:/home/yams/.local/share/yams ghcr.io/trvon/yams:latest --version

# Run MCP server
docker run -it --rm -v yams-data:/home/yams/.local/share/yams ghcr.io/trvon/yams:latest serve
```

## Build from Source

### Requirements
- **Compiler:** GCC 13+, Clang 16+, or MSVC 2022+ (C++20 minimum)
- **Build tools:** meson, ninja-build, cmake, pkg-config, conan
- **System libs:** libssl-dev, libsqlite3-dev, protobuf-compiler (Linux/macOS)

### Quick Setup

#### macOS
```bash
brew install meson conan ninja cmake pkg-config openssl@3 protobuf
```

#### Ubuntu/Debian
```bash
sudo apt update
sudo apt install -y build-essential git python3-pip python3-venv \
    libssl-dev libsqlite3-dev protobuf-compiler cmake ninja-build pkg-config
pip3 install --user meson conan
```

### Get the Source
```bash
git clone https://github.com/trvon/yams.git
cd yams
```

### Build

**Linux/macOS:**
```bash
# Quick build (auto-detects Clang/GCC, configures Conan + Meson)
./setup.sh Release

# Build
meson compile -C build/release

# Optional: Install system-wide
meson install -C build/release
```

**Windows:**
```pwsh
# Quick build (MSVC + Conan + Meson)
./setup.ps1 Release

# Build
meson compile -C build/release
```

**Advanced options (Linux/macOS):**
```bash
# Debug build with tests
./setup.sh Debug
meson compile -C builddir

# Coverage (Debug only)
./setup.sh Debug --coverage

# Cross-compilation
YAMS_CONAN_HOST_PROFILE=path/to/profile ./setup.sh Release

# Custom C++ standard
YAMS_CPPSTD=20 ./setup.sh Release

# Custom install prefix
YAMS_INSTALL_PREFIX=/opt/yams ./setup.sh Release
```

See [BUILD.md](../BUILD.md) for detailed build instructions, compiler configuration, Conan profiles, and troubleshooting.

## Initialize Storage

### Quick Start
```bash
# Initialize YAMS storage in current directory
yams init .

# Or specify custom location
export YAMS_STORAGE="$HOME/.local/share/yams"
yams init

# Non-interactive init (uses defaults)
yams init --non-interactive
```

### Symbol Extraction (Tree-sitter Grammars)

YAMS can extract symbols (functions, classes, etc.) from source code using tree-sitter grammars. During `yams init`, you'll be prompted to download grammars:

```bash
# Interactive: choose recommended, all, or specific languages
yams init

# Auto mode: downloads recommended grammars (C, C++, Python, JS, TS, Rust, Go)
yams init --auto
```

**Supported languages:** C, C++, Python, JavaScript, TypeScript, Rust, Go, Java, C#, PHP, Kotlin, Dart, SQL, Solidity

**Requirements:** git + compiler (MSVC/MinGW on Windows, gcc/clang on Unix)

**Manual grammar paths:** Set environment variables like `YAMS_TS_CPP_LIB=/path/to/libtree-sitter-cpp.so`

### XDG Paths (Defaults)
- **Data:** `$XDG_DATA_HOME/yams` or `~/.local/share/yams`
- **Config:** `$XDG_CONFIG_HOME/yams` or `~/.config/yams`
- **Keys:** `~/.config/yams/keys`

### What init creates
- Data dir with `yams.db` and `storage/`
- Config at `~/.config/yams/config.toml`
- Ed25519 keys at `~/.config/yams/keys` (private key 0600)
- An initial API key stored in config

### Override paths
- **Data:** `--storage` flag or `YAMS_STORAGE` env var
- **Config:** `XDG_CONFIG_HOME` env var
- **Data default root:** `XDG_DATA_HOME` env var

### Security notes
- `yams init --print` shows config with secrets masked. Do not rely on stdout for secret recovery.
- Re-running init without `--force` leaves existing setup intact and exits successfully.
- Use `--force` to overwrite config/keys.

## Verify Installation
```bash
yams --version
yams stats
yams search "test" --limit 1
```

## Uninstallation

### Homebrew
```bash
brew uninstall yams
brew untap trvon/yams
```

### Manual Installation
```bash
rm ~/.local/bin/yams
rm -rf ~/.local/share/bash-completion/completions/yams
rm -rf ~/.local/share/zsh/site-functions/_yams
rm -rf ~/.config/fish/completions/yams.fish
```

### Data Cleanup (optional)
```bash
rm -rf ~/.local/share/yams
rm -rf ~/.config/yams
```

## Troubleshooting

### Build Issues

**OpenSSL not found (macOS):**
```bash
export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"
```

**Permission issues:**
```bash
# chown/chmod your storage path; avoid sudo for runtime
```

**Reconfigure build:**
```bash
rm -rf build && ./setup.sh Release
```

### Plugin Issues

**Plugin discovery:** Verify with `yams plugin list`. If empty:
- Check trusted directories: `yams plugin trust list`
- Add plugin path: `yams plugin trust add ~/.local/lib/yams/plugins`
- Verify shared libs: `ldd libyams_onnx_plugin.so`

### Runtime Issues

**Binary not found after installation:**
```bash
# Add installation directory to PATH
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

**Monitor and diagnose:**
```bash
yams stats --verbose
yams doctor
```

### Getting Help

- [GitHub Issues](https://github.com/trvon/yams/issues)
- [Documentation](https://yamsmemory.ai)
- [Discord](https://discord.gg/rTBmRHdTEc)

## Next Steps

- **CLI usage:** `yams add`, `yams search`, `yams get`, `yams list`, `yams stats`
- **MCP server:** `yams serve` (stdio transport)
- **Configuration:** Edit `~/.config/yams/config.toml`
- **Full docs:** [yamsmemory.ai](https://yamsmemory.ai)
