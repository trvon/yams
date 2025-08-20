# YAMS Installation Guide

YAMS provides multiple installation methods to suit different users and environments.

### Environment Variables

You can customize the installation by setting environment variables:

```bash
export YAMS_VERSION="1.0.0"              # Specific version
export YAMS_INSTALL_DIR="/usr/local/bin" # Installation directory
export YAMS_GITHUB_USER="trvon"          # GitHub username
export YAMS_GITHUB_REPO="yams"          # Repository name

curl -fsSL https://raw.githubusercontent.com/trvon/yams/main/install.sh | bash
```

## Package Manager Installation

### Homebrew (macOS/Linux) - Coming Soon

```bash
# Add the YAMS tap
brew tap your-username/yams

# Install YAMS
brew install yams

# Update to latest version
brew upgrade yams
```

### APT (Debian/Ubuntu) - Coming Soon

```bash
# Add repository and key
curl -fsSL https://your-username.github.io/yams/gpg.key | sudo gpg --dearmor -o /usr/share/keyrings/yams.gpg
echo "deb [signed-by=/usr/share/keyrings/yams.gpg] https://your-username.github.io/yams/apt/ stable main" | sudo tee /etc/apt/sources.list.d/yams.list

# Install
sudo apt update
sudo apt install yams
```

## Developer Installation

### Conan Package Manager - Coming Soon

```bash
# Add YAMS to your conanfile.txt
[requires]
yams/[>=1.0.0]

[generators]
CMakeDeps
CMakeToolchain

# Or install directly
conan install --requires=yams/[>=1.0.0]
```

## Manual Installation from GitHub Releases

1. Visit the [releases page](https://github.com/trvon/yams/releases)
2. Download the appropriate binary for your platform:
   - `yams-VERSION-linux-x86_64.tar.gz` for Linux x86_64
   - `yams-VERSION-macos-x86_64.zip` for macOS Intel
   - `yams-VERSION-macos-arm64.zip` for macOS Apple Silicon
3. Extract the archive
4. Move the `yams` binary to a directory in your PATH

## Build from Source

### Requirements
- Compiler: C++20 (Clang 14+/GCC 11+/MSVC 2022)
- CMake: 3.20+
- Libraries: OpenSSL 3.x, SQLite3, Protocol Buffers 3.x
- Optional: Pandoc (for manpages and embedded verbose help when YAMS_BUILD_DOCS=ON)
- OS: macOS 12+ or Linux (Windows experimental)

### Quick Setup

#### macOS
```bash
brew install cmake openssl@3 sqlite3 protobuf
export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"
```

#### Ubuntu/Debian
```bash
sudo apt update
sudo apt install -y build-essential cmake git libssl-dev libsqlite3-dev protobuf-compiler
```

## Get the Source
```bash
git clone https://github.com/trvon/yams.git
cd yams
```

## Build (Release)
```bash
mkdir -p build && cd build
cmake -DYAMS_BUILD_PROFILE=release ..
cmake --build . -j
# Optional
cmake --install . --prefix /usr/local
```

Notes:
- Disable MCP server if needed: cmake -DYAMS_BUILD_PROFILE=custom -DYAMS_BUILD_MCP_SERVER=OFF ..
- Development build (tests, debug): cmake -DYAMS_BUILD_PROFILE=dev ..
- Optional docs: If Pandoc is installed and -DYAMS_BUILD_DOCS=ON, manpages are generated and verbose help is embedded (yams --help-all, yams <command> --help --verbose). Without Pandoc, builds still succeed without these assets.

## Initialize Storage (XDG paths)
Defaults:
- Data: $XDG_DATA_HOME/yams or ~/.local/share/yams
- Config: $XDG_CONFIG_HOME/yams or ~/.config/yams
- Keys: ~/.config/yams/keys

Non-interactive init (uses defaults):
```bash
yams init --non-interactive
```

Custom storage directory:
```bash
yams init --non-interactive --storage "$HOME/.local/share/yams"
# or via env
export YAMS_STORAGE="$HOME/.local/share/yams"
yams init --non-interactive
```

What init creates:
- Data dir with yams.db and storage/
- Config at ~/.config/yams/config.toml
- Ed25519 keys at ~/.config/yams/keys (private key 0600)
- An initial API key stored in config

Security:
- yams init --print shows config with secrets masked. Do not rely on stdout for secret recovery.

Idempotency:
- Re-running init without --force leaves existing setup intact and exits successfully.
- Use --force to overwrite config/keys.

## Verify
```bash
yams --version
yams stats
yams search "test" --limit 1
```

## Paths Summary
- Data: ~/.local/share/yams (default)
- Config: ~/.config/yams/config.toml
- Keys: ~/.config/yams/keys/ed25519.pem, ~/.config/yams/keys/ed25519.pub

Override with:
- Data: --storage or YAMS_STORAGE
- Config: XDG_CONFIG_HOME
- Data default root: XDG_DATA_HOME

## Troubleshooting (fast)
- OpenSSL not found (macOS):
  - export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"
- Permission issues:
  - chown/chmod your storage path; avoid sudo for runtime
- Reconfigure build:
  - rm -rf build && mkdir build && cd build && cmake ...

## Verification

After installation, verify YAMS is working:

```bash
yams --version
yams --help
```

## Uninstallation

### Quick Install
If you used the quick install script:

```bash
rm ~/.local/bin/yams
rm -rf ~/.local/share/bash-completion/completions/yams
rm -rf ~/.local/share/zsh/site-functions/_yams
rm -rf ~/.config/fish/completions/yams.fish
```

### Package Managers

```bash
# Homebrew (when available)
brew uninstall yams
brew untap your-username/yams

# APT (when available)
sudo apt remove yams
```

## Troubleshooting

### Common Issues

**Script fails with "curl: command not found"**
```bash
# Install curl first
# Ubuntu/Debian: sudo apt install curl
# CentOS/RHEL: sudo yum install curl
# macOS: curl is pre-installed
```

**"Permission denied" errors**
```bash
# Install to user directory instead of system directory
curl -fsSL https://raw.githubusercontent.com/your-username/yams/main/install.sh | bash -s -- --install-dir ~/.local/bin

# Or use sudo for system directory
curl -fsSL https://raw.githubusercontent.com/your-username/yams/main/install.sh | sudo bash -s -- --install-dir /usr/local/bin
```

**Binary not found after installation**
```bash
# Add installation directory to PATH
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

**Unsupported platform error**
Currently supported platforms:
- Linux x86_64
- macOS x86_64 (Intel)
- macOS ARM64 (Apple Silicon)

For other platforms, please [build from source](#build-from-source).

### Build Issues
- OpenSSL not found (macOS):
  - export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"
- Permission issues:
  - chown/chmod your storage path; avoid sudo for runtime
- Reconfigure build:
  - rm -rf build && mkdir build && cd build && cmake ...

### Getting Help

- [GitHub Issues](https://github.com/your-username/yams/issues)
- [Documentation](https://github.com/your-username/yams/tree/main/docs)
- [Discussions](https://github.com/your-username/yams/discussions)

## Security

The installation script:
- Downloads binaries only from official GitHub Releases
- Verifies binary integrity where possible
- Uses HTTPS for all downloads
- Does not require root privileges by default

For maximum security:
1. Review the installation script before running
2. Use package managers when available
3. Verify checksums from releases page
4. Consider building from source for sensitive environments

## Next Steps
- CLI usage: yams add/search/get/list/stats
- MCP: build with -DYAMS_BUILD_MCP_SERVER=ON and run yams serve (stdio)
- Docs and API examples: see README and docs/
