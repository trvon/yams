# YAMS Installation

Minimal, fast, and to the point. This guide shows how to build, install, and initialize YAMS using XDG paths.

## Requirements
- Compiler: C++20 (Clang 14+/GCC 11+/MSVC 2022)
- CMake: 3.20+
- Libraries: OpenSSL 3.x, SQLite3, Protocol Buffers 3.x
- Optional: Pandoc (for manpages and embedded verbose help when YAMS_BUILD_DOCS=ON)
- OS: macOS 12+ or Linux (Windows experimental)

## Quick Setup

### macOS
```bash
brew install cmake openssl@3 sqlite3 protobuf
export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"
```

### Ubuntu/Debian
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

## Next Steps
- CLI usage: yams add/search/get/list/stats
- MCP: build with -DYAMS_BUILD_MCP_SERVER=ON and run yams serve (stdio)
- Docs and API examples: see README and docs/