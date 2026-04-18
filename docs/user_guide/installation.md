# Installation

Supported platforms: Linux x86_64/ARM64, macOS x86_64/ARM64, Windows x86_64.

## Homebrew (macOS, Linux)

```bash
brew install trvon/yams/yams           # stable
brew install trvon/yams/yams-nightly   # nightly
brew services start yams               # optional: run as a service
brew upgrade yams                      # update
brew link --overwrite yams             # if linking conflicts
```

## APT (Debian, Ubuntu) {#apt}

```bash
curl -fsSL https://repo.yamsmemory.ai/gpg.key \
  | sudo gpg --dearmor -o /usr/share/keyrings/yams.gpg

echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/yams.gpg] \
https://repo.yamsmemory.ai/aptrepo stable main" \
  | sudo tee /etc/apt/sources.list.d/yams.list

sudo apt-get update && sudo apt-get install yams
sudo systemctl enable --now yams-daemon   # optional
yams daemon status
```

## DNF / YUM (Fedora, RHEL) {#dnf}

```bash
sudo tee /etc/yum.repos.d/yams.repo <<'REPO'
[yams]
name=YAMS Repository
baseurl=https://repo.yamsmemory.ai/yumrepo/
enabled=1
gpgcheck=0
REPO
sudo dnf makecache && sudo dnf install yams
```

!!! note
    Current CI installs the published `.rpm` artifact directly rather than pulling through repo metadata. The hosted `yumrepo/` path is documented but not covered by automated validation.

Tested lanes: x86_64 `.deb` on Debian trixie and x86_64 `.rpm` on Fedora 42, both in clean systemd containers with `yams-daemon.service` enabled.

## Docker

```bash
docker pull ghcr.io/trvon/yams:latest
docker run --rm -v yams-data:/home/yams/.local/share/yams ghcr.io/trvon/yams:latest --version
docker run -i --rm -v yams-data:/home/yams/.local/share/yams ghcr.io/trvon/yams:latest serve
```

## GitHub releases

Grab a prebuilt archive from [github.com/trvon/yams/releases](https://github.com/trvon/yams/releases):

- `yams-VERSION-linux-x86_64.tar.gz` / `-arm64`
- `yams-VERSION-macos-x86_64.zip` / `-arm64`
- `yams-VERSION-windows-x86_64.zip`

Extract and place the `yams` binary on your `PATH`.

## Build from source

```bash
# Linux / macOS
./setup.sh Release && meson compile -C build/release

# Windows
./setup.ps1 Release ; meson compile -C build/release
```

Prerequisites, compiler matrix, offline/system-deps builds, and troubleshooting: [docs/BUILD.md](../BUILD.md).

## Initialize

```bash
yams init                        # interactive
yams init --auto                 # containers / headless
yams init --non-interactive      # defaults, no prompts
yams init --force                # overwrite config/keys
```

`yams init` creates:

- Data dir with `yams.db` and `storage/`
- `~/.config/yams/config.toml`
- Ed25519 keys in `~/.config/yams/keys` (private key 0600)
- An initial API key in config

### Paths (XDG)

| Purpose | Default                                            | Override                   |
|---------|----------------------------------------------------|----------------------------|
| Data    | `$XDG_DATA_HOME/yams` (`~/.local/share/yams`)      | `--storage` or `YAMS_STORAGE` |
| Config  | `$XDG_CONFIG_HOME/yams` (`~/.config/yams`)         | `XDG_CONFIG_HOME`          |
| Keys    | `~/.config/yams/keys`                              | —                          |

### Tree-sitter grammars

`yams init` can download grammars for 18 languages ([list](cli.md#symbol-extraction)). Requirements: `git` + a C compiler (gcc/clang on Unix, MSVC/MinGW on Windows). Pin an existing grammar with `YAMS_TS_<LANG>_LIB=/path/to/libtree-sitter-<lang>.so`.

## Shell completion

```bash
# Bash
source <(yams completion bash)

# Zsh (persistent)
mkdir -p ~/.local/share/zsh/site-functions
yams completion zsh > ~/.local/share/zsh/site-functions/_yams
# Ensure ~/.local/share/zsh/site-functions is on fpath before compinit

# Fish
mkdir -p ~/.config/fish/completions
yams completion fish > ~/.config/fish/completions/yams.fish

# PowerShell (current session)
pwsh -NoLogo -NoProfile -Command 'Invoke-Expression (yams completion powershell | Out-String)'
```

Homebrew already installs bash/zsh/fish completions; only shell activation is your side.

## Verify

```bash
yams --version
yams stats
yams search "test" --limit 1

# ONNX-enabled builds: confirm the model provider is loadable
YAMS_SKIP_MODEL_LOADING=1 yams doctor plugin onnx --no-daemon
# Expect: "Interface: model_provider_v1 v1 -> AVAILABLE"
```

## Uninstall

```bash
# Homebrew
brew uninstall yams && brew untap trvon/yams

# Manual
rm ~/.local/bin/yams
rm -rf ~/.local/share/bash-completion/completions/yams \
       ~/.local/share/zsh/site-functions/_yams \
       ~/.config/fish/completions/yams.fish

# Data (optional, destructive)
rm -rf ~/.local/share/yams ~/.config/yams
```

## Troubleshooting

| Symptom                                   | Fix                                                            |
|-------------------------------------------|----------------------------------------------------------------|
| OpenSSL not found (macOS source build)    | `export OPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"`         |
| Permission issues on storage path         | `chown`/`chmod` the dir; avoid `sudo` for runtime              |
| Build cache stale                         | `rm -rf build && ./setup.sh Release`                           |
| `yams plugin list` empty                  | `yams plugin trust add ~/.local/lib/yams/plugins` and re-check |
| `yams` not on PATH                        | Add install dir: `export PATH="$HOME/.local/bin:$PATH"`        |

Further diagnostics: `yams doctor`, `yams stats --verbose`. Build issues: [docs/BUILD.md](../BUILD.md).

## Next

- [CLI reference](cli.md) · [MCP server](mcp.md) · [Embeddings](embeddings.md) · [Plugins](../PLUGINS.md)
- Issues: https://github.com/trvon/yams/issues · Discord: https://discord.gg/rTBmRHdTEc
