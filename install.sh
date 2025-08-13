#!/bin/bash

# YAMS Installation Script
# Usage: curl -fsSL https://raw.githubusercontent.com/username/yams/main/install.sh | bash

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# GitHub repository configuration
GITHUB_USER="${YAMS_GITHUB_USER:-trvon}"
GITHUB_REPO="${YAMS_GITHUB_REPO:-yams}"
GITHUB_API_URL="https://api.github.com/repos/${GITHUB_USER}/${GITHUB_REPO}"

# Installation configuration
INSTALL_DIR="${YAMS_INSTALL_DIR:-$HOME/.local/bin}"
BIN_NAME="yams"
VERSION="${YAMS_VERSION:-latest}"

# Logging functions
info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

fatal() {
    error "$1"
    exit 1
}

# Detect system information
detect_system() {
    local os arch suffix
    
    # Detect OS
    case "$(uname -s)" in
        Darwin) os="macos" ;;
        Linux) os="linux" ;;
        *) fatal "Unsupported operating system: $(uname -s)" ;;
    esac
    
    # Detect architecture
    case "$(uname -m)" in
        x86_64|amd64) arch="x86_64" ;;
        arm64|aarch64) arch="arm64" ;;
        *) fatal "Unsupported architecture: $(uname -m)" ;;
    esac
    
    # Determine file suffix and asset pattern
    if [[ "$os" == "macos" ]]; then
        suffix="zip"
    else
        suffix="tar.gz"
    fi
    
    ASSET_PATTERN="${BIN_NAME}-*-${os}-${arch}.${suffix}"
    SYSTEM_OS="$os"
    SYSTEM_ARCH="$arch"
    ARCHIVE_SUFFIX="$suffix"
    
    info "Detected system: $os-$arch"
}

# Check for required dependencies
check_dependencies() {
    local deps=("curl" "tar")
    
    if [[ "$ARCHIVE_SUFFIX" == "zip" ]]; then
        deps+=("unzip")
    fi
    
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" >/dev/null 2>&1; then
            fatal "Required dependency '$dep' not found. Please install it and try again."
        fi
    done
}

# Get the latest release or specified version
get_release_info() {
    local api_url
    
    if [[ "$VERSION" == "latest" ]]; then
        api_url="${GITHUB_API_URL}/releases/latest"
        info "Fetching latest release information..."
    else
        api_url="${GITHUB_API_URL}/releases/tags/v${VERSION}"
        info "Fetching release information for version $VERSION..."
    fi
    
    RELEASE_INFO=$(curl -fsSL "$api_url") || fatal "Failed to fetch release information from GitHub API"
    RELEASE_VERSION=$(echo "$RELEASE_INFO" | grep '"tag_name":' | sed -E 's/.*"tag_name": "v?([^"]+)".*/\1/')
    
    if [[ -z "$RELEASE_VERSION" ]]; then
        fatal "Could not determine release version"
    fi
    
    info "Target version: $RELEASE_VERSION"
}

# Find and download the appropriate asset
download_asset() {
    local download_url asset_name temp_dir checksum_url checksum_file
    
    # Extract asset information from release JSON
    download_url=$(echo "$RELEASE_INFO" | grep '"browser_download_url":' | grep "$ASSET_PATTERN" | head -1 | sed -E 's/.*"browser_download_url": "([^"]+)".*/\1/')
    
    if [[ -z "$download_url" ]]; then
        fatal "Could not find asset matching pattern: $ASSET_PATTERN"
    fi
    
    asset_name=$(basename "$download_url")
    temp_dir=$(mktemp -d)
    
    info "Downloading $asset_name..."
    
    # Download with retry logic
    local max_retries=3
    local retry_count=0
    while [[ $retry_count -lt $max_retries ]]; do
        if curl -fsSL -o "$temp_dir/$asset_name" "$download_url"; then
            break
        fi
        retry_count=$((retry_count + 1))
        if [[ $retry_count -eq $max_retries ]]; then
            rm -rf "$temp_dir"
            fatal "Failed to download asset from $download_url after $max_retries attempts"
        fi
        warn "Download failed, retrying ($retry_count/$max_retries)..."
        sleep 2
    done
    
    # Try to download and verify checksums if available
    checksum_url="${download_url}.sha256"
    checksum_file="$temp_dir/${asset_name}.sha256"
    
    if curl -fsSL -o "$checksum_file" "$checksum_url" 2>/dev/null; then
        info "Verifying checksum..."
        if command -v sha256sum >/dev/null 2>&1; then
            if (cd "$temp_dir" && sha256sum -c "${asset_name}.sha256" >/dev/null 2>&1); then
                success "Checksum verification passed"
            else
                warn "Checksum verification failed, but continuing..."
            fi
        elif command -v shasum >/dev/null 2>&1; then
            local expected_checksum actual_checksum
            expected_checksum=$(awk '{print $1}' "$checksum_file")
            actual_checksum=$(shasum -a 256 "$temp_dir/$asset_name" | awk '{print $1}')
            if [[ "$expected_checksum" == "$actual_checksum" ]]; then
                success "Checksum verification passed"
            else
                warn "Checksum verification failed, but continuing..."
            fi
        else
            info "No checksum utility found, skipping verification"
        fi
    else
        info "No checksum file found, skipping verification"
    fi
    
    DOWNLOADED_FILE="$temp_dir/$asset_name"
    TEMP_DIR="$temp_dir"
    
    success "Downloaded $asset_name"
}

# Extract the downloaded archive
extract_archive() {
    local extract_dir="$TEMP_DIR/extract"
    
    mkdir -p "$extract_dir"
    
    info "Extracting archive..."
    
    case "$ARCHIVE_SUFFIX" in
        "tar.gz")
            tar -xzf "$DOWNLOADED_FILE" -C "$extract_dir" || fatal "Failed to extract tar.gz archive"
            ;;
        "zip")
            unzip -q "$DOWNLOADED_FILE" -d "$extract_dir" || fatal "Failed to extract zip archive"
            ;;
        *)
            fatal "Unsupported archive format: $ARCHIVE_SUFFIX"
            ;;
    esac
    
    # Find the binary (it might be in a subdirectory)
    BINARY_PATH=$(find "$extract_dir" -name "$BIN_NAME" -type f | head -1)
    
    if [[ -z "$BINARY_PATH" ]] || [[ ! -f "$BINARY_PATH" ]]; then
        fatal "Could not find $BIN_NAME binary in extracted archive"
    fi
    
    success "Extracted binary: $BINARY_PATH"
}

# Install the binary
install_binary() {
    info "Installing $BIN_NAME to $INSTALL_DIR..."
    
    # Create install directory if it doesn't exist
    mkdir -p "$INSTALL_DIR"
    
    # Copy binary and make executable
    cp "$BINARY_PATH" "$INSTALL_DIR/$BIN_NAME"
    chmod +x "$INSTALL_DIR/$BIN_NAME"
    
    success "Installed $BIN_NAME to $INSTALL_DIR/$BIN_NAME"
}

# Setup shell completion (if available)
setup_completions() {
    # Check if the binary supports shell completions
    if "$INSTALL_DIR/$BIN_NAME" --help 2>/dev/null | grep -q "completion\|completions"; then
        info "Setting up shell completions..."
        
        # Try to setup completions for common shells
        for shell in bash zsh fish; do
            if command -v "$shell" >/dev/null 2>&1; then
                case "$shell" in
                    bash)
                        comp_dir="$HOME/.local/share/bash-completion/completions"
                        mkdir -p "$comp_dir"
                        if "$INSTALL_DIR/$BIN_NAME" completion bash > "$comp_dir/$BIN_NAME" 2>/dev/null; then
                            info "Installed bash completions"
                        fi
                        ;;
                    zsh)
                        comp_dir="$HOME/.local/share/zsh/site-functions"
                        mkdir -p "$comp_dir"
                        if "$INSTALL_DIR/$BIN_NAME" completion zsh > "$comp_dir/_$BIN_NAME" 2>/dev/null; then
                            info "Installed zsh completions"
                        fi
                        ;;
                    fish)
                        comp_dir="$HOME/.config/fish/completions"
                        mkdir -p "$comp_dir"
                        if "$INSTALL_DIR/$BIN_NAME" completion fish > "$comp_dir/$BIN_NAME.fish" 2>/dev/null; then
                            info "Installed fish completions"
                        fi
                        ;;
                esac
            fi
        done
    fi
}

# Verify installation
verify_installation() {
    info "Verifying installation..."
    
    if [[ ! -x "$INSTALL_DIR/$BIN_NAME" ]]; then
        fatal "Installation verification failed: $INSTALL_DIR/$BIN_NAME is not executable"
    fi
    
    # Test that the binary runs
    if ! "$INSTALL_DIR/$BIN_NAME" --version >/dev/null 2>&1; then
        warn "Binary was installed but --version command failed. Installation may be incomplete."
        return
    fi
    
    local installed_version
    installed_version=$("$INSTALL_DIR/$BIN_NAME" --version 2>/dev/null | head -1 || echo "unknown")
    
    success "Installation verified. Installed version: $installed_version"
}

# Add to PATH instructions
show_path_instructions() {
    if ! echo "$PATH" | grep -q "$INSTALL_DIR"; then
        warn "Installation directory $INSTALL_DIR is not in your PATH"
        info "Add the following to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
        echo ""
        echo "    export PATH=\"$INSTALL_DIR:\$PATH\""
        echo ""
        info "Then restart your shell or run: source ~/.bashrc (or ~/.zshrc)"
    fi
}

# Cleanup temporary files
cleanup() {
    if [[ -n "${TEMP_DIR:-}" ]] && [[ -d "$TEMP_DIR" ]]; then
        rm -rf "$TEMP_DIR"
    fi
}

# Main installation function
main() {
    info "YAMS Installation Script"
    info "========================"
    
    # Setup cleanup trap
    trap cleanup EXIT
    
    # Detect system and check dependencies
    detect_system
    check_dependencies
    
    # Get release information and download
    get_release_info
    download_asset
    extract_archive
    
    # Install binary
    install_binary
    setup_completions
    verify_installation
    
    # Show final instructions
    info ""
    success "🎉 YAMS $RELEASE_VERSION has been successfully installed!"
    info ""
    info "Usage: $BIN_NAME --help"
    show_path_instructions
    
    info ""
    info "Thank you for installing YAMS!"
}

# Handle command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            VERSION="$2"
            shift 2
            ;;
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --help)
            cat << EOF
YAMS Installation Script

Usage: $0 [OPTIONS]

Options:
  --version VERSION     Install specific version (default: latest)
  --install-dir DIR     Installation directory (default: ~/.local/bin)
  --help               Show this help message

Environment Variables:
  YAMS_VERSION         Version to install (default: latest)
  YAMS_INSTALL_DIR     Installation directory (default: ~/.local/bin)
  YAMS_GITHUB_USER     GitHub username (default: your-username)
  YAMS_GITHUB_REPO     GitHub repository (default: yams)

Examples:
  $0                           # Install latest version
  $0 --version 1.0.0           # Install specific version
  $0 --install-dir /usr/local/bin  # Install to system directory

EOF
            exit 0
            ;;
        *)
            fatal "Unknown option: $1. Use --help for usage information."
            ;;
    esac
done

# Run main installation
main