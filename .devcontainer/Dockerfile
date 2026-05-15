# syntax=docker/dockerfile:1.5
FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

# Core build and CI dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    meson \
    ninja-build \
    pkg-config \
    curl \
    pipx \
    git \
    jq \
    zip \
    unzip \
    tar \
    clangd \
    cppcheck \
    clang-tidy \
    clang-format \
    ca-certificates \
    openssh-client \
    libssl-dev \
    libsqlite3-dev \
    libncurses-dev \
    protobuf-compiler \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Install nektos/act for running GitHub Actions locally (Linux jobs)
# Automatically choose the correct binary for common architectures.
ARG ACT_VERSION=v0.2.59
RUN set -euo pipefail; \
    ARCH="$(uname -m)"; \
    case "$ARCH" in \
    x86_64)   ACT_ASSET="act_Linux_x86_64" ;; \
    aarch64)  ACT_ASSET="act_Linux_arm64" ;; \
    arm64)    ACT_ASSET="act_Linux_arm64" ;; \
    *)        ACT_ASSET="act_Linux_x86_64" ;; \
    esac; \
    curl -fsSL -o /usr/local/bin/act "https://github.com/nektos/act/releases/download/${ACT_VERSION}/${ACT_ASSET}" && \
    chmod +x /usr/local/bin/act && \
    /usr/local/bin/act --version || true

# Conan setup (not in Ubuntu 22.04 apt repos — use pip3)
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install conan && \
    conan profile detect --force && \
    sed -i 's/compiler.cppstd=.*/compiler.cppstd=20/' /root/.conan2/profiles/default || true

# Lint tools (actionlint, osv-scanner, shellcheck, opengrep)
# Installed as pre-built binaries from GitHub releases.

# actionlint — GitHub Actions workflow linter
# https://github.com/rhysd/actionlint/releases
ARG ACTIONLINT_VERSION=v1.7.12
RUN set -euo pipefail; \
    ARCH=$(uname -m); \
    case "$ARCH" in \
    x86_64)   ACTIONLINT_ASSET="actionlint_1.7.12_linux_amd64" ;; \
    aarch64)  ACTIONLINT_ASSET="actionlint_1.7.12_linux_arm64" ;; \
    arm64)    ACTIONLINT_ASSET="actionlint_1.7.12_linux_arm64" ;; \
    *)        ACTIONLINT_ASSET="actionlint_1.7.12_linux_amd64" ;; \
    esac; \
    curl -fsSL -o /tmp/actionlint.tar.gz "https://github.com/rhysd/actionlint/releases/download/${ACTIONLINT_VERSION}/${ACTIONLINT_ASSET}.tar.gz" && \
    tar -xzf /tmp/actionlint.tar.gz -C /tmp actionlint && \
    install /tmp/actionlint /usr/local/bin/ && \
    rm -f /tmp/actionlint.tar.gz

# osv-scanner — dependency CVE scanner (Google)
# https://github.com/google/osv-scanner/releases
ARG OSV_SCANNER_VERSION=v2.3.8
RUN set -euo pipefail; \
    ARCH=$(uname -m); \
    case "$ARCH" in \
    x86_64)   OSV_ASSET="osv-scanner_linux_amd64" ;; \
    aarch64)  OSV_ASSET="osv-scanner_linux_arm64" ;; \
    arm64)    OSV_ASSET="osv-scanner_linux_arm64" ;; \
    *)        OSV_ASSET="osv-scanner_linux_amd64" ;; \
    esac; \
    curl -fsSL -o /usr/local/bin/osv-scanner "https://github.com/google/osv-scanner/releases/download/${OSV_SCANNER_VERSION}/${OSV_ASSET}" && \
    chmod +x /usr/local/bin/osv-scanner

# shellcheck — shell script static analysis
# https://github.com/koalaman/shellcheck/releases
ARG SHELLCHECK_VERSION=v0.11.0
RUN set -euo pipefail; \
    curl -fsSL "https://github.com/koalaman/shellcheck/releases/download/${SHELLCHECK_VERSION}/shellcheck-${SHELLCHECK_VERSION}.linux.x86_64.tar.xz" -o /tmp/shellcheck.tar.xz && \
    tar -xJf /tmp/shellcheck.tar.xz -C /tmp && \
    install /tmp/shellcheck-${SHELLCHECK_VERSION#/v}/shellcheck /usr/local/bin/ && \
    rm -rf /tmp/shellcheck-${SHELLCHECK_VERSION#/v} /tmp/shellcheck.tar.xz

# opengrep — static analysis / code scanning engine
# https://github.com/opengrep/opengrep/releases
# The repo ships its own rules under tools/opengrep/rules/
ARG OPENGREP_VERSION=v1.21.0
RUN set -euo pipefail; \
    ARCH=$(uname -m); \
    case "$ARCH" in \
    x86_64)   OG_ASSET="opengrep-core_linux_x86.tar.gz" ;; \
    aarch64)  OG_ASSET="opengrep-core_linux_aarch64.tar.gz" ;; \
    arm64)    OG_ASSET="opengrep-core_linux_aarch64.tar.gz" ;; \
    *)        OG_ASSET="opengrep-core_linux_x86.tar.gz" ;; \
    esac; \
    curl -fsSL -o /tmp/opengrep.tar.gz "https://github.com/opengrep/opengrep/releases/download/${OPENGREP_VERSION}/${OG_ASSET}" && \
    tar -xzf /tmp/opengrep.tar.gz -C /tmp && \
    install /tmp/opengrep-core /usr/local/bin/opengrep && \
    chmod +x /usr/local/bin/opengrep && \
    rm -f /tmp/opengrep.tar.gz /tmp/opengrep-core

# Optional: place project setup scripts in a known location
# A matching postCreate.sh should exist at .devcontainer/postCreate.sh in the repo.
RUN install -d -o root -g root /usr/local/share/yams
COPY postCreate.sh /usr/local/share/yams/postCreate.sh
RUN chmod +x /usr/local/share/yams/postCreate.sh

# Default working directory (Dev Containers mounts the repo at /workspaces/<name>)
WORKDIR /workspaces

# Display toolchain versions for quick verification
RUN echo "gcc: $(gcc --version | head -n1)" && \
    echo "cmake: $(cmake --version | head -n1)" && \
    echo "ninja: $(ninja --version || true)" && \
    echo "git: $(git --version)" && \
    echo "act: $(/usr/local/bin/act --version || true)" && \
    echo "clang-format: $(clang-format --version | head -n1)" && \
    echo "clang-tidy: $(clang-tidy --version | head -n1)" && \
    echo "cppcheck: $(cppcheck --version)" && \
    echo "actionlint: $(actionlint --version 2>&1 || echo not found)" && \
    echo "osv-scanner: $(osv-scanner --version 2>&1 || echo not found)" && \
    echo "shellcheck: $(shellcheck --version | head -n1)" && \
    echo "npm: $(npm --version)" && \
    echo "node: $(node --version)" && \
    echo "conan: $(conan --version)"
