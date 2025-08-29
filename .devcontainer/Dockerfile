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
    ninja-build \
    pkg-config \
    curl \
    pipx \
    git \
    jq \
    zip \
    unzip \
    tar \
    cppcheck \
    clang-tidy \
    clang-format \
    ca-certificates \
    openssh-client \
    libssl-dev \
    libsqlite3-dev \
    libncurses-dev \
    protobuf-compiler \
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

# Conan setuo
RUN pipx install conan
RUN conan profile detect --force
RUN sed -i 's/compiler.cppstd=.*/compiler.cppstd=20/'  /home/vscode/.conan2/profiles/default || true

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
    echo "act: $(/usr/local/bin/act --version || true)"
