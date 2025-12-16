FROM ubuntu:24.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

RUN set -euxo pipefail; \
    for i in 1 2 3; do \
      apt-get update && \
      apt-get install -y --no-install-recommends ca-certificates && \
      update-ca-certificates && break || { \
        echo "[apt] ca-certificates bootstrap attempt $i failed, retrying..."; \
        apt-get clean; rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/partial/*; \
        sleep $((i*2)); \
      }; \
    done

RUN set -euxo pipefail; \
    # Disable default sources and write a clean sources.list, selecting the right mirror for the architecture
    rm -f /etc/apt/sources.list; \
    rm -f /etc/apt/sources.list.d/* || true; \
    arch="$(dpkg --print-architecture)"; \
    if [[ "$arch" == "arm64" || "$arch" == "armhf" ]]; then \
      printf '%s\n' \
        'deb https://ports.ubuntu.com/ubuntu-ports noble main restricted universe multiverse' \
        'deb https://ports.ubuntu.com/ubuntu-ports noble-updates main restricted universe multiverse' \
        'deb https://ports.ubuntu.com/ubuntu-ports noble-security main restricted universe multiverse' \
        'deb https://ports.ubuntu.com/ubuntu-ports noble-backports main restricted universe multiverse' \
        > /etc/apt/sources.list; \
    else \
      printf '%s\n' \
        'deb https://archive.ubuntu.com/ubuntu noble main restricted universe multiverse' \
        'deb https://archive.ubuntu.com/ubuntu noble-updates main restricted universe multiverse' \
        'deb https://security.ubuntu.com/ubuntu noble-security main restricted universe multiverse' \
        'deb https://archive.ubuntu.com/ubuntu noble-backports main restricted universe multiverse' \
        > /etc/apt/sources.list; \
    fi

RUN set -euxo pipefail; \
    # Harden apt behavior
    echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-retries; \
    echo 'Acquire::http::No-Cache "true"; Acquire::https::No-Cache "true";' > /etc/apt/apt.conf.d/99-no-cache; \
    echo 'Acquire::By-Hash "yes";' > /etc/apt/apt.conf.d/99-by-hash; \
    echo 'Acquire::ForceIPv4 "true";' > /etc/apt/apt.conf.d/99-ipv4; \
    # Install build dependencies with retries
    for i in 1 2 3 4 5; do \
      apt-get update && \
      apt-get -y dist-upgrade && \
      apt-get install -y --no-install-recommends \
        build-essential \
        clang-18 \
        libc++-18-dev \
        libc++abi-18-dev \
        ccache \
        cmake \
        ninja-build \
        pkg-config \
        git \
        curl \
        jq \
        zip \
        unzip \
        tar \
        libkrb5-3 \
        libssl-dev \
        libsqlite3-dev \
        libncurses-dev \
        protobuf-compiler \
        meson \
        python3 \
        python3-pip \
        python3-venv && break; \
      echo "[apt] install attempt $i failed, retrying..."; \
      apt-get clean; rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/partial/*; \
      sleep $((i*2)); \
    done; \
    command -v python3 >/dev/null && command -v cmake >/dev/null && command -v git >/dev/null || { echo "[apt] install failed after retries"; exit 1; }; \
    python3 -m venv /opt/venv; \
    /opt/venv/bin/pip install --no-cache-dir "conan<3"; \
    apt-get clean; rm -rf /var/lib/apt/lists/*

# Ensure venv tools (including conan) are on PATH
ENV PATH="/opt/venv/bin:${PATH}"

# Upgrade meson in venv to latest version (system meson may be too old)
RUN /opt/venv/bin/pip install --no-cache-dir --upgrade meson

# Set Clang as default compiler
ENV CC=clang-18 \
    CXX=clang++-18

# Create symlinks for clang/clang++ to version 18
RUN ln -sf /usr/bin/clang-18 /usr/bin/clang && \
    ln -sf /usr/bin/clang++-18 /usr/bin/clang++

WORKDIR /work

# Quick sanity check of core tools
RUN echo "gcc: $(gcc --version | head -n1)" && \
    echo "clang: $(clang --version | head -n1)" && \
    echo "cmake: $(cmake --version | head -n1)" && \
    echo "ninja: $(ninja --version || true)" && \
    echo "git: $(git --version)" && \
    echo "conan: $(conan --version)" && \
    echo "ccache: $(ccache --version | head -n1)"

# Configure Conan on container startup
RUN conan profile detect --force && \
    conan remote list || conan remote add conancenter https://center.conan.io && \
    conan remote update conancenter https://center.conan.io || true

# Set environment variables to disable optional features that have missing dependencies
# These can be overridden when running the container
ENV YAMS_DISABLE_ONNX="true" \
    YAMS_DISABLE_SYMBOL_EXTRACTION="true"
