# syntax=docker/dockerfile:1.5
FROM --platform=linux/amd64 ubuntu:24.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

# Local CI container for building/testing/installing YAMS on Ubuntu
# Usage example (from repo root):
#   docker build -t yams/ci-ubuntu:24.04 -f docker/local-ci/ubuntu.Dockerfile .
#   docker run --rm -v "$PWD:/work" -w /work yams/ci-ubuntu:24.04 bash -lc '
#     rm -rf build prefix consumer_build || true &&
#     cmake -S . -B build -G Ninja -DYAMS_BUILD_PROFILE=dev -DCMAKE_INSTALL_PREFIX=/work/prefix &&
#     cmake --build build -j &&
#     ctest --test-dir build --output-on-failure &&
#     cmake --install build &&
#     cmake -S test/consumer -B consumer_build -G Ninja -DCMAKE_PREFIX_PATH=/work/prefix &&
#     cmake --build consumer_build -j &&
#     ctest --test-dir consumer_build --output-on-failure
#   '

# Base toolchain + CI dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    ninja-build \
    pkg-config \
    git \
    curl \
    jq \
    zip \
    unzip \
    tar \
    ca-certificates \
    libssl-dev \
    libsqlite3-dev \
    protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /work

# Quick sanity check of core tools
RUN echo "gcc: $(gcc --version | head -n1)" && \
    echo "cmake: $(cmake --version | head -n1)" && \
    echo "ninja: $(ninja --version || true)" && \
    echo "git: $(git --version)"
