# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 YAMS Contributors
#
# Builds the YAMS daemon (saged) + CLI from source for the P2P container
# harness. Single-stage for simplicity and correctness: build tools remain in
# the image, but the binaries are copied to /usr/local/bin. Optional heavy
# integrations (ONNX, tree-sitter, zyp, MCP) are disabled so the build needs no
# runtime downloads beyond apt packages.

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        python3 \
        python3-pip \
        git \
        curl \
        ca-certificates \
        pkg-config \
        libssl-dev \
        zlib1g-dev \
        libsqlite3-dev \
        libboost-all-dev \
        libtbb-dev \
        libre2-dev \
        libprotobuf-dev \
        protobuf-compiler \
        libabsl-dev \
        libcli11-dev \
    && rm -rf /var/lib/apt/lists/*

# Additional dependencies (kept separate so the cached boost/cli11 layer above
# is reused across iterations; this layer only fetches the smaller packages).
# Retry the index fetch: ports.ubuntu.com can transiently time out.
RUN for _ in 1 2 3 4 5; do \
        apt-get update && apt-get install -y --no-install-recommends \
            nlohmann-json3-dev \
            libzstd-dev \
            libarchive-dev \
            libcurl4-openssl-dev \
            liblzma-dev \
            libspdlog-dev \
            libfmt-dev \
        && break || sleep 5; \
    done && rm -rf /var/lib/apt/lists/*

# tl-expected is a single-header library (normally from Conan, not in apt).
# Fetch it into the Homebrew-style prefix that meson's fallback probes.
RUN mkdir -p /usr/local/include/tl && \
    curl -fsSL https://cdn.jsdelivr.net/gh/TartanLlama/expected@v1.1.0/include/tl/expected.hpp \
        -o /usr/local/include/tl/expected.hpp

RUN pip3 install --break-system-packages meson

WORKDIR /yams

COPY . .

# Submodules are already materialized in the host checkout (e.g. third_party/
# simeon, sqlite-vec-cpp, symspell), and .dockerignore excludes .git/, so no
# git-submodule step is needed (or possible) here.

RUN meson setup build/release --buildtype=release \
        -Denable-onnx=disabled \
        -Dplugin-onnx=false \
        -Dplugin-symbols=false \
        -Dplugin-zyp=false \
        -Dplugin-glint=false

RUN meson compile -C build/release yams-daemon
RUN meson compile -C build/release yams-cli

RUN cp /yams/build/release/src/daemon/yams-daemon /usr/local/bin/saged && \
    cp /yams/build/release/tools/yams-cli/yams-cli /usr/local/bin/yams

# The daemon dynamically links the plugin resource host, which is built as a
# shared library even with ONNX disabled. Place it where the loader finds it.
RUN cp /yams/build/release/src/daemon/resource/libyams_onnx_resource.so.0.19.0 \
        /usr/local/lib/libyams_onnx_resource.so.0 && \
    ldconfig

ENTRYPOINT ["/usr/local/bin/saged"]
