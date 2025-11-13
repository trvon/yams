# SPDX-License-Identifier: Apache-2.0
# Dockerfile for AFL++ fuzzing environment
FROM aflplusplus/aflplusplus:latest

# Install system dependencies (per docs/BUILD-GCC.md)
RUN apt-get update && apt-get install -y \
    libssl-dev \
    libcurl4-openssl-dev \
    libsqlite3-dev \
    protobuf-compiler \
    libprotobuf-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Conan and dependencies
RUN pip3 install conan

# Setup Conan profiles - detect creates default profile for build tools
RUN conan profile detect --force

# Copy source code
WORKDIR /src
COPY . .

# Export custom zstd recipe (fixes macOS Docker symlink issues)
RUN conan export conan/zstd --name=zstd --version=1.5.5

# Export other custom recipes
RUN conan export conan/qpdf && \
    conan export conan/onnxruntime

# Build fuzzers with AFL++ instrumentation
RUN mkdir -p build/fuzzing && \
    conan install . -of build/fuzzing \
      --profile:host=conan/profiles/aflplusplus-docker \
      --profile:build=default \
      --build=missing \
      -o build_tests=True \
      -o "sqlite3/*:fts5=True" \
      -o "zstd/*:build_programs=False" && \
    cd build/fuzzing && \
    . ./build-debug/conan/conanbuild.sh && \
    CC=afl-clang-fast CXX=afl-clang-fast++ meson setup --native-file build-debug/conan/conan_meson_native.ini --buildtype=debug -Dbuild-fuzzers=true ../.. && \
    meson compile

# Set up fuzzing directories
RUN mkdir -p /fuzz/corpus /fuzz/findings

WORKDIR /src
CMD ["/bin/bash"]
