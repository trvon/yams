# SPDX-License-Identifier: Apache-2.0
# Dockerfile for AFL++ fuzzing environment
FROM aflplusplus/aflplusplus:latest

# Install system dependencies (per docs/BUILD-GCC.md)
RUN apt-get update && apt-get install -y \
    gcc-12 \
    g++-12 \
    g++ \
    libc6-dev \
    libstdc++-12-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libsqlite3-dev \
    protobuf-compiler \
    libprotobuf-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Ensure clang toolchains can locate standard library headers on arm64 and x86_64
ENV CPLUS_INCLUDE_PATH=/usr/include/c++/12:/usr/include/aarch64-linux-gnu/c++/12:/usr/include/x86_64-linux-gnu/c++/12:/usr/include
ENV C_INCLUDE_PATH=/usr/include
ENV CXXFLAGS="-Wno-error=pointer-to-int-cast -Wno-error=int-to-pointer-cast"

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
      -o build_tests=False \
      -o "sqlite3/*:fts5=True" \
      -o "zstd/*:build_programs=False" && \
    cd build/fuzzing && \
    . ./build-debug/conan/conanbuild.sh && \
    CC=afl-clang-fast CXX=afl-clang-fast++ meson setup --native-file build-debug/conan/conan_meson_native.ini --buildtype=debug -Dbuild-fuzzers=true -Dbuild-tests=false -Dbuild-benchmarks=false ../.. && \
    meson compile

# Set up fuzzing directories
RUN mkdir -p /fuzz/corpus /fuzz/findings

WORKDIR /src
CMD ["/bin/bash"]
