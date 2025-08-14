# Multi-stage build for YAMS
# Build stage: Full development environment
FROM ubuntu:22.04 AS builder

# Set build arguments
ARG TARGETARCH
ARG YAMS_VERSION=dev
ARG GITHUB_SHA=""

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    curl \
    pkg-config \
    libssl-dev \
    libsqlite3-dev \
    protobuf-compiler \
    libprotobuf-dev \
    libboost-all-dev \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Conan
RUN pip3 install conan==2.0.*

# Set working directory
WORKDIR /src

# Copy source code
COPY . .

# Configure Conan profile with C++20
RUN conan profile detect --force && \
    sed -i 's/compiler.cppstd=.*/compiler.cppstd=20/' ~/.conan2/profiles/default && \
    echo "Conan profile configured with C++20:" && \
    conan profile show

# Build YAMS using Conan
RUN conan install . \
    --output-folder=build/conan-release \
    -s build_type=Release \
    --build=missing && \
    cd build/conan-release && \
    cmake ../.. -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DYAMS_BUILD_PROFILE=release \
    -DYAMS_BUILD_DOCS=OFF \
    -DYAMS_BUILD_TESTS=OFF \
    -DYAMS_BUILD_MCP_SERVER=ON \
    -DYAMS_VERSION=${YAMS_VERSION} && \
    cmake --build . --parallel

# Install to staging directory
RUN cmake --install build/conan-release --prefix /opt/yams

# Runtime stage: Minimal image
FROM ubuntu:22.04 AS runtime

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libssl3 \
    libsqlite3-0 \
    libprotobuf23 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user
RUN groupadd -r yams && useradd -r -g yams -s /bin/false yams

# Copy built binary and data from builder stage
COPY --from=builder /opt/yams/bin/yams /usr/local/bin/yams
COPY --from=builder /opt/yams/share/yams/data/magic_numbers.json /usr/local/share/yams/data/magic_numbers.json

# Set up directories and permissions
RUN mkdir -p /home/yams/.local/share/yams /home/yams/.config/yams && \
    chown -R yams:yams /home/yams

# Switch to non-root user
USER yams
WORKDIR /home/yams

# Set up environment
ENV YAMS_STORAGE="/home/yams/.local/share/yams"
ENV XDG_DATA_HOME="/home/yams/.local/share"
ENV XDG_CONFIG_HOME="/home/yams/.config"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD yams --version || exit 1

# Default command
ENTRYPOINT ["yams"]
CMD ["--help"]

# Note: Labels for metadata are added via docker/metadata-action in CI
# These are fallback labels when building locally
LABEL org.opencontainers.image.title="YAMS" \
      org.opencontainers.image.description="Yet Another Memory System - High-performance content-addressed storage" \
      org.opencontainers.image.url="https://github.com/trvon/yams" \
      org.opencontainers.image.source="https://github.com/trvon/yams" \
      org.opencontainers.image.licenses="Apache-2.0"