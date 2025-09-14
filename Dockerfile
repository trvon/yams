# syntax=docker/dockerfile:1.7-labs

# Stage 0: base build dependencies & toolchain
FROM ubuntu:22.04 AS deps
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git curl pkg-config ca-certificates \
    libssl-dev libsqlite3-dev protobuf-compiler libprotobuf-dev \
    libncurses-dev libcurl4-openssl-dev \
    python3 python3-venv python3-pip \
    gcc g++ ninja-build openssl lld llvm clang \
    liburing-dev ccache \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install "conan==2.3.*" "cmake==3.27.*" "ninja==1.11.*"
ENV PATH="/opt/venv/bin:${PATH}"
WORKDIR /src

# Stage 1: conan-base (only dependency graph resolution) — leveraged by warm-cache seeding
FROM deps AS conan-base
ARG YAMS_VERSION=dev
ARG GIT_COMMIT=""
ARG GIT_TAG=""

# Copy only dependency graph inputs (limits cache busting)
COPY conanfile.py conanfile.txt* CMakeLists.txt CMakePresets.json* ./
COPY conan/ ./conan/
# COPY conan.lock ./  # Uncomment if you maintain a lockfile for deterministic builds

# Detect profile + install dependencies (cached via BuildKit cache mount)
RUN --mount=type=cache,target=/root/.conan2 \
    conan profile detect --force && \
    sed -i 's/compiler.cppstd=.*/compiler.cppstd=20/' /root/.conan2/profiles/default && \
    conan install . --output-folder=build/yams-release -s build_type=Release --build=missing

# Stage 2: full build
FROM conan-base AS builder
COPY . .
# Configure & build (reuse Conan cache) — tests/benchmarks/docs OFF for lean runtime image
RUN --mount=type=cache,target=/root/.conan2 \
    cmake --preset yams-release -DYAMS_BUILD_DOCS=OFF -DYAMS_BUILD_TESTS=OFF -DYAMS_BUILD_BENCHMARKS=OFF
RUN --mount=type=cache,target=/root/.conan2 \
    cmake --build --preset yams-release --target install
RUN --mount=type=cache,target=/root/.conan2 test -d /opt/yams || cmake --install build/yams-release --prefix /opt/yams

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates liburing2 \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd -r yams && useradd -r -g yams -s /bin/false yams
COPY --from=builder /opt/yams /opt/yams
ENV PATH="/opt/yams/bin:${PATH}"
# Backward compatibility: retain standalone yams symlink
RUN ln -sf /opt/yams/bin/yams /usr/local/bin/yams && \
    if [ -f /opt/yams/bin/yams-daemon ]; then ln -sf /opt/yams/bin/yams-daemon /usr/local/bin/yams-daemon; fi && \
    mkdir -p /usr/local/share/yams/data && \
    [ -f /src/data/magic_numbers.json ] || true
COPY --from=builder /src/data/magic_numbers.json /usr/local/share/yams/data
RUN mkdir -p /home/yams/.local/share/yams /home/yams/.config/yams && chown -R yams:yams /home/yams
USER yams
WORKDIR /home/yams
ENV YAMS_STORAGE="/home/yams/.local/share/yams" \
    XDG_DATA_HOME="/home/yams/.local/share" \
    XDG_CONFIG_HOME="/home/yams/.config"
LABEL org.opencontainers.image.description="YAMS server & daemon. Run 'yams serve' for MCP or 'yams-daemon' for socket daemon."
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD yams --version || exit 1
ENTRYPOINT ["yams"]
CMD ["--help"]
