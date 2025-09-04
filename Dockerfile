# syntax=docker/dockerfile:1.7-labs
FROM ubuntu:22.04 AS builder

ARG YAMS_VERSION=dev
ARG GIT_COMMIT=""
ARG GIT_TAG=""

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git curl pkg-config ca-certificates \
    libssl-dev libsqlite3-dev protobuf-compiler libprotobuf-dev \
    libncurses-dev libcurl4-openssl-dev \
    python3 python3-venv python3-pip \
    gcc g++ ninja-build openssl lld llvm clang \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install "conan==2.3.*" "cmake==3.27.*" "ninja==1.11.*"
ENV PATH="/opt/venv/bin:${PATH}"

WORKDIR /src

# 1) Copy only dependency graph inputs (keeps conan install cacheable)
COPY conanfile.py conanfile.txt* CMakeLists.txt CMakePresets.json* ./
COPY conan/ ./conan/
# Optional but recommended if you maintain it:
# COPY conan.lock ./

# 2) Create/update profile once (stable layer)
RUN --mount=type=cache,target=/root/.conan2 \
    conan profile detect --force && \
    sed -i 's/compiler.cppstd=.*/compiler.cppstd=20/' /root/.conan2/profiles/default && \
    conan profile show && \
    conan install . --output-folder=build/yams-release -s build_type=Release --build=missing

# 3) Now copy the rest of the source (this won’t invalidate the conan install layer)
COPY . .

RUN --mount=type=cache,target=/root/.conan2 cmake --preset yams-release -DYAMS_BUILD_DOCS=OFF
RUN --mount=type=cache,target=/root/.conan2 cmake --build --preset yams-release
RUN --mount=type=cache,target=/root/.conan2 cmake --install build/yams-release --prefix /opt/yams

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd -r yams && useradd -r -g yams -s /bin/false yams
COPY --from=builder /opt/yams/bin/yams /usr/local/bin/yams
COPY --from=builder /src/data/magic_numbers.json /usr/local/share/yams/data
RUN mkdir -p /home/yams/.local/share/yams /home/yams/.config/yams && chown -R yams:yams /home/yams
USER yams
WORKDIR /home/yams
ENV YAMS_STORAGE="/home/yams/.local/share/yams" \
    XDG_DATA_HOME="/home/yams/.local/share" \
    XDG_CONFIG_HOME="/home/yams/.config"
LABEL org.opencontainers.image.description="YAMS MCP server (stdio only). Run 'yams serve'."
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD yams --version || exit 1
ENTRYPOINT ["yams"]
CMD ["--help"]
