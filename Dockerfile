# syntax=docker/dockerfile:1.7-labs

# Stage 0: base build dependencies & toolchain
# Using Ubuntu 24.04 for GCC 13+ which supports C++23 <format> header
FROM ubuntu:24.04 AS deps
# FROM debian:trixie-slim AS deps
ARG DEBIAN_FRONTEND=noninteractive
RUN set -eux; \
  for i in 1 2 3; do \
  apt-get update && \
  apt-get install -y --no-install-recommends --fix-missing \
  build-essential git curl pkg-config ca-certificates xz-utils \
  libssl-dev libsqlite3-dev libsqlite3-0 protobuf-compiler libprotobuf-dev \
  libcurl4-openssl-dev \
  python3 python3-venv python3-pip \
  gcc g++ ninja-build openssl lld llvm clang \
  libc++-dev libc++abi-dev \
  liburing-dev ccache \
  cmake && \
  rm -rf /var/lib/apt/lists/* && break || \
  { echo "Attempt $i failed, retrying after 5s..."; sleep 5; apt-get clean; rm -rf /var/lib/apt/lists/*; }; \
  done

# Install Rust toolchain for libsql build (pinned version to avoid cross-device link issues)
ENV RUSTUP_HOME=/usr/local/rustup \
  CARGO_HOME=/usr/local/cargo \
  PATH=/usr/local/cargo/bin:$PATH
RUN set -eux; \
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.83.0 --profile minimal && \
  rustup set auto-self-update disable && \
  rustc --version && cargo --version

# Install Zig 0.15.2 stable for zyp PDF plugin
RUN set -eux; \
  ARCH=$(uname -m); \
  case "$ARCH" in \
  x86_64) ZIG_ARCH="x86_64" ;; \
  aarch64) ZIG_ARCH="aarch64" ;; \
  *) echo "Unsupported architecture: $ARCH"; exit 1 ;; \
  esac; \
  ZIG_VERSION="0.15.2"; \
  curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" -o /tmp/zig.tar.xz && \
  tar -xJf /tmp/zig.tar.xz -C /opt && \
  mv /opt/zig-${ZIG_ARCH}-linux-${ZIG_VERSION} /opt/zig && \
  ln -sf /opt/zig/zig /usr/local/bin/zig && \
  rm /tmp/zig.tar.xz && \
  zig version

RUN python3 -m venv /opt/venv \
  && /opt/venv/bin/pip install --upgrade pip \
  && /opt/venv/bin/pip install "conan==2.5.*" "meson==1.4.*" "ninja==1.11.*"
ENV PATH="/opt/venv/bin:${PATH}"
WORKDIR /src

# Stage 1: builder - use setup.sh for consistent build process
FROM deps AS builder
ARG DOCKERFILE_CONF_REV=5
ARG YAMS_VERSION=dev
ARG GIT_COMMIT=""
ARG GIT_TAG=""
ARG BUILD_TESTS=false
ARG BUILD_BENCHMARKS=false
ARG BUILD_DOCS=false
ARG YAMS_CPPSTD=20
ARG BUILD_JOBS=2

COPY . .

# Initialize git submodules (sqlite-vec-cpp, etc.)
RUN git init 2>/dev/null || true && \
  git config --global url."https://github.com/".insteadOf "git@github.com:" && \
  git submodule update --init --recursive 2>/dev/null || \
  echo "Submodule init skipped (not a git repo or no submodules)"

# Use setup.sh with retry logic for Conan remote issues
# Configure Conan to retry downloads more aggressively for transient network failures
RUN --mount=type=cache,target=/root/.conan2 \
  --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=/usr/local/cargo/git \
  --mount=type=cache,target=/src/subprojects/libsql/target \
  set -eux; \
  # Prevent rustup from auto-updating when rust-toolchain.toml requests different targets
  export RUSTUP_TOOLCHAIN=1.83.0 && \
  mkdir -p /root/.conan2 && \
  printf 'core.net.http:timeout=120\ncore.net.http:max_retries=10\ncore.download:retry=10\ncore.download:retry_wait=5\n' > /root/.conan2/global.conf && \
  conan --version && \
  conan profile detect --force && \
  echo '=== Conan remotes ==='; conan remote list || true && \
  if ! conan remote list | grep -q 'conancenter'; then \
  conan remote add conancenter https://center2.conan.io; \
  fi && \
  conan remote update conancenter --url https://center2.conan.io || true && \
  export YAMS_COMPILER=gcc; \
  export YAMS_CPPSTD=${YAMS_CPPSTD}; \
  export YAMS_EXTRA_MESON_FLAGS="-Drequire-sqlite-vec=false"; \
  sed -i 's/\r$//' setup.sh && chmod +x setup.sh && \
  for attempt in 1 2 3; do \
  echo "=== Build attempt $attempt ==="; \
  if ./setup.sh Release; then \
  break; \
  else \
  echo "setup.sh attempt $attempt failed"; \
  if [ $attempt -lt 3 ]; then \
  echo "Cleaning cache and retrying after 30s..."; \
  rm -rf /root/.conan2/p/*/metadata.json 2>/dev/null || true; \
  conan cache clean "*" || true; \
  sleep 30; \
  else \
  echo "All attempts failed"; \
  exit 1; \
  fi; \
  fi; \
  done; \
  meson compile -C build/release -j${BUILD_JOBS} && \
  meson install -C build/release --destdir /opt/yams

# Stage 1.5: runtime self-test (fails the build if runtime deps are missing)
FROM builder AS runtime-test
RUN set -eux; \
  /opt/yams/usr/local/bin/yams --version >/dev/null; \
  if [ -x /opt/yams/usr/local/bin/yams-mcp-server ]; then /opt/yams/usr/local/bin/yams-mcp-server --help >/dev/null; fi

FROM debian:trixie-slim AS runtime
ARG DEBIAN_FRONTEND=noninteractive
RUN set -eux; \
  for i in 1 2 3; do \
  apt-get update && \
  apt-get install -y --no-install-recommends --fix-missing ca-certificates liburing2 && \
  rm -rf /var/lib/apt/lists/* && break || \
  { echo "Attempt $i failed, retrying after 5s..."; sleep 5; apt-get clean; rm -rf /var/lib/apt/lists/*; }; \
  done
RUN groupadd -r yams && useradd -r -g yams -s /bin/false yams
COPY --from=builder /opt/yams /opt/yams
ENV YAMS_PREFIX=/opt/yams/usr/local
ENV PATH="${YAMS_PREFIX}/bin:${PATH}"
# Backward compatibility: retain standalone yams symlink
RUN ln -sf ${YAMS_PREFIX}/bin/yams /usr/local/bin/yams && \
  if [ -f ${YAMS_PREFIX}/bin/yams-daemon ]; then ln -sf ${YAMS_PREFIX}/bin/yams-daemon /usr/local/bin/yams-daemon; fi && \
  mkdir -p /usr/local/share/yams/data
# Copy data files if they exist (optional)
# Note: Disabled because /src/data/magic_numbers.json doesn't exist yet
# RUN --mount=type=bind,from=builder,source=/src/data,target=/tmp/data \
#   if [ -f /tmp/data/magic_numbers.json ]; then \
#     cp /tmp/data/magic_numbers.json /usr/local/share/yams/data/; \
#   fi
RUN mkdir -p /home/yams/.local/share/yams /home/yams/.config/yams && chown -R yams:yams /home/yams
RUN mknod /dev/tty c 5 0 2>/dev/null || true && \
  chmod 666 /dev/tty 2>/dev/null || true
USER yams
WORKDIR /home/yams
ENV YAMS_STORAGE="/home/yams/.local/share/yams" \
  XDG_DATA_HOME="/home/yams/.local/share" \
  XDG_CONFIG_HOME="/home/yams/.config"
LABEL org.opencontainers.image.description="YAMS server & daemon. Run 'yams serve' for MCP or 'yams-daemon' for socket daemon."
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD yams --version || exit 1
ENTRYPOINT ["yams"]
CMD ["--help"]
