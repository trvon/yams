# syntax=docker/dockerfile:1.7-labs

# Stage 0: base build dependencies & toolchain
FROM ubuntu:22.04 AS deps
# FROM debian:trixie-slim AS deps
ARG DEBIAN_FRONTEND=noninteractive
RUN set -eux; \
  for i in 1 2 3; do \
  apt-get update && \
  apt-get install -y --no-install-recommends --fix-missing \
  build-essential git curl pkg-config ca-certificates \
  libssl-dev libsqlite3-dev protobuf-compiler libprotobuf-dev \
  libcurl4-openssl-dev \
  python3 python3-venv python3-pip \
  gcc g++ ninja-build openssl lld llvm clang \
  libc++-dev libc++abi-dev \
  liburing-dev ccache \
  cmake && \
  rm -rf /var/lib/apt/lists/* && break || \
  { echo "Attempt $i failed, retrying after 5s..."; sleep 5; apt-get clean; rm -rf /var/lib/apt/lists/*; }; \
  done

RUN python3 -m venv /opt/venv \
  && /opt/venv/bin/pip install --upgrade pip \
  && /opt/venv/bin/pip install "conan==2.5.*" "meson==1.4.*" "ninja==1.11.*"
ENV PATH="/opt/venv/bin:${PATH}"
WORKDIR /src

# Stage 1: builder - use setup.sh for consistent build process
FROM deps AS builder
ARG DOCKERFILE_CONF_REV=4
ARG YAMS_VERSION=dev
ARG GIT_COMMIT=""
ARG GIT_TAG=""
ARG BUILD_TESTS=false
ARG BUILD_BENCHMARKS=false
ARG BUILD_DOCS=false
ARG YAMS_CPPSTD=20

COPY . .

# Use setup.sh with retry logic for Conan remote issues
RUN --mount=type=cache,target=/root/.conan2 \
  set -eux; \
  # Configure Conan remotes with retry
  for i in 1 2 3; do \
  conan --version && \
  conan config set general.parallel_downloads=8 || true && \
  conan profile detect --force && \
  echo '=== Conan remotes ==='; conan remote list || true && \
  if ! conan remote list | grep -q 'conancenter'; then \
  conan remote add conancenter https://center.conan.io; \
  fi && \
  conan remote update conancenter https://center.conan.io || true && \
  conan search tree-sitter -r=conancenter || true && \
  break || \
  { echo "Conan setup attempt $i failed, retrying after 3s..."; sleep 3; }; \
  done; \
  # Force GCC for Docker builds (consistent with profiles)
  export YAMS_COMPILER=gcc; \
  export YAMS_CPPSTD=${YAMS_CPPSTD}; \
  # Run setup.sh for dependency resolution and build configuration
  if ! ./setup.sh Release; then \
  echo 'Initial setup.sh failed; attempting retry with cache clean.'; \
  rm -rf /root/.conan2/p/*/metadata.json 2>/dev/null || true; \
  conan cache clean --temp --locks || true; \
  conan search tree-sitter/0.25.9 -r=conancenter || true; \
  ./setup.sh Release; \
  fi; \
  # Build the project
  meson compile -C build/release && \
  meson install -C build/release --destdir /opt/yams

FROM debian:trixie-slim AS runtime
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
