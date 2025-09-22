# syntax=docker/dockerfile:1.7-labs

# Stage 0: base build dependencies & toolchain
FROM ubuntu:22.04 AS deps
# FROM debian:trixie-slim AS deps
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential git curl pkg-config ca-certificates \
  libssl-dev libsqlite3-dev protobuf-compiler libprotobuf-dev \
  libncurses-dev libcurl4-openssl-dev \
  python3 python3-venv python3-pip \
  gcc g++ ninja-build openssl lld llvm clang \
  liburing-dev ccache \
  cmake \
  && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv \
  && /opt/venv/bin/pip install --upgrade pip \
  && /opt/venv/bin/pip install "conan==2.5.*" "meson==1.4.*" "ninja==1.11.*"
ENV PATH="/opt/venv/bin:${PATH}"
WORKDIR /src

# Stage 1: conan-base (only dependency graph resolution) — leveraged by warm-cache seeding
FROM deps AS conan-base
ARG YAMS_VERSION=dev
ARG GIT_COMMIT=""
ARG GIT_TAG=""

# Copy only dependency graph inputs (limits cache busting). We no longer rely on CMake after Meson migration.
COPY conanfile.py conanfile.txt* ./
COPY conan/ ./conan/
# COPY conan.lock ./  # Uncomment if you maintain a lockfile for deterministic builds

# Detect profile + install dependencies (cached via BuildKit cache mount)
RUN --mount=type=cache,target=/root/.conan2 \
  set -eux; \
  conan --version; \
  # Speed up fetching from ConanCenter
  conan config set general.parallel_downloads=8 || true; \
  conan profile detect --force; \
  sed -i 's/compiler.cppstd=.*/compiler.cppstd=20/' /root/.conan2/profiles/default; \
  echo '=== Conan remotes (before ensure) ==='; conan remote list || true; \
  # Ensure conancenter remote exists (some base images may have empty config)
  if ! conan remote list | grep -q 'conancenter'; then \
  conan remote add conancenter https://center.conan.io; \
  fi; \
  # Ensure the URL is correct (update in-place if needed)
  conan remote update conancenter https://center.conan.io || true; \
  echo '=== Conan remotes (after ensure) ==='; conan remote list || true; \
  echo '=== Searching for libarchive/3.8.1 recipe (pre-install) ==='; conan search libarchive/3.8.1 -r=conancenter || true; \
  # Align custom host profile's compiler.version with detected clang and Conan's supported settings
  if command -v clang >/dev/null 2>&1; then \
    CLANG_MAJOR=$(clang --version | sed -n 's/.*clang version \([0-9][0-9]*\).*/\1/p' | head -1); \
    if [ -z "$CLANG_MAJOR" ]; then CLANG_MAJOR=$(clang -dumpversion | cut -d. -f1); fi; \
    if [ -n "$CLANG_MAJOR" ]; then \
      # Cap at 18 if Conan settings don't yet include newer versions
      if [ "$CLANG_MAJOR" -gt 18 ]; then CLANG_MAJOR=18; fi; \
      sed -i -E "s/^compiler.version=.*/compiler.version=${CLANG_MAJOR}/" ./conan/profiles/host-linux-clang || true; \
    fi; \
  fi; \
  if ! conan install . -pr:h ./conan/profiles/host-linux-clang -pr:b=default \
    -c tools.cmake.cmaketoolchain:cache_variables={'CMAKE_POLICY_VERSION_MINIMUM':'3.5'} \
    --output-folder=build/yams-release -s build_type=Release --build=missing; then \
  echo 'Initial conan install failed; dumping remotes and attempting a retry with cache clean.'; \
  conan cache clean --temp --locks || true; \
  # Re-try resolution of openjpeg prior to full install for clearer diagnostics
  conan search openjpeg -r=conancenter || true; \
  conan search libarchive -r=conancenter || true; \
  conan install . -pr:h ./conan/profiles/host-linux-clang -pr:b=default \
    -c tools.cmake.cmaketoolchain:cache_variables={'CMAKE_POLICY_VERSION_MINIMUM':'3.5'} \
    --output-folder=build/yams-release -s build_type=Release --build=missing; \
  fi

# Stage 2: full build
FROM conan-base AS builder
ARG BUILD_TESTS=false
ARG BUILD_BENCHMARKS=false
ARG BUILD_DOCS=false
COPY . .
# Configure & build (reuse Conan cache) — keep runtime lean by default
RUN --mount=type=cache,target=/root/.conan2 \
  # Re-evaluate OpenJPEG version in builder stage as a safeguard
  if conan search openjpeg/2.5.3 -r=conancenter >/dev/null 2>&1; then \
    export YAMS_OPENJPEG_VERSION=2.5.3; \
  else \
    export YAMS_OPENJPEG_VERSION=2.5.0; \
  fi; \
  conan install . -pr:h ./conan/profiles/host-linux-clang -pr:b=default \
    -c tools.cmake.cmaketoolchain:cache_variables={'CMAKE_POLICY_VERSION_MINIMUM':'3.5'} \
    --output-folder=build/yams-release -s build_type=Release --build=missing && \
  meson setup build/yams-release \
  $( \
    TOOLCHAIN_DIR=build/yams-release/build-release/conan; \
    if [ -f "$TOOLCHAIN_DIR/conan_meson_native.ini" ]; then \
      echo --native-file "$TOOLCHAIN_DIR/conan_meson_native.ini"; \
    elif [ -f "$TOOLCHAIN_DIR/conan_meson_cross.ini" ]; then \
      echo --cross-file "$TOOLCHAIN_DIR/conan_meson_cross.ini"; \
    else \
      echo "Conan Meson toolchain file not found in $TOOLCHAIN_DIR" >&2; \
      exit 1; \
    fi ) \
  -Dbuild-tests=${BUILD_TESTS} || (echo 'Meson setup failed' && cat build/yams-release/meson-logs/meson-log.txt && false) && \
  meson compile -C build/yams-release && \
  meson install -C build/yams-release --destdir /opt/yams

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
