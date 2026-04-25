# syntax=docker/dockerfile:1.7-labs

# Stage 0: base build dependencies & toolchain
# Using Ubuntu 24.04 for GCC 13+ which supports C++23 <format> header
FROM ubuntu:24.04 AS deps
# FROM debian:trixie-slim AS deps
ARG DEBIAN_FRONTEND=noninteractive
ARG ZIG_VERSION=0.15.2
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

# Install Zig 0.15.x for zyp PDF plugin.
# zpdf is not Zig 0.16-compatible yet, so fail fast if this pin is overridden.
RUN set -eux; \
  ARCH=$(uname -m); \
  case "$ARCH" in \
  x86_64) ZIG_ARCH="x86_64" ;; \
  aarch64) ZIG_ARCH="aarch64" ;; \
  *) echo "Unsupported architecture: $ARCH"; exit 1 ;; \
  esac; \
  case "${ZIG_VERSION}" in \
  0.15.*) ;; \
  *) echo "Unsupported Zig version ${ZIG_VERSION}; zpdf requires Zig 0.15.x"; exit 1 ;; \
  esac; \
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
  if [ "${BUILD_TESTS}" = "true" ]; then \
  echo "BUILD_TESTS=true: enabling Release tests"; \
  SETUP_ARGS="Release --with-tests"; \
  else \
  SETUP_ARGS="Release"; \
  fi; \
  # shellcheck disable=SC2086
  if ./setup.sh ${SETUP_ARGS}; then \
  break; \
  else \
  echo "setup.sh attempt $attempt failed"; \
  if [ $attempt -lt 3 ]; then \
  echo "Cleaning cache and retrying after 30s..."; \
  rm -rf /src/build/release; \
  rm -rf /root/.conan2/p/*/metadata.json 2>/dev/null || true; \
  conan cache clean "*" || true; \
  sleep 30; \
  else \
  echo "All attempts failed"; \
  exit 1; \
  fi; \
  fi; \
  done; \
  CONAN_BUILD_ENV="/src/build/release/build-release/conan/conanbuild.sh"; \
  if [ -f "${CONAN_BUILD_ENV}" ]; then set +u; . "${CONAN_BUILD_ENV}"; set -u; fi && \
  meson compile -C build/release -j${BUILD_JOBS} && \
  meson install -C build/release --destdir /opt/yams && \
  # Ensure Conan-provided runtime libs are present as real files (not only symlinks)
  # so that Meson tests can run in later stages without access to /root/.conan2.
  python3 - <<'PY'
import glob
import os
import platform
import shutil
import re

destinations = ['/src/build/release', '/opt/yams/usr/local/lib']
for dst in destinations:
    os.makedirs(dst, exist_ok=True)

copied = set()
created_links = set()

elf_machine_codes = {
    'x86_64': 0x3E,
    'aarch64': 0xB7,
    'arm64': 0xB7,
}
expected_machine = elf_machine_codes.get(platform.machine().lower())

def elf_machine(path: str):
    try:
        with open(path, 'rb') as fh:
            header = fh.read(20)
    except OSError:
        return None
    if len(header) < 20 or header[:4] != b'\x7fELF':
        return None
    endian = header[5]
    if endian == 1:
        return int.from_bytes(header[18:20], 'little')
    if endian == 2:
        return int.from_bytes(header[18:20], 'big')
    return None

for pat in [
    '/src/build/release/**/libtbb*.so*',
    '/src/build/release/**/libonnxruntime*.so*',
    '/root/.conan2/p/**/p/lib/libtbb*.so*',
    '/root/.conan2/p/**/p/lib/libtbbbind*.so*',
    '/root/.conan2/p/**/p/lib/libonnxruntime*.so*',
    '/root/.conan2/p/**/lib/libtbb*.so*',
    '/root/.conan2/p/**/lib/libtbbbind*.so*',
    '/root/.conan2/p/**/lib/libonnxruntime*.so*',
]:
    for p in glob.glob(pat, recursive=True):
        base = os.path.basename(p)
        rp = os.path.realpath(p)
        if not os.path.isfile(rp):
            continue
        machine = elf_machine(rp)
        if expected_machine is not None and machine is not None and machine != expected_machine:
            continue
        real_base = os.path.basename(rp)
        if real_base not in copied:
            for dst in destinations:
                dest_path = os.path.join(dst, real_base)
                if not (os.path.exists(dest_path) and os.path.samefile(rp, dest_path)):
                    shutil.copy2(rp, dest_path)
            copied.add(real_base)
        if base != real_base:
            for dst in destinations:
                link_path = os.path.join(dst, base)
                if not os.path.lexists(link_path):
                    os.symlink(real_base, link_path)
                    created_links.add((dst, base))

def ensure_soname_links(directory: str) -> int:
    created = 0
    # If we have libfoo.so.M.N or libfoo.so.M.N.K, create libfoo.so.M -> that file.
    rx = re.compile(r'^(?P<stem>lib.+\.so\.(?P<maj>\d+))\.(?P<rest>\d+(?:\.\d+)*)$')
    for name in os.listdir(directory):
        m = rx.match(name)
        if not m:
            continue
        target = name
        link_name = m.group('stem')
        link_path = os.path.join(directory, link_name)
        if os.path.lexists(link_path):
            continue
        os.symlink(target, link_path)
        created += 1
    return created

links = sum(ensure_soname_links(dst) for dst in destinations)

print(
    f'Copied {len(copied)} runtime libs into {", ".join(destinations)}; '
    f'preserved {len(created_links)} package symlinks; created {links} SONAME symlinks'
)
PY

RUN set -eux; \
  bash scripts/prune-runtime-install.sh /opt/yams/usr/local

# Stage 1.25: smoke tests (optional)
# Note: kept separate so Linux builder images can be produced even when
# unrelated tests are failing.
FROM builder AS smoke-tests
ARG BUILD_TESTS=false
ARG BUILD_JOBS=2
RUN set -eux; \
  if [ "${BUILD_TESTS}" != "true" ]; then \
  echo "BUILD_TESTS=false: skipping smoke tests"; \
  exit 0; \
  fi; \
  # Conan runtime_deploy copies shared libs into build dir; add it to loader path.
  export LD_LIBRARY_PATH="/opt/yams/usr/local/lib:/src/build/release:${LD_LIBRARY_PATH:-}"; \
  meson test -C build/release -j${BUILD_JOBS} --no-rebuild --print-errorlogs mcp_submodule cli_submodule

# Stage 1.3: full test suite (optional)
FROM builder AS tests
ARG BUILD_TESTS=false
ARG BUILD_JOBS=2
RUN set -eux; \
  if [ "${BUILD_TESTS}" != "true" ]; then \
  echo "BUILD_TESTS=false: skipping full test suite"; \
  exit 0; \
  fi; \
  export LD_LIBRARY_PATH="/opt/yams/usr/local/lib:/src/build/release:${LD_LIBRARY_PATH:-}"; \
  meson test -C build/release -j${BUILD_JOBS} --no-rebuild --print-errorlogs

# Stage 1.5: runtime self-test (fails the build if runtime deps are missing)
FROM builder AS runtime-test
RUN set -eux; \
  export LD_LIBRARY_PATH="/opt/yams/usr/local/lib:${LD_LIBRARY_PATH:-}"; \
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
ENV LD_LIBRARY_PATH="${YAMS_PREFIX}/lib:${LD_LIBRARY_PATH}"
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
