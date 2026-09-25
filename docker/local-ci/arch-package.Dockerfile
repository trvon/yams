# Cached builder image for the local Arch package build+validate lane.
#
# Pre-bakes the Arch build toolchain so the actual build doesn't need to
# reinstall packages on every run. The build itself uses scripts/build-arch-pkg.sh
# which invokes makepkg inside this container.
#
# Pattern: mirrors docker/local-ci/debian-package.Dockerfile.
# Built and driven by scripts/local-ci/package-lane.sh (--only arch).

# archlinux/archlinux is amd64-only; aarch64 lanes pass an Arch Linux ARM
# image (ghcr.io/menci/archlinuxarm, built from the official ALARM rootfs).
ARG ARCH_BASE_IMAGE=archlinux/archlinux:latest
FROM ${ARCH_BASE_IMAGE}

ENV PATH="/root/.local/bin:${PATH}"

# Provision base build toolchain in a single layer (keeps caching clean).
RUN set -euxo pipefail; \
    grep -q '^DisableSandbox$' /etc/pacman.conf || sed -i '/^\[options\]/a DisableSandbox' /etc/pacman.conf; \
    grep -q '^[[:space:]]*DownloadUser' /etc/pacman.conf && sed -i 's/^[[:space:]]*DownloadUser/# DownloadUser/' /etc/pacman.conf || true; \
    # Pacman keyring must be initialized before installing packages
    pacman-key --init 2>/dev/null || true; \
    pacman-key --populate archlinux 2>/dev/null || true; \
    pacman-key --populate archlinuxarm 2>/dev/null || true; \
    pacman -Syu --noconfirm --needed; \
    pacman -S --noconfirm --needed \
        base-devel \
        clang \
        cmake \
        meson \
        ninja \
        python \
        python-pip \
        ccache \
        git \
        zip \
        pkgconf \
        liburing \
        libarchive \
        taglib \
        sqlite \
        openssl \
        curl \
        protobuf \
        boost \
        lld \
        fakeroot \
        sudo; \
    # Clean package cache to keep image size manageable
    yes | pacman -Scc 2>/dev/null || true

# Install Conan via pip (not available as an Arch package at a stable version).
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir 'conan<3' && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade meson ninja

# Ensure venv tools (conan, meson, ninja) are on PATH
ENV PATH="/opt/venv/bin:${PATH}"

# Set Clang as default compiler
ENV CC=clang \
    CXX=clang++

# Boost's b2 clang toolset passes --target=arm64-pc-linux on aarch64. Clang
# cannot map that triple to Arch Linux ARM's GCC install
# (aarch64-unknown-linux-gnu), so it loses libstdc++ headers and crt objects.
# Clang reads <triple>.cfg next to its binary; point that triple at the host GCC.
RUN if [ "$(uname -m)" = "aarch64" ]; then \
        printf -- '--gcc-triple=%s\n' "$(gcc -dumpmachine)" > /usr/bin/arm64-pc-linux.cfg; \
    fi

# Configure Conan baseline profile
RUN conan profile detect --force && \
    conan remote list 2>/dev/null || conan remote add conancenter https://center.conan.io && \
    conan remote update conancenter https://center.conan.io 2>/dev/null || true

# Allow makepkg to run as root (CI containers don't have a non-root user).
# The package-lane.sh sets HOST_UID to chown artifacts back to the host user.
ENV MAKEPKG_USER=root
RUN printf '#!/bin/sh\nexit 0\n' > /usr/bin/sudo && chmod +x /usr/bin/sudo

# Persist profile.d PATH for login shells (makepkg uses bash -lc).
RUN printf 'export PATH=/opt/venv/bin:/root/.local/bin:$PATH\n' > /etc/profile.d/00-yams-path.sh

WORKDIR /workspace
