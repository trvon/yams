# Minimal Arch Linux systemd-in-Docker substrate for package validation.
#
# Used by scripts/local-ci/package-validate.sh to boot an Arch container under
# /usr/lib/systemd/systemd, install the built .pkg.tar.zst package, and
# smoke-test the yams-daemon service end-to-end.
#
# Pattern: mirrors packaging/systemd/debian-lane.Dockerfile and
# packaging/systemd/fedora-lane.Dockerfile.

# archlinux/archlinux is amd64-only; aarch64 lanes pass an Arch Linux ARM
# image (ghcr.io/menci/archlinuxarm, built from the official ALARM rootfs).
ARG ARCH_BASE_IMAGE=archlinux/archlinux:latest
FROM ${ARCH_BASE_IMAGE}

ENV container=docker

RUN set -eux; \
    grep -q '^DisableSandbox$' /etc/pacman.conf || sed -i '/^\[options\]/a DisableSandbox' /etc/pacman.conf; \
    grep -q '^[[:space:]]*DownloadUser' /etc/pacman.conf && sed -i 's/^[[:space:]]*DownloadUser/# DownloadUser/' /etc/pacman.conf || true; \
    pacman-key --init 2>/dev/null || true; \
    pacman-key --populate archlinux 2>/dev/null || true; \
    pacman-key --populate archlinuxarm 2>/dev/null || true; \
    pacman -Syu --noconfirm --needed; \
    pacman -S --noconfirm --needed \
        systemd \
        procps-ng \
        iproute2 \
        shadow \
        findutils \
        ca-certificates; \
    yes | pacman -Scc 2>/dev/null || true

RUN useradd -m -s /bin/bash tester

STOPSIGNAL SIGRTMIN+3
CMD ["/usr/lib/systemd/systemd"]
