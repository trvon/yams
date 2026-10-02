# Debian/Ubuntu systemd-in-Docker substrate for scripts/local-ci/package-validate.sh.
# BASE_IMAGE selects the distro (debian:trixie-slim, ubuntu:24.04, ...).
ARG BASE_IMAGE=debian:trixie-slim
FROM ${BASE_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive
ENV container=docker

RUN set -eux; \
  apt-get update && apt-get install -y --no-install-recommends \
    systemd \
    systemd-sysv \
    dbus \
    procps \
    psmisc \
    iproute2 \
    binutils \
    passwd \
    dbus-user-session \
    libpam-systemd \
    util-linux \
    ca-certificates && \
  rm -rf /var/lib/apt/lists/*

STOPSIGNAL SIGRTMIN+3
CMD ["/sbin/init"]
