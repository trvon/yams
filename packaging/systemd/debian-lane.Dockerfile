FROM debian:trixie-slim

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
    ca-certificates \
    gnupg && \
  rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash tester

STOPSIGNAL SIGRTMIN+3
CMD ["/sbin/init"]
