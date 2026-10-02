# Fedora systemd-in-Docker substrate for scripts/local-ci/package-validate.sh.
ARG BASE_IMAGE=fedora:42
FROM ${BASE_IMAGE}

ENV container=docker

RUN set -eux; \
  dnf install -y \
    systemd \
    systemd-pam \
    procps-ng \
    iproute \
    shadow-utils \
    util-linux \
    binutils \
    findutils \
    ca-certificates && \
  dnf clean all

STOPSIGNAL SIGRTMIN+3
CMD ["/usr/sbin/init"]
