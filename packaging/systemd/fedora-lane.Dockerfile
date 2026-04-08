FROM fedora:42

ENV container=docker

RUN set -eux; \
  dnf install -y \
    systemd \
    procps-ng \
    iproute \
    shadow-utils \
    findutils \
    ca-certificates && \
  dnf clean all

RUN useradd -m -s /bin/bash tester

STOPSIGNAL SIGRTMIN+3
CMD ["/usr/sbin/init"]
