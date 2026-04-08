ARG YAMS_IMAGE=yams-local-runtime
FROM ${YAMS_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive
ENV container=docker

USER root

RUN set -eux; \
  for attempt in 1 2 3; do \
    if apt-get update && apt-get install -y --no-install-recommends \
      systemd \
      systemd-sysv \
      dbus \
      procps \
      psmisc \
      iproute2; then \
      rm -rf /var/lib/apt/lists/*; \
      break; \
    fi; \
    if [ "$attempt" -eq 3 ]; then \
      exit 1; \
    fi; \
    sleep 5; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
  done

RUN useradd -m -s /bin/bash tester && mkdir -p /etc/yams /usr/lib/systemd/system

RUN ln -sf /usr/local/bin/yams /usr/bin/yams && \
  if [ -f /usr/local/bin/yams-daemon ]; then ln -sf /usr/local/bin/yams-daemon /usr/bin/yams-daemon; fi

COPY packaging/systemd/yams-daemon.service /usr/lib/systemd/system/yams-daemon.service

STOPSIGNAL SIGRTMIN+3
CMD ["/sbin/init"]
