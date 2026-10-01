# Generic runtime image for the YAMS p2p mesh harness (tests/integration/p2p_mesh/mesh.py).
#
# The image carries no YAMS build of its own. mesh.py stages the binaries under payload/ in the
# build context: either a copy of a host build (bin/ + lib/) or a released .deb. The base image's
# glibc/libstdc++ must be at least as new as the build host's; the default tracks Debian testing
# for that reason. Use --base ubuntu:24.04 (or similar) for a released .deb.
ARG BASE=debian:testing-slim
FROM ${BASE}

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates openssl iproute2 procps libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY payload/ /payload/
RUN set -e; \
    if ls /payload/*.deb >/dev/null 2>&1; then \
        apt-get update && apt-get install -y --no-install-recommends /payload/*.deb \
        && rm -rf /var/lib/apt/lists/*; \
    else \
        mkdir -p /opt/yams && cp -a /payload/. /opt/yams/; \
    fi; \
    rm -rf /payload

# One unprivileged user and one volume root per node; nothing is shared with the host.
RUN useradd --uid 1000 --create-home --home-dir /home/yams yams \
    && mkdir -p /node \
    && chown yams:yams /node
COPY node.sh /usr/local/bin/mesh-node.sh
RUN chmod 0755 /usr/local/bin/mesh-node.sh

ENV PATH=/opt/yams/bin:${PATH}
USER yams
WORKDIR /node
ENTRYPOINT ["/usr/local/bin/mesh-node.sh"]
