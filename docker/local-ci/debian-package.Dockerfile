# Cached builder image for the local package build+validate lane.
#
# It pre-bakes the toolchain by invoking scripts/build-deb.sh's own `provision`
# entrypoint, so the package list is single-sourced in that script and never
# drifts from what the real build uses. The actual build still runs the full
# scripts/build-deb.sh at `docker run` time.
#
# Built and driven by scripts/local-ci/package-lane.sh.
FROM debian:trixie

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/root/.local/bin:${PATH}"

# Only the packaging script is needed to provision; copying it alone keeps this
# layer cached until the script's toolchain logic actually changes.
COPY scripts/build-deb.sh /opt/yams/scripts/build-deb.sh
RUN bash /opt/yams/scripts/build-deb.sh provision

# pipx installs conan to /root/.local/bin; ensure login shells (bash -lc) resolve
# it too, since /etc/profile resets PATH and would otherwise drop it.
RUN printf 'export PATH=/root/.local/bin:$PATH\n' > /etc/profile.d/00-yams-path.sh

WORKDIR /workspace
