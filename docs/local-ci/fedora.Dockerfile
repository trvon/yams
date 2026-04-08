# syntax=docker/dockerfile:1.5
FROM --platform=linux/amd64 fedora:41

# Install-test container for verifying YAMS RPM installation from the yum repo.
# Unlike the debian/ubuntu Dockerfiles (build containers), this only tests
# that the published RPM installs and runs correctly.
#
# Usage (from repo root):
#   docker build -t yams/test-fedora -f docs/local-ci/fedora.Dockerfile .
#   docker run --rm yams/test-fedora --version
#   docker run --rm yams/test-fedora rpm -qi yams

# Configure YAMS yum repository
RUN printf '[yams]\nname=YAMS Repository\nbaseurl=https://repo.yamsmemory.ai/yumrepo/\nenabled=1\ngpgcheck=0\nrepo_gpgcheck=0\n' \
      > /etc/yum.repos.d/yams.repo

# Install YAMS from repository
RUN dnf makecache && \
    dnf download -y --arch x86_64 yams && \
    ls -la *.rpm && \
    rpm -ivh --replacefiles *.rpm && \
    rm -f *.rpm && \
    dnf clean all

# Verify installation
RUN yams --version

ENTRYPOINT ["yams"]
