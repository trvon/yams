# syntax=docker/dockerfile:1.5
FROM --platform=linux/amd64 fedora:41

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG YAMS_REPO_BASEURL=https://repo.yamsmemory.ai/yumrepo/

# Install-test container for verifying YAMS RPM installation from the yum repo.
# Unlike the debian/ubuntu Dockerfiles (build containers), this only tests
# that the packaged RPM installs and runs correctly.
#
# Usage (from repo root):
#   docker build -t yams/test-fedora -f docs/local-ci/fedora.Dockerfile .
#   docker run --rm yams/test-fedora --version
#   docker run --rm yams/test-fedora rpm -qi yams
#
# Source precedence during build:
#   1) local yum repo at ./yumrepo/
#   2) local RPM artifact under the build context (for example ./yams-*.rpm or
#      ./docs/local-ci/test-artifacts/yams-*.rpm)
#   3) published repo at ${YAMS_REPO_BASEURL}

# Install YAMS from local artifacts when available, otherwise fall back to the
# published yum repo.
RUN --mount=type=bind,source=.,target=/context,readonly \
    set -euxo pipefail && \
    install_from_repo() { \
      local repo_name="$1"; \
      local baseurl="$2"; \
      printf '[%s]\nname=YAMS Repository (%s)\nbaseurl=%s\nenabled=1\ngpgcheck=0\nrepo_gpgcheck=0\n' \
        "$repo_name" "$repo_name" "$baseurl" > /etc/yum.repos.d/yams.repo; \
      dnf makecache --disablerepo='*' --enablerepo="$repo_name"; \
      dnf download -y --disablerepo='*' --enablerepo="$repo_name" --arch x86_64 yams; \
      ls -la *.rpm; \
      rpm -ivh --replacefiles *.rpm; \
      rm -f ./*.rpm; \
      dnf clean all; \
    }; \
    local_rpm="$(find /context -maxdepth 5 -type f -name 'yams-*-linux-x86_64.rpm' | sort | tail -n 1 || true)"; \
    if [ -f /context/yumrepo/repodata/repomd.xml ]; then \
      echo "Installing YAMS from local yumrepo/"; \
      install_from_repo yams-local file:///context/yumrepo; \
    elif [ -n "$local_rpm" ]; then \
      echo "Installing YAMS from local RPM: ${local_rpm#/context/}"; \
      dnf install -y "$local_rpm"; \
      dnf clean all; \
    else \
      echo "Installing YAMS from published repo: ${YAMS_REPO_BASEURL}"; \
      install_from_repo yams "${YAMS_REPO_BASEURL}"; \
    fi

# Verify installation
RUN yams --version

ENTRYPOINT ["yams"]
