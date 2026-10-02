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

# Test-substrate only: Fedora's systemd-user PAM stack (pam_unix via
# unix_chkpwd, pam_selinux, pam_loginuid) cannot run inside this container, so
# user@.service never starts. The package validation needs a user manager to
# exercise the yams systemd user unit; give it a minimal stack that still runs
# pam_systemd (which sets XDG_RUNTIME_DIR).
RUN printf '%s\n' 'account  required pam_permit.so' 'session  required pam_permit.so' \
    'session  optional pam_systemd.so' > /etc/pam.d/systemd-user

STOPSIGNAL SIGRTMIN+3
CMD ["/usr/sbin/init"]
