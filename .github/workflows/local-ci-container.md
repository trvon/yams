# Local containerized CI for YAMS (devcontainer + matrix simulation)

This document provides a ready-to-use developer container and local tooling to simulate the GitHub Actions Linux job(s) in a container before pushing. It also shows how to run a downstream “consumer” integration test locally exactly like CI does. macOS runners can’t be containerized; for macOS we either test on a mac host or rely on GitHub-hosted runners. We offer notes for cross-arch testing and options to validate per-arch Linux builds locally.

Contents
- Devcontainer for an isolated, reproducible CI dev environment
- Local “CI matrix” simulation scripts
- Integration with CTest to run the downstream consumer test locally
- act-based local GitHub Actions runner (Linux jobs)
- How to split macOS release into per-arch artifacts in CI (already done)
- Guidance to test mac builds on mac hardware (non-containerized)

Prerequisites
- Docker 20+ with BuildKit (for OOTB performance)
- VS Code with Dev Containers extension (recommended), OR plain Docker + bash
- A Linux host or macOS host with Docker Desktop
- For running act: Docker and network access to fetch images


1) Devcontainer for YAMS CI

Create the following files to spin up a containerized developer environment that mirrors the Ubuntu GitHub Actions jobs.

.devcontainer/devcontainer.json
```/.devcontainer/devcontainer.json#L1-75
{
  "name": "YAMS CI DevContainer",
  "build": {
    "dockerfile": "Dockerfile"
  },
  "features": {
    "ghcr.io/devcontainers/features/docker-in-docker:2": {}
  },
  "runArgs": [
    "--init"
  ],
  "containerEnv": {
    "DEBIAN_FRONTEND": "noninteractive"
  },
  "customizations": {
    "vscode": {
      "extensions": [
        "ms-vscode.cpptools",
        "ms-vscode.cmake-tools",
        "twxs.cmake",
        "ms-azuretools.vscode-docker"
      ]
    }
  },
  "remoteUser": "vscode",
  "postCreateCommand": "bash /usr/local/share/yams/postCreate.sh",
  "mounts": [
    "source=/var/lib/docker,target=/var/lib/docker,type=volume"
  ]
}
```

.devcontainer/Dockerfile
```/.devcontainer/Dockerfile#L1-120
FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Base tools + YAMS CI deps (Ubuntu path)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential cmake ninja-build pkg-config git curl jq zip unzip tar \
      libssl-dev libsqlite3-dev protobuf-compiler ca-certificates \
      openssh-client && \
    rm -rf /var/lib/apt/lists/*

# Optional: install nektos/act for local GH Actions runs (Linux-only jobs)
ARG ACT_VERSION=v0.2.59
RUN curl -fsSL -o /usr/local/bin/act \
      "https://github.com/nektos/act/releases/download/${ACT_VERSION}/act_Linux_x86_64" && \
    chmod +x /usr/local/bin/act

# Provide a postCreate script to prepare the workspace env
RUN install -d -o root -g root /usr/local/share/yams
COPY postCreate.sh /usr/local/share/yams/postCreate.sh
RUN chmod +x /usr/local/share/yams/postCreate.sh

# Convenience: non-root user is already set to "vscode" by base image
```

.devcontainer/postCreate.sh
```/.devcontainer/postCreate.sh#L1-120
#!/usr/bin/env bash
set -euo pipefail

echo "[postCreate] Configuring environment..."

# Enable git safe directory in case of volume permissions
git config --global --add safe.directory /workspaces/yams || true

# Confirm toolchain
echo "[postCreate] gcc version: $(gcc --version | head -n1)"
echo "[postCreate] cmake version: $(cmake --version | head -n1)"
echo "[postCreate] act version: $(act --version || echo 'act not installed')"
echo "[postCreate] Done."
```

How to use the devcontainer
- Open the repository in VS Code
- Reopen in Container (Dev Containers extension)
- The container has cmake, gcc/clang toolchains, OpenSSL + SQLite headers, protobuf-compiler, and act.
- You can build, test, install, and run the downstream consumer test (see sections 2–4).


2) Local CI matrix simulation (Linux)

We provide a Dockerized harness to mimic the Ubuntu job. This will:
- Build and test YAMS
- Install into a prefix
- Configure and build the downstream consumer against find_package(Yams)

Create the local CI harness files:

docker/local-ci/ubuntu.Dockerfile
```/docker/local-ci/ubuntu.Dockerfile#L1-120
FROM ubuntu:22.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential cmake ninja-build pkg-config git curl jq zip unzip tar \
      libssl-dev libsqlite3-dev protobuf-compiler ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /work
```

scripts/local-ci/matrix.sh
```/scripts/local-ci/matrix.sh#L1-200
#!/usr/bin/env bash
set -euo pipefail

# Simulate "ubuntu-latest" job in a container. macOS is not containerized.
# Usage: scripts/local-ci/matrix.sh [path-to-repo]
REPO_DIR="${1:-$(pwd)}"

IMAGE="yams/ci-ubuntu:22.04"
CONTAINER_NAME="yams-ci-ubuntu"

# Build CI image (one-time; cached later)
docker build -t "${IMAGE}" -f docker/local-ci/ubuntu.Dockerfile .

# Run the CI steps:
# - Configure, build, test
# - Install to /work/prefix
# - Configure/build downstream consumer test using CMAKE_PREFIX_PATH
docker run --rm --name "${CONTAINER_NAME}" \
  -v "${REPO_DIR}:/work:rw" \
  -w /work \
  "${IMAGE}" bash -lc '
    set -euxo pipefail
    rm -rf build prefix consumer_build || true
    cmake -S . -B build -G Ninja \
      -DYAMS_BUILD_PROFILE=dev \
      -DCMAKE_INSTALL_PREFIX=/work/prefix
    cmake --build build -j
    ctest --test-dir build --output-on-failure
    cmake --install build

    cmake -S test/consumer -B consumer_build -G Ninja \
      -DCMAKE_PREFIX_PATH=/work/prefix
    cmake --build consumer_build -j
    ctest --test-dir consumer_build --output-on-failure
  '

echo "Local CI completed successfully (Ubuntu container)."
```

How to run local matrix (Linux)
- Linux host or macOS with Docker Desktop:
  - bash scripts/local-ci/matrix.sh
- This uses docker/local-ci/ubuntu.Dockerfile as the job image and maps the repo in.


3) Run the downstream consumer test locally via CTest (no Docker required)

This repo already includes a CTest-driven downstream consumer test that:
- Installs the current build into a stage prefix inside the build tree
- Configures/builds the minimal consumer against that staged install
- Runs the consumer’s own tests (tiny executable)

From the host or devcontainer:
- cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
- cmake --build build -j
- ctest --test-dir build --output-on-failure -R '^consumer_'

This uses the tests added in CMake to perform the stage/install/configure/build/run cycle end-to-end.


4) Run GitHub Actions locally with act (Linux jobs only)

You can exercise your GitHub Actions workflow for “ubuntu-latest” using act. macOS runners cannot be emulated in containers, so macOS jobs will be skipped.

Recommended runner image mapping:
- act uses a Docker image to represent ubuntu-latest. Use:
  - -P ubuntu-latest=catthehacker/ubuntu:act-22.04
- Ensure Docker is available.

Examples
- Run default CI workflow on push event:
  - act push -P ubuntu-latest=catthehacker/ubuntu:act-22.04
- Run a specific job (from current ci.yml):
  - act -j build-test-package -P ubuntu-latest=catthehacker/ubuntu:act-22.04
- Skip macOS in act:
  - macOS jobs won’t run because no mac runner is available; act will just run the Linux matrix.

Notes
- act runs jobs inside containers, so it matches our Dockerized Linux environment reasonably well.
- OpenSSL and SQLite dev headers are already installed by our container/devcontainer images. If act uses a different image, make sure to install them within the job steps or bake them into a custom image.


5) macOS builds locally (per-arch) and in CI

- In GitHub Actions, we already split macOS build artifacts into per-arch via:
  - -DCMAKE_OSX_ARCHITECTURES="x86_64"
  - -DCMAKE_OSX_ARCHITECTURES="arm64"
- To test macOS builds locally on mac hardware:
  - Ensure Homebrew provides openssl@3 and sqlite3
  - export OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)
  - cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
  - cmake --build build -j
  - ctest --test-dir build --output-on-failure
  - cmake --install build --prefix ./prefix
  - cmake -S test/consumer -B consumer_build -DCMAKE_PREFIX_PATH=$PWD/prefix
  - cmake --build consumer_build -j && ctest --test-dir consumer_build --output-on-failure

Note: macOS builds cannot run inside a Linux container.


6) Optional advanced: multi-arch Linux builds (x86_64/arm64) with buildx

If you want to sanity-check Linux multi-arch packaging:
- Enable QEMU on your host for emulated builds:
  - docker run --rm --privileged tonistiigi/binfmt --install all
- Build images with buildx to verify cross-arch build/install steps. For example, you could wrap the CI steps in a Dockerfile and build it for linux/arm64 and linux/amd64. This does not replace running the C++ compiler for each target unless your toolchain is also cross-compiling, but it’s helpful for image-based packaging validation.


7) Suggested workflow

- Day-to-day:
  1) Use the DevContainer for a clean, reproducible environment.
  2) Run the local containerized CI for Linux:
     - bash scripts/local-ci/matrix.sh
  3) Run the downstream consumer test via CTest:
     - ctest --test-dir build -R '^consumer_'
  4) Use act to smoke-test your CI workflow logic for Linux jobs:
     - act -j build-test-package -P ubuntu-latest=catthehacker/ubuntu:act-22.04
- Before tagging a release:
  - Validate Linux packaging locally via the container harness, then tag (vX.Y.Z) and let the GitHub Actions “Release” workflow build and publish macOS (per-arch) and Linux artifacts.


8) Troubleshooting

- Missing OpenSSL on macOS:
  - export OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)
- CMake can’t find Yams in consumer build:
  - Ensure you installed Yams to a prefix and pointed CMAKE_PREFIX_PATH to that prefix
- act pulling large images / slow:
  - First run can be large; consider pre-pulling the images or using a closer mirror
- Docker permission errors:
  - On Linux, add your user to the docker group and re-login; in devcontainer, the dind feature handles daemon wiring


Changelog (local)
- Add DevContainer with gcc/clang, cmake, act, OpenSSL/SQLite/protobuf
- Add Docker-based local CI harness to simulate ubuntu-latest
- Document act usage to run GH Actions locally (Linux only)
- Explain how to test consumer integration via CTest
- Document macOS per-arch approach and local testing on mac hardware