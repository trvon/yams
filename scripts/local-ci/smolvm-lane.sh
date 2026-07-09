#!/usr/bin/env bash
# yams/scripts/local-ci/smolvm-lane.sh
#
# smolvm-backed local validation lanes.
#
# Static profiles are fast script/workflow sanity checks. linux-ci is the
# pre-push Linux build+test gate: it runs the repo's CI-style Debug build and
# unit+integration Meson suites inside a clean Linux microVM.
#
# Examples:
#   bash scripts/local-ci/smolvm-lane.sh                         # static profile
#   bash scripts/local-ci/smolvm-lane.sh --profile linux-ci      # Linux build+tests
#   bash scripts/local-ci/smolvm-lane.sh --profile smoke         # mount/boot only
#   bash scripts/local-ci/smolvm-lane.sh --dry-run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOG_ROOT_DEFAULT="${REPO_ROOT}/build/local-ci/smolvm-lane"

PROFILE="static"
LOG_ROOT="${LOG_ROOT_DEFAULT}"
DRY_RUN=0
SMOLVM_BIN="${SMOLVM_BIN:-}"
IMAGE="${SMOLVM_IMAGE:-}"
ACTIONLINT_VERSION="${ACTIONLINT_VERSION:-v1.7.7}"
RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)-$$"

usage() {
	cat <<'USAGE'
Usage: smolvm-lane.sh [options]

Profiles:
  smoke     Boot Linux under smolvm and verify the repo mount.
  syntax    smoke + bash -n for local CI/package scripts.
  static    syntax + shellcheck and actionlint inside the VM (default).
  linux-ci  CI-style Linux Debug build + unit/integration tests inside smolvm.

Options:
  --profile NAME   smoke|syntax|static|linux-ci (default: static)
  --image IMAGE    smolvm image to run (default: alpine; linux-ci: ubuntu:24.04)
  --log-root DIR   Log directory (default: build/local-ci/smolvm-lane)
  --dry-run        Print the smolvm command without executing it
  -h, --help       Show this help

Environment for linux-ci:
  YAMS_SMOLVM_BUILD_DIR       Build dir (default: build/smolvm-linux)
  YAMS_SMOLVM_CLEAN_BUILD     Clean build dir before linux-ci (default: 1)
  YAMS_SMOLVM_CONAN_JOBS      Conan/dependency build jobs (default: 1)
  YAMS_SMOLVM_NINJA_JOBS      Meson/Ninja compile jobs (default: 1)
  YAMS_SMOLVM_COMPILE_TARGETS Optional space-separated Meson compile targets
  YAMS_SMOLVM_TEST_JOBS       Meson test jobs when YAMS_SMOLVM_TEST_ARGS unset (default: 1)
  YAMS_SMOLVM_TEST_ARGS       Meson test args (default: --suite unit --suite integration ...)
USAGE
}

log() { printf '\033[1;34m[smolvm-lane]\033[0m %s\n' "$*"; }
ok() { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; }

while [ "$#" -gt 0 ]; do
	case "$1" in
	--profile)
		PROFILE="${2:-}"
		shift 2
		;;
	--image)
		IMAGE="${2:-}"
		shift 2
		;;
	--log-root)
		LOG_ROOT="${2:-}"
		shift 2
		;;
	--dry-run)
		DRY_RUN=1
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		fail "unknown argument: $1"
		usage
		exit 2
		;;
	esac
done

case "${PROFILE}" in
smoke | syntax | static | linux-ci) ;;
*)
	fail "invalid profile: ${PROFILE}"
	echo "valid profiles: smoke, syntax, static, linux-ci" >&2
	exit 2
	;;
esac

if [ -z "${IMAGE}" ]; then
	case "${PROFILE}" in
	linux-ci) IMAGE="ubuntu:24.04" ;;
	*) IMAGE="alpine" ;;
	esac
fi

find_smolvm() {
	if [ -n "${SMOLVM_BIN}" ]; then
		[ -x "${SMOLVM_BIN}" ] || command -v "${SMOLVM_BIN}" >/dev/null 2>&1 || return 1
		return 0
	fi
	if command -v smolvm >/dev/null 2>&1; then
		SMOLVM_BIN="smolvm"
		return 0
	fi
	if [ -x "${HOME}/.local/bin/smolvm" ]; then
		SMOLVM_BIN="${HOME}/.local/bin/smolvm"
		return 0
	fi
	return 1
}

if ! find_smolvm; then
	fail "smolvm not found on PATH (or at ~/.local/bin/smolvm). Set SMOLVM_BIN."
	exit 2
fi

mkdir -p "${LOG_ROOT}"
SUMMARY="${LOG_ROOT}/summary-${RUN_TAG}.md"
LOG_FILE="${LOG_ROOT}/${RUN_TAG}-${PROFILE}.log"

static_guest_script() {
	cat <<'GUEST_SCRIPT'
set -eu
printf '[guest] pwd=%s\n' "$(pwd)"
printf '[guest] uname=%s\n' "$(uname -m)"
test -f meson.build
test -d scripts/local-ci

case "${YAMS_SMOLVM_PROFILE}" in
smoke)
	echo '[guest] smoke profile complete'
	;;
syntax | static)
	apk add --no-cache bash >/dev/null
	bash -n \
		scripts/local-ci/push-lane.sh \
		scripts/local-ci/package-lane.sh \
		scripts/local-ci/package-validate.sh \
		scripts/local-ci/pre-push-ci-gate.sh \
		scripts/local-ci/native-macos-lane.sh \
		scripts/local-ci/smolvm-lane.sh \
		scripts/build-deb.sh \
		scripts/build-arch-pkg.sh
	if [ "${YAMS_SMOLVM_PROFILE}" = "syntax" ]; then
		echo '[guest] syntax profile complete'
		exit 0
	fi

	apk add --no-cache curl tar shellcheck >/dev/null
	guest_arch="$(uname -m)"
	case "${guest_arch}" in
	x86_64) actionlint_arch="amd64" ;;
	aarch64 | arm64) actionlint_arch="arm64" ;;
	*) echo "unsupported actionlint arch: ${guest_arch}" >&2; exit 2 ;;
	esac
	actionlint_url="https://github.com/rhysd/actionlint/releases/download/${ACTIONLINT_VERSION}/actionlint_${ACTIONLINT_VERSION#v}_linux_${actionlint_arch}.tar.gz"
	curl -fsSL "${actionlint_url}" | tar -xz -C /usr/local/bin actionlint
	shellcheck \
		scripts/local-ci/push-lane.sh \
		scripts/local-ci/package-lane.sh \
		scripts/local-ci/package-validate.sh \
		scripts/local-ci/pre-push-ci-gate.sh \
		scripts/local-ci/native-macos-lane.sh \
		scripts/local-ci/smolvm-lane.sh
	actionlint -shellcheck= .github/workflows/act-sanity.yml .github/workflows/tests.yml .github/workflows/release.yml
	echo '[guest] static profile complete'
	;;
*)
	echo "unknown YAMS_SMOLVM_PROFILE=${YAMS_SMOLVM_PROFILE}" >&2
	exit 2
	;;
esac
GUEST_SCRIPT
}

linux_ci_guest_script() {
	cat <<'GUEST_SCRIPT'
set -eu
export DEBIAN_FRONTEND=noninteractive
printf '[guest] pwd=%s\n' "$(pwd)"
printf '[guest] uname=%s\n' "$(uname -m)"
test -f meson.build

git config --global --add safe.directory /workspace || true
apt-get -o Acquire::Retries=5 -o Acquire::ForceIPv4=true update
apt-get -o Acquire::Retries=5 -o Acquire::ForceIPv4=true install -y --no-install-recommends \
	build-essential ca-certificates ccache clang clang-tools cmake curl git lld \
	ninja-build pkg-config python3 python3-pip python3-venv xz-utils \
	libbenchmark-dev libboost-all-dev libomp-dev

PYTOOLS_DIR=/tmp/yams-python-tools
python3 -m venv "${PYTOOLS_DIR}"
"${PYTOOLS_DIR}/bin/python" -m pip install --upgrade pip
"${PYTOOLS_DIR}/bin/python" -m pip install --upgrade 'conan==2.21.*' 'meson==1.4.*' 'ninja==1.11.*'
export PATH="${PYTOOLS_DIR}/bin:${PATH}"
conan profile detect --force

# The repo is a host bind mount. Do not run `git submodule update` inside
# the guest because host .git paths can be absolute macOS paths that are invalid
# in the Linux VM. Require host-initialized submodules instead.
for submodule_dir in third_party/sqlite-vec-cpp third_party/simeon third_party/symspell; do
	if [ ! -d "${submodule_dir}" ] || ! find "${submodule_dir}" -mindepth 1 -maxdepth 2 -print -quit | grep -q .; then
		echo "required submodule missing or empty: ${submodule_dir}" >&2
		echo "run on host: git submodule update --init --depth 1 ${submodule_dir}" >&2
		exit 128
	fi
done

case "$(uname -m)" in
x86_64) conan_arch="x86_64" ;;
aarch64 | arm64) conan_arch="armv8" ;;
*) echo "unsupported linux-ci arch: $(uname -m)" >&2; exit 2 ;;
esac

export CI=true
export RUNNER_OS=Linux
export YAMS_DISABLE_FAISS=1
export ENABLE_TSAN=false
export YAMS_ENABLE_MOBILE_BINDINGS=false
export YAMS_BUILD_DIR="${YAMS_SMOLVM_BUILD_DIR:-build/smolvm-linux}"
export YAMS_CONAN_HOST_PROFILE="${YAMS_CONAN_HOST_PROFILE:-./conan/profiles/host-linux-clang}"
export YAMS_CONAN_ARCH="${YAMS_CONAN_ARCH:-${conan_arch}}"
export YAMS_EXTRA_MESON_FLAGS="${YAMS_EXTRA_MESON_FLAGS:---buildtype=debug -Dbuild-tests=true -Denable-onnx=disabled -Dtest-timeout-scale=2 -Denable-bench-tests=false -Dbuild-benchmarks=false -Denable-dcheck=true}"
export YAMS_SMOLVM_CLEAN_BUILD="${YAMS_SMOLVM_CLEAN_BUILD:-1}"
export YAMS_SMOLVM_CONAN_JOBS="${YAMS_SMOLVM_CONAN_JOBS:-1}"
export YAMS_CONAN_EXTRA_OPTIONS="-c tools.build:jobs=${YAMS_SMOLVM_CONAN_JOBS} ${YAMS_CONAN_EXTRA_OPTIONS:-}"
export CONAN_CPU_COUNT="${YAMS_SMOLVM_CONAN_JOBS}"
export CMAKE_BUILD_PARALLEL_LEVEL="${YAMS_SMOLVM_CONAN_JOBS}"
export MAKEFLAGS="-j${YAMS_SMOLVM_CONAN_JOBS}"

if [ "${YAMS_SMOLVM_CLEAN_BUILD}" != "0" ]; then
	case "${YAMS_BUILD_DIR}" in
	build/* | ./build/*)
		echo "[guest] cleaning smolvm build dir: ${YAMS_BUILD_DIR}"
		rm -rf -- "${YAMS_BUILD_DIR}"
		;;
	*)
		echo "refusing to clean non-build YAMS_BUILD_DIR=${YAMS_BUILD_DIR}" >&2
		exit 2
		;;
	esac
fi

./setup.sh Debug

CONAN_BUILD_ENV=""
for candidate in \
	"${YAMS_BUILD_DIR}/build-debug/conan/conanbuild.sh" \
	"${YAMS_BUILD_DIR}/conan/conanbuild.sh"
do
	if [ -f "${candidate}" ]; then
		CONAN_BUILD_ENV="${candidate}"
		break
	fi
done
if [ -n "${CONAN_BUILD_ENV}" ]; then
	set +u
	# shellcheck source=/dev/null
	. "${CONAN_BUILD_ENV}"
	set -u
fi

export YAMS_TEST_SAFE_SINGLE_INSTANCE=1
export YAMS_SQLITE_MINIMAL_PRAGMAS=1
export YAMS_SQLITE_VEC_INIT_TIMEOUT_MS=1500
export YAMS_SQLITE_BUSY_TIMEOUT_MS=1000
export YAMS_VDB_IN_MEMORY=1
export YAMS_SQLITE_VEC_SKIP_INIT=1
export YAMS_DISABLE_VECTORS=1
export YAMS_TESTING=1
export YAMS_SMOLVM_NINJA_JOBS="${YAMS_SMOLVM_NINJA_JOBS:-1}"
export YAMS_SMOLVM_TEST_JOBS="${YAMS_SMOLVM_TEST_JOBS:-1}"

printf '[guest] compile jobs=%s test jobs=%s\n' "${YAMS_SMOLVM_NINJA_JOBS}" "${YAMS_SMOLVM_TEST_JOBS}"
if [ -n "${YAMS_SMOLVM_COMPILE_TARGETS:-}" ]; then
	# shellcheck disable=SC2086
	meson compile -C "${YAMS_BUILD_DIR}" -j "${YAMS_SMOLVM_NINJA_JOBS}" ${YAMS_SMOLVM_COMPILE_TARGETS}
else
	meson compile -C "${YAMS_BUILD_DIR}" -j "${YAMS_SMOLVM_NINJA_JOBS}"
fi

if [ -n "${YAMS_SMOLVM_TEST_ARGS:-}" ]; then
	# shellcheck disable=SC2086
	meson test -C "${YAMS_BUILD_DIR}" ${YAMS_SMOLVM_TEST_ARGS}
else
	meson test -C "${YAMS_BUILD_DIR}" \
		--suite unit \
		--suite integration \
		--print-errorlogs \
		--timeout-multiplier 2 \
		--num-processes "${YAMS_SMOLVM_TEST_JOBS}"
fi
echo '[guest] linux-ci profile complete'
GUEST_SCRIPT
}

case "${PROFILE}" in
linux-ci)
	GUEST_SCRIPT_CONTENT="$(linux_ci_guest_script)"
	VOLUME_SPEC="${REPO_ROOT}:/workspace"
	if [ "${YAMS_SMOLVM_CLEAN_BUILD:-1}" != "0" ]; then
		HOST_BUILD_DIR="${YAMS_SMOLVM_BUILD_DIR:-build/smolvm-linux}"
		case "${HOST_BUILD_DIR}" in
		build/* | ./build/*)
			HOST_CLEAN_PATH="${REPO_ROOT}/${HOST_BUILD_DIR#./}"
			log "host cleaning smolvm build dir: ${HOST_BUILD_DIR}"
			rm -rf -- "${HOST_CLEAN_PATH:?}"
			;;
		*)
			fail "refusing to host-clean non-build YAMS_SMOLVM_BUILD_DIR=${HOST_BUILD_DIR}"
			exit 2
			;;
		esac
	fi
	;;
*)
	GUEST_SCRIPT_CONTENT="$(static_guest_script)"
	VOLUME_SPEC="${REPO_ROOT}:/workspace:ro"
	;;
esac

CMD=(
	"${SMOLVM_BIN}" machine run
	--net
	--image "${IMAGE}"
	-v "${VOLUME_SPEC}"
	-w /workspace
	-e "YAMS_SMOLVM_PROFILE=${PROFILE}"
	-e "ACTIONLINT_VERSION=${ACTIONLINT_VERSION}"
	-e "YAMS_SMOLVM_BUILD_DIR=${YAMS_SMOLVM_BUILD_DIR:-build/smolvm-linux}"
	-e "YAMS_SMOLVM_CLEAN_BUILD=${YAMS_SMOLVM_CLEAN_BUILD:-1}"
	-e "YAMS_SMOLVM_CONAN_JOBS=${YAMS_SMOLVM_CONAN_JOBS:-1}"
	-e "YAMS_SMOLVM_NINJA_JOBS=${YAMS_SMOLVM_NINJA_JOBS:-1}"
	-e "YAMS_SMOLVM_COMPILE_TARGETS=${YAMS_SMOLVM_COMPILE_TARGETS:-}"
	-e "YAMS_SMOLVM_TEST_JOBS=${YAMS_SMOLVM_TEST_JOBS:-1}"
	-e "YAMS_SMOLVM_TEST_ARGS=${YAMS_SMOLVM_TEST_ARGS:-}"
	-- sh -lc "${GUEST_SCRIPT_CONTENT}"
)

{
	echo "# YAMS smolvm lane (${PROFILE})"
	echo
	echo "- Started: ${RUN_TAG}"
	echo "- Image: ${IMAGE}"
	echo "- smolvm: ${SMOLVM_BIN}"
	echo
	echo '```sh'
	printf '%q ' "${CMD[@]}"
	echo
	echo '```'
	echo
} >"${SUMMARY}"

log "profile=${PROFILE} image=${IMAGE}"
if [ "${DRY_RUN}" -eq 1 ]; then
	printf '%q ' "${CMD[@]}" | tee "${LOG_FILE}"
	echo | tee -a "${LOG_FILE}"
	echo "- Result: dry-run" >>"${SUMMARY}"
	ok "summary: ${SUMMARY#"${REPO_ROOT}"/}"
	exit 0
fi

set +e
"${CMD[@]}" > >(tee "${LOG_FILE}") 2> >(tee -a "${LOG_FILE}" >&2)
rc=$?
set -e

if [ "${rc}" -eq 0 ]; then
	echo "- Result: pass" >>"${SUMMARY}"
	echo "- Log: ${LOG_FILE#"${REPO_ROOT}"/}" >>"${SUMMARY}"
	ok "summary: ${SUMMARY#"${REPO_ROOT}"/}"
else
	echo "- Result: fail (${rc})" >>"${SUMMARY}"
	echo "- Log: ${LOG_FILE#"${REPO_ROOT}"/}" >>"${SUMMARY}"
	fail "smolvm lane failed (${rc}); log: ${LOG_FILE}"
	exit "${rc}"
fi
