#!/usr/bin/env bash
# yams/scripts/local-ci/package-validate-checks.sh
#
# In-container half of scripts/local-ci/package-validate.sh (the entry point;
# run that, not this). It runs as root inside a distro container booted with
# systemd as PID 1, and prints one line per check:
#
#   PASS <id> <detail>      FAIL <id> <detail>      INFO <id> <detail>
#
# The packages ship a systemd *user* unit; the checks drive it through a
# normal user with lingering enabled (so a user manager runs without a login).
#
# Usage: package-validate-checks.sh <phase> <pkg-manager> [args]
#   install  <pm> <pkg-file>        install like a user would (no env vars)
#   layout   <pm>                   installed file list + ELF + permission audit
#   service  <pm>                   user unit + preset installed, globally enabled,
#                                   no system unit
#   user     <pm>                   a user's unit is enabled and active, socket in
#                                   $XDG_RUNTIME_DIR, CLI works with no env vars,
#                                   yams daemon stop/start/restart go via systemd
#   onnx     <pm>                   after `yams init` the user daemon loads the
#                                   onnx plugin with the bundled ONNX Runtime
#   persist  <pm>                   after a container reboot
#   seed     <pm>                   state of an older release (system service)
#   upgraded <pm>                   after upgrading over `seed`
#   remove   <pm>                   uninstall (deb: remove + purge)
#   minimal  <pm> <pkg-file>        raw base image, no systemd, nothing
#                                   preinstalled: install pulls only declared deps
#
# <pm> is deb, rpm or arch. The exit status is the number of FAIL lines (0 = ok).
set -uo pipefail

PHASE="${1:?phase}"
PM="${2:?package manager}"
ARG="${3:-}"

UNIT=yams-daemon.service
USER_UNIT_DIR=/usr/lib/systemd/user
TUSER=alice   # the user whose daemon the checks drive (lingering)
OUSER=bob     # a second user, for isolation
PUSER=carol   # probes the bundled ONNX Runtime with its own config
TOKEN_FILE=/var/tmp/yams-validate-tokens

FAILS=0
pass() { printf 'PASS %s %s\n' "$1" "${2:-}"; }
fail() {
	printf 'FAIL %s %s\n' "$1" "${2:-}"
	FAILS=$((FAILS + 1))
}
info() { printf 'INFO %s %s\n' "$1" "${2:-}"; }
check() { # check <id> <detail> <command...>
	local id="$1" detail="$2"
	shift 2
	if "$@" >/dev/null 2>&1; then pass "${id}" "${detail}"; else fail "${id}" "${detail}"; fi
}

token() { printf 'yamsval%s%s' "$1" "$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"; }
remember() { printf '%s=%s\n' "$1" "$2" >>"${TOKEN_FILE}"; }
recall() { sed -n "s/^$1=//p" "${TOKEN_FILE}" 2>/dev/null | tail -n1; }

# Run a command as <user> through a login shell (pam_systemd sets
# XDG_RUNTIME_DIR like a real login); no YAMS_* variable leaks in.
as_user() {
	local user="$1" rt
	shift
	rt="$(user_runtime "${user}")"
	su - "${user}" -c "[ -n \"\${XDG_RUNTIME_DIR:-}\" ] || export XDG_RUNTIME_DIR=${rt}; $*"
}

ensure_user() {
	id "$1" >/dev/null 2>&1 || useradd -m -s /bin/bash "$1"
	loginctl enable-linger "$1" >/dev/null 2>&1 || true
}

user_runtime() { printf '/run/user/%s' "$(id -u "$1")"; }

# systemctl --user for <user>, with the runtime dir and bus set explicitly.
uctl() {
	local user="$1" rt
	shift
	rt="$(user_runtime "${user}")"
	local -a busenv=()
	if [ -S "${rt}/bus" ]; then busenv=("DBUS_SESSION_BUS_ADDRESS=unix:path=${rt}/bus"); fi
	runuser -u "${user}" -- env XDG_RUNTIME_DIR="${rt}" ${busenv[@]+"${busenv[@]}"} \
		systemctl --user "$@" 2>/dev/null || true
}

wait_user_manager() {
	local uid
	uid="$(id -u "$1")"
	for _ in $(seq 1 120); do
		[ "$(systemctl is-active "user@${uid}.service" 2>/dev/null)" = active ] &&
			[ -S "/run/user/${uid}/systemd/private" ] && return 0
		sleep 0.5
	done
	return 1
}

wait_user_active() {
	local sock
	sock="$(user_runtime "$1")/yams-daemon.sock"
	for _ in $(seq 1 120); do
		[ "$(uctl "$1" is-active "${UNIT}")" = active ] && [ -S "${sock}" ] && return 0
		sleep 0.5
	done
	return 1
}

user_journal() {
	uctl "$1" status "${UNIT}" --no-pager -n 15 | sed 's/^/INFO user-unit /'
}

# Exactly one daemon runs for TUSER, and it is the unit's MainPID (no CLI-spawned copy).
check_single_daemon() {
	local main pids
	main="$(uctl "${TUSER}" show -p MainPID --value "${UNIT}")"
	pids="$(pgrep -u "${TUSER}" -f '^/usr/bin/yams-daemon' | paste -sd' ' - || true)"
	if [ -n "${main}" ] && [ "${main}" != 0 ] && [ "${pids}" = "${main}" ]; then
		pass "$1" "the only daemon for ${TUSER} is the unit's (pid ${main})"
	else
		fail "$1" "unit MainPID=${main:-?}, ${TUSER}'s daemons: ${pids:-none}"
	fi
}

wait_plugins() { # wait_plugins <user> <name-substring>
	for _ in $(seq 1 30); do
		as_user "$1" 'timeout 30 yams plugin health' 2>/dev/null | grep -qi "$2" && return 0
		sleep 2
	done
	return 1
}

# Poll a search until it reports <needle> (indexing is asynchronous).
search_finds() { # search_finds <user> <query> <needle>
	local user="$1" query="$2" needle="$3" out=""
	for _ in $(seq 1 60); do
		out="$(as_user "${user}" "timeout 30 yams search '${query}'" 2>&1 || true)"
		case "${out}" in *"${needle}"*) return 0 ;; esac
		sleep 1
	done
	printf '%s\n' "${out}" | tail -n 5 | sed 's/^/INFO search-output /'
	return 1
}

pkg_files() {
	case "${PM}" in
	deb) dpkg -L yams ;;
	rpm) rpm -ql yams ;;
	arch) pacman -Qlq yams ;;
	esac 2>/dev/null | sed 's#/$##' | while IFS= read -r f; do
		[ -n "${f}" ] || continue
		# Files and symlinks only; directories are shared with other packages.
		if [ -L "${f}" ] || [ ! -d "${f}" ]; then printf '%s\n' "${f}"; fi
	done | sort -u
}

# ---------------------------------------------------------------------------
phase_install() {
	local pkg="${ARG:?package file}"
	local log=/var/tmp/yams-validate-install.log
	local rc=0
	case "${PM}" in
	deb)
		# The Docker base image's policy-rc.d blocks service starts; a real host has none.
		rm -f /usr/sbin/policy-rc.d
		apt-get update >/dev/null 2>&1 || true
		apt-get install -y "${pkg}" >"${log}" 2>&1 || rc=$?
		;;
	rpm) dnf install -y "${pkg}" >"${log}" 2>&1 || rc=$? ;;
	arch) pacman -U --noconfirm "${pkg}" >"${log}" 2>&1 || rc=$? ;;
	esac
	if [ "${rc}" -eq 0 ]; then
		pass install "$(basename "${pkg}")"
	else
		fail install "$(basename "${pkg}") (rc=${rc})"
		tail -n 20 "${log}" | sed 's/^/INFO install-log /'
	fi
}

# ---------------------------------------------------------------------------
phase_layout() {
	local files
	files="$(pkg_files)"
	if [ -z "${files}" ]; then
		fail layout-filelist "package manager lists no files for yams"
		return
	fi
	info layout-filelist "$(printf '%s\n' "${files}" | wc -l) files"

	# Runtime package allowlist. Anything else is either a leak (headers,
	# static libs, pkg-config, build trees) or a new file someone must review.
	local allow='^/usr/bin/(yams|yams-cli|yams-daemon|yams-mcp-server)$'
	allow+='|^/usr/lib(64)?/(yams/)?libyams_[A-Za-z0-9_]+\.so(\.[0-9]+)*$'
	allow+='|^/usr/lib(64)?/yams/plugins/[A-Za-z0-9_.-]+\.so$'
	# Private fallback ONNX Runtime used by the ONNX/Glint plugins when no compatible
	# system copy exists (core library, its soname symlinks, providers_shared).
	allow+='|^/usr/lib(64)?/yams/onnxruntime/libonnxruntime(_providers_shared)?\.so(\.[0-9]+)*$'
	allow+='|^/usr/lib/systemd/user/yams-daemon\.service$'
	allow+='|^/usr/lib/systemd/user-preset/80-yams\.preset$'
	allow+='|^/usr/share/doc/yams(/[A-Za-z0-9_./-]+)?$'
	allow+='|^/usr/share/licenses/yams(/[A-Za-z0-9_.-]+)?$'
	# rpm's debuginfo build-id links (symlinks to the shipped ELF files).
	allow+='|^/usr/lib/\.build-id(/[0-9a-f]{2}(/[0-9a-f]+)?)?$'
	allow+='|^/usr/share/yams/[A-Za-z0-9_./-]+$'
	local unexpected
	unexpected="$(printf '%s\n' "${files}" | grep -Ev "${allow}" || true)"
	if [ -z "${unexpected}" ]; then
		pass layout-allowlist "every installed path is on the runtime allowlist"
	else
		fail layout-allowlist "unexpected installed paths: $(printf '%s ' ${unexpected})"
	fi

	local devel
	devel="$(printf '%s\n' "${files}" | grep -E '^/usr/include/|\.a$|\.pc$|\.(h|hpp|hh)$|/cmake/' || true)"
	if [ -z "${devel}" ]; then
		pass layout-no-devel "no headers, static libraries, pkg-config or CMake files"
	else
		fail layout-no-devel "development files in runtime package: $(printf '%s ' ${devel})"
	fi

	for required in /usr/bin/yams /usr/bin/yams-daemon /usr/lib/systemd/user/yams-daemon.service \
		/usr/lib/systemd/user-preset/80-yams.preset; do
		check "layout-has:${required}" "installed" test -e "${required}"
	done

	local bad_perm=""
	local f
	while IFS= read -r f; do
		# Skip symlinks and paths the image's dpkg path-exclude dropped (docs).
		if [ -L "${f}" ] || [ ! -e "${f}" ]; then continue; fi
		if [ -n "$(find "${f}" -maxdepth 0 \( -perm -0002 -o -perm -4000 -o -perm -2000 \) 2>/dev/null)" ]; then
			bad_perm+="${f} "
		fi
		if [ "$(stat -c %U "${f}")" != root ]; then
			bad_perm+="${f}(owner=$(stat -c %U "${f}")) "
		fi
	done <<<"${files}"
	if [ -z "${bad_perm}" ]; then
		pass layout-perms "no world-writable, setuid/setgid or non-root-owned files"
	else
		fail layout-perms "${bad_perm}"
	fi

	# ELF: dependencies resolve on a clean system and no build-tree rpath leaks.
	local elf_bad="" rpath_bad="" elf_count=0
	while IFS= read -r f; do
		if [ -L "${f}" ] || [ ! -e "${f}" ]; then continue; fi
		[ "$(head -c4 "${f}" 2>/dev/null | od -An -c | tr -d ' ')" = "177ELF" ] || continue
		elf_count=$((elf_count + 1))
		local missing
		missing="$(ldd "${f}" 2>&1 | grep -E 'not found' || true)"
		[ -n "${missing}" ] && elf_bad+="${f}: $(printf '%s' "${missing}" | tr -s ' \n' ' ') "
		local rp
		rp="$(readelf -d "${f}" 2>/dev/null | sed -n 's/.*(R\(UN\)\{0,1\}PATH).*\[\(.*\)\]/\2/p' | tr ':' '\n')"
		local entry
		while IFS= read -r entry; do
			[ -z "${entry}" ] && continue
			case "${entry}" in
			'$ORIGIN' | '$ORIGIN/'* | /usr/lib | /usr/lib64 | /usr/lib/yams | /usr/lib/yams/*) ;;
			*) rpath_bad+="${f}:${entry} " ;;
			esac
		done <<<"${rp}"
	done <<<"${files}"
	info layout-elf "${elf_count} ELF objects"
	if [ -z "${elf_bad}" ]; then pass layout-elf-needed "all DT_NEEDED resolve (ldd)"; else fail layout-elf-needed "${elf_bad}"; fi
	if [ -z "${rpath_bad}" ]; then pass layout-elf-runpath "RPATH/RUNPATH only \$ORIGIN or /usr/lib[/yams]"; else fail layout-elf-runpath "${rpath_bad}"; fi
}

# ---------------------------------------------------------------------------
# Packaging-level state of the user unit (as root, no user session needed).
phase_service() {
	check service-user-unit "user unit installed in ${USER_UNIT_DIR}" test -f "${USER_UNIT_DIR}/${UNIT}"
	check service-user-preset "user preset installed" test -f /usr/lib/systemd/user-preset/80-yams.preset
	if [ -e "/usr/lib/systemd/system/${UNIT}" ] || [ -e "/lib/systemd/system/${UNIT}" ]; then
		fail service-no-system-unit "a system unit is still installed"
	else
		pass service-no-system-unit "no system unit (the daemon is per-user)"
	fi
	local g
	g="$(systemctl --global is-enabled "${UNIT}" 2>/dev/null || true)"
	if [ "${g}" = enabled ]; then
		pass service-global-enabled "enabled for all users (user preset applied)"
	else
		fail service-global-enabled "systemctl --global is-enabled says '${g:-?}'"
	fi
	if grep -Eq '^(ProtectHome|PrivateTmp)=' "${USER_UNIT_DIR}/${UNIT}"; then
		fail unit-reads-home "unit hides the user's files (ProtectHome/PrivateTmp)"
	else
		pass unit-reads-home "unit can read the user's home and /tmp"
	fi
}

# ---------------------------------------------------------------------------
# A normal user with lingering: the user manager starts the unit (globally
# enabled), and the CLI uses it with no environment set up by hand.
phase_user() {
	ensure_user "${TUSER}"
	if ! wait_user_manager "${TUSER}"; then
		fail user-manager "user@$(id -u "${TUSER}").service did not start"
		journalctl -u "user@$(id -u "${TUSER}").service" --no-pager -n 8 2>/dev/null | sed 's/^/INFO user-manager /'
		return
	fi
	pass user-manager "user manager running for ${TUSER} (linger)"
	local verify
	verify="$(runuser -u "${TUSER}" -- env XDG_RUNTIME_DIR="$(user_runtime "${TUSER}")" \
		systemd-analyze --user verify "${USER_UNIT_DIR}/${UNIT}" 2>&1 | grep -viE 'cgroup' || true)"
	if [ -z "${verify}" ]; then
		pass unit-verify "systemd-analyze --user verify is clean"
	else
		fail unit-verify "$(printf '%s' "${verify}" | tr '\n' ' ')"
	fi
	local xdg
	xdg="$(su - "${TUSER}" -c 'printf %s "${XDG_RUNTIME_DIR:-}"')"
	if [ "${xdg}" = "$(user_runtime "${TUSER}")" ]; then
		pass user-login-env "su login has XDG_RUNTIME_DIR=${xdg} (pam_systemd)"
	else
		info user-login-env "su login lacks XDG_RUNTIME_DIR (no pam_systemd in su); the checks set it like a real login"
	fi
	local en
	en="$(uctl "${TUSER}" is-enabled "${UNIT}")"
	if [ "${en}" = enabled ]; then pass user-enabled "${UNIT} enabled for ${TUSER}"; else fail user-enabled "is-enabled: '${en}'"; fi
	if wait_user_active "${TUSER}"; then
		pass user-active "${UNIT} active for ${TUSER} without a manual start"
	else
		fail user-active "${UNIT} is '$(uctl "${TUSER}" is-active "${UNIT}")'"
		user_journal "${TUSER}"
		return
	fi
	local sock perms
	sock="$(user_runtime "${TUSER}")/yams-daemon.sock"
	perms="$(stat -c '%a %U' "${sock}" 2>/dev/null || true)"
	case "${perms}" in
	*" ${TUSER}") pass user-socket "socket ${sock} (${perms})" ;;
	*) fail user-socket "no socket owned by ${TUSER} at ${sock} (${perms:-missing})" ;;
	esac

	local t rc=0 out
	t="$(token user)"
	remember USER "${t}"
	as_user "${TUSER}" "printf 'user note %s\n' '${t}' > ~/user-note.txt; printf 'tmp note %s\n' '${t}x' > /tmp/user-tmp-note.txt"
	out="$(as_user "${TUSER}" 'timeout 60 yams daemon status' 2>&1)" || rc=$?
	if [ "${rc}" -eq 0 ] && ! printf '%s' "${out}" | grep -qi 'not running'; then
		pass cli-status "yams daemon status reaches the unit's daemon"
	else
		fail cli-status "rc=${rc}: $(printf '%s' "${out}" | tail -n 2 | tr '\n' ' ')"
	fi
	rc=0
	out="$(as_user "${TUSER}" 'timeout 120 yams add ~/user-note.txt && timeout 120 yams add /tmp/user-tmp-note.txt' 2>&1)" || rc=$?
	if [ "${rc}" -eq 0 ]; then pass cli-add "yams add from \$HOME and /tmp"; else fail cli-add "rc=${rc}: $(printf '%s' "${out}" | tail -n 2 | tr '\n' ' ')"; fi
	if search_finds "${TUSER}" "${t}" user-note; then pass cli-search "home file searchable"; else fail cli-search "home note not found by yams search"; fi
	if as_user "${TUSER}" 'test -e ~/.local/share/yams/yams.db'; then
		pass cli-data-dir "corpus in ~${TUSER}/.local/share/yams"
	else
		fail cli-data-dir "no ~/.local/share/yams/yams.db for ${TUSER}"
	fi
	check_single_daemon cli-single-daemon
	check user-log "daemon log in ~/.local/state/yams" as_user "${TUSER}" 'test -s ~/.local/state/yams/daemon.log'

	# Lifecycle commands go through systemd.
	as_user "${TUSER}" 'timeout 60 yams daemon stop' >/dev/null 2>&1 || true
	local st
	st="$(uctl "${TUSER}" is-active "${UNIT}")"
	if [ "${st}" != active ]; then pass cli-stop "yams daemon stop stopped the unit (${st})"; else fail cli-stop "unit still active after yams daemon stop"; fi
	as_user "${TUSER}" 'timeout 60 yams daemon start' >/dev/null 2>&1 || true
	if wait_user_active "${TUSER}"; then
		pass cli-start-unit "yams daemon start started the unit"
	else
		fail cli-start-unit "unit is '$(uctl "${TUSER}" is-active "${UNIT}")' after yams daemon start"
	fi
	check_single_daemon cli-start-single
	local before after
	before="$(uctl "${TUSER}" show -p MainPID --value "${UNIT}")"
	as_user "${TUSER}" 'timeout 60 yams daemon restart' >/dev/null 2>&1 || true
	wait_user_active "${TUSER}" || true
	after="$(uctl "${TUSER}" show -p MainPID --value "${UNIT}")"
	if [ -n "${after}" ] && [ "${after}" != 0 ] && [ "${after}" != "${before}" ]; then
		pass cli-restart "yams daemon restart restarted the unit (pid ${before} -> ${after})"
	else
		fail cli-restart "MainPID ${before} -> ${after}"
	fi
	check cli-install-user "yams daemon install --user enables the packaged unit" \
		as_user "${TUSER}" 'yams daemon install --user | grep -q "packaged" && test ! -e ~/.config/systemd/user/yams-daemon.service'

	# Users are isolated: another user's runtime dir and socket are private.
	ensure_user "${OUSER}"
	if as_user "${OUSER}" "test -r $(user_runtime "${TUSER}")/yams-daemon.sock || test -x $(user_runtime "${TUSER}")"; then
		fail user-isolation "${OUSER} can reach ${TUSER}'s runtime dir"
	else
		pass user-isolation "${OUSER} cannot reach ${TUSER}'s daemon"
	fi
}

# ---------------------------------------------------------------------------
# `yams init` writes the user's config; with it the unit loads the plugins and
# the onnx plugin uses the bundled ONNX Runtime (lane images have no system copy).
phase_onnx() {
	local bundled
	bundled="$(ls -d /usr/lib/yams/onnxruntime /usr/lib64/yams/onnxruntime 2>/dev/null | head -n 1)"
	if [ -n "${bundled}" ] && ls "${bundled}"/libonnxruntime.so* >/dev/null 2>&1; then
		pass onnx-runtime-shipped "bundled runtime present in ${bundled}"
	else
		fail onnx-runtime-shipped "no libonnxruntime.so under /usr/lib/yams/onnxruntime"
	fi
	check onnx-plugin-shipped "onnx plugin installed" test -e /usr/lib/yams/plugins/libyams_onnx_plugin.so
	local rc=0 out
	out="$(as_user "${TUSER}" 'timeout 300 yams init --non-interactive --no-keygen' 2>&1)" || rc=$?
	if as_user "${TUSER}" "grep -Eq '^[[:space:]]*auto_load_plugins[[:space:]]*=[[:space:]]*true' ~/.config/yams/config.toml"; then
		pass onnx-init-config "yams init wrote auto_load_plugins = true (rc=${rc})"
	else
		fail onnx-init-config "rc=${rc}; config lacks auto_load_plugins = true: $(printf '%s' "${out}" | tail -n 2 | tr '\n' ' ')"
		as_user "${TUSER}" 'mkdir -p ~/.config/yams && printf "[daemon]\nauto_load_plugins = true\n" >> ~/.config/yams/config.toml'
	fi
	uctl "${TUSER}" restart "${UNIT}" >/dev/null
	if ! wait_user_active "${TUSER}"; then
		fail onnx-restart "unit did not come back after yams init"
		user_journal "${TUSER}"
		return
	fi
	if wait_plugins "${TUSER}" glint; then
		pass onnx-autoload "init's config autoloads plugins (glint loaded)"
	else
		fail onnx-autoload "no plugins loaded with init's config"
		as_user "${TUSER}" 'timeout 60 yams plugin health' 2>&1 | tail -n 4 | sed 's/^/INFO plugin-health /'
	fi
	check_single_daemon onnx-single-daemon

	# The bundled ONNX Runtime, probed with a separate user so TUSER keeps
	# init's configuration for the later phases: plugin autoload with the
	# default ("auto") embedding backend adopts the onnx model provider, which
	# resolves the runtime at load (no model is installed, so nothing embeds).
	ensure_user "${PUSER}"
	if ! wait_user_manager "${PUSER}"; then
		fail onnx-probe-user "no user manager for ${PUSER}"
		return
	fi
	# config_version = 3 keeps the daemon from migrating this file to the full
	# template (whose default embedding backend is the built-in simeon).
	as_user "${PUSER}" 'mkdir -p ~/.config/yams && printf "[version]\nconfig_version = 3\n\n[daemon]\nauto_load_plugins = true\n\n[embeddings]\nenable = true\nbackend = \"auto\"\n" > ~/.config/yams/config.toml'
	local since
	since="$(date '+%Y-%m-%d %H:%M:%S')"
	uctl "${PUSER}" restart "${UNIT}" >/dev/null
	wait_user_active "${PUSER}" || true
	local line=""
	for _ in $(seq 1 60); do
		line="$(journalctl -q --no-pager _UID="$(id -u "${PUSER}")" --since "${since}" 2>/dev/null |
			grep -E 'Using (bundled|system) ONNX Runtime' | tail -n 1)"
		[ -n "${line}" ] && break
		sleep 1
	done
	case "${line}" in
	*"Using bundled ONNX Runtime"*) pass onnx-runtime-bundled "user daemon: ${line##*] }" ;;
	*)
		fail onnx-runtime-bundled "no 'Using bundled ONNX Runtime' from the user daemon (${line:-none})"
		journalctl -q --no-pager _UID="$(id -u "${PUSER}")" --since "${since}" 2>/dev/null |
			grep -iE 'onnx|plugin' | tail -n 8 | sed 's/^/INFO onnx-log /'
		;;
	esac
	if wait_plugins "${PUSER}" onnx; then
		pass onnx-plugin-loaded "yams plugin health lists the onnx plugin"
	else
		fail onnx-plugin-loaded "yams plugin health does not list onnx"
	fi
	uctl "${PUSER}" stop "${UNIT}" >/dev/null
	loginctl disable-linger "${PUSER}" >/dev/null 2>&1 || true
}

# ---------------------------------------------------------------------------
phase_persist() {
	if ! wait_user_manager "${TUSER}"; then
		fail reboot-user-manager "no user manager for ${TUSER} after reboot (linger)"
		return
	fi
	if wait_user_active "${TUSER}"; then
		pass reboot-active "${UNIT} active for ${TUSER} after reboot"
	else
		fail reboot-active "${UNIT} is '$(uctl "${TUSER}" is-active "${UNIT}")' after reboot"
		user_journal "${TUSER}"
		return
	fi
	local t
	t="$(recall USER)"
	if [ -n "${t}" ] && search_finds "${TUSER}" "${t}" user-note; then
		pass reboot-data-persists "${TUSER}'s document survives a reboot"
	else
		fail reboot-data-persists "${TUSER}'s document missing after reboot"
	fi
}

# ---------------------------------------------------------------------------
# Before upgrading from a release that ran a system service.
phase_seed() {
	: >"${TOKEN_FILE}"
	local state=""
	for _ in $(seq 1 120); do
		state="$(systemctl is-active "${UNIT}" 2>/dev/null || true)"
		[ "${state}" = active ] && break
		sleep 0.5
	done
	if [ "${state}" = active ]; then
		pass upgrade-old-system-unit "old release runs the system ${UNIT}"
	else
		info upgrade-old-system-unit "old release's system unit is '${state:-?}'"
	fi
	ensure_user "${TUSER}"
	wait_user_manager "${TUSER}" || true
	systemctl show -p MainPID --value "${UNIT}" >/var/tmp/yams-validate-oldpid 2>/dev/null || true
}

phase_upgraded() {
	if [ "$(systemctl is-active "${UNIT}" 2>/dev/null)" = active ]; then
		fail upgrade-system-unit-stopped "old system unit still running"
	else
		pass upgrade-system-unit-stopped "old system unit stopped"
	fi
	if [ -e "/usr/lib/systemd/system/${UNIT}" ] || [ -e "/lib/systemd/system/${UNIT}" ]; then
		fail upgrade-system-unit-removed "system unit file still installed"
	else
		pass upgrade-system-unit-removed "system unit file removed"
	fi
	check_no_dangling_links upgrade-no-dangling-links
	check upgrade-old-corpus-kept "/var/lib/yams left in place" test -e /var/lib/yams
	local g
	g="$(systemctl --global is-enabled "${UNIT}" 2>/dev/null || true)"
	if [ "${g}" = enabled ]; then pass upgrade-global-enabled "user unit enabled for all users"; else fail upgrade-global-enabled "'${g:-?}'"; fi
	# The already-running user manager learns about the unit; it starts at the
	# next login, or now on request.
	uctl "${TUSER}" daemon-reload >/dev/null
	local en
	en="$(uctl "${TUSER}" is-enabled "${UNIT}")"
	if [ "${en}" = enabled ]; then pass upgrade-user-enabled "${UNIT} enabled for ${TUSER}"; else fail upgrade-user-enabled "'${en}'"; fi
	uctl "${TUSER}" start "${UNIT}" >/dev/null
	if wait_user_active "${TUSER}"; then
		pass upgrade-user-active "${TUSER}'s user daemon runs after the upgrade"
	else
		fail upgrade-user-active "'$(uctl "${TUSER}" is-active "${UNIT}")'"
		user_journal "${TUSER}"
		return
	fi
	local t rc=0
	t="$(token upgrade)"
	as_user "${TUSER}" "printf 'upgrade note %s\n' '${t}' > ~/upgrade-note.txt"
	as_user "${TUSER}" 'timeout 120 yams add ~/upgrade-note.txt' >/dev/null 2>&1 || rc=$?
	if [ "${rc}" -eq 0 ] && search_finds "${TUSER}" "${t}" upgrade-note; then
		pass upgrade-cli "${TUSER}'s CLI works against the user daemon"
	else
		fail upgrade-cli "add rc=${rc} or document not found"
	fi
}

# ---------------------------------------------------------------------------
phase_minimal() {
	local pkg="${ARG:?package file}"
	local log=/var/tmp/yams-validate-minimal.log rc=0
	case "${PM}" in
	deb)
		export DEBIAN_FRONTEND=noninteractive
		apt-get update >/dev/null 2>&1 || true
		apt-get install -y "${pkg}" >"${log}" 2>&1 || rc=$?
		;;
	rpm) dnf install -y "${pkg}" >"${log}" 2>&1 || rc=$? ;;
	arch)
		pacman -Sy --noconfirm >/dev/null 2>&1 || true
		pacman -U --noconfirm "${pkg}" >"${log}" 2>&1 || rc=$?
		;;
	esac
	if [ "${rc}" -eq 0 ]; then
		pass minimal-install "$(basename "${pkg}") installs on the bare base image"
	else
		fail minimal-install "rc=${rc}"
		tail -n 15 "${log}" | sed 's/^/INFO minimal-install-log /'
		return
	fi
	if [ "${PM}" = deb ]; then
		local status
		status="$(dpkg-query -W -f='${Status}' yams 2>/dev/null)"
		if [ "${status}" = "install ok installed" ]; then
			pass minimal-configured "dpkg status '${status}'"
		else
			fail minimal-configured "dpkg status '${status}' (half-configured?)"
		fi
	fi
	check minimal-user-unit "user unit installed" test -f "${USER_UNIT_DIR}/${UNIT}"
	check minimal-version "yams --version runs" yams --version
	local t out
	t="$(token minimal)"
	mkdir -p /var/tmp/yams-min-home
	printf 'minimal note %s\n' "${t}" >/var/tmp/yams-min-home/note.txt
	out="$(env -i HOME=/var/tmp/yams-min-home PATH=/usr/bin:/bin timeout 180 yams add /var/tmp/yams-min-home/note.txt 2>&1)" || true
	local found=0
	for _ in $(seq 1 30); do
		if env -i HOME=/var/tmp/yams-min-home PATH=/usr/bin:/bin timeout 60 yams search "${t}" 2>/dev/null | grep -q note; then
			found=1
			break
		fi
		sleep 1
	done
	if [ "${found}" -eq 1 ]; then
		pass minimal-cli "yams add + search work without systemd"
	else
		fail minimal-cli "$(printf '%s' "${out}" | tail -n 2 | tr '\n' ' ')"
	fi
}

# ---------------------------------------------------------------------------
check_no_dangling_links() { # check_no_dangling_links <id>
	local dangling
	dangling="$(find /etc/systemd/system /etc/systemd/user -name "${UNIT}" -xtype l 2>/dev/null || true)"
	if [ -z "${dangling}" ]; then
		pass "$1" "no dangling enablement symlinks"
	else
		fail "$1" "$(printf '%s ' ${dangling})"
	fi
}

phase_remove() {
	local rc=0 log=/var/tmp/yams-validate-remove.log
	case "${PM}" in
	deb) apt-get remove -y yams >"${log}" 2>&1 || rc=$? ;;
	rpm) dnf remove -y yams >"${log}" 2>&1 || rc=$? ;;
	arch) pacman -R --noconfirm yams >"${log}" 2>&1 || rc=$? ;;
	esac
	if [ "${rc}" -eq 0 ]; then pass remove "package removed"; else
		fail remove "rc=${rc}"
		tail -n 20 "${log}" | sed 's/^/INFO remove-log /'
	fi
	local st
	st="$(uctl "${TUSER}" is-active "${UNIT}")"
	if [ "${st}" = active ]; then
		fail remove-user-stopped "${TUSER}'s user daemon still active after removal"
	else
		pass remove-user-stopped "${TUSER}'s user daemon stopped (${st:-gone})"
	fi
	if pgrep -f '^/usr/bin/yams-daemon' >/dev/null 2>&1; then
		fail remove-no-daemons "yams-daemon processes left: $(pgrep -fa '^/usr/bin/yams-daemon' | tr '\n' ' ')"
	else
		pass remove-no-daemons "no yams-daemon processes left"
	fi
	check remove-binaries "binaries removed" test ! -e /usr/bin/yams-daemon
	check remove-global-disabled "global enablement removed" test ! -e "/etc/systemd/user/default.target.wants/${UNIT}"
	check_no_dangling_links remove-no-dangling-links
	check remove-user-data-kept "${TUSER}'s corpus kept" as_user "${TUSER}" 'test -e ~/.local/share/yams/yams.db'
	if [ "${PM}" = deb ]; then
		rc=0
		apt-get purge -y yams >>"${log}" 2>&1 || rc=$?
		if [ "${rc}" -eq 0 ]; then pass purge "package purged"; else fail purge "rc=${rc}"; fi
		check purge-user-data-kept "${TUSER}'s corpus kept after purge" as_user "${TUSER}" 'test -e ~/.local/share/yams/yams.db'
	fi
}

case "${PHASE}" in
install) phase_install ;;
layout) phase_layout ;;
service) phase_service ;;
user) phase_user ;;
onnx) phase_onnx ;;
persist) phase_persist ;;
seed) phase_seed ;;
upgraded) phase_upgraded ;;
remove) phase_remove ;;
minimal) phase_minimal ;;
*)
	echo "unknown phase: ${PHASE}" >&2
	exit 64
	;;
esac
exit "${FAILS}"
