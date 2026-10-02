#!/usr/bin/env bash
# yams/scripts/local-ci/package-validate-checks.sh
#
# In-container half of scripts/local-ci/package-validate.sh (the entry point;
# run that, not this). It runs as root inside a distro container booted with
# systemd as PID 1, and prints one line per check:
#
#   PASS <id> <detail>      FAIL <id> <detail>      INFO <id> <detail>
#
# Usage: package-validate-checks.sh <phase> <pkg-manager> [args]
#   install  <pm> <pkg-file>        install like a user would (no env vars)
#   layout   <pm>                   installed file list + ELF + permission audit
#   service  <pm>                   unit enabled/active, account, socket, sandbox
#   cli      <pm>                   root / group member / outsider CLI behaviour
#   persist  <pm>                   after a container reboot
#   seed     <pm>                   store a document through an older release
#   upgraded <pm>                   after upgrading over `seed`
#   remove   <pm>                   uninstall (deb: remove + purge)
#
# <pm> is deb, rpm or arch. The exit status is the number of FAIL lines (0 = ok).
set -uo pipefail

PHASE="${1:?phase}"
PM="${2:?package manager}"
ARG="${3:-}"

UNIT=yams-daemon.service
SOCKET=/run/yams/yams-daemon.sock
STATE_DIR=/var/lib/yams
LOG_DIR=/var/log/yams
TOKEN_FILE=/var/tmp/yams-validate-tokens
# Highest acceptable `systemd-analyze security` exposure (0 = locked down, 10 = unsafe).
MAX_EXPOSURE="${YAMS_VALIDATE_MAX_EXPOSURE:-3.0}"

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

wait_active() {
	local state=""
	for _ in $(seq 1 120); do
		state="$(systemctl is-active "${UNIT}" 2>/dev/null || true)"
		[ "${state}" = "active" ] && [ -S "${SOCKET}" ] && return 0
		sleep 0.5
	done
	return 1
}

# Run a command as <user> through a login shell, so group membership and HOME
# match a real login and no YAMS_* variable leaks in.
as_user() {
	local user="$1"
	shift
	su - "${user}" -c "$*"
}

# Poll a search until it reports <needle> (indexing is asynchronous).
# SEARCH_SOCKET, when set, pins root's search to that socket (to test data,
# not discovery).
search_finds() { # search_finds <user> <query> <needle>
	local user="$1" query="$2" needle="$3" out=""
	local -a pin=()
	if [ -n "${SEARCH_SOCKET:-}" ]; then pin=("YAMS_DAEMON_SOCKET=${SEARCH_SOCKET}"); fi
	for _ in $(seq 1 60); do
		if [ "${user}" = root ]; then
			out="$(cd /root && env -i HOME=/root PATH=/usr/bin:/bin ${pin[@]+"${pin[@]}"} timeout 30 yams search "${query}" 2>&1 || true)"
		else
			out="$(as_user "${user}" "timeout 30 yams search '${query}'" 2>&1 || true)"
		fi
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

journal_tail() {
	journalctl -u "${UNIT}" --no-pager -n 25 2>/dev/null | sed 's/^/INFO journal /'
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
	allow+='|^/usr/lib(64)?/libyams_[A-Za-z0-9_]+\.so(\.[0-9]+)*$'
	allow+='|^/usr/lib(64)?/yams/plugins/[A-Za-z0-9_.-]+\.so$'
	allow+='|^/usr/lib/systemd/system/yams-daemon\.service$'
	allow+='|^/usr/lib/systemd/system-preset/80-yams\.preset$'
	allow+='|^/usr/lib/sysusers\.d/yams\.conf$'
	allow+='|^/usr/share/doc/yams(/[A-Za-z0-9_.-]+)?$'
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

	for required in /usr/bin/yams /usr/bin/yams-daemon /usr/lib/systemd/system/yams-daemon.service \
		/usr/lib/systemd/system-preset/80-yams.preset /usr/lib/sysusers.d/yams.conf; do
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
phase_service() {
	if [ "$(systemctl is-enabled "${UNIT}" 2>/dev/null)" = enabled ]; then
		pass service-enabled "${UNIT} enabled by the package preset"
	else
		fail service-enabled "${UNIT} is '$(systemctl is-enabled "${UNIT}" 2>&1)'"
	fi
	if wait_active; then
		pass service-active "${UNIT} active and socket present"
	else
		fail service-active "${UNIT} is '$(systemctl is-active "${UNIT}" 2>&1)'"
		journal_tail
		return
	fi

	local verify
	verify="$(systemd-analyze verify "/usr/lib/systemd/system/${UNIT}" 2>&1 | grep -v 'Failed to .*Cgroup\|cgroup' || true)"
	if [ -z "${verify}" ]; then
		pass unit-verify "systemd-analyze verify is clean"
	else
		fail unit-verify "$(printf '%s' "${verify}" | tr '\n' ' ')"
	fi

	local main_pid user
	main_pid="$(systemctl show -p MainPID --value "${UNIT}")"
	user="$(ps -o user= -p "${main_pid}" 2>/dev/null | tr -d ' ')"
	if [ "${user}" = yams ]; then
		pass service-account "daemon runs as the yams system account"
	else
		fail service-account "daemon runs as '${user:-?}', expected yams"
	fi
	check account-group "yams system group exists" getent group yams

	local mode owner group
	mode="$(stat -c %a "${SOCKET}")"
	owner="$(stat -c %U "${SOCKET}")"
	group="$(stat -c %G "${SOCKET}")"
	if [ "$((8#${mode} & 8#007))" -eq 0 ] && [ "${group}" = yams ]; then
		pass socket-perms "${SOCKET} ${mode} ${owner}:${group} (no access for others)"
	else
		fail socket-perms "${SOCKET} is ${mode} ${owner}:${group}; want group yams and no access for others"
	fi
	local dmode
	dmode="$(stat -c %a /run/yams)"
	if [ "$((8#${dmode} & 8#007))" -eq 0 ]; then
		pass runtime-dir-perms "/run/yams ${dmode}"
	else
		fail runtime-dir-perms "/run/yams is ${dmode}; others can list it"
	fi

	local world
	world="$(find /run/yams "${STATE_DIR}/" "${LOG_DIR}/" -xdev \( -perm -0002 ! -type l \) 2>/dev/null | head -n 5)"
	if [ -z "${world}" ]; then
		pass state-not-world-writable "nothing under /run/yams, ${STATE_DIR}, ${LOG_DIR} is world-writable"
	else
		fail state-not-world-writable "$(printf '%s ' ${world})"
	fi

	if grep -q '^PermissionsStartOnly' "/usr/lib/systemd/system/${UNIT}"; then
		fail unit-deprecated "unit uses deprecated PermissionsStartOnly="
	else
		pass unit-deprecated "no deprecated directives"
	fi
	if grep -Eq 'chmod[[:space:]]+0?666' "/usr/lib/systemd/system/${UNIT}"; then
		fail unit-no-chmod666 "unit makes the socket world-writable (chmod 0666)"
	else
		pass unit-no-chmod666 "unit leaves socket mode to the daemon (group rw, others none)"
	fi

	local exposure
	exposure="$(systemd-analyze security --no-pager "${UNIT}" 2>/dev/null | sed -n 's/.*Overall exposure level for .*: \([0-9.]*\).*/\1/p')"
	if [ -z "${exposure}" ]; then
		info sandbox-exposure "systemd-analyze security unavailable"
	elif awk -v e="${exposure}" -v m="${MAX_EXPOSURE}" 'BEGIN{exit !(e <= m)}'; then
		pass sandbox-exposure "systemd-analyze security exposure ${exposure} <= ${MAX_EXPOSURE}"
	else
		fail sandbox-exposure "systemd-analyze security exposure ${exposure} > ${MAX_EXPOSURE}"
	fi

	check data-dir "state directory ${STATE_DIR} holds the corpus" test -e "${STATE_DIR}/yams.db"
	check log-file "daemon log written to ${LOG_DIR}/daemon.log" test -s "${LOG_DIR}/daemon.log"
}

# ---------------------------------------------------------------------------
phase_cli() {
	: >"${TOKEN_FILE}"
	id member >/dev/null 2>&1 || useradd -m -s /bin/bash member
	id outsider >/dev/null 2>&1 || useradd -m -s /bin/bash outsider
	if getent group yams >/dev/null 2>&1; then
		usermod -aG yams member
	else
		fail cli-member-group "no yams group to add a user to"
	fi

	# (a) root, no environment beyond a login's.
	local rt
	rt="$(token root)"
	remember ROOT "${rt}"
	printf 'root note %s\n' "${rt}" >/root/root-note.txt
	local out rc=0
	out="$(cd /root && env -i HOME=/root PATH=/usr/bin:/bin timeout 120 yams daemon status 2>&1)" || rc=$?
	if [ "${rc}" -eq 0 ] && ! printf '%s' "${out}" | grep -qi 'not running'; then
		pass cli-root-status "yams daemon status reaches a daemon"
	else
		fail cli-root-status "rc=${rc}: $(printf '%s' "${out}" | tail -n 3 | tr '\n' ' ')"
	fi
	rc=0
	out="$(cd /root && env -i HOME=/root PATH=/usr/bin:/bin timeout 120 yams add /root/root-note.txt 2>&1)" || rc=$?
	if [ "${rc}" -eq 0 ]; then pass cli-root-add "yams add /root/root-note.txt"; else fail cli-root-add "rc=${rc}: $(printf '%s' "${out}" | tail -n 3 | tr '\n' ' ')"; fi
	if search_finds root "${rt}" root-note; then
		pass cli-root-search "root's document is searchable"
	else
		fail cli-root-search "root's document not found by yams search"
	fi
	# Proof the system daemon stored it: no private per-user store appeared.
	if [ -e /root/.local/share/yams/yams.db ]; then
		fail cli-root-uses-system-daemon "CLI wrote a private store /root/.local/share/yams instead of using ${SOCKET}"
	else
		pass cli-root-uses-system-daemon "no private store under /root; the system daemon served the CLI"
	fi

	# (b) a member of the yams group, files in $HOME and /tmp.
	local mt mt2
	mt="$(token member)"
	mt2="$(token membertmp)"
	remember MEMBER "${mt}"
	as_user member "printf 'member note %s\n' '${mt}' > ~/member-note.txt; printf 'member tmp %s\n' '${mt2}' > /tmp/member-tmp.txt; mkdir -p ~/notes && printf 'x\n' > ~/notes/a.txt"
	check cli-member-socket-access "group member can open ${SOCKET}" as_user member "test -w ${SOCKET}"
	rc=0
	out="$(as_user member 'timeout 120 yams add ~/member-note.txt' 2>&1)" || rc=$?
	if [ "${rc}" -eq 0 ]; then pass cli-member-add-home "yams add ~/member-note.txt"; else fail cli-member-add-home "rc=${rc}: $(printf '%s' "${out}" | tail -n 3 | tr '\n' ' ')"; fi
	rc=0
	out="$(as_user member 'timeout 120 yams add /tmp/member-tmp.txt' 2>&1)" || rc=$?
	if [ "${rc}" -eq 0 ]; then pass cli-member-add-tmp "yams add /tmp/member-tmp.txt"; else fail cli-member-add-tmp "rc=${rc}: $(printf '%s' "${out}" | tail -n 3 | tr '\n' ' ')"; fi
	if search_finds member "${mt}" member-note; then pass cli-member-search-home "home file searchable"; else fail cli-member-search-home "member home note not found by yams search"; fi
	if search_finds member "${mt2}" member-tmp; then pass cli-member-search-tmp "/tmp file searchable"; else fail cli-member-search-tmp "/tmp/member-tmp.txt not found by yams search"; fi
	if search_finds member "${rt}" root-note; then
		pass cli-member-shared-corpus "member sees the system corpus (root's document)"
	else
		fail cli-member-shared-corpus "member does not see root's document: not using the system daemon"
	fi
	if as_user member 'test -e ~/.local/share/yams/yams.db'; then
		fail cli-member-uses-system-daemon "CLI wrote a private store ~member/.local/share/yams instead of using ${SOCKET}"
	else
		pass cli-member-uses-system-daemon "no private store for member"
	fi
	# A directory under $HOME cannot be walked by the sandboxed daemon: the CLI must say so.
	rc=0
	out="$(as_user member 'timeout 120 yams add ~/notes' 2>&1)" || rc=$?
	if [ "${rc}" -ne 0 ] && printf '%s' "${out}" | grep -qi 'another user'; then
		pass cli-member-add-dir-clear-error "directory add fails with an explanation"
	else
		fail cli-member-add-dir-clear-error "rc=${rc}: $(printf '%s' "${out}" | tail -n 2 | tr '\n' ' ')"
	fi

	# (c) a user outside the yams group must not reach the system socket.
	if as_user outsider "test -w ${SOCKET}" 2>/dev/null; then
		fail cli-outsider-denied "non-member can open ${SOCKET} (world-writable socket)"
	else
		pass cli-outsider-denied "non-member cannot open ${SOCKET}"
	fi

	# (d) a per-user service (`yams daemon install --user`) keeps working beside the
	# system one, and that user's CLI uses it rather than the system socket.
	local ouid
	ouid="$(id -u outsider)"
	loginctl enable-linger outsider >/dev/null 2>&1 || true
	for _ in $(seq 1 60); do
		[ -S "/run/user/${ouid}/bus" ] && break
		sleep 0.5
	done
	local uenv="export XDG_RUNTIME_DIR=/run/user/${ouid} DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/${ouid}/bus;"
	local ot
	ot="$(token outsider)"
	as_user outsider "printf 'outsider note %s\n' '${ot}' > ~/outsider-note.txt"
	rc=0
	out="$(as_user outsider "${uenv} timeout 120 yams daemon install --user" 2>&1)" || rc=$?
	local uactive=""
	for _ in $(seq 1 60); do
		uactive="$(systemctl --user -M outsider@ is-active yams-daemon.service 2>/dev/null || true)"
		[ "${uactive}" = active ] && [ -S "/run/user/${ouid}/yams-daemon.sock" ] && break
		sleep 0.5
	done
	if [ "${rc}" -eq 0 ] && [ "${uactive}" = active ]; then
		pass cli-user-service "yams daemon install --user starts a per-user unit"
	else
		fail cli-user-service "rc=${rc} state=${uactive:-?}: $(printf '%s' "${out}" | tail -n 3 | tr '\n' ' ')"
	fi
	rc=0
	out="$(as_user outsider "${uenv} timeout 120 yams add ~/outsider-note.txt && timeout 60 yams daemon status" 2>&1)" || rc=$?
	if [ "${rc}" -eq 0 ] && as_user outsider 'test -e ~/.local/share/yams/yams.db'; then
		pass cli-user-service-used "per-user CLI talks to its own daemon and store"
	else
		fail cli-user-service-used "rc=${rc}: $(printf '%s' "${out}" | tail -n 3 | tr '\n' ' ')"
	fi
	as_user outsider "${uenv} yams daemon uninstall --user" >/dev/null 2>&1 || true
	loginctl disable-linger outsider >/dev/null 2>&1 || true

	# Daemon lifecycle stays with systemd.
	rc=0
	out="$(cd /root && env -i HOME=/root PATH=/usr/bin:/bin timeout 60 yams daemon stop 2>&1)" || rc=$?
	if [ "$(systemctl is-active "${UNIT}" 2>/dev/null)" = active ]; then
		pass cli-stop-defers-to-systemd "yams daemon stop leaves the system unit running"
	else
		fail cli-stop-defers-to-systemd "yams daemon stop killed the system unit: $(printf '%s' "${out}" | tail -n 2 | tr '\n' ' ')"
		systemctl start "${UNIT}" >/dev/null 2>&1 || true
		wait_active || true
	fi
}

# ---------------------------------------------------------------------------
phase_persist() {
	if [ "$(systemctl is-enabled "${UNIT}" 2>/dev/null)" = enabled ] && wait_active; then
		pass reboot-active "${UNIT} active after reboot"
	else
		fail reboot-active "${UNIT} is '$(systemctl is-active "${UNIT}" 2>&1)' after reboot"
		journal_tail
		return
	fi
	local mode
	mode="$(stat -c %a "${SOCKET}")"
	if [ "$((8#${mode} & 8#007))" -eq 0 ]; then pass reboot-socket-perms "${mode}"; else fail reboot-socket-perms "${SOCKET} is ${mode} after reboot"; fi
	local rt
	rt="$(recall ROOT)"
	if [ -n "${rt}" ] && search_finds root "${rt}" root-note; then
		pass reboot-data-persists "root's document survives a reboot"
	else
		fail reboot-data-persists "root's document missing after reboot"
	fi
}

# ---------------------------------------------------------------------------
phase_seed() {
	: >"${TOKEN_FILE}"
	if ! wait_active; then
		fail upgrade-seed-active "old release's ${UNIT} not active"
		journal_tail
		return
	fi
	local ut
	ut="$(token upgrade)"
	remember UPGRADE "${ut}"
	printf 'upgrade note %s\n' "${ut}" >/var/tmp/upgrade-note.txt
	chmod 0644 /var/tmp/upgrade-note.txt
	# Older CLIs cannot find the system socket; feed content on stdin so the
	# sandboxed daemon need not read a path.
	local rc=0
	env -i HOME=/root PATH=/usr/bin:/bin YAMS_DAEMON_SOCKET="${SOCKET}" \
		timeout 120 yams add - --name upgrade-note.txt </var/tmp/upgrade-note.txt >/dev/null 2>&1 || rc=$?
	if [ "${rc}" -eq 0 ]; then pass upgrade-seed "stored a document through the old release"; else fail upgrade-seed "old release add rc=${rc}"; fi
	local out=""
	for _ in $(seq 1 60); do
		out="$(env -i HOME=/root PATH=/usr/bin:/bin YAMS_DAEMON_SOCKET="${SOCKET}" timeout 30 yams search "${ut}" 2>&1 || true)"
		case "${out}" in *upgrade-note*) break ;; esac
		sleep 1
	done
	case "${out}" in
	*upgrade-note*) pass upgrade-seed-indexed "old release indexed the document" ;;
	*) fail upgrade-seed-indexed "old release never indexed the document" ;;
	esac
	systemctl show -p MainPID --value "${UNIT}" >/var/tmp/yams-validate-oldpid
}

phase_upgraded() {
	if wait_active; then
		pass upgrade-active "${UNIT} active after upgrade"
	else
		fail upgrade-active "${UNIT} is '$(systemctl is-active "${UNIT}" 2>&1)' after upgrade"
		journal_tail
		return
	fi
	local old new
	old="$(cat /var/tmp/yams-validate-oldpid 2>/dev/null || true)"
	new="$(systemctl show -p MainPID --value "${UNIT}")"
	if [ -n "${new}" ] && [ "${new}" != "${old}" ] && ! readlink "/proc/${new}/exe" | grep -q deleted; then
		pass upgrade-restarted "daemon restarted onto the new binary (pid ${old} -> ${new})"
	else
		fail upgrade-restarted "daemon still runs the old binary (pid ${old} -> ${new})"
	fi
	local ut
	ut="$(recall UPGRADE)"
	# Pin the socket: this checks the corpus survived, not client discovery.
	if [ -n "${ut}" ] && SEARCH_SOCKET="${SOCKET}" search_finds root "${ut}" upgrade-note; then
		pass upgrade-data-kept "document stored before the upgrade is still searchable"
	else
		fail upgrade-data-kept "document stored before the upgrade is gone or unreachable"
	fi
}

check_no_dangling_links() { # check_no_dangling_links <id>
	local dangling
	dangling="$(find /etc/systemd/system -name "${UNIT}" -xtype l 2>/dev/null || true)"
	if [ -z "${dangling}" ]; then
		pass "$1" "no dangling enablement symlinks"
	else
		fail "$1" "$(printf '%s ' ${dangling})"
	fi
}

# ---------------------------------------------------------------------------
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
	if [ "$(systemctl is-active "${UNIT}" 2>/dev/null)" = active ]; then
		fail remove-stopped "${UNIT} still active after removal"
	else
		pass remove-stopped "${UNIT} stopped"
	fi
	check remove-binaries "binaries removed" test ! -e /usr/bin/yams-daemon
	# Debian keeps enablement state until purge (deb-systemd-helper masks on
	# remove), so for deb the dangling-link check runs after purge.
	[ "${PM}" = deb ] || check_no_dangling_links remove-no-dangling-links
	if [ "${PM}" = deb ]; then
		rc=0
		apt-get purge -y yams >>"${log}" 2>&1 || rc=$?
		if [ "${rc}" -eq 0 ]; then pass purge "package purged"; else fail purge "rc=${rc}"; fi
		local left=""
		for d in "${STATE_DIR}" "${LOG_DIR}" /var/lib/private/yams /var/log/private/yams; do
			{ [ -e "${d}" ] || [ -L "${d}" ]; } && left+="${d} "
		done
		if [ -z "${left}" ]; then pass purge-state "state and logs removed on purge"; else fail purge-state "left behind after purge: ${left}"; fi
		check_no_dangling_links purge-no-dangling-links
	else
		info remove-state "state kept by design on ${PM} removal: $(ls -d ${STATE_DIR} 2>/dev/null || echo none)"
	fi
}

case "${PHASE}" in
install) phase_install ;;
layout) phase_layout ;;
service) phase_service ;;
cli) phase_cli ;;
persist) phase_persist ;;
seed) phase_seed ;;
upgraded) phase_upgraded ;;
remove) phase_remove ;;
*)
	echo "unknown phase: ${PHASE}" >&2
	exit 64
	;;
esac
exit "${FAILS}"
