#!/bin/sh
# Container entrypoint. mesh.py generates keys and writes the config through `docker exec` while
# this process idles, then creates /node/ready. Waiting here (instead of starting the daemon
# straight away) lets `docker restart` re-run the same daemon with the same node id and data dir.
set -eu

mkdir -p /node/home /node/data /node/log /node/run \
    "${XDG_CONFIG_HOME}" "${XDG_DATA_HOME}" "${XDG_STATE_HOME}"
chmod 700 /node/run

while [ ! -f /node/ready ]; do
    sleep 0.3
done

rm -f "${YAMS_DAEMON_SOCKET}"
exec yams-daemon --foreground \
    --config "${XDG_CONFIG_HOME}/yams/config.toml" \
    --socket "${YAMS_DAEMON_SOCKET}" \
    --data-dir "${YAMS_DATA_DIR}" \
    --log-file /node/log/daemon.log \
    --log-level "${MESH_LOG_LEVEL:-info}"
