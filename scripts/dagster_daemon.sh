#!/usr/bin/env bash
# Run Dagster as two macOS LaunchAgents so the daily_refresh schedule fires
# without `dagster dev` open. Both start at login and are restarted if they exit.
#
#   io.cfpb-platform.dagster-code-server  a persistent gRPC code server on 127.0.0.1:4266
#   io.cfpb-platform.dagster-daemon       the daemon, reading dagster_workspace.yaml
#
# The daemon connects to the long-running code server instead of starting its
# own every 60 seconds. A freshly spawned server can lose the race against
# macOS Power Nap, when the Mac goes back to sleep before the server is up.
#
#   scripts/dagster_daemon.sh install     # write both plists, load them, start the schedule
#   scripts/dagster_daemon.sh uninstall   # stop the schedule, unload and delete both plists
#   scripts/dagster_daemon.sh reload      # restart the code server to pick up code changes
#   scripts/dagster_daemon.sh status      # agent states, schedule state, recent log lines
#
# Run it from the env that has Dagster installed (`conda activate cfpb`), or
# point PYTHON at that env's interpreter.
set -euo pipefail

DAEMON_LABEL="io.cfpb-platform.dagster-daemon"
CODE_LABEL="io.cfpb-platform.dagster-code-server"
SCHEDULE="daily_refresh_schedule"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORKSPACE="$REPO/dagster_workspace.yaml"
CODE_HOST="127.0.0.1"
CODE_PORT="4266"
DAGSTER_HOME="$REPO/.dagster"
# Not under $DAGSTER_HOME/logs: Dagster manages that folder and may remove it,
# and launchd won't recreate a missing log directory.
DAEMON_LOG="$REPO/logs/dagster-daemon.log"
CODE_LOG="$REPO/logs/dagster-code-server.log"
PYTHON="${PYTHON:-python}"
BIN="$("$PYTHON" -c 'import os, sys; print(os.path.dirname(sys.executable))')"
DOMAIN="gui/$(id -u)"
export DAGSTER_HOME

if [[ ! -x "$BIN/dagster-daemon" ]]; then
  echo "dagster-daemon not found in $BIN. Activate the cfpb env or set PYTHON." >&2
  exit 1
fi

# Run `dagster` CLI commands from the repo root against the same workspace the
# daemon uses, so the code location is "cfpb", which is the name the
# schedule's on/off state is stored under.
dagster_cli() { (cd "$REPO" && "$BIN/dagster" "$@" -w "$WORKSPACE"); }

plist_path() { echo "$HOME/Library/LaunchAgents/$1.plist"; }

# write_plist LABEL LOG ARG...
write_plist() {
  local label="$1" log="$2"; shift 2
  local plist; plist="$(plist_path "$label")"
  mkdir -p "$(dirname "$plist")" "$(dirname "$log")"
  {
    cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>               <string>$label</string>
  <key>ProgramArguments</key>
  <array>
EOF
    for arg in "$@"; do echo "    <string>$arg</string>"; done
    cat <<EOF
  </array>
  <key>WorkingDirectory</key>    <string>$REPO</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>DAGSTER_HOME</key>      <string>$DAGSTER_HOME</string>
    <key>PATH</key>              <string>$BIN:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>RunAtLoad</key>           <true/>
  <key>KeepAlive</key>           <true/>
  <key>ThrottleInterval</key>    <integer>30</integer>
  <key>StandardOutPath</key>     <string>$log</string>
  <key>StandardErrorPath</key>   <string>$log</string>
</dict>
</plist>
EOF
  } > "$plist"
  plutil -lint "$plist" >/dev/null
}

unload() {
  launchctl bootout "$DOMAIN/$1" 2>/dev/null || true
  # bootout returns before the old agent is gone; bootstrapping too early
  # fails with "5: Input/output error".
  for _ in {1..20}; do
    launchctl print "$DOMAIN/$1" >/dev/null 2>&1 || return 0
    sleep 0.5
  done
}

wait_for_code_server() {
  # Loading the definitions parses the dbt manifest, which takes a few seconds.
  for _ in {1..60}; do
    "$BIN/dagster" api grpc-health-check -h "$CODE_HOST" -p "$CODE_PORT" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "Code server did not come up on $CODE_HOST:$CODE_PORT; see $CODE_LOG" >&2
  return 1
}

agent_state() {
  echo "$1:"
  launchctl print "$DOMAIN/$1" 2>/dev/null | grep -E '^\s*(state|pid|last exit code) =' \
    || echo "  not loaded"
}

case "${1:-}" in
  install)
    unload "$DAEMON_LABEL"
    unload "$CODE_LABEL"
    write_plist "$CODE_LABEL" "$CODE_LOG" \
      "$BIN/dagster" code-server start \
      -h "$CODE_HOST" -p "$CODE_PORT" \
      -m cfpb_platform.orchestration.definitions --location-name cfpb \
      --log-level warning
    write_plist "$DAEMON_LABEL" "$DAEMON_LOG" \
      "$BIN/dagster-daemon" run -w "$WORKSPACE"
    launchctl bootstrap "$DOMAIN" "$(plist_path "$CODE_LABEL")"
    wait_for_code_server
    launchctl bootstrap "$DOMAIN" "$(plist_path "$DAEMON_LABEL")"
    if ! dagster_cli schedule list --running 2>/dev/null | grep -q "$SCHEDULE"; then
      dagster_cli schedule start "$SCHEDULE"
    fi
    echo "Installed $CODE_LABEL and $DAEMON_LABEL; logs in $REPO/logs/"
    ;;
  uninstall)
    dagster_cli schedule stop "$SCHEDULE" || true
    unload "$DAEMON_LABEL"
    unload "$CODE_LABEL"
    rm -f "$(plist_path "$DAEMON_LABEL")" "$(plist_path "$CODE_LABEL")"
    echo "Removed $DAEMON_LABEL and $CODE_LABEL"
    ;;
  reload)
    launchctl kickstart -k "$DOMAIN/$CODE_LABEL"
    wait_for_code_server
    echo "Code server restarted; the daemon reconnects within 60 seconds"
    ;;
  status)
    agent_state "$CODE_LABEL"
    agent_state "$DAEMON_LABEL"
    dagster_cli schedule list 2>/dev/null | grep -E '^Schedule:' || true
    for log in "$CODE_LOG" "$DAEMON_LOG"; do
      [[ -f "$log" ]] && { echo "--- $(basename "$log")"; tail -n 5 "$log"; }
    done
    ;;
  *)
    echo "usage: $0 {install|uninstall|reload|status}" >&2
    exit 2
    ;;
esac
