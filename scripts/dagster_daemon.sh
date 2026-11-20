#!/usr/bin/env bash
# Run the Dagster daemon as a macOS LaunchAgent so the daily_refresh schedule
# fires without `dagster dev` open. The agent starts at login and is restarted
# if it exits.
#
#   scripts/dagster_daemon.sh install     # write the plist, load it, start the schedule
#   scripts/dagster_daemon.sh uninstall   # stop the schedule, unload and delete the plist
#   scripts/dagster_daemon.sh status      # agent state, schedule state, recent log lines
#
# Run it from the env that has Dagster installed (`conda activate cfpb`), or
# point PYTHON at that env's interpreter.
set -euo pipefail

LABEL="io.cfpb-platform.dagster-daemon"
SCHEDULE="daily_refresh_schedule"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
DAGSTER_HOME="$REPO/.dagster"
# Not under $DAGSTER_HOME/logs: Dagster manages that folder and may remove it,
# and launchd won't recreate a missing log directory.
LOG="$REPO/logs/dagster-daemon.log"
PYTHON="${PYTHON:-python}"
BIN="$("$PYTHON" -c 'import os, sys; print(os.path.dirname(sys.executable))')"
export DAGSTER_HOME

if [[ ! -x "$BIN/dagster-daemon" ]]; then
  echo "dagster-daemon not found in $BIN. Activate the cfpb env or set PYTHON." >&2
  exit 1
fi

# `dagster` CLI commands read [tool.dagster] from pyproject.toml, so they must
# run from the repo root. That keeps the code location named "cfpb", which is
# what the schedule's on/off state is stored under.
dagster_cli() { (cd "$REPO" && "$BIN/dagster" "$@"); }

write_plist() {
  mkdir -p "$(dirname "$PLIST")" "$(dirname "$LOG")"
  cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>               <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$BIN/dagster-daemon</string>
    <string>run</string>
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
  <key>StandardOutPath</key>     <string>$LOG</string>
  <key>StandardErrorPath</key>   <string>$LOG</string>
</dict>
</plist>
EOF
  plutil -lint "$PLIST" >/dev/null
}

case "${1:-}" in
  install)
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
    # bootout returns before the old agent is gone; bootstrapping too early
    # fails with "5: Input/output error".
    for _ in {1..20}; do
      launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || break
      sleep 0.5
    done
    write_plist
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
    dagster_cli schedule start "$SCHEDULE"
    echo "Installed $PLIST; daemon log: $LOG"
    ;;
  uninstall)
    dagster_cli schedule stop "$SCHEDULE" || true
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
    rm -f "$PLIST"
    echo "Removed $LABEL"
    ;;
  status)
    launchctl print "gui/$(id -u)/$LABEL" 2>/dev/null | grep -E '^\s*(state|pid|last exit code) =' \
      || echo "agent not loaded"
    dagster_cli schedule list 2>/dev/null | grep -E '^Schedule:' || true
    [[ -f "$LOG" ]] && tail -n 5 "$LOG"
    ;;
  *)
    echo "usage: $0 {install|uninstall|status}" >&2
    exit 2
    ;;
esac
