#!/usr/bin/env bash
# End-to-end DESS demo: broker, two producers, two consumers, integrity probe, admin, restart.
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA="java"
fi

CONFIG="${DEMO_CONFIG:-$ROOT/config/config.xml}"
BROKER_JAR="$ROOT/dess-broker/target/dess-broker.jar"
CLIENT_JAR="$ROOT/dess-client/target/dess-client.jar"
ADMIN_JAR="$ROOT/dess-admin/target/dess-admin.jar"
WORK="${DEMO_WORKDIR:-$ROOT/data/demo}"

CLEANED=0
BROKER_PID=""
CONSUME_A_PID=""
CONSUME_B_PID=""
BROKER_OUT=""
TCP_PORT=""
ADMIN_PORT=""
PRODUCER_P12=""
CONSUMER_P12=""
TRUST_P12=""
PASSWORD_FILE=""
RUNTIME_CONFIG=""
OFFSETS=""

say() {
    printf '%s\n' "$*"
}

xml_text() {
    sed -n "s:.*<$2>\\(.*\\)</$2>.*:\\1:p" "$1" | head -n 1
}

resolve_path() {
    case "$1" in
        /*) printf '%s\n' "$1" ;;
        *) printf '%s\n' "$ROOT/$1" ;;
    esac
}

wait_contains() {
    local file="$1"
    local needle="$2"
    local seconds="$3"
    local pid="${4:-}"
    python3 - "$file" "$needle" "$seconds" "$pid" <<'PY'
import os, sys, time
from pathlib import Path
path = Path(sys.argv[1])
needle = sys.argv[2]
deadline = time.monotonic() + float(sys.argv[3])
pid = int(sys.argv[4]) if sys.argv[4] else 0
while time.monotonic() < deadline:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        text = ""
    if needle in text:
        sys.exit(0)
    if pid:
        try:
            os.kill(pid, 0)
        except OSError:
            sys.exit(1)
    time.sleep(0.05)
sys.exit(1)
PY
}

wait_count() {
    local file="$1"
    local needle="$2"
    local count="$3"
    local seconds="$4"
    local pid="${5:-}"
    python3 - "$file" "$needle" "$count" "$seconds" "$pid" <<'PY'
import os, sys, time
from pathlib import Path
path = Path(sys.argv[1])
needle = sys.argv[2]
needed = int(sys.argv[3])
deadline = time.monotonic() + float(sys.argv[4])
pid = int(sys.argv[5]) if sys.argv[5] else 0
while time.monotonic() < deadline:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        text = ""
    if text.count(needle) >= needed:
        sys.exit(0)
    if pid:
        try:
            os.kill(pid, 0)
        except OSError:
            sys.exit(1)
    time.sleep(0.05)
sys.exit(1)
PY
}

stop_pid() {
    local pid="${1:-}"
    if [ -z "$pid" ]; then
        return 0
    fi
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        python3 - "$pid" <<'PY'
import os, sys, time, signal
pid = int(sys.argv[1])
deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except OSError:
        sys.exit(0)
    time.sleep(0.05)
try:
    os.kill(pid, signal.SIGKILL)
except OSError:
    pass
PY
    fi
    wait "$pid" 2>/dev/null || true
}

cleanup() {
    if [ "$CLEANED" -eq 1 ]; then
        return
    fi
    CLEANED=1
    stop_pid "${CONSUME_A_PID:-}"
    stop_pid "${CONSUME_B_PID:-}"
    stop_pid "${BROKER_PID:-}"
    CONSUME_A_PID=""
    CONSUME_B_PID=""
    BROKER_PID=""
}

fail_step() {
    local name="$1"
    say "failed step: $name"
    if [ -n "${BROKER_OUT:-}" ] && [ -f "$BROKER_OUT" ]; then
        cat "$BROKER_OUT" || true
    fi
    exit 1
}

bind_hint() {
    local log="$1"
    if grep -q "Could not bind port" "$log" 2>/dev/null \
        || grep -q "Could not bind admin port" "$log" 2>/dev/null \
        || grep -q "Could not bind discovery port" "$log" 2>/dev/null; then
        grep -E "Could not bind (admin |discovery )?port" "$log" || true
        say "Change the matching element in config/config.xml"
    fi
}

port_from() {
    python3 - "$1" "$2" <<'PY'
import re, sys
text = open(sys.argv[1], errors="replace").read()
match = re.search(re.escape(sys.argv[2]) + r"(\d+)", text)
if not match:
    sys.exit(1)
print(match.group(1))
PY
}

start_broker() {
    local log="$1"
    local step="$2"
    : >"$log"
    "$JAVA" -jar "$BROKER_JAR" --config "$RUNTIME_CONFIG" >"$log" 2>&1 &
    BROKER_PID=$!
    BROKER_OUT="$log"
    if ! wait_contains "$log" "event=broker.started" 30 "$BROKER_PID"; then
        bind_hint "$log"
        fail_step "$step"
    fi
    TCP_PORT="$(port_from "$log" "tcpPort=")" || fail_step "$step"
    ADMIN_PORT="$(port_from "$log" "adminPort=")" || fail_step "$step"
    cat "$log"
}

start_consumer() {
    local log="$1"
    shift
    : >"$log"
    "$JAVA" -jar "$CLIENT_JAR" consume --topic demo --host 127.0.0.1 --port "$TCP_PORT" \
        --heartbeat-ms 10000 --log-level WARNING \
        --identity "$CONSUMER_P12" --trust-store "$TRUST_P12" --password-file "$PASSWORD_FILE" \
        "$@" >"$log" 2>&1 &
    echo $!
}

run_produce() {
    local message="$1"
    local step="$2"
    if ! "$JAVA" -jar "$CLIENT_JAR" produce --topic demo --message "$message" \
        --host 127.0.0.1 --port "$TCP_PORT" --log-level WARNING \
        --identity "$PRODUCER_P12" --trust-store "$TRUST_P12" --password-file "$PASSWORD_FILE"; then
        fail_step "$step"
    fi
}

run_admin() {
    local verb="$1"
    local step="$2"
    local log="$3"
    local -a cmd
    if [ -f "$ADMIN_JAR" ]; then
        cmd=("$JAVA" -jar "$ADMIN_JAR")
    else
        cmd=("$JAVA" -cp "$ROOT/dess-admin/target/classes:$ROOT/dess-common/target/classes" dev.dess.admin.AdminMain)
    fi
    if ! "${cmd[@]}" --url "http://127.0.0.1:${ADMIN_PORT}" "$verb" >"$log" 2>&1; then
        cat "$log" || true
        fail_step "$step"
    fi
    cat "$log"
}

trap cleanup EXIT

if [ ! -f "$CONFIG" ]; then
    say "missing configuration file"
    exit 2
fi
if [ ! -f "$BROKER_JAR" ] || [ ! -f "$CLIENT_JAR" ]; then
    say "missing jars"
    exit 2
fi
if [ ! -f "$ADMIN_JAR" ] && [ ! -d "$ROOT/dess-admin/target/classes" ]; then
    say "missing jars"
    exit 2
fi

mkdir -p "$WORK"
RUNTIME_CONFIG="$WORK/config.xml"
cp "$CONFIG" "$RUNTIME_CONFIG"
OFFSETS="$WORK/offsets"
KEY_STORE="$(resolve_path "$(xml_text "$RUNTIME_CONFIG" keyStore)")"
TRUST_P12="$(resolve_path "$(xml_text "$RUNTIME_CONFIG" trustStore)")"
PASSWORD_FILE="$(resolve_path "$(xml_text "$RUNTIME_CONFIG" passwordFile)")"
PKI_DIR="$(dirname "$KEY_STORE")"
PRODUCER_P12="$PKI_DIR/producer.p12"
CONSUMER_P12="$PKI_DIR/consumer.p12"

if [ ! -f "$PRODUCER_P12" ] || [ ! -f "$CONSUMER_P12" ] || [ ! -f "$TRUST_P12" ] || [ ! -f "$PASSWORD_FILE" ] || [ ! -f "$KEY_STORE" ]; then
    say "missing PKI"
    exit 2
fi

say "step=start-broker"
start_broker "$WORK/broker-1.out" start-broker

say "step=consume-a"
CONSUME_A_OUT="$WORK/consume-a.out"
CONSUME_A_PID="$(start_consumer "$CONSUME_A_OUT" --from earliest)"
if ! wait_count "$BROKER_OUT" "event=subscription.added" 1 15 "$BROKER_PID"; then
    cat "$CONSUME_A_OUT" || true
    fail_step consume-a
fi
if ! kill -0 "$CONSUME_A_PID" 2>/dev/null; then
    cat "$CONSUME_A_OUT" || true
    fail_step consume-a
fi

say "step=consume-b"
CONSUME_B_OUT="$WORK/consume-b.out"
CONSUME_B_PID="$(start_consumer "$CONSUME_B_OUT" --resume --resume-file "$OFFSETS")"
if ! wait_count "$BROKER_OUT" "event=subscription.added" 2 15 "$BROKER_PID"; then
    cat "$CONSUME_B_OUT" || true
    fail_step consume-b
fi
if ! kill -0 "$CONSUME_B_PID" 2>/dev/null; then
    cat "$CONSUME_B_OUT" || true
    fail_step consume-b
fi

say "step=produce-a"
run_produce "alpha" produce-a
if ! wait_contains "$CONSUME_A_OUT" "demo alpha" 15; then
    cat "$CONSUME_A_OUT" || true
    fail_step produce-a
fi
if ! wait_contains "$CONSUME_B_OUT" "demo alpha" 15; then
    cat "$CONSUME_B_OUT" || true
    fail_step produce-a
fi

say "step=produce-b"
run_produce "bravo" produce-b
if ! wait_contains "$CONSUME_A_OUT" "demo bravo" 15; then
    cat "$CONSUME_A_OUT" || true
    fail_step produce-b
fi
if ! wait_contains "$CONSUME_B_OUT" "demo bravo" 15; then
    cat "$CONSUME_B_OUT" || true
    fail_step produce-b
fi
cat "$CONSUME_A_OUT"
cat "$CONSUME_B_OUT"

say "step=integrity-reject"
PROBE_OUT="$WORK/probe.out"
if ! "$JAVA" -cp "$CLIENT_JAR" dev.dess.client.IntegrityProbe \
    --topic demo --host 127.0.0.1 --port "$TCP_PORT" \
    --identity "$PRODUCER_P12" --trust-store "$TRUST_P12" --password-file "$PASSWORD_FILE" \
    >"$PROBE_OUT" 2>&1; then
    cat "$PROBE_OUT" || true
    fail_step integrity-reject
fi
if ! grep -q "INTEGRITY_FAILED" "$PROBE_OUT"; then
    cat "$PROBE_OUT" || true
    fail_step integrity-reject
fi
cat "$PROBE_OUT"

say "step=admin-health"
run_admin health admin-health "$WORK/health.out"
if ! grep -q "STATUS UP" "$WORK/health.out"; then
    fail_step admin-health
fi

say "step=admin-metrics"
run_admin metrics admin-metrics "$WORK/metrics.out"
if ! grep -q "takenAt " "$WORK/metrics.out"; then
    fail_step admin-metrics
fi

say "step=stop-broker"
stop_pid "$BROKER_PID"
BROKER_PID=""
if ! wait_contains "$BROKER_OUT" "event=broker.stopped" 10 || ! grep -q "reason=SIGNAL" "$BROKER_OUT"; then
    fail_step stop-broker
fi
cat "$BROKER_OUT"
stop_pid "$CONSUME_A_PID"
stop_pid "$CONSUME_B_PID"
CONSUME_A_PID=""
CONSUME_B_PID=""

say "step=start-broker-again"
start_broker "$WORK/broker-2.out" start-broker-again
if ! grep -q "event=log.recovered" "$BROKER_OUT"; then
    fail_step start-broker-again
fi

say "step=resume-consume"
CONSUME_A_OUT="$WORK/consume-a-2.out"
CONSUME_B_OUT="$WORK/consume-b-2.out"
CONSUME_A_PID="$(start_consumer "$CONSUME_A_OUT" --from earliest)"
CONSUME_B_PID="$(start_consumer "$CONSUME_B_OUT" --resume --resume-file "$OFFSETS")"
if ! wait_count "$BROKER_OUT" "event=subscription.added" 2 15 "$BROKER_PID"; then
    cat "$CONSUME_A_OUT" "$CONSUME_B_OUT" || true
    fail_step resume-consume
fi
if ! kill -0 "$CONSUME_B_PID" 2>/dev/null; then
    cat "$CONSUME_B_OUT" || true
    fail_step resume-consume
fi

say "step=produce-after-recovery"
run_produce "charlie" produce-after-recovery
if ! wait_contains "$CONSUME_A_OUT" "demo charlie" 15; then
    cat "$CONSUME_A_OUT" || true
    fail_step produce-after-recovery
fi
if ! wait_contains "$CONSUME_B_OUT" "demo charlie" 15; then
    cat "$CONSUME_B_OUT" || true
    fail_step produce-after-recovery
fi
cat "$CONSUME_A_OUT"
cat "$CONSUME_B_OUT"

say "step=stop-all"
cleanup

say "demo complete"
exit 0
