#!/usr/bin/env bash
# End-to-end demo of the merge service against the local (slow) annotation source.
#
#   docker compose up --build -d
#   ./scripts/demo.sh
#
# Override endpoints with BASE_URL / SOURCE_URL.  Needs curl + python3.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
SOURCE_URL="${SOURCE_URL:-http://localhost:8001}"
EXAMPLES="$(cd "$(dirname "$0")/../examples" && pwd)"

py() { python3 -c "$@"; }
field() { py "import json,sys; print(json.load(sys.stdin)['$1'])"; }

upload() { # upload <csv file> -> batch_id
  curl -fsS -X POST "$BASE_URL/batches" -F "file=@$1" \
    | py "import json,sys; print(json.load(sys.stdin)['batch_id'])"
}

wait_terminal() { # wait_terminal <batch_id> -> final state
  local id="$1" state
  for _ in $(seq 1 400); do
    state="$(curl -fsS "$BASE_URL/batches/$id" | field state)"
    if [ "$state" != "running" ]; then echo "$state"; return 0; fi
    sleep 0.1
  done
  echo "timeout"
}

show() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

show "health"
curl -fsS "$BASE_URL/healthz"; echo
curl -fsS "$SOURCE_URL/healthz"; echo

show "one batch: version suffix stripped for lookup, original id kept, duplicates kept"
A="$(upload "$EXAMPLES/batch_a.csv")"
echo "batch A = $A -> $(wait_terminal "$A")"
curl -fsS "$BASE_URL/batches/$A/download"

show "two batches share the in-flight TP53 lookup; cancel one, the other completes"
B="$(upload "$EXAMPLES/batch_large.csv")"
C="$(upload "$EXAMPLES/batch_large_b.csv")"
echo "batch B = $B (to cancel), batch C = $C (must complete)"
sleep 0.5   # both batches now wait on the same slow lookup
curl -fsS -X POST "$BASE_URL/batches/$B/cancel" \
  | py "import json,sys; print('cancel requested ->', json.load(sys.stdin)['state'])"
echo "B final: $(wait_terminal "$B")"
echo "C final: $(wait_terminal "$C")"
echo "-- C output head:"
curl -fsS "$BASE_URL/batches/$C/download" > "$TMP/C.csv"
sed -n '1,4p' "$TMP/C.csv"
echo "-- B download (must be refused, the batch was cancelled):"
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' "$BASE_URL/batches/$B/download"

show "cancel the only batch waiting for a gene, then immediately query it again"
D="$(upload "$EXAMPLES/batch_b.csv")"
echo "batch D = $D"
sleep 0.4
curl -fsS -X POST "$BASE_URL/batches/$D/cancel" > /dev/null
echo "D final: $(wait_terminal "$D")"
E="$(upload "$EXAMPLES/batch_b.csv")"
echo "batch E (same manifest, fresh upstream requests) = $E"
echo "E final: $(wait_terminal "$E")"
echo "-- E output (ENSG00000000007 is a 404 -> unmatched):"
curl -fsS "$BASE_URL/batches/$E/download"

show "finished"
echo "Swagger UI: $BASE_URL/docs"
