#!/usr/bin/env bash
# End-to-end demo of samples and the rebuilt count matrix.
#
#   docker compose up --build -d
#   ./scripts/matrix_demo.sh
#
# Override the endpoint with BASE_URL.  Needs curl + python3.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
EXAMPLES="$(cd "$(dirname "$0")/../examples" && pwd)"

py() { python3 -c "$@"; }
field() { py "import json,sys; d=json.load(sys.stdin); print(d$1)"; }

upload() { # upload <sample_id> <csv> -> HTTP status + summary
  curl -fsS -X PUT "$BASE_URL/samples/$1" -F "file=@$2" \
    | py "import json,sys; d=json.load(sys.stdin); s=d['sample']; m=d['matrix']; print(f\"HTTP {d['created'] and 201 or 200} sample={s['sample_id']} genes={s['gene_count']} matrix={m['state']}\")"
}

wait_current() { # wait_current [timeout-tenths]
  local state
  for _ in $(seq 1 "${1:-100}"); do
    state="$(curl -fsS "$BASE_URL/matrix" | field "['state']")"
    case "$state" in
      current) echo "$state"; return 0 ;;
      building|stale) sleep 0.1 ;;
      *) echo "$state"; return 0 ;;
    esac
  done
  echo "timeout"
}

show() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

show "health"
curl -fsS "$BASE_URL/healthz"; echo

show "deliver three samples (s3 contains genes the others do not have)"
upload s1 "$EXAMPLES/sample_s1.csv"
upload s2 "$EXAMPLES/sample_s2.csv"
upload s3 "$EXAMPLES/sample_s3.csv"
echo "matrix after rebuilds: $(wait_current)"
curl -fsS "$BASE_URL/matrix"; echo

show "matrix (genes as rows, samples as columns, missing cells empty)"
curl -fsS "$BASE_URL/matrix/download"

show "whole-sample correction of s2: the new file replaces the old one"
upload s2 "$EXAMPLES/sample_s2_corrected.csv"
echo "matrix after rebuilds: $(wait_current)"
curl -fsS "$BASE_URL/matrix/download"

show "an invalid correction is rejected; the stored s2 stays untouched"
printf 'gene_id,count\nENSG00000141510,-1\n' > /tmp/bad_sample.csv
curl -sS -o /dev/null -w 'negative count -> HTTP %{http_code}\n' \
  -X PUT "$BASE_URL/samples/s2" -F "file=@/tmp/bad_sample.csv"
curl -fsS "$BASE_URL/samples/s2"; echo

show "duplicate after version-suffix stripping rejects the whole file"
printf 'gene_id,count\nENSG00000141510.18,5\nENSG00000141510.6,7\n' > /tmp/dup_sample.csv
curl -sS -o /dev/null -w 'duplicate gene -> HTTP %{http_code}\n' \
  -X PUT "$BASE_URL/samples/s2" -F "file=@/tmp/dup_sample.csv"
curl -fsS "$BASE_URL/samples/s2/download"

show "matrix status endpoint"
curl -fsS "$BASE_URL/matrix"; echo

show "stored samples"
curl -fsS "$BASE_URL/samples"; echo
