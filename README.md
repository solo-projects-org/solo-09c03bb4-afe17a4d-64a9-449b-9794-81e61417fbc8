# Human Gene Annotation Merge API

FastAPI service that annotates sequencing-software expression exports with a
fixed human annotation release (GRCh38 / Ensembl 110) before they are merged.

## Contract

- `POST /batches` — upload a CSV with a single `gene_id` column. Returns
  `202 {batch_id, ...}` and processes in the background.
- `GET /batches/{batch_id}` — progress: state, rows processed/total, matched
  and unmatched gene counts, percentage.
- `POST /batches/{batch_id}/cancel` — cancel a running batch. Takes effect
  immediately.
- `GET /batches/{batch_id}/download` — the annotated CSV, only for completed
  batches.

Behaviour:

| Rule | Detail |
| --- | --- |
| Lookup key | trailing `.<digits>` stripped: `ENSG00000141510.18` → `ENSG00000141510` |
| Output identifier | original string, untouched (`.18` stays) |
| Row order / duplicates | preserved exactly; only `symbol` + `chromosome` are appended |
| Source 404 | that gene becomes `unmatched/unmatched` |
| Other source errors | whole batch `failed`, download refused (409) |
| Sharing | one HTTP request per gene while in flight, shared by all batches |
| Cancellation | immediate; other batches waiting on the same query still complete |
| Last waiter cancelled | the upstream request is aborted |
| Later batch for that gene | issues a fresh request |
| Late/aborted results | discarded; can't touch the newer query or revive a cancelled batch |
| Caching | none; finished lookups are not reused |
| State | in memory only |

## Run it

```bash
docker compose up --build
```

- Merge API + Swagger UI: <http://localhost:8000/docs>
- Local annotation source: <http://localhost:8001/docs> (`/stats` shows per-gene
  request counters; a per-request delay of 1s emulates the slow upstream)

Then run the scripted demo (needs `curl` and `python3`):

```bash
./scripts/demo.sh
```

The demo uploads the manifests in `examples/`, shows the shared in-flight TP53
lookup, cancels one of two batches while the other completes, cancels a batch
that is the sole waiter for a gene, then immediately re-queries those genes in
a fresh batch.

### Local (no Docker)

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
ANNOTATION_SOURCE_DELAY_SECONDS=1 .venv/bin/uvicorn annotation_source.main:app --port 8001 &
ANNOTATION_BASE_URL=http://localhost:8001 .venv/bin/uvicorn app.main:app --port 8000
```

## Example manifests

| File | Purpose |
| --- | --- |
| `examples/batch_a.csv` | version suffixes, duplicate identifier with different versions |
| `examples/batch_b.csv` | unknown identifier (`ENSG00000000007` → `unmatched`) |
| `examples/batch_c.csv` | mixed known/unknown |
| `examples/batch_large.csv`, `examples/batch_large_b.csv` | overlapping genes for the shared-lookup demo |

Quick manual run:

```bash
ID=$(curl -s -X POST localhost:8000/batches -F file=@examples/batch_a.csv \
     | python3 -c 'import json,sys; print(json.load(sys.stdin)["batch_id"])')
curl -s localhost:8000/batches/$ID
curl -s -X POST localhost:8000/batches/$ID/cancel
curl -s localhost:8000/batches/$ID/download
```

## Tests

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

The suite runs the real FastAPI apps in-process (mock transport): it covers
ordering/duplicate preservation, suffix stripping, 404 → `unmatched`,
non-404 → whole-batch failure, shared in-flight lookups, cancel-one-of-two,
abort-on-last-waiter, immediate re-query, late-result isolation and the
no-cache guarantee.

## Configuration

| Env | Default | Meaning |
| --- | --- | --- |
| `ANNOTATION_BASE_URL` | `http://annotation-source:8001` | upstream source |
| `ANNOTATION_TIMEOUT_SECONDS` | `10` | per-request timeout |
| `PER_RUN_CONCURRENCY` | `8` | parallel lookups per batch |
| `MAX_RETAINED_RUNS` | `1000` | finished batches kept in memory |
| `MAX_UPLOAD_BYTES` / `MAX_ROWS` | 32 MiB / 100000 | upload limits |
| `ANNOTATION_SOURCE_DELAY_SECONDS` | `1.0` (compose) | source latency for demos |

All batch/lookup state is process-local: run the API with one worker (the
included Docker image does). Finished batches are bounded by
`MAX_RETAINED_RUNS`; the oldest finished batches are dropped first.
