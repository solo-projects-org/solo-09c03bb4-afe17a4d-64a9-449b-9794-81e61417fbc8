# Human Gene Annotation Merge & Count Matrix API

FastAPI service that annotates sequencing-software expression exports with a
fixed human annotation release (GRCh38 / Ensembl 110), and assembles delivered
per-sample counts into one aligned count matrix for differential expression.

Single instance; all state lives in one process plus a data volume.

## Annotation batches

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
| State | in memory only (annotation runs) |

## Samples and the count matrix

- `PUT /samples/{sample_id}` — upload or replace one sample: a CSV with
  exactly the columns `gene_id,count`. Returns `201` on create, `200` on
  replace, with the stored sample metadata and the current matrix status.
- `GET /samples` — list stored samples.
- `GET /samples/{sample_id}` — metadata (`gene_count`, `content_hash`, …).
- `GET /samples/{sample_id}/download` — the stored canonical CSV.
- `GET /matrix` — matrix status: state, built_at, build id, dimensions,
  source samples, pending samples, error.
- `GET /matrix/download` — the last complete count matrix as CSV.

Sample rules:

| Rule | Detail |
| --- | --- |
| Columns | exactly `gene_id,count` (header checked, order fixed) |
| Identifier | trailing `.<digits>` stripped; stored normalised |
| Duplicates | two rows colliding after stripping reject the whole file (422) |
| Counts | decimal non-negative integers only (`0` allowed, `-1`/`1.5`/empty rejected) |
| Sample id | `[A-Za-z0-9][A-Za-z0-9._-]*`, at most 128 chars, usable as a file name |
| Replacement | the new file replaces the sample as a whole; genes not in it disappear |
| Failed validation/save | the previously stored sample stays untouched and served |

Matrix rules:

| Rule | Detail |
| --- | --- |
| Layout | genes as rows, samples as columns, both sorted by identifier |
| Missing gene | empty cell — never `0`; a missing value is not a zero count |
| Header | `gene_id` followed by the sorted sample ids |
| Rebuilds | background, debounced; triggered by every effective sample save |
| Availability | uploads and the previous complete matrix download keep working during a rebuild |
| Publication | a build is published only if it still matches every current sample; otherwise it is discarded and rebuilt |
| Staleness | `GET /matrix` reports `building`/`stale` (with `pending_samples`) while newer samples are not included |
| Failure | build failures report `failed` with the error; the last complete matrix stays downloadable |
| Atomicity | the matrix is written and fsynced as a temp file, then renamed into place; a crash never loses the old matrix |
| Restart | samples, the published matrix and the manifest are reloaded; an interrupted rebuild is resumed automatically |
| Storage | `<DATA_DIR>/samples/<id>.csv`, `<DATA_DIR>/matrix/matrix.csv` + `matrix.json` |

Matrix status looks like:

```json
{"state": "current", "status": "up to date", "stale": false,
 "download_available": true, "built_at": "2026-11-13T17:18:32+00:00",
 "build_id": "build-…", "duration_seconds": 0.0005, "columns": 2, "rows": 3,
 "source_samples": ["alpha", "zeta"], "pending_samples": [], "error": null}
```

`state` is one of `none` (nothing delivered yet), `building`, `current`,
`stale`, `failed`. A download is refused with `409` only when no complete
matrix has ever been published; the body carries the status document.

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

The sample/matrix demo uploads `examples/sample_s*.csv`, shows the matrix, then
corrects `s2` with an out-of-date and an invalid file to show what is kept:

```bash
./scripts/matrix_demo.sh
```

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
| `examples/sample_s1.csv`, `sample_s2.csv`, `sample_s3.csv` | three `gene_id,count` deliveries; `s3` adds genes only it has (empty cells for the others) |
| `examples/sample_s2_corrected.csv` | whole-sample correction of `s2` |

Quick manual run:

```bash
ID=$(curl -s -X POST localhost:8000/batches -F file=@examples/batch_a.csv \
     | python3 -c 'import json,sys; print(json.load(sys.stdin)["batch_id"])')
curl -s localhost:8000/batches/$ID
curl -s -X POST localhost:8000/batches/$ID/cancel
curl -s localhost:8000/batches/$ID/download
```

Samples and matrix:

```bash
curl -s -X PUT localhost:8000/samples/s1 -F file=@examples/sample_s1.csv
curl -s -X PUT localhost:8000/samples/s2 -F file=@examples/sample_s2.csv
curl -s localhost:8000/matrix
curl -s localhost:8000/matrix/download
curl -s -X PUT localhost:8000/samples/s2 -F file=@examples/sample_s2_corrected.csv
```

The same uploads are available in Swagger UI (`PUT /samples/{sample_id}` —
use *Try it out* and pick a file); downloads are plain links.

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

For samples and the matrix it covers validation (including post-strip
duplicates), whole-sample replacement with rollback on rejected corrections,
sorted/empty-cell layout, stale status while rebuilding, discarding a build
that raced a correction, failed builds/publication keeping the old matrix,
uploads during rebuilds, restart reload and resuming an interrupted rebuild
(including a corrupt manifest).

## Configuration

| Env | Default | Meaning |
| --- | --- | --- |
| `ANNOTATION_BASE_URL` | `http://annotation-source:8001` | upstream source |
| `ANNOTATION_TIMEOUT_SECONDS` | `10` | per-request timeout |
| `PER_RUN_CONCURRENCY` | `8` | parallel lookups per batch |
| `MAX_RETAINED_RUNS` | `1000` | finished batches kept in memory |
| `MAX_UPLOAD_BYTES` / `MAX_ROWS` | 32 MiB / 100000 | upload limits (batch rows and sample genes) |
| `ANNOTATION_SOURCE_DELAY_SECONDS` | `1.0` (compose) | source latency for demos |
| `DATA_DIR` | `/data` in compose | sample + matrix volume; empty uses a temp dir |
| `MATRIX_REBUILD_DEBOUNCE_SECONDS` | `0.05` | coalesce bursts of corrections into one rebuild |

Annotation batch/lookup state is process-local: run the API with one worker
(the included Docker image does). Finished batches are bounded by
`MAX_RETAINED_RUNS`; the oldest finished batches are dropped first. Samples
and the matrix are on the `merge-data` volume (`docker compose down` keeps it;
`docker compose down -v` removes it).
