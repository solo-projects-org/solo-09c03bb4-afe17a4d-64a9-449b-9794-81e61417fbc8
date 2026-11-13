"""Reference annotation data and the local annotation-source service.

The merge job aligns ENSG identifiers against one fixed human annotation
release.  This module holds that fixed release in a small lookup table plus a
local HTTP stand-in for the upstream annotation service, so the pipeline can
be demonstrated without external network access.

Upstream contract::

    GET /annotations/{gene_id} -> 200 {"gene_id", "symbol", "chromosome"}
                              -> 404 when the identifier is unknown

Callers send the version-suffix-stripped identifier; the original identifier
is preserved by the merge service.
"""

from __future__ import annotations

import asyncio
import csv
import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response

log = logging.getLogger("annotation_source")

# Excerpt of the fixed GRCh38 / Ensembl 110 human annotation release used by
# this demo.  Includes multi-mapped symbols, mitochondrial genes, an unplaced
# contig and one placeholder entry.
REFERENCE_ROWS: list[tuple[str, str, str]] = [
    ("ENSG00000141510", "TP53", "17"),
    ("ENSG00000012048", "BRCA1", "17"),
    ("ENSG00000139618", "BRCA2", "13"),
    ("ENSG00000157764", "BRAF", "7"),
    ("ENSG00000133703", "KRAS", "12"),
    ("ENSG00000121879", "PIK3CA", "3"),
    ("ENSG00000147889", "CDKN2A", "9"),
    ("ENSG00000091831", "ESR1", "6"),
    ("ENSG00000065361", "ERBB3", "12"),
    ("ENSG00000141736", "ERBB2", "17"),
    ("ENSG00000146648", "EGFR", "7"),
    ("ENSG00000138413", "IDH1", "2"),
    ("ENSG00000116044", "NFE2L2", "2"),
    ("ENSG00000107147", "ACVR1", "2"),
    ("ENSG00000171862", "PTEN", "10"),
    ("ENSG00000181143", "MUC16", "19"),
    ("ENSG00000105383", "CD33", "19"),
    ("ENSG00000244734", "HBB", "11"),
    ("ENSG00000237683", "ALB", "4"),
    ("ENSG00000198786", "MT-CO1", "MT"),
    ("ENSG00000198804", "MT-CO2", "MT"),
    ("ENSG00000210194", "MT-CYB", "MT"),
    ("ENSG00000099956", "GGT1", "22"),
    ("ENSG00000204628", "GNA12", "7"),
    ("ENSG00000168542", "COL3A1", "2"),
    ("ENSG00000196839", "ADA", "20"),
    ("ENSG00000090686", "ULK1", "12"),
    ("ENSG00000104133", "SPG11", "15"),
    ("ENSG00000108821", "COL1A1", "17"),
    ("ENSG00000139219", "COL2A1", "12"),
    ("ENSG00000241860", "MIR1302-2HG", "1"),
    ("ENSG00000238009", "RP11-34P13.7", "1"),
    ("ENSG00000279457", "WASH9P", "1"),
    ("ENSG00000233750", "CICP27", "1"),
    ("ENSG00000288528", "", "Un"),
]


def reference_table() -> dict[str, dict[str, str]]:
    """Return ``{gene_id: {"gene_id", "symbol", "chromosome"}}``."""
    return {
        gene_id: {"gene_id": gene_id, "symbol": symbol, "chromosome": chrom}
        for gene_id, symbol, chrom in REFERENCE_ROWS
    }


def load_reference(data_dir: str, filename: str) -> dict[str, dict[str, str]]:
    """Load a TSV reference (``gene_id``, ``symbol``, ``chromosome``) if present.

    Falls back to the embedded excerpt so the demo source always starts from
    the same fixed annotation release.
    """
    path = Path(data_dir) / filename
    if not path.is_file():
        log.info("reference file %s not found; using embedded excerpt", path)
        return reference_table()

    table: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            gene_id = (row.get("gene_id") or "").strip()
            if not gene_id:
                continue
            table[gene_id] = {
                "gene_id": gene_id,
                "symbol": (row.get("symbol") or "").strip(),
                "chromosome": (row.get("chromosome") or "").strip(),
            }
    log.info("loaded %d reference genes from %s", len(table), path)
    return table


def build_router(delay_seconds: float = 0.0) -> APIRouter:
    """Build the annotation-source router.

    ``delay_seconds`` emulates the slow upstream: the server sleeps before
    answering, making it visible when several batches share one in-flight
    request or when a lookup is aborted for lack of interested batches.
    """
    router = APIRouter()

    @router.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/stats")
    async def stats(request: Request) -> dict[str, Any]:
        """Request counters, so shared vs. duplicated lookups are visible."""
        return {
            "requests_total": request.app.state.requests_total,
            "per_gene": dict(request.app.state.requests_per_gene),
            "delay_seconds": delay_seconds,
        }

    @router.get("/annotations/{gene_id}")
    async def get_annotation(
        gene_id: str, request: Request, response: Response
    ) -> dict[str, str]:
        table = request.app.state.reference
        # Counted on arrival, before the artificial delay: the demo shows how
        # many HTTP requests the merge service actually issued per gene.
        request.app.state.requests_total += 1
        counts = request.app.state.requests_per_gene
        counts[gene_id] = counts.get(gene_id, 0) + 1
        if delay_seconds:
            await asyncio.sleep(delay_seconds)
        row = table.get(gene_id)
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"unknown gene_id after version-suffix normalisation: {gene_id}",
            )
        response.headers["Cache-Control"] = "no-store"
        return row

    @router.post("/stats/reset")
    async def reset_stats(request: Request) -> dict[str, str]:
        request.app.state.requests_total = 0
        request.app.state.requests_per_gene.clear()
        return {"status": "reset"}

    @router.get("/")
    async def index() -> dict[str, Any]:
        return {
            "service": "local human gene annotation source",
            "release": "GRCh38 / Ensembl 110 (fixed demo excerpt)",
            "endpoints": ["/annotations/{gene_id}", "/healthz", "/stats"],
            "note": "callers send identifiers without .<version> suffixes",
        }

    return router
