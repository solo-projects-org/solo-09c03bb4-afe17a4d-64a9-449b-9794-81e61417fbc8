"""CSV ingestion and emission.

Input is a CSV with a ``gene_id`` column (the delivery contract is a single
such column).  Output preserves every uploaded row in its original order,
duplicates included, with the original identifier untouched, and appends
``symbol`` and ``chromosome``.  Only the lookup key drops a trailing
``.<digits>`` version suffix.
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field

GENE_ID_COLUMN = "gene_id"
SYMBOL_COLUMN = "symbol"
CHROMOSOME_COLUMN = "chromosome"
UNMATCHED = "unmatched"

_VERSION_SUFFIX = re.compile(r"\.\d+$")


class CsvValidationError(ValueError):
    """The uploaded file is not a usable gene_id CSV."""


def normalise_gene_id(raw: str) -> str:
    """Drop a trailing ``.<version>`` but otherwise preserve the identifier."""
    return _VERSION_SUFFIX.sub("", raw.strip())


@dataclass(slots=True)
class ParsedBatch:
    header: list[str]
    records: list[list[str]]
    gene_index: int
    lookup_ids: list[str] = field(default_factory=list)

    @property
    def row_count(self) -> int:
        return len(self.records)

    @property
    def distinct_lookup_ids(self) -> list[str]:
        seen: dict[str, None] = {}
        for gene in self.lookup_ids:
            seen.setdefault(gene, None)
        return list(seen)


def parse_gene_csv(data: bytes) -> ParsedBatch:
    """Parse and validate the upload, preserving order and duplicates."""
    if not data.strip():
        raise CsvValidationError("uploaded CSV is empty")

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError("CSV must be UTF-8 encoded") from exc

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = next(reader)
    except StopIteration as exc:  # pragma: no cover - guarded above
        raise CsvValidationError("uploaded CSV is empty") from exc

    header = [cell.strip() for cell in header]
    if GENE_ID_COLUMN not in header:
        raise CsvValidationError(
            f"CSV header must contain a '{GENE_ID_COLUMN}' column, got {header!r}"
        )
    if len(header) != 1:
        raise CsvValidationError(
            "CSV must contain exactly one column, 'gene_id', "
            f"got {header!r}"
        )
    gene_index = header.index(GENE_ID_COLUMN)

    records: list[list[str]] = []
    lookup_ids: list[str] = []
    for line_no, record in enumerate(reader, start=2):
        if not record or all(cell.strip() == "" for cell in record):
            raise CsvValidationError(
                f"row {line_no} is blank; every input row must carry a {GENE_ID_COLUMN}"
            )
        if len(record) != len(header):
            raise CsvValidationError(
                f"row {line_no} has {len(record)} field(s), header has {len(header)}"
            )
        raw = record[gene_index]
        if not raw.strip():
            raise CsvValidationError(f"row {line_no} has an empty {GENE_ID_COLUMN}")
        records.append(record)
        lookup_ids.append(normalise_gene_id(raw))

    if not records:
        raise CsvValidationError("CSV contains a header but no gene rows")

    return ParsedBatch(
        header=header,
        records=records,
        gene_index=gene_index,
        lookup_ids=lookup_ids,
    )


def render_output_csv(
    parsed: ParsedBatch,
    annotations: dict[str, dict[str, str] | None],
) -> bytes:
    """Render the merged CSV.

    Column order is the uploaded header plus ``symbol`` and ``chromosome``
    (replacing same-named uploaded columns in place if any were present).
    Rows stay in upload order; duplicates stay duplicated.  ``unmatched``
    marks identifiers the annotation source answered 404 for.
    """
    out_header = [
        name
        for name in parsed.header
        if name not in (SYMBOL_COLUMN, CHROMOSOME_COLUMN)
    ]
    out_header.extend((SYMBOL_COLUMN, CHROMOSOME_COLUMN))
    dropped = [
        index
        for index, name in enumerate(parsed.header)
        if name in (SYMBOL_COLUMN, CHROMOSOME_COLUMN)
    ]

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(out_header)
    for record, gene in zip(parsed.records, parsed.lookup_ids):
        out_record = [
            cell for index, cell in enumerate(record) if index not in dropped
        ]
        hit = annotations.get(gene)
        if hit is None:
            out_record.extend((UNMATCHED, UNMATCHED))
        else:
            out_record.extend(
                (hit.get("symbol") or UNMATCHED, hit.get("chromosome") or UNMATCHED)
            )
        writer.writerow(out_record)

    return buffer.getvalue().encode("utf-8")
