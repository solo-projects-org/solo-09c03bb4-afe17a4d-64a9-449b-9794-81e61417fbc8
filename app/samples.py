"""Durable per-sample count files.

A *sample* is one delivery (or later correction) of a count CSV with the
columns ``gene_id,count``.  Gene identifiers are normalised by dropping a
trailing ``.<digits>`` version suffix; two rows that collide after that
normalisation reject the whole upload.  Counts are non-negative integers.

Layout on disk::

    <data_dir>/samples/<sample_id>.csv     canonical gene_id,count CSV
    <data_dir>/matrix/matrix.csv           published count matrix
    <data_dir>/matrix/*.tmp                in-progress builds (never served)

Every sample is written to a temporary file in the same directory, flushed and
then moved into place with :func:`os.replace`.  A failed validation or write
therefore leaves the previously stored sample untouched.  Because a sample is
a single file, its content and its registration cannot disagree after a crash.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
import re
import threading
import uuid
from contextlib import contextmanager
from typing import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .csvio import normalise_gene_id

log = logging.getLogger("samples")

GENE_ID_COLUMN = "gene_id"
COUNT_COLUMN = "count"
REQUIRED_HEADER = (GENE_ID_COLUMN, COUNT_COLUMN)

#: Conservative sample-name rule: safe as a file name, no path traversal.
SAMPLE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_MAX_SAMPLE_ID_LENGTH = 128

_COUNT_PATTERN = re.compile(r"[0-9]+")


class SampleError(Exception):
    """Base class for sample problems the API translates to HTTP errors."""


class InvalidSampleId(SampleError):
    pass


class SampleValidationError(SampleError):
    """The uploaded CSV is not a valid gene_id,count file."""


class SampleSaveError(SampleError):
    """The sample could not be durably written; the old one is intact."""


class SampleNotFound(SampleError, KeyError):
    pass


class SampleConflict(SampleError):
    """The on-disk sample changed in a way that prevents a safe update."""


@dataclass(frozen=True, slots=True)
class ParsedSample:
    #: Normalised gene identifiers and counts, in upload order.
    columns: dict[str, int]
    #: Canonical serialisation, used for hashing and for storage.
    canonical: bytes

    @property
    def gene_count(self) -> int:
        return len(self.columns)


@dataclass(frozen=True, slots=True)
class SampleMeta:
    sample_id: str
    gene_count: int
    content_hash: str
    byte_size: int
    uploaded_at: str

    def snapshot(self) -> dict[str, object]:
        # Kept separate from the dataclass fields to make the JSON shape
        # explicit in one place (and used by both metadata endpoints).
        return {
            "sample_id": self.sample_id,
            "gene_count": self.gene_count,
            "content_hash": self.content_hash,
            "byte_size": self.byte_size,
            "uploaded_at": self.uploaded_at,
        }


def validate_sample_id(sample_id: str) -> str:
    if not sample_id:
        raise InvalidSampleId("sample_id must not be empty")
    if len(sample_id) > _MAX_SAMPLE_ID_LENGTH:
        raise InvalidSampleId(
            f"sample_id must be at most {_MAX_SAMPLE_ID_LENGTH} characters"
        )
    if not SAMPLE_ID_PATTERN.fullmatch(sample_id):
        raise InvalidSampleId(
            "sample_id must start with a letter or digit and contain only "
            "letters, digits, '.', '_' or '-'"
        )
    return sample_id


def parse_sample_csv(data: bytes) -> ParsedSample:
    """Validate and canonicalise an uploaded sample CSV.

    The whole file is rejected when any row is malformed or when two rows
    normalise to the same gene identifier.  Nothing is written by this
    function.
    """
    if not data.strip():
        raise SampleValidationError("uploaded CSV is empty")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SampleValidationError("CSV must be UTF-8 encoded") from exc

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = next(reader)
    except StopIteration as exc:  # pragma: no cover - guarded above
        raise SampleValidationError("uploaded CSV is empty") from exc

    header = [cell.strip() for cell in header]
    if tuple(header) != REQUIRED_HEADER:
        raise SampleValidationError(
            "CSV header must be exactly 'gene_id,count', got "
            f"{header!r}"
        )

    columns: dict[str, int] = {}
    rows: dict[str, int] = {}
    records: list[tuple[str, int]] = []
    for line_no, record in enumerate(reader, start=2):
        if not record or all(cell.strip() == "" for cell in record):
            raise SampleValidationError(
                f"row {line_no} is blank; every row needs a gene_id and a count"
            )
        if len(record) != 2:
            raise SampleValidationError(
                f"row {line_no} has {len(record)} field(s); expected 2 "
                "(gene_id,count)"
            )
        raw_gene, raw_count = record
        gene = normalise_gene_id(raw_gene)
        if not gene:
            raise SampleValidationError(f"row {line_no} has an empty gene_id")
        count_text = raw_count.strip()
        if not _COUNT_PATTERN.fullmatch(count_text):
            raise SampleValidationError(
                f"row {line_no}: count {raw_count!r} is not a non-negative "
                "integer"
            )
        first_seen = rows.get(gene)
        if first_seen is not None:
            raise SampleValidationError(
                f"row {line_no}: gene_id {gene!r} duplicates row "
                f"{first_seen} after version-suffix normalisation; "
                "the whole sample is rejected"
            )
        rows[gene] = line_no
        count = int(count_text)
        columns[gene] = count
        records.append((gene, count))

    if not records:
        raise SampleValidationError("CSV contains a header but no sample rows")

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(REQUIRED_HEADER)
    for gene, count in records:
        writer.writerow((gene, count))
    return ParsedSample(columns=columns, canonical=buffer.getvalue().encode("utf-8"))


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SampleStore:
    """Thread-safe registry of durable sample files.

    The store is the source of truth: every sample is one canonical CSV on
    disk.  In memory it keeps only metadata plus a content hash, so a change
    to a sample is detectable without re-reading it.
    """

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.samples_dir = data_dir / "samples"
        self._lock = threading.RLock()
        self._meta: dict[str, SampleMeta] = {}
        self._generation = 0

    # -- paths ---------------------------------------------------------

    def sample_path(self, sample_id: str) -> Path:
        return self.samples_dir / f"{sample_id}.csv"

    # -- startup -------------------------------------------------------

    def load(self) -> None:
        """Scan the data volume and register the samples already on disk."""
        self.samples_dir.mkdir(parents=True, exist_ok=True)
        # Anything still named *.tmp is an interrupted write. It was never
        # registered and must never be served.
        for leftover in self.samples_dir.glob("*.tmp"):
            leftover.unlink(missing_ok=True)

        found: dict[str, SampleMeta] = {}
        for path in sorted(self.samples_dir.glob("*.csv")):
            sample_id = path.name[: -len(".csv")]
            try:
                validate_sample_id(sample_id)
            except InvalidSampleId:
                log.warning("ignoring sample file with unsafe name: %s", path)
                continue
            try:
                # Same path used for serving, so the registered hash is the
                # hash of exactly the bytes downloads will return.
                parsed = parse_sample_csv(path.read_bytes())
            except (OSError, SampleValidationError):
                log.exception("ignoring unreadable/invalid sample %s", path)
                continue
            found[sample_id] = SampleMeta(
                sample_id=sample_id,
                gene_count=parsed.gene_count,
                content_hash=_hash_bytes(parsed.canonical),
                byte_size=len(parsed.canonical),
                uploaded_at=self._mtime(path),
            )

        with self._lock:
            self._meta = found
            self._bump_locked()
        log.info("loaded %d sample(s) from %s", len(found), self.samples_dir)

    def _mtime(self, path: Path) -> str:
        try:
            stamp = path.stat().st_mtime
        except OSError:
            return _now()
        return datetime.fromtimestamp(stamp, timezone.utc).isoformat(
            timespec="seconds"
        )

    # -- reads ---------------------------------------------------------

    def list_samples(self) -> list[SampleMeta]:
        with self._lock:
            return [self._meta[name] for name in sorted(self._meta)]

    def get(self, sample_id: str) -> SampleMeta:
        with self._lock:
            meta = self._meta.get(sample_id)
            if meta is None:
                raise SampleNotFound(sample_id)
            return meta

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Hold the store lock across a multi-step decision.

        Matrix publication uses this so that "is this build current?" and the
        rename that replaces the published file cannot interleave with an
        upload writing a new sample.
        """
        with self._lock:
            yield

    def read(self, sample_id: str) -> bytes:
        """Read the stored CSV; the open file survives a concurrent replace."""
        with self._lock:
            meta = self._meta.get(sample_id)
            if meta is None:
                raise SampleNotFound(sample_id)
            path = self.sample_path(sample_id)
        try:
            return path.read_bytes()
        except FileNotFoundError as exc:
            raise SampleNotFound(sample_id) from exc
        except OSError as exc:
            raise SampleSaveError(f"could not read sample {sample_id}: {exc}") from exc

    def read_verified(self, sample_id: str, expected_hash: str) -> bytes:
        """Read exactly the version named by ``expected_hash``."""
        raw = self.read(sample_id)
        if _hash_bytes(raw) != expected_hash:
            raise SampleConflict(
                f"sample {sample_id} changed while the matrix was being built"
            )
        return raw

    def source_state(self) -> tuple[tuple[tuple[str, str], ...], int]:
        """Snapshot ``((sample_id, content_hash), ...), generation``.

        Callers use it to tell whether a matrix build is still current.  The
        hash is computed from the canonical bytes, so a rewrite with identical
        content does not count as a change (and ``replace`` skips the rename
        in that case anyway).
        """
        with self._lock:
            return (
                tuple(
                    (name, meta.content_hash)
                    for name, meta in sorted(self._meta.items())
                ),
                self._generation,
            )

    # -- writes --------------------------------------------------------

    def replace(
        self, sample_id: str, parsed: ParsedSample
    ) -> tuple[SampleMeta, bool, bool]:
        """Durably store ``parsed``; return ``(meta, changed, created)``.

        ``created`` is decided under the store lock, so two concurrent uploads
        for a new sample cannot both report "created".  The file is written to
        a temporary sibling, fsynced, then renamed over the old version.  On
        any failure the previous sample remains the registered one and is
        still served unchanged.
        """
        content_hash = _hash_bytes(parsed.canonical)
        self.samples_dir.mkdir(parents=True, exist_ok=True)
        path = self.sample_path(sample_id)
        tmp_path = self.samples_dir / f"{sample_id}.{uuid.uuid4().hex}.tmp"
        try:
            with open(tmp_path, "xb") as handle:
                handle.write(parsed.canonical)
                handle.flush()
                os.fsync(handle.fileno())
            with self._lock:
                previous = self._meta.get(sample_id)
                created = previous is None
                if previous is not None and previous.content_hash == content_hash:
                    # Identical correction: nothing on disk changed, so no
                    # rebuild is needed either.
                    tmp_path.unlink(missing_ok=True)
                    return previous, False, False
                os.replace(tmp_path, path)
                meta = SampleMeta(
                    sample_id=sample_id,
                    gene_count=parsed.gene_count,
                    content_hash=content_hash,
                    byte_size=len(parsed.canonical),
                    uploaded_at=_now(),
                )
                self._meta[sample_id] = meta
                self._bump_locked()
            log.info(
                "sample %s stored (%d genes, %d bytes, %s)",
                sample_id,
                meta.gene_count,
                meta.byte_size,
                "created" if created else "updated",
            )
            return meta, True, created
        except OSError as exc:
            tmp_path.unlink(missing_ok=True)
            raise SampleSaveError(
                f"could not save sample {sample_id}: {exc}"
            ) from exc

    def _bump_locked(self) -> None:
        self._generation += 1
