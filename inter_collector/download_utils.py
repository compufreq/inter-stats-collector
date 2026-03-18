"""Shared download and extraction utilities used by all data sources."""

from __future__ import annotations

import gzip
import logging
from pathlib import Path
from typing import Callable

import httpx

log = logging.getLogger(__name__)

# Callback type: called with (bytes_just_received, file_total_or_none)
BytesCallback = Callable[[int, int | None], None]


# ---------------------------------------------------------------------------
# Central format registry: maps format labels (used in state tracking and
# DownloadResult.successes) to file suffixes written to disk.
#
# Both downloaders and the --verify logic reference this single source of
# truth so they always agree on what "data file on disk" means.
# ---------------------------------------------------------------------------

FORMAT_SUFFIXES: dict[str, list[str]] = {
    # Tabular data
    "csv": [".csv"],
    "xls": [".xls"],
    "xlsx": [".xlsx"],
    "ods": [".ods"],
    "json": [".json"],
    "tsv": [".tsv", ".tsv.gz"],
    "px": [".px"],
    # Structured / semantic
    "sdmx": [".sdmx.xml", ".sdmx.xml.gz"],
    "csvw": [".csv-metadata.json"],
    # UNHCR paginated data
    "data": ["_data.json"],
    # Metadata (not counted as "data")
    "info": ["_info.json"],
    "meta": ["_meta.json"],
    "dsd": ["_dsd.xml"],
    "dataflow": ["_dataflow.xml"],
    "constraint": ["_constraint.xml"],
    "metadata": ["_metadata.html"],
}


def get_data_suffixes(data_file_types: set[str]) -> set[str]:
    """Build a file-suffix set from format labels.

    E.g., {"csv", "xls"} → {".csv", ".xls"}
    Unknown labels get ".{label}" as a fallback.
    """
    suffixes: set[str] = set()
    for ft in data_file_types:
        if ft in FORMAT_SUFFIXES:
            suffixes.update(FORMAT_SUFFIXES[ft])
        else:
            suffixes.add(f".{ft}")
    return suffixes


class DownloadResult:
    """Result of downloading all files for a single dataset."""

    __slots__ = ("dataset_code", "successes", "failures", "bytes_downloaded")

    def __init__(self, dataset_code: str):
        self.dataset_code = dataset_code
        self.successes: list[str] = []
        self.failures: list[tuple[str, str]] = []  # (file_type, error)
        self.bytes_downloaded: int = 0

    @property
    def ok(self) -> bool:
        return len(self.failures) == 0


# Default I/O buffer for gzip extraction (64 MB).
# Large buffers reduce network round-trips on SMB/NFS drives.
DEFAULT_EXTRACT_BUFFER_MB = 64


def _extract_gz(gz_path: Path, buffer_mb: int = DEFAULT_EXTRACT_BUFFER_MB) -> tuple[Path, int, int]:
    """Extract a .gz file alongside the original.

    Returns ``(extracted_path, compressed_bytes, decompressed_bytes)``
    so the caller doesn't need extra ``stat()`` round-trips (costly on
    network volumes).

    *buffer_mb* controls the read/write chunk size in megabytes.  Larger
    values mean fewer syscalls (faster on network drives) but more RAM per
    worker.

    e.g.  foo.tsv.gz  -> foo.tsv
          foo.sdmx.xml.gz -> foo.sdmx.xml

    The compressed file is kept so resumability still works (skip_existing
    checks the .gz name).  If the extracted file already exists it is
    overwritten to stay in sync with the archive.
    """
    name = gz_path.name
    if name.endswith(".gz"):
        extracted_name = name[:-3]  # strip ".gz"
    else:
        extracted_name = name + ".extracted"

    extracted_path = gz_path.parent / extracted_name

    # Get compressed size before extraction
    compressed_bytes = gz_path.stat().st_size

    _BUF = buffer_mb * 1_048_576  # convert MB → bytes
    decompressed_bytes = 0
    with gzip.open(gz_path, "rb") as f_in, open(extracted_path, "wb") as f_out:
        while True:
            chunk = f_in.read(_BUF)
            if not chunk:
                break
            f_out.write(chunk)
            decompressed_bytes += len(chunk)

    return extracted_path, compressed_bytes, decompressed_bytes


async def _download_file(
    client: httpx.AsyncClient,
    url: str,
    dest: Path,
    *,
    timeout: float = 300,
    decompress_gz: bool = False,
    params: dict | None = None,
    on_bytes: BytesCallback | None = None,
) -> int:
    """Download a URL to a local file. Returns total bytes written.

    Calls on_bytes(chunk_size, content_length_or_none) for every chunk received
    so progress can be tracked in real time.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    total_written = 0

    async with client.stream("GET", url, params=params, timeout=timeout) as resp:
        resp.raise_for_status()

        content_length: int | None = None
        cl_header = resp.headers.get("content-length")
        if cl_header:
            content_length = int(cl_header)

        if decompress_gz:
            raw = b""
            async for chunk in resp.aiter_bytes(chunk_size=65536):
                raw += chunk
                if on_bytes:
                    on_bytes(len(chunk), content_length)
            try:
                data = gzip.decompress(raw)
            except gzip.BadGzipFile:
                data = raw
            dest.write_bytes(data)
            total_written = len(data)
        else:
            with open(dest, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    f.write(chunk)
                    total_written += len(chunk)
                    if on_bytes:
                        on_bytes(len(chunk), content_length)

    return total_written
