"""Generic CKAN dataset downloader — works with any CKAN-based portal.

Downloads data resources (CSV, XLS, etc.) using GET requests.
Handles duplicate format deduplication via configurable filename strategies.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import unicodedata
from collections import Counter
from pathlib import Path

import httpx

from .catalog import FOLDER_STYLE_DISPLAY, CkanEntry
from .config import CkanPortalConfig
from ...download_utils import BytesCallback, DownloadResult, _download_file

log = logging.getLogger(__name__)

# Retry configuration for individual file downloads
_DOWNLOAD_MAX_RETRIES = 4          # total attempts per file
_DOWNLOAD_BACKOFF_BASE = 10.0      # seconds — progression: 10s, 20s, 40s, 80s
_INTER_FILE_DELAY = 0.5            # seconds between resource downloads within one dataset


async def _download_file_with_retry(
    client: httpx.AsyncClient,
    url: str,
    dest: Path,
    *,
    label: str = "",
    on_bytes: BytesCallback | None = None,
) -> int:
    """Download a file with automatic retry on transient errors.

    Retries on HTTP 429 (rate limit) and 5xx (server error) with
    exponential backoff.  Also retries on connection timeouts and
    network errors.  On exhausted retries, raises HTTPStatusError
    with a synthetic 429 response so callers can record the failure.
    """
    for attempt in range(1, _DOWNLOAD_MAX_RETRIES + 1):
        try:
            n = await _download_file(client, url, dest, on_bytes=on_bytes)
            return n
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429 or e.response.status_code >= 500:
                wait = _DOWNLOAD_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning("HTTP %d downloading %s — retry %d/%d in %.0fs",
                            e.response.status_code, label or url,
                            attempt, _DOWNLOAD_MAX_RETRIES, wait)
                await asyncio.sleep(wait)
                continue
            raise
        except (httpx.TimeoutException, httpx.ConnectError):
            if attempt < _DOWNLOAD_MAX_RETRIES:
                wait = _DOWNLOAD_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning("Connection error on %s — retry %d/%d in %.0fs",
                            label or url, attempt, _DOWNLOAD_MAX_RETRIES, wait)
                await asyncio.sleep(wait)
                continue
            raise
    # Python for...else: all retry attempts exhausted on 429/5xx
    raise httpx.HTTPStatusError(
        f"Exhausted {_DOWNLOAD_MAX_RETRIES} retries for {label or url}",
        request=httpx.Request("GET", url),
        response=httpx.Response(429),
    )


def _safe_filename(name: str, ext: str) -> str:
    """Convert a resource name to a filesystem-safe filename.

    Normalises Unicode to ASCII, lowercases, strips non-alphanumeric
    characters, and ensures the correct extension is appended.
    Truncates to 120 characters to stay within filesystem name limits.
    """
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9._-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s.endswith(f".{ext}"):
        s = f"{s}.{ext}"
    if len(s) > 120:
        s = s[:120]
    return s or f"resource.{ext}"


def _build_filenames(
    code: str,
    resources: list[dict],
    strategy: str = "index",
) -> list[tuple[dict, str, str]]:
    """Determine destination filename for each resource.

    Returns list of (resource_dict, filename, success_label).

    Strategies:
      "index":          {code}_{0}.{ext} for duplicates
      "language_tags":  {code}_{lang}.{ext} using resource language field
      "resource_name":  sanitized resource name
    """
    fmt_counts: Counter[str] = Counter()
    for res in resources:
        fmt_counts[res["format"].upper()] += 1

    result = []
    fmt_seen: Counter[str] = Counter()

    for res in resources:
        fmt = res["format"].upper()
        ext = fmt.lower()

        if fmt_counts[fmt] > 1:
            if strategy == "language_tags":
                langs = res.get("language", [])
                tag = langs[0] if langs else f"v{fmt_seen[fmt]}"
                filename = f"{code}_{tag}.{ext}"
            elif strategy == "resource_name":
                res_name = res.get("name", "")
                if res_name:
                    filename = _safe_filename(res_name, ext)
                else:
                    filename = f"{code}_{fmt_seen[fmt]}.{ext}"
            else:  # "index"
                filename = f"{code}_{fmt_seen[fmt]}.{ext}"
        else:
            filename = f"{code}.{ext}"

        result.append((res, filename, ext))
        fmt_seen[fmt] += 1

    return result


async def download_dataset(
    client: httpx.AsyncClient,
    entry: CkanEntry,
    output_dir: Path,
    config: CkanPortalConfig,
    *,
    skip_existing: bool = True,
    on_bytes: BytesCallback | None = None,
    folder_style: str = FOLDER_STYLE_DISPLAY,
    download_formats: set[str] | None = None,
) -> DownloadResult:
    """Download all resources for a single CKAN dataset.

    Creates this structure under output_dir / entry.get_folder_path(style):
        {code}_info.json   - Catalog snapshot (always written first)
        {code}.csv         - CSV data (if resource available)
        {code}.xls         - Excel data (if resource available)
        {code}.xlsx        - Excel data (if resource available)
        ... other formats as available

    Args:
        client:           Async HTTP client for downloads.
        entry:            CkanEntry with pre-filtered resource list.
        output_dir:       Root output directory.
        config:           Portal configuration (filename strategy, etc.).
        skip_existing:    Skip files already present on disk.
        on_bytes:         Callback for progress tracking.
        folder_style:     "display" or "code".
        download_formats: If set, only download matching format resources.
    """
    code = entry.code
    dest_dir = output_dir / entry.get_folder_path(folder_style)
    dest_dir.mkdir(parents=True, exist_ok=True)

    result = DownloadResult(code)

    # Filter resources by requested formats
    resources = entry.resources
    if download_formats:
        fmt_upper = {f.upper() for f in download_formats}
        resources = [r for r in resources if r["format"].upper() in fmt_upper]

    # 1. Save catalog entry info
    info_file = dest_dir / f"{code}_info.json"
    if not (skip_existing and info_file.exists()):
        info = {
            "code": code,
            "title": entry.title,
            "description": entry.description[:500] if entry.description else "",
            "type": entry.entry_type,
            "path": entry.full_path,
            "folder_path": str(entry.folder_path),
            "updated": entry.updated,
            "organization": entry.organization,
            "license_url": entry.license_url,
            "license_title": entry.license_title,
            "identifier": entry.identifier,
            "groups": entry.groups,
            "source": config.name,
            "resources": [
                {"format": r["format"], "download_url": r["download_url"],
                 "name": r.get("name", "")}
                for r in resources
            ],
        }
        data = json.dumps(info, indent=2, ensure_ascii=False).encode()
        info_file.write_bytes(data)
        result.bytes_downloaded += len(data)
        result.successes.append("info")

    # 2. Download data resources
    file_plan = _build_filenames(code, resources, config.filename_strategy)

    for i, (res, filename, success_label) in enumerate(file_plan):
        dest_file = dest_dir / filename
        dl_url = res["download_url"]

        if skip_existing and dest_file.exists():
            continue

        if i > 0:
            await asyncio.sleep(_INTER_FILE_DELAY)

        try:
            n = await _download_file_with_retry(
                client, dl_url, dest_file,
                label=f"{code}/{filename}", on_bytes=on_bytes,
            )
            result.bytes_downloaded += n
            result.successes.append(success_label)
            log.info("  [%s] %s OK (%d bytes)", success_label.upper(), code, n)
        except Exception as e:
            result.failures.append((success_label, str(e)))
            log.warning("  [%s] %s FAILED: %s", success_label.upper(), code, e)

    return result
