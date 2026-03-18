"""Download dataset resources from opendata.swiss (CKAN)."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from pathlib import Path

import httpx

from .catalog import FOLDER_STYLE_DISPLAY, SwissEntry
from ...download_utils import BytesCallback, DownloadResult, _download_file

log = logging.getLogger(__name__)

_DOWNLOAD_MAX_RETRIES = 4
_DOWNLOAD_BACKOFF_BASE = 10.0  # seconds — 10s, 20s, 40s, 80s
_INTER_FILE_DELAY = 0.5  # seconds between resource downloads


async def _download_file_with_retry(
    client: httpx.AsyncClient,
    url: str,
    dest: Path,
    *,
    label: str = "",
    on_bytes: BytesCallback | None = None,
) -> int:
    """Download a file with retry on 429/5xx and exponential backoff."""
    for attempt in range(1, _DOWNLOAD_MAX_RETRIES + 1):
        try:
            n = await _download_file(client, url, dest, on_bytes=on_bytes)
            return n
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 429 or status >= 500:
                wait = _DOWNLOAD_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "HTTP %d downloading %s — retry %d/%d in %.0fs",
                    status, label or url, attempt, _DOWNLOAD_MAX_RETRIES, wait,
                )
                await asyncio.sleep(wait)
                continue
            raise
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            if attempt < _DOWNLOAD_MAX_RETRIES:
                wait = _DOWNLOAD_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "Connection error on %s — retry %d/%d in %.0fs",
                    label or url, attempt, _DOWNLOAD_MAX_RETRIES, wait,
                )
                await asyncio.sleep(wait)
                continue
            raise
    raise httpx.HTTPStatusError(
        f"Exhausted {_DOWNLOAD_MAX_RETRIES} retries for {label or url}",
        request=httpx.Request("GET", url),
        response=httpx.Response(429),
    )


def _build_filenames(
    code: str,
    resources: list[dict],
) -> list[tuple[dict, str, str]]:
    """Determine destination filename for each resource.

    Returns list of (resource_dict, filename, success_label).

    If a format appears once, use {code}.{ext}.
    If a format appears multiple times (different languages), use {code}_{lang}.{ext}.
    """
    # Count how many resources per format
    fmt_counts: Counter[str] = Counter()
    for res in resources:
        fmt_counts[res["format"].upper()] += 1

    result = []
    # Track per-format index for dedup
    fmt_seen: Counter[str] = Counter()
    for res in resources:
        fmt = res["format"].upper()
        ext = fmt.lower()
        # Normalise common extensions
        if ext == "xlsx":
            ext = "xlsx"
        elif ext == "xls":
            ext = "xls"

        if fmt_counts[fmt] > 1:
            # Multiple resources of same format — add language suffix
            langs = res.get("language", [])
            lang_tag = langs[0] if langs else f"v{fmt_seen[fmt]}"
            filename = f"{code}_{lang_tag}.{ext}"
        else:
            filename = f"{code}.{ext}"

        # Success label is the lowercase format
        success_label = ext
        result.append((res, filename, success_label))
        fmt_seen[fmt] += 1

    return result


async def download_dataset(
    client: httpx.AsyncClient,
    entry: SwissEntry,
    output_dir: Path,
    *,
    skip_existing: bool = True,
    on_bytes: BytesCallback | None = None,
    folder_style: str = FOLDER_STYLE_DISPLAY,
    download_formats: set[str] | None = None,
) -> DownloadResult:
    """Download all resources for a single opendata.swiss dataset.

    Creates this structure under output_dir / entry.get_folder_path(style):
        {code}_info.json   - Dataset catalog snapshot
        {code}.csv         - CSV data (if available)
        {code}.xls         - Excel data (if available)
        {code}.ods         - ODS data (if available)
        {code}.json        - JSON data (if available)

    Multiple resources of the same format get language suffixes:
        {code}_de.csv, {code}_fr.csv
    """
    code = entry.code
    dest_dir = output_dir / entry.get_folder_path(folder_style)
    dest_dir.mkdir(parents=True, exist_ok=True)

    result = DownloadResult(code)

    # Filter resources by requested formats (if specified)
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
            "identifier": entry.identifier,
            "groups": entry.groups,
            "resources": [
                {"format": r["format"], "download_url": r["download_url"]}
                for r in resources
            ],
        }
        data = json.dumps(info, indent=2, ensure_ascii=False).encode()
        info_file.write_bytes(data)
        result.bytes_downloaded += len(data)
        result.successes.append("info")

    # 2. Download each data resource
    file_plan = _build_filenames(code, resources)

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
