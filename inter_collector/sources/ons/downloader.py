"""Download dataset files (CSV, XLSX, CSV-W metadata) from UK ONS."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import httpx

from .catalog import FOLDER_STYLE_DISPLAY, ONSEntry, _parse_retry_after
from ...download_utils import BytesCallback, DownloadResult, _download_file

log = logging.getLogger(__name__)

_DOWNLOAD_MAX_RETRIES = 5
_DOWNLOAD_BACKOFF_BASE = 15.0  # seconds


async def _download_file_with_retry(
    client: httpx.AsyncClient,
    url: str,
    dest: Path,
    *,
    label: str = "",
    on_bytes: BytesCallback | None = None,
) -> int:
    """Download a file with 429 retry and exponential backoff."""
    for attempt in range(1, _DOWNLOAD_MAX_RETRIES + 1):
        try:
            n = await _download_file(client, url, dest, on_bytes=on_bytes)
            return n
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = _parse_retry_after(e.response)
                wait = retry_after if retry_after else _DOWNLOAD_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "Rate limited (429) downloading %s — retry %d/%d in %.0fs",
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


async def _get_json_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    label: str = "",
) -> dict:
    """GET a JSON endpoint with 429 retry and exponential backoff."""
    for attempt in range(1, _DOWNLOAD_MAX_RETRIES + 1):
        try:
            resp = await client.get(url, timeout=60)
            if resp.status_code == 429:
                retry_after = _parse_retry_after(resp)
                wait = retry_after if retry_after else _DOWNLOAD_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "Rate limited (429) fetching %s — retry %d/%d in %.0fs",
                    label or url, attempt, _DOWNLOAD_MAX_RETRIES, wait,
                )
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = _parse_retry_after(e.response)
                wait = retry_after if retry_after else _DOWNLOAD_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "Rate limited (429) fetching %s — retry %d/%d in %.0fs",
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


async def download_dataset(
    client: httpx.AsyncClient,
    entry: ONSEntry,
    output_dir: Path,
    *,
    skip_existing: bool = True,
    on_bytes: BytesCallback | None = None,
    folder_style: str = FOLDER_STYLE_DISPLAY,
) -> DownloadResult:
    """Download all files for a single ONS dataset.

    Creates this structure under output_dir / entry.get_folder_path(style):
        {code}_info.json           - Dataset catalog snapshot
        {code}_meta.json           - Full version metadata (dimensions, release info)
        {code}.csv                 - Data in CSV format
        {code}.xlsx                - Data in Excel format
        {code}.csv-metadata.json   - CSV-W metadata (data dictionary)
    """
    code = entry.code
    dest_dir = output_dir / entry.get_folder_path(folder_style)
    dest_dir.mkdir(parents=True, exist_ok=True)

    result = DownloadResult(code)

    # 1. Save catalog entry info
    info_file = dest_dir / f"{code}_info.json"
    if not (skip_existing and info_file.exists()):
        info = {
            "code": code,
            "title": entry.title,
            "description": entry.description,
            "type": entry.entry_type,
            "path": entry.full_path,
            "folder_path": str(entry.folder_path),
            "taxonomy_path": entry.taxonomy_path,
            "edition": entry.edition,
            "version": entry.version,
            "last_updated": entry.last_updated,
            "release_frequency": entry.release_frequency,
            "national_statistic": entry.national_statistic,
            "version_url": entry.version_url,
            "csv_url": entry.csv_url,
            "xlsx_url": entry.xlsx_url,
            "csvw_url": entry.csvw_url,
        }
        data = json.dumps(info, indent=2, ensure_ascii=False).encode()
        info_file.write_bytes(data)
        result.bytes_downloaded += len(data)
        result.successes.append("info")

    # 2. Download version metadata (full dimensions, release info)
    meta_file = dest_dir / f"{code}_meta.json"
    if not (skip_existing and meta_file.exists()) and entry.version_url:
        try:
            meta = await _get_json_with_retry(
                client, entry.version_url, label=f"{code}/meta",
            )
            data = json.dumps(meta, indent=2, ensure_ascii=False).encode()
            meta_file.write_bytes(data)
            result.bytes_downloaded += len(data)
            result.successes.append("meta")
            log.info("  [META] %s OK", code)
        except Exception as e:
            result.failures.append(("meta", str(e)))
            log.warning("  [META] %s FAILED: %s", code, e)

    # Small delay between file downloads to avoid burst
    _INTER_FILE_DELAY = 1.0

    # 3. Download CSV data
    csv_file = dest_dir / f"{code}.csv"
    if not (skip_existing and csv_file.exists()) and entry.csv_url:
        try:
            await asyncio.sleep(_INTER_FILE_DELAY)
            n = await _download_file_with_retry(
                client, entry.csv_url, csv_file,
                label=f"{code}/csv", on_bytes=on_bytes,
            )
            result.bytes_downloaded += n
            result.successes.append("csv")
            log.info("  [CSV] %s OK (%d bytes)", code, n)
        except Exception as e:
            result.failures.append(("csv", str(e)))
            log.warning("  [CSV] %s FAILED: %s", code, e)

    # 4. Download XLSX data
    xlsx_file = dest_dir / f"{code}.xlsx"
    if not (skip_existing and xlsx_file.exists()) and entry.xlsx_url:
        try:
            await asyncio.sleep(_INTER_FILE_DELAY)
            n = await _download_file_with_retry(
                client, entry.xlsx_url, xlsx_file,
                label=f"{code}/xlsx", on_bytes=on_bytes,
            )
            result.bytes_downloaded += n
            result.successes.append("xlsx")
            log.info("  [XLSX] %s OK (%d bytes)", code, n)
        except Exception as e:
            result.failures.append(("xlsx", str(e)))
            log.warning("  [XLSX] %s FAILED: %s", code, e)

    # 5. Download CSV-W metadata (data dictionary)
    csvw_file = dest_dir / f"{code}.csv-metadata.json"
    if not (skip_existing and csvw_file.exists()) and entry.csvw_url:
        try:
            await asyncio.sleep(_INTER_FILE_DELAY)
            n = await _download_file_with_retry(
                client, entry.csvw_url, csvw_file,
                label=f"{code}/csvw", on_bytes=on_bytes,
            )
            result.bytes_downloaded += n
            result.successes.append("csvw")
            log.info("  [CSVW] %s OK (%d bytes)", code, n)
        except Exception as e:
            result.failures.append(("csvw", str(e)))
            log.warning("  [CSVW] %s FAILED: %s", code, e)

    return result
