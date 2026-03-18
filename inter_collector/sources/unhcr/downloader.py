"""Download UNHCR refugee statistics data by paginating API endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import httpx

from . import api
from .catalog import FOLDER_STYLE_DISPLAY, UNHCREntry
from ...download_utils import BytesCallback, DownloadResult

log = logging.getLogger(__name__)

_DOWNLOAD_MAX_RETRIES = 4
_DOWNLOAD_BACKOFF_BASE = 10.0


async def _fetch_all_pages(
    client: httpx.AsyncClient,
    endpoint_url: str,
    year: int,
    max_pages: int,
    *,
    label: str = "",
) -> tuple[list[dict], dict]:
    """Fetch all pages for an endpoint+year, return (all_items, totals).

    Returns a tuple of:
    - all_items: merged list of items from all pages
    - totals: the 'total' field from the first page (aggregate sums)
    """
    all_items: list[dict] = []
    totals: dict = {}

    for page in range(1, max_pages + 1):
        url = f"{endpoint_url}?year={year}&limit={api.DEFAULT_LIMIT}&page={page}"

        for attempt in range(1, _DOWNLOAD_MAX_RETRIES + 1):
            try:
                resp = await client.get(url, timeout=60)
                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = _DOWNLOAD_BACKOFF_BASE * (2 ** (attempt - 1))
                    log.warning(
                        "HTTP %d on %s p%d — retry %d/%d in %.0fs",
                        resp.status_code, label, page, attempt, _DOWNLOAD_MAX_RETRIES, wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                items = data.get("items", [])
                all_items.extend(items)
                if page == 1:
                    totals = data.get("total", {})
                break
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < _DOWNLOAD_MAX_RETRIES:
                    wait = _DOWNLOAD_BACKOFF_BASE * (2 ** (attempt - 1))
                    log.warning("Connection error on %s p%d — retry in %.0fs", label, page, wait)
                    await asyncio.sleep(wait)
                    continue
                raise
        else:
            # Python for...else: fires when the retry loop completed without
            # 'break' — i.e., all retry attempts exhausted on 429/5xx.
            # We raise a synthetic HTTPStatusError (429) so the caller's
            # generic Exception handler records a clean failure message.
            log.error("Exhausted retries for %s page %d", label, page)
            raise httpx.HTTPStatusError(
                f"Exhausted retries for {label} page {page}",
                request=httpx.Request("GET", url),
                response=httpx.Response(429),
            )

        if page < max_pages:
            await asyncio.sleep(api.INTER_REQUEST_DELAY)

    return all_items, totals


async def download_dataset(
    client: httpx.AsyncClient,
    entry: UNHCREntry,
    output_dir: Path,
    *,
    skip_existing: bool = True,
    on_bytes: BytesCallback | None = None,
    folder_style: str = FOLDER_STYLE_DISPLAY,
) -> DownloadResult:
    """Download all data for a single UNHCR endpoint+year combination.

    Creates this structure under output_dir / entry.get_folder_path(style):
        {code}_info.json     - Dataset catalog snapshot
        {code}_data.json     - Complete paginated data (all rows)

    The _data.json file contains:
    {
        "endpoint": "population",
        "year": 2023,
        "total": {...},       # aggregate totals from API
        "count": 1234,        # number of rows
        "items": [...]        # all data rows
    }
    """
    code = entry.code
    dest_dir = output_dir / entry.get_folder_path(folder_style)
    dest_dir.mkdir(parents=True, exist_ok=True)

    result = DownloadResult(code)

    # 1. Save info snapshot
    info_file = dest_dir / f"{code}_info.json"
    if not (skip_existing and info_file.exists()):
        info = {
            "code": code,
            "title": entry.title,
            "description": entry.description,
            "type": entry.entry_type,
            "path": entry.full_path,
            "folder_path": str(entry.folder_path),
            "endpoint": entry.endpoint_key,
            "endpoint_url": entry.endpoint_url,
            "year": entry.year,
            "max_pages": entry.max_pages,
            "license": "CC BY 4.0",
            "attribution": "UNHCR Refugee Population Statistics Database",
            "source_url": "https://www.unhcr.org/refugee-statistics",
        }
        data_bytes = json.dumps(info, indent=2, ensure_ascii=False).encode()
        info_file.write_bytes(data_bytes)
        result.bytes_downloaded += len(data_bytes)
        result.successes.append("info")

    # 2. Download all paginated data
    data_file = dest_dir / f"{code}_data.json"
    if skip_existing and data_file.exists():
        return result

    label = f"{entry.endpoint_key}/{entry.year}"
    try:
        all_items, totals = await _fetch_all_pages(
            client, entry.endpoint_url, entry.year, entry.max_pages,
            label=label,
        )

        dataset = {
            "endpoint": entry.endpoint_key,
            "year": entry.year,
            "total": totals,
            "count": len(all_items),
            "items": all_items,
        }

        data_bytes = json.dumps(dataset, indent=2, ensure_ascii=False).encode()
        data_file.write_bytes(data_bytes)
        result.bytes_downloaded += len(data_bytes)
        result.successes.append("data")
        log.info("  [DATA] %s OK (%d rows, %d bytes)", code, len(all_items), len(data_bytes))

    except Exception as e:
        result.failures.append(("data", str(e)))
        log.warning("  [DATA] %s FAILED: %s", code, e)

    return result
