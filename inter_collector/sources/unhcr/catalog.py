"""Build the UNHCR catalog — one dataset per endpoint × year combination.

Unlike Eurostat or opendata.swiss which have thousands of individual datasets,
UNHCR has 6 fixed API endpoints. We create "datasets" by combining each
endpoint with each available year, producing ~450 dataset entries
(6 endpoints × 75 years). Each dataset is downloaded as a complete JSON
file containing all country-level rows for that endpoint+year.
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from . import api

log = logging.getLogger(__name__)


def _slugify(title: str, max_length: int = 80) -> str:
    """Convert a human-readable title to a filesystem-safe folder name."""
    s = unicodedata.normalize("NFKD", title)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    if len(s) > max_length:
        s = s[:max_length].rsplit("_", 1)[0]
    return s or "unnamed"


FOLDER_STYLE_DISPLAY = "display"
FOLDER_STYLE_CODE = "code"


@dataclass
class UNHCREntry:
    """A node in the UNHCR catalog tree (root, endpoint group, or dataset)."""

    code: str                    # e.g., "population_2023" or "asylum-applications"
    title: str                   # human-readable title
    entry_type: str              # "root", "endpoint", "dataset"
    parent_path: str = ""
    parent_folder_path: str = ""
    children: list[UNHCREntry] = field(default_factory=list)

    # Dataset-specific fields
    description: str = ""
    endpoint_key: str = ""       # e.g., "population", "asylum-decisions"
    endpoint_url: str = ""       # full API URL for this endpoint
    year: int = 0                # the year this dataset covers
    max_pages: int = 0           # total pages available (discovered during catalog)

    @property
    def display_name(self) -> str:
        """Folder name: code for datasets, slugified title for groups."""
        if self.is_dataset:
            return self.code
        return _slugify(self.title) if self.title else self.code

    @property
    def full_path(self) -> str:
        """Slash-separated code path (e.g., 'unhcr/population/population_2023')."""
        if self.parent_path:
            return f"{self.parent_path}/{self.code}"
        return self.code

    @property
    def is_dataset(self) -> bool:
        """True if this entry represents a downloadable dataset (not a group)."""
        return self.entry_type == "dataset"

    @property
    def folder_path(self) -> Path:
        """Filesystem path using display names for browsing."""
        if self.parent_folder_path:
            return Path(self.parent_folder_path) / self.display_name
        return Path(self.display_name)

    @property
    def folder_path_code(self) -> Path:
        """Filesystem path using raw codes for programmatic access."""
        return Path(self.full_path)

    def get_folder_path(self, style: str = FOLDER_STYLE_DISPLAY) -> Path:
        """Get filesystem path for the chosen naming style ('display' or 'code')."""
        if style == FOLDER_STYLE_CODE:
            return self.folder_path_code
        return self.folder_path


def collect_datasets(entry: UNHCREntry) -> list[UNHCREntry]:
    """Flatten the tree to get all dataset entries."""
    results: list[UNHCREntry] = []
    if entry.is_dataset:
        results.append(entry)
    for child in entry.children:
        results.extend(collect_datasets(child))
    return results


_CATALOG_MAX_RETRIES = 4
_CATALOG_BACKOFF_BASE = 10.0


async def _fetch_years(client: httpx.AsyncClient) -> list[int]:
    """Fetch available years from the UNHCR API."""
    for attempt in range(1, _CATALOG_MAX_RETRIES + 1):
        try:
            resp = await client.get(api.YEARS, timeout=30)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = _CATALOG_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning("HTTP %d fetching years — retry in %.0fs", resp.status_code, wait)
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            return sorted(item["year"] for item in data.get("items", []))
        except Exception as e:
            if attempt < _CATALOG_MAX_RETRIES:
                await asyncio.sleep(_CATALOG_BACKOFF_BASE)
                continue
            log.error("Failed to fetch years: %s", e)
            return []
    return []


async def _probe_endpoint(
    client: httpx.AsyncClient,
    endpoint_url: str,
    year: int,
) -> int:
    """Probe an endpoint+year to discover how many pages of data exist.

    Returns maxPages (0 if no data).
    """
    url = f"{endpoint_url}?year={year}&limit={api.DEFAULT_LIMIT}&page=1"
    for attempt in range(1, _CATALOG_MAX_RETRIES + 1):
        try:
            resp = await client.get(url, timeout=30)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = _CATALOG_BACKOFF_BASE * (2 ** (attempt - 1))
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data.get("maxPages", 0)
        except Exception:
            if attempt < _CATALOG_MAX_RETRIES:
                await asyncio.sleep(_CATALOG_BACKOFF_BASE)
                continue
            return 0
    return 0


async def fetch_catalog(
    client: httpx.AsyncClient,
    *,
    output_dir: Path | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
) -> UNHCREntry:
    """Build the UNHCR catalog: endpoint groups → year datasets.

    For each of the 6 data endpoints, probes each year to discover
    which years have data. Creates a dataset entry for each endpoint+year
    combination that has at least 1 page of results.

    Args:
        year_from: Start year filter (default: all available years)
        year_to: End year filter (default: all available years)

    Returns the root UNHCREntry tree.
    """
    root = UNHCREntry(
        code="unhcr",
        title="UNHCR Refugee Statistics",
        entry_type="root",
    )

    # Fetch available years
    log.info("Fetching available years from UNHCR API...")
    all_years = await _fetch_years(client)
    if not all_years:
        log.error("No years returned from UNHCR API")
        return root

    # Apply year filters
    if year_from is not None:
        all_years = [y for y in all_years if y >= year_from]
    if year_to is not None:
        all_years = [y for y in all_years if y <= year_to]

    if not all_years:
        log.error("No years available after applying year filters (year_from=%s, year_to=%s)", year_from, year_to)
        return root

    log.info("UNHCR years range: %d–%d (%d years)", all_years[0], all_years[-1], len(all_years))

    # For each endpoint, probe each year to see if data exists
    for ep_key, ep_info in api.DATA_ENDPOINTS.items():
        ep_entry = UNHCREntry(
            code=ep_key,
            title=ep_info["title"],
            entry_type="endpoint",
            parent_path=root.code,
            parent_folder_path=root.display_name,
            description=ep_info["description"],
            endpoint_key=ep_key,
            endpoint_url=ep_info["url"],
        )

        log.info("Probing endpoint '%s' across %d years...", ep_key, len(all_years))

        for year in all_years:
            max_pages = await _probe_endpoint(client, ep_info["url"], year)
            if max_pages > 0:
                ds_code = f"{ep_key}_{year}"
                ds = UNHCREntry(
                    code=ds_code,
                    title=f"{ep_info['title']} — {year}",
                    entry_type="dataset",
                    parent_path=ep_entry.full_path,
                    parent_folder_path=str(ep_entry.folder_path),
                    description=ep_info["description"],
                    endpoint_key=ep_key,
                    endpoint_url=ep_info["url"],
                    year=year,
                    max_pages=max_pages,
                )
                ep_entry.children.append(ds)

            await asyncio.sleep(api.INTER_REQUEST_DELAY)

        if ep_entry.children:
            root.children.append(ep_entry)
            log.info("  '%s': %d years with data", ep_key, len(ep_entry.children))
        else:
            log.info("  '%s': no data found", ep_key)

        await asyncio.sleep(api.INTER_ENDPOINT_DELAY)

    total_datasets = sum(len(ep.children) for ep in root.children)
    log.info("UNHCR catalog: %d endpoints, %d datasets", len(root.children), total_datasets)

    return root
