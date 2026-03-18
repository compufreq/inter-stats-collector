"""Fetch the UK ONS dataset catalog via the CMD API."""

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


def _slugify(title: str, max_length: int = 60) -> str:
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


# Folder naming styles
FOLDER_STYLE_DISPLAY = "display"
FOLDER_STYLE_CODE = "code"


@dataclass
class ONSEntry:
    """A node in the ONS catalog tree (root, category, or dataset)."""

    code: str                    # dataset id (e.g., "cpih01") or category slug
    title: str                   # human-readable title
    entry_type: str              # "root", "category", "dataset"
    parent_path: str = ""        # slash-separated path of parent codes
    parent_folder_path: str = "" # slash-separated path of parent display names
    children: list[ONSEntry] = field(default_factory=list)

    # Dataset-specific fields
    description: str = ""
    last_updated: str = ""
    release_frequency: str = ""
    national_statistic: bool = False
    edition: str = ""             # e.g., "time-series", "2021"
    version: str = ""             # e.g., "9"
    taxonomy_path: str = ""       # e.g., "peoplepopulationandcommunity/wellbeing"

    # Download URLs (resolved from latest version)
    csv_url: str = ""
    csv_size: int = 0
    xlsx_url: str = ""
    xlsx_size: int = 0
    csvw_url: str = ""
    csvw_size: int = 0

    # Version metadata URL
    version_url: str = ""

    @property
    def display_name(self) -> str:
        """Folder name using slugified title (categories) or code (datasets)."""
        if self.is_dataset:
            return self.code
        return _slugify(self.title) if self.title else self.code

    @property
    def full_path(self) -> str:
        """Code-based path (for internal use: state tracking, --filter-path)."""
        if self.parent_path:
            return f"{self.parent_path}/{self.code}"
        return self.code

    @property
    def is_dataset(self) -> bool:
        return self.entry_type == "dataset"

    @property
    def folder_path(self) -> Path:
        """Filesystem path using display names."""
        if self.parent_folder_path:
            return Path(self.parent_folder_path) / self.display_name
        return Path(self.display_name)

    @property
    def folder_path_code(self) -> Path:
        """Filesystem path using raw codes."""
        return Path(self.full_path)

    def get_folder_path(self, style: str = FOLDER_STYLE_DISPLAY) -> Path:
        """Get filesystem path for the chosen naming style."""
        if style == FOLDER_STYLE_CODE:
            return self.folder_path_code
        return self.folder_path


def collect_datasets(entry: ONSEntry) -> list[ONSEntry]:
    """Flatten the tree to get all dataset entries."""
    results: list[ONSEntry] = []
    if entry.is_dataset:
        results.append(entry)
    for child in entry.children:
        results.extend(collect_datasets(child))
    return results


def _extract_taxonomy(links: dict) -> str:
    """Extract taxonomy path from dataset links.

    The taxonomy link looks like:
        https://api.beta.ons.gov.uk/v1/peoplepopulationandcommunity/wellbeing
    We extract "peoplepopulationandcommunity/wellbeing".
    """
    tax = links.get("taxonomy", {})
    href = tax.get("href", "")
    if not href:
        return ""
    # Strip the API base URL prefix
    prefix = f"{api.API_BASE}/"
    if href.startswith(prefix):
        return href[len(prefix):]
    return ""


# ONS API is aggressive with 429s, so we use generous retry settings.
# Wait progression: 20s, 40s, 80s, 160s, 320s, 640s (max ~10.7 min single wait).
_RESOLVE_MAX_RETRIES = 6
_RESOLVE_BACKOFF_BASE = 20.0  # seconds — doubled on each retry attempt


def _parse_retry_after(resp: httpx.Response) -> float | None:
    """Extract Retry-After header value in seconds, if present."""
    val = resp.headers.get("retry-after", "")
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


async def _resolve_version(
    client: httpx.AsyncClient,
    version_url: str,
) -> dict:
    """Fetch version detail to get download URLs and dimensions.

    Retries on 429 with exponential backoff (20s, 40s, 80s, 160s, 320s, 640s).
    Respects Retry-After header if present.
    """
    for attempt in range(1, _RESOLVE_MAX_RETRIES + 1):
        try:
            resp = await client.get(version_url, timeout=60)
            if resp.status_code == 429:
                retry_after = _parse_retry_after(resp)
                wait = retry_after if retry_after else _RESOLVE_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "Rate limited (429) on %s — retry %d/%d in %.0fs",
                    version_url, attempt, _RESOLVE_MAX_RETRIES, wait,
                )
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = _parse_retry_after(e.response)
                wait = retry_after if retry_after else _RESOLVE_BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "Rate limited (429) on %s — retry %d/%d in %.0fs",
                    version_url, attempt, _RESOLVE_MAX_RETRIES, wait,
                )
                await asyncio.sleep(wait)
                continue
            log.warning("Failed to resolve version %s: %s", version_url, e)
            return {}
        except Exception as e:
            log.warning("Failed to resolve version %s: %s", version_url, e)
            return {}
    log.warning("Exhausted retries for %s", version_url)
    return {}


async def fetch_catalog(
    client: httpx.AsyncClient,
    *,
    resolve_downloads: bool = True,
) -> ONSEntry:
    """Fetch the full ONS dataset catalog.

    1. Paginate through GET /datasets to get all ~337 datasets
    2. Optionally resolve latest_version for each to get download URLs
    3. Organise into a tree by taxonomy path

    Returns the root ONSEntry with categories and datasets.
    """
    # Step 1: Fetch all datasets (paginated)
    all_items: list[dict] = []
    offset = 0

    while True:
        url = f"{api.DATASETS}?offset={offset}&limit={api.PAGE_SIZE}"
        for _page_attempt in range(1, _RESOLVE_MAX_RETRIES + 1):
            resp = await client.get(url, timeout=60)
            if resp.status_code == 429:
                wait = _RESOLVE_BACKOFF_BASE * (2 ** (_page_attempt - 1))
                log.warning("Rate limited (429) on catalog page — retry in %.0fs", wait)
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            log.error("Exhausted retries for catalog page at offset %d", offset)
            break

        data = resp.json()

        items = data.get("items", [])
        all_items.extend(items)

        total = data.get("total_count", 0)
        offset += len(items)

        log.info("Fetched %d/%d datasets from ONS catalog", len(all_items), total)

        if offset >= total or not items:
            break
        # Small delay between pages
        await asyncio.sleep(1.0)

    log.info("Total ONS datasets: %d", len(all_items))

    # Step 2: Build taxonomy tree
    root = ONSEntry(
        code="ons",
        title="UK ONS",
        entry_type="root",
    )

    # Group datasets by taxonomy path
    categories: dict[str, ONSEntry] = {}  # taxonomy_path → category entry

    for item in all_items:
        ds_id = item.get("id", "")
        ds_title = item.get("title", ds_id)
        links = item.get("links", {})
        taxonomy_path = _extract_taxonomy(links)

        # Get latest version URL
        latest = links.get("latest_version", {})
        version_url = latest.get("href", "")
        version_id = latest.get("id", "")

        # Edition from the version URL: .../editions/{edition}/versions/{ver}
        edition = ""
        if "/editions/" in version_url:
            parts = version_url.split("/editions/")
            if len(parts) > 1:
                edition = parts[1].split("/versions/")[0]

        # Determine parent category
        if taxonomy_path:
            cat_key = taxonomy_path
        else:
            cat_key = "uncategorised"

        # Create category if needed
        if cat_key not in categories:
            cat_title = cat_key.replace("/", " / ").replace("and", "& ")
            cat_entry = ONSEntry(
                code=cat_key.replace("/", "_"),
                title=cat_title,
                entry_type="category",
                parent_path=root.code,
                parent_folder_path=root.display_name,
            )
            categories[cat_key] = cat_entry

        cat = categories[cat_key]

        ds_entry = ONSEntry(
            code=ds_id,
            title=ds_title,
            entry_type="dataset",
            parent_path=cat.full_path,
            parent_folder_path=str(cat.folder_path),
            description=item.get("description", ""),
            last_updated=item.get("last_updated", ""),
            release_frequency=item.get("release_frequency", ""),
            national_statistic=item.get("national_statistic", False),
            edition=edition,
            version=version_id,
            taxonomy_path=taxonomy_path,
            version_url=version_url,
        )
        cat.children.append(ds_entry)

    # Step 3: Resolve download URLs for each dataset (sequential batches).
    # The ONS API is aggressive with 429s, so we use small batches with
    # generous delays: 5 datasets at a time, 1 concurrent, 3s between batches.
    if resolve_downloads:
        all_datasets = [ds for cat in categories.values() for ds in cat.children if ds.is_dataset]

        async def _resolve(ds: ONSEntry) -> None:
            """Fetch version detail and mutate ds in-place with download URLs."""
            if not ds.version_url:
                return
            ver = await _resolve_version(client, ds.version_url)
            if not ver:
                return
            downloads = ver.get("downloads", {})
            csv_info = downloads.get("csv", {})
            xlsx_info = downloads.get("xls", {})
            csvw_info = downloads.get("csvw", {})

            ds.csv_url = csv_info.get("href", "")
            ds.csv_size = int(csv_info.get("size", 0) or 0)
            ds.xlsx_url = xlsx_info.get("href", "")
            ds.xlsx_size = int(xlsx_info.get("size", 0) or 0)
            ds.csvw_url = csvw_info.get("href", "")
            ds.csvw_size = int(csvw_info.get("size", 0) or 0)

        # Small batches with delays to stay under ONS's undocumented rate limit.
        # Total time for ~337 datasets: ~337s + 67*2s ≈ 8 minutes.
        _CATALOG_BATCH_SIZE = 5      # resolve 5 datasets per batch
        _CATALOG_BATCH_DELAY = 2.0   # seconds between batches
        _PER_REQUEST_DELAY = 1.0     # seconds between individual requests within a batch

        for i in range(0, len(all_datasets), _CATALOG_BATCH_SIZE):
            batch = all_datasets[i : i + _CATALOG_BATCH_SIZE]
            # Run batch sequentially (one at a time) to avoid bursts
            for j, ds in enumerate(batch):
                await _resolve(ds)
                # Delay between individual requests within a batch
                if j < len(batch) - 1:
                    await asyncio.sleep(_PER_REQUEST_DELAY)
            resolved = min(i + _CATALOG_BATCH_SIZE, len(all_datasets))
            log.info("Resolved download URLs: %d/%d", resolved, len(all_datasets))
            if resolved < len(all_datasets):
                await asyncio.sleep(_CATALOG_BATCH_DELAY)

    # Attach categories to root, sorted
    for cat_key in sorted(categories.keys()):
        root.children.append(categories[cat_key])

    total_datasets = sum(len(c.children) for c in root.children)
    log.info("ONS catalog: %d categories, %d datasets", len(root.children), total_datasets)

    return root
