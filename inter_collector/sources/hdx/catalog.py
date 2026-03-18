"""Fetch the HDX humanitarian data catalog (CKAN API).

Adapted from the Swiss opendata.swiss source — same CKAN API pattern.
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
class HDXEntry:
    """A node in the HDX catalog tree (root, group, or dataset)."""

    code: str                    # CKAN package name (e.g., "unhcr-population-data-for-afg")
    title: str                   # human-readable title
    entry_type: str              # "root", "group", "dataset"
    parent_path: str = ""        # slash-separated path of parent codes
    parent_folder_path: str = "" # slash-separated path of parent display names
    children: list[HDXEntry] = field(default_factory=list)

    # Dataset-specific fields
    description: str = ""        # dataset description (may be multilingual)
    updated: str = ""            # metadata_modified timestamp from CKAN
    organization: str = ""       # org slug (e.g., "unhcr")
    license_url: str = ""        # license URL (e.g., CC BY-IGO link)
    license_title: str = ""      # license name (e.g., "Creative Commons Attribution for IGOs")
    identifier: str = ""         # CKAN identifier field
    groups: list[str] = field(default_factory=list)  # CKAN group/tag names
    resources: list[dict] = field(default_factory=list)
    # Each resource dict: {format, download_url, url, name, byte_size}

    @property
    def display_name(self) -> str:
        """Folder name: code for datasets, slugified title for groups."""
        if self.is_dataset:
            return self.code
        return _slugify(self.title) if self.title else self.code

    @property
    def full_path(self) -> str:
        """Slash-separated code path for state tracking and --filter-path."""
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


def collect_datasets(entry: HDXEntry) -> list[HDXEntry]:
    """Flatten the tree to get all dataset entries."""
    results: list[HDXEntry] = []
    if entry.is_dataset:
        results.append(entry)
    for child in entry.children:
        results.extend(collect_datasets(child))
    return results


def _extract_resources(pkg: dict, download_formats: set[str]) -> list[dict]:
    """Filter and extract downloadable resources from a CKAN package."""
    result = []
    for res in pkg.get("resources", []):
        fmt = (res.get("format") or "").upper()
        if fmt not in download_formats:
            continue
        dl_url = res.get("download_url") or res.get("url") or ""
        if not dl_url:
            continue
        if dl_url.endswith(".html") or "text/html" in (res.get("mimetype") or res.get("media_type") or ""):
            continue
        result.append({
            "format": fmt,
            "download_url": dl_url,
            "url": res.get("url", ""),
            "name": res.get("name", ""),
            "byte_size": int(res.get("size") or res.get("byte_size") or 0),
        })
    return result


_CATALOG_MAX_RETRIES = 4
_CATALOG_BACKOFF_BASE = 10.0


async def fetch_catalog(
    client: httpx.AsyncClient,
    *,
    output_dir: Path | None = None,
    org_filter: str = api.DEFAULT_ORG,
    download_formats: set[str] | None = None,
) -> HDXEntry:
    """Fetch the HDX catalog from CKAN.

    1. Paginate through package_search
    2. Filter resources to downloadable data formats
    3. Organise into a tree by CKAN group (tag)
    """
    if download_formats is None:
        download_formats = api.DEFAULT_DOWNLOAD_FORMATS
    download_formats = {f.upper() for f in download_formats}

    all_items: list[dict] = []
    start = 0

    fq = f"organization:{org_filter}" if org_filter else ""

    while True:
        url = f"{api.PACKAGE_SEARCH}?rows={api.PAGE_SIZE}&start={start}"
        if fq:
            url += f"&fq={fq}"

        for attempt in range(1, _CATALOG_MAX_RETRIES + 1):
            try:
                resp = await client.get(url, timeout=60)
                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = _CATALOG_BACKOFF_BASE * (2 ** (attempt - 1))
                    log.warning("HTTP %d on catalog page — retry %d/%d in %.0fs",
                                resp.status_code, attempt, _CATALOG_MAX_RETRIES, wait)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            except (httpx.TimeoutException, httpx.ConnectError):
                if attempt < _CATALOG_MAX_RETRIES:
                    await asyncio.sleep(_CATALOG_BACKOFF_BASE * (2 ** (attempt - 1)))
                    continue
                raise
        else:
            log.error("Exhausted retries for catalog page at start=%d", start)
            break

        body = resp.json()
        if not body.get("success"):
            log.error("CKAN API returned success=false at start=%d", start)
            break

        result = body.get("result", {})
        items = result.get("results", [])
        total = result.get("count", 0)
        all_items.extend(items)

        start += len(items)
        log.info("Fetched %d/%d datasets from HDX", len(all_items), total)

        if start >= total or not items:
            break
        await asyncio.sleep(1.0)

    log.info("Total datasets from HDX: %d", len(all_items))

    # Build tree by groups
    root = HDXEntry(code="hdx", title="HDX Humanitarian Data", entry_type="root")
    groups: dict[str, HDXEntry] = {}
    skipped = 0

    for pkg in all_items:
        pkg_name = pkg.get("name", "")
        if not pkg_name:
            continue

        resources = _extract_resources(pkg, download_formats)
        if not resources:
            skipped += 1
            continue

        # Group by first tag or "uncategorised"
        pkg_groups = pkg.get("groups", [])
        if pkg_groups:
            grp = pkg_groups[0]
            grp_name = grp.get("name", "uncategorised")
            grp_title = grp.get("display_name") or grp.get("title", grp_name)
        else:
            grp_name = "uncategorised"
            grp_title = "Uncategorised"

        if grp_name not in groups:
            groups[grp_name] = HDXEntry(
                code=grp_name, title=grp_title, entry_type="group",
                parent_path=root.code, parent_folder_path=root.display_name,
            )

        grp_entry = groups[grp_name]
        org = pkg.get("organization", {})
        org_name = org.get("name", "") if isinstance(org, dict) else ""

        ds = HDXEntry(
            code=pkg_name,
            title=pkg.get("title", pkg_name),
            entry_type="dataset",
            parent_path=grp_entry.full_path,
            parent_folder_path=str(grp_entry.folder_path),
            description=(pkg.get("notes") or "")[:500],
            updated=pkg.get("metadata_modified", ""),
            organization=org_name,
            license_url=pkg.get("license_url", ""),
            license_title=pkg.get("license_title", ""),
            identifier=pkg.get("id", ""),
            groups=[g.get("name", "") for g in pkg_groups],
            resources=resources,
        )
        grp_entry.children.append(ds)

    if skipped:
        log.info("Skipped %d datasets with no downloadable resources", skipped)

    for grp_name in sorted(groups.keys()):
        if groups[grp_name].children:
            root.children.append(groups[grp_name])

    total_datasets = sum(len(g.children) for g in root.children)
    log.info("HDX catalog: %d groups, %d datasets", len(root.children), total_datasets)
    return root
