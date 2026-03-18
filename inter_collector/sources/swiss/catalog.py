"""Fetch the Swiss open data catalog from opendata.swiss (CKAN API)."""

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


# Folder naming styles
FOLDER_STYLE_DISPLAY = "display"
FOLDER_STYLE_CODE = "code"


@dataclass
class SwissEntry:
    """A node in the Swiss catalog tree (root, group, or dataset)."""

    code: str                    # CKAN package name or group slug
    title: str                   # human-readable title
    entry_type: str              # "root", "group", "dataset"
    parent_path: str = ""        # slash-separated path of parent codes
    parent_folder_path: str = "" # slash-separated path of parent display names
    children: list[SwissEntry] = field(default_factory=list)

    # Dataset-specific fields
    description: str = ""
    updated: str = ""            # metadata_modified from CKAN
    organization: str = ""       # org slug (e.g., "bundesamt-fur-statistik-bfs")
    license_url: str = ""        # terms-of-use rights URL
    identifier: str = ""         # CKAN identifier field
    groups: list[str] = field(default_factory=list)  # CKAN group names

    # Download resources (filtered to data formats only)
    resources: list[dict] = field(default_factory=list)
    # Each resource dict: {format, download_url, url, language, byte_size}

    @property
    def display_name(self) -> str:
        """Folder name using slugified title (groups) or code (datasets)."""
        if self.is_dataset:
            return self.code
        return _slugify(self.title) if self.title else self.code

    @property
    def full_path(self) -> str:
        """Code-based path (for state tracking, --filter-path)."""
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


def collect_datasets(entry: SwissEntry) -> list[SwissEntry]:
    """Flatten the tree to get all dataset entries."""
    results: list[SwissEntry] = []
    if entry.is_dataset:
        results.append(entry)
    for child in entry.children:
        results.extend(collect_datasets(child))
    return results


def _get_title(pkg: dict, lang: str = "en") -> str:
    """Extract best title from a CKAN package (multilingual or plain)."""
    title = pkg.get("title", {})
    if isinstance(title, dict):
        return title.get(lang) or title.get("de") or title.get("fr") or next(iter(title.values()), "")
    return str(title) if title else pkg.get("name", "")


def _get_description(pkg: dict, lang: str = "en") -> str:
    """Extract best description from a CKAN package."""
    desc = pkg.get("description", {}) or pkg.get("notes", "")
    if isinstance(desc, dict):
        return desc.get(lang) or desc.get("de") or desc.get("fr") or next(iter(desc.values()), "")
    return str(desc) if desc else ""


def _extract_resources(
    pkg: dict,
    download_formats: set[str],
) -> list[dict]:
    """Filter and extract downloadable resources from a CKAN package.

    CKAN resources have two URL fields:
    - download_url: direct file download (present for actual data files)
    - url: may point to a web page rather than a file

    We prefer download_url when available, falling back to url.  Some
    resources have format="CSV" but point to HTML pages (landing pages),
    so we also filter by URL suffix and media_type to catch these.
    """
    result = []
    for res in pkg.get("resources", []):
        fmt = (res.get("format") or "").upper()
        if fmt not in download_formats:
            continue
        # Prefer download_url (direct file link), fall back to url (may be a page)
        dl_url = res.get("download_url") or res.get("url") or ""
        if not dl_url:
            continue
        # Guard against misclassified resources: some have format=CSV but
        # the URL is an HTML landing page, not a downloadable file.
        if dl_url.endswith(".html") or "text/html" in (res.get("media_type") or ""):
            continue
        lang = res.get("language") or []
        if isinstance(lang, str):
            lang = [lang]
        result.append({
            "format": fmt,
            "download_url": dl_url,
            "url": res.get("url", ""),
            "language": lang,
            "byte_size": int(res.get("byte_size") or 0),
        })
    return result


_CATALOG_MAX_RETRIES = 4
_CATALOG_BACKOFF_BASE = 10.0


async def fetch_catalog(
    client: httpx.AsyncClient,
    *,
    output_dir: Path | None = None,
    org_filter: str = api.BFS_ORG,
    download_formats: set[str] | None = None,
) -> SwissEntry:
    """Fetch the Swiss open data catalog from CKAN.

    1. Paginate through package_search to get all datasets
    2. Filter resources to downloadable data formats
    3. Organise into a tree by CKAN group (theme)

    Returns the root SwissEntry with groups and datasets.
    """
    if download_formats is None:
        download_formats = api.DEFAULT_DOWNLOAD_FORMATS

    # Normalise to uppercase for matching
    download_formats = {f.upper() for f in download_formats}

    # Step 1: Fetch all datasets (paginated)
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
                    log.warning(
                        "HTTP %d on catalog page — retry %d/%d in %.0fs",
                        resp.status_code, attempt, _CATALOG_MAX_RETRIES, wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < _CATALOG_MAX_RETRIES:
                    wait = _CATALOG_BACKOFF_BASE * (2 ** (attempt - 1))
                    log.warning("Connection error — retry %d/%d in %.0fs", attempt, _CATALOG_MAX_RETRIES, wait)
                    await asyncio.sleep(wait)
                    continue
                raise
        else:
            # Python for...else: this branch fires only when the loop
            # completed without 'break' — i.e., all retry attempts exhausted.
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
        log.info("Fetched %d/%d datasets from opendata.swiss", len(all_items), total)

        if start >= total or not items:
            break
        # Polite delay between pages
        await asyncio.sleep(1.0)

    log.info("Total datasets from CKAN: %d", len(all_items))

    # Step 2: Build tree by groups (themes)
    root = SwissEntry(
        code="swiss",
        title="Swiss Open Data",
        entry_type="root",
    )

    groups: dict[str, SwissEntry] = {}  # group_name → group entry

    skipped_no_resources = 0

    for pkg in all_items:
        pkg_name = pkg.get("name", "")
        if not pkg_name:
            continue

        # Filter resources to downloadable formats
        resources = _extract_resources(pkg, download_formats)
        if not resources:
            skipped_no_resources += 1
            continue

        # Extract group info
        pkg_groups = pkg.get("groups", [])
        if pkg_groups:
            grp = pkg_groups[0]  # use first group to avoid duplication
            grp_name = grp.get("name", "uncategorised")
            grp_title = grp.get("display_name") or grp.get("title", grp_name)
            if isinstance(grp_title, dict):
                grp_title = grp_title.get("en") or grp_title.get("de") or next(iter(grp_title.values()), grp_name)
        else:
            grp_name = "uncategorised"
            grp_title = "Uncategorised"

        # Create group entry if needed
        if grp_name not in groups:
            groups[grp_name] = SwissEntry(
                code=grp_name,
                title=grp_title,
                entry_type="group",
                parent_path=root.code,
                parent_folder_path=root.display_name,
            )

        grp_entry = groups[grp_name]

        # Extract license/rights
        rights = ""
        for res in pkg.get("resources", []):
            r = res.get("rights", "")
            if r:
                rights = r
                break

        # Build dataset entry
        org = pkg.get("organization", {})
        org_name = org.get("name", "") if isinstance(org, dict) else ""

        ds = SwissEntry(
            code=pkg_name,
            title=_get_title(pkg),
            entry_type="dataset",
            parent_path=grp_entry.full_path,
            parent_folder_path=str(grp_entry.folder_path),
            description=_get_description(pkg),
            updated=pkg.get("metadata_modified", ""),
            organization=org_name,
            license_url=rights,
            identifier=pkg.get("identifier", ""),
            groups=[g.get("name", "") for g in pkg_groups],
            resources=resources,
        )
        grp_entry.children.append(ds)

    if skipped_no_resources:
        log.info("Skipped %d datasets with no downloadable resources", skipped_no_resources)

    # Attach groups to root, sorted
    for grp_name in sorted(groups.keys()):
        grp_entry = groups[grp_name]
        if grp_entry.children:  # skip empty groups
            root.children.append(grp_entry)

    total_datasets = sum(len(g.children) for g in root.children)
    log.info(
        "Swiss catalog: %d groups, %d datasets (with downloadable resources)",
        len(root.children), total_datasets,
    )

    return root
