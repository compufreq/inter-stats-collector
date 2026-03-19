"""Generic CKAN catalog fetcher — works with any CKAN-based open data portal.

Fetches datasets via the CKAN package_search API, filters resources
to downloadable data formats, and organises them into a tree by
CKAN group (theme).
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from .config import CkanPortalConfig

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
class CkanEntry:
    """A node in a CKAN catalog tree (root, group, or dataset).

    Used by all CKAN-based sources (Swiss, HDX, Netherlands, Germany, etc.).
    """

    code: str                    # CKAN package name or group slug
    title: str                   # human-readable title
    entry_type: str              # "root", "group", "dataset"
    parent_path: str = ""        # slash-separated path of parent codes
    parent_folder_path: str = "" # slash-separated path of parent display names
    children: list[CkanEntry] = field(default_factory=list)

    # Dataset-specific fields
    description: str = ""
    updated: str = ""            # metadata_modified from CKAN
    organization: str = ""       # org slug
    license_url: str = ""        # terms-of-use / license URL
    license_title: str = ""      # license name (if available)
    identifier: str = ""         # CKAN identifier field
    groups: list[str] = field(default_factory=list)  # CKAN group names

    # Download resources (filtered to data formats only)
    resources: list[dict] = field(default_factory=list)
    # Each resource dict: {format, download_url, url, language, byte_size, name}

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


def collect_datasets(entry: CkanEntry) -> list[CkanEntry]:
    """Flatten the tree to get all dataset entries."""
    results: list[CkanEntry] = []
    if entry.is_dataset:
        results.append(entry)
    for child in entry.children:
        results.extend(collect_datasets(child))
    return results


def _get_text(value, langs: tuple[str, ...] = ("en",), multilingual: bool = False) -> str:
    """Extract text from a CKAN field that may be a string or multilingual dict."""
    if not value:
        return ""
    if isinstance(value, dict) and multilingual:
        for lang in langs:
            if lang in value and value[lang]:
                return str(value[lang])
        # Fall back to any available value
        for v in value.values():
            if v:
                return str(v)
        return ""
    return str(value)


def _get_description(pkg: dict, config: CkanPortalConfig) -> str:
    """Extract description from a CKAN package, checking configured fields."""
    for field_name in config.description_fields:
        val = pkg.get(field_name)
        if val:
            return _get_text(val, config.title_langs, config.multilingual)
    return ""


def _extract_resources(
    pkg: dict,
    download_formats: set[str],
    config: CkanPortalConfig,
) -> list[dict]:
    """Filter and extract downloadable resources from a CKAN package.

    CKAN resources have two URL fields:
    - download_url: direct file download (present for actual data files)
    - url: may point to a web page rather than a file

    We prefer download_url when available, falling back to url.  Some
    resources have format=CSV but point to HTML pages (landing pages),
    so we filter by URL suffix and media_type to catch these.
    """
    result = []
    for res in pkg.get("resources", []):
        fmt = (res.get("format") or "").upper()
        # Handle EU authority URI format labels, e.g.:
        #   "http://publications.europa.eu/resource/authority/file-type/CSV" → "CSV"
        if fmt.startswith("HTTP"):
            fmt = fmt.rsplit("/", 1)[-1].upper()
        if fmt not in download_formats:
            continue
        # Prefer download_url (direct file link), fall back to url
        dl_url = res.get("download_url") or res.get("url") or ""
        # Some CKAN portals return lists — take first element
        if isinstance(dl_url, list):
            dl_url = dl_url[0] if dl_url else ""
        if not dl_url or not isinstance(dl_url, str):
            continue
        # Guard against misclassified resources
        if dl_url.endswith(".html"):
            continue
        # Check all configured media type fields for text/html
        is_html = False
        for mt_field in config.media_type_fields:
            if "text/html" in (res.get(mt_field) or ""):
                is_html = True
                break
        if is_html:
            continue

        lang = res.get("language") or []
        if isinstance(lang, str):
            lang = [lang]
        result.append({
            "format": fmt,
            "download_url": dl_url,
            "url": res.get("url", ""),
            "language": lang,
            "byte_size": int(res.get("byte_size") or res.get("size") or 0),
            "name": res.get("name", ""),
        })
    return result


_CATALOG_MAX_RETRIES = 4
_CATALOG_BACKOFF_BASE = 10.0


async def fetch_catalog(
    client: httpx.AsyncClient,
    config: CkanPortalConfig,
    *,
    output_dir: Path | None = None,
    org_filter: str | None = None,
    download_formats: set[str] | None = None,
) -> CkanEntry:
    """Fetch a CKAN catalog and organise into a tree by group.

    1. Paginate through package_search to get all datasets
    2. Filter resources to downloadable data formats
    3. Organise into a tree by CKAN group (theme)

    Args:
        client:           Async HTTP client.
        config:           Portal configuration.
        output_dir:       Not used directly, reserved for subclass compatibility.
        org_filter:       Organization slug to filter by (None = use config default).
        download_formats: Format set to download (None = use config default).

    Returns the root CkanEntry with groups and datasets.
    """
    if org_filter is None:
        org_filter = config.default_org
    if download_formats is None:
        download_formats = config.default_formats

    # Normalise to uppercase for matching
    download_formats = {f.upper() for f in download_formats}

    # Step 1: Fetch all datasets (paginated)
    all_items: list[dict] = []
    start = 0
    base_url = config.ckan_base_url.rstrip("/")

    fq = f"organization:{org_filter}" if org_filter else ""

    while True:
        url = f"{base_url}/package_search?rows={config.page_size}&start={start}"
        if fq:
            url += f"&fq={fq}"

        for attempt in range(1, _CATALOG_MAX_RETRIES + 1):
            try:
                resp = await client.get(url, timeout=60)
                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = _CATALOG_BACKOFF_BASE * (2 ** (attempt - 1))
                    log.warning(
                        "HTTP %d on %s catalog page — retry %d/%d in %.0fs",
                        resp.status_code, config.name, attempt, _CATALOG_MAX_RETRIES, wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                break
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < _CATALOG_MAX_RETRIES:
                    wait = _CATALOG_BACKOFF_BASE * (2 ** (attempt - 1))
                    log.warning("Connection error on %s — retry %d/%d in %.0fs",
                                config.name, attempt, _CATALOG_MAX_RETRIES, wait)
                    await asyncio.sleep(wait)
                    continue
                raise
        else:
            # Python for...else: all retry attempts exhausted
            log.error("Exhausted retries for %s catalog page at start=%d", config.name, start)
            break

        body = resp.json()
        if not body.get("success"):
            log.error("CKAN API returned success=false for %s at start=%d", config.name, start)
            break

        result = body.get("result", {})
        items = result.get("results", [])
        total = result.get("count", 0)
        all_items.extend(items)

        start += len(items)
        log.info("[%s] Fetched %d/%d datasets", config.name, len(all_items), total)

        if start >= total or not items:
            break
        # Polite delay between pages
        await asyncio.sleep(1.0)

    log.info("[%s] Total datasets from CKAN: %d", config.name, len(all_items))

    # Step 2: Build tree by groups (themes)
    root = CkanEntry(
        code=config.root_code,
        title=config.root_title,
        entry_type="root",
    )

    groups: dict[str, CkanEntry] = {}
    skipped_no_resources = 0

    for pkg in all_items:
        pkg_name = pkg.get("name", "")
        if not pkg_name:
            continue

        # Filter resources to downloadable formats
        resources = _extract_resources(pkg, download_formats, config)
        if not resources:
            skipped_no_resources += 1
            continue

        # Extract group info
        pkg_groups = pkg.get("groups", [])
        if pkg_groups:
            grp = pkg_groups[0]  # use first group to avoid duplication
            grp_name = grp.get("name", "uncategorised")
            grp_title = grp.get("display_name") or grp.get("title", grp_name)
            grp_title = _get_text(grp_title, config.title_langs, config.multilingual) or grp_name
        else:
            grp_name = "uncategorised"
            grp_title = "Uncategorised"

        # Create group entry if needed
        if grp_name not in groups:
            groups[grp_name] = CkanEntry(
                code=grp_name,
                title=grp_title,
                entry_type="group",
                parent_path=root.code,
                parent_folder_path=root.display_name,
            )

        grp_entry = groups[grp_name]

        # Extract license/rights
        rights = ""
        rights_title = ""
        for res in pkg.get("resources", []):
            r = res.get("rights", "") or res.get("license", "")
            if r:
                rights = r
                break
        rights_title = pkg.get("license_title", "") or ""

        # Build dataset entry
        org = pkg.get("organization", {})
        org_name = org.get("name", "") if isinstance(org, dict) else ""

        title = _get_text(
            pkg.get("title", pkg_name),
            config.title_langs,
            config.multilingual,
        )

        ds = CkanEntry(
            code=pkg_name,
            title=title,
            entry_type="dataset",
            parent_path=grp_entry.full_path,
            parent_folder_path=str(grp_entry.folder_path),
            description=_get_description(pkg, config),
            updated=pkg.get("metadata_modified", ""),
            organization=org_name,
            license_url=rights,
            license_title=rights_title,
            identifier=pkg.get("identifier", ""),
            groups=[g.get("name", "") for g in pkg_groups],
            resources=resources,
        )
        grp_entry.children.append(ds)

    if skipped_no_resources:
        log.info("[%s] Skipped %d datasets with no downloadable resources",
                 config.name, skipped_no_resources)

    # Attach groups to root, sorted
    for grp_name in sorted(groups.keys()):
        grp_entry = groups[grp_name]
        if grp_entry.children:
            root.children.append(grp_entry)

    total_datasets = sum(len(g.children) for g in root.children)
    log.info("[%s] Catalog: %d groups, %d datasets (with downloadable resources)",
             config.name, len(root.children), total_datasets)

    return root
