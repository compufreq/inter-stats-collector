"""Parse Eurostat Table of Contents (TOC) XML to build category tree and dataset mapping."""

from __future__ import annotations

import re
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx

from . import api


def _slugify(title: str, max_length: int = 60) -> str:
    """Convert a human-readable title to a filesystem-safe folder name.

    Examples:
        "General and regional statistics" → "general_and_regional_statistics"
        "Economy and finance"             → "economy_and_finance"
        "HICP (2015 = 100)"              → "hicp_2015_100"
    """
    # Normalize unicode (accented chars → ASCII equivalents)
    s = unicodedata.normalize("NFKD", title)
    s = s.encode("ascii", "ignore").decode("ascii")
    # Lowercase
    s = s.lower()
    # Replace any non-alphanumeric character with underscore
    s = re.sub(r"[^a-z0-9]+", "_", s)
    # Collapse multiple underscores
    s = re.sub(r"_+", "_", s)
    # Strip leading/trailing underscores
    s = s.strip("_")
    # Truncate to max_length at a word boundary if possible
    if len(s) > max_length:
        s = s[:max_length].rsplit("_", 1)[0]
    return s or "unnamed"


# Folder naming styles
FOLDER_STYLE_DISPLAY = "display"  # slugified titles for categories, codes for datasets
FOLDER_STYLE_CODE = "code"  # raw Eurostat codes for everything


@dataclass
class TocEntry:
    """A node in the Eurostat category tree (folder or dataset)."""

    code: str
    title: str
    entry_type: str  # "folder", "dataset", "table"
    parent_path: str = ""  # slash-separated path of parent codes (internal)
    parent_folder_path: str = ""  # slash-separated path of parent display names (disk)
    children: list[TocEntry] = field(default_factory=list)
    last_update: str = ""
    last_structure_change: str = ""
    data_start: str = ""
    data_end: str = ""
    values: str = ""
    metadata_url: str = ""
    sdmx_download: str = ""
    tsv_download: str = ""
    subtitle: str = ""
    short_description: str = ""

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
        return self.entry_type in ("dataset", "table")

    @property
    def folder_path(self) -> Path:
        """Filesystem path using display names (default).

        Uses human-readable slugified titles for category folders
        and canonical codes for dataset leaves.
        """
        if self.parent_folder_path:
            return Path(self.parent_folder_path) / self.display_name
        return Path(self.display_name)

    @property
    def folder_path_code(self) -> Path:
        """Filesystem path using raw Eurostat codes for everything."""
        return Path(self.full_path)

    def get_folder_path(self, style: str = FOLDER_STYLE_DISPLAY) -> Path:
        """Get filesystem path for the chosen naming style.

        Args:
            style: "display" for slugified titles, "code" for raw Eurostat codes
        """
        if style == FOLDER_STYLE_CODE:
            return self.folder_path_code
        return self.folder_path


NS = {
    "nt": "urn:eu.europa.ec.eurostat.navtree",
}


def _text(el: ET.Element | None, tag: str, ns: dict = NS, lang: str = "en") -> str:
    """Extract text from a child element, optionally filtering by language."""
    if el is None:
        return ""

    # Try language-specific variant first (e.g., title.en)
    child = el.find(f"nt:{tag}.{lang}", ns)
    if child is not None and child.text:
        return child.text.strip()

    # Fall back to plain tag
    child = el.find(f"nt:{tag}", ns)
    if child is not None and child.text:
        return child.text.strip()
    return ""


def _parse_branch(
    el: ET.Element,
    parent_path: str = "",
    parent_folder_path: str = "",
) -> TocEntry:
    """Recursively parse a <branch> or <leaf> element into a TocEntry."""
    code = _text(el, "code")
    title = _text(el, "title")

    # Determine type
    tag_local = el.tag.split("}")[-1] if "}" in el.tag else el.tag
    if tag_local == "branch":
        entry_type = "folder"
    else:
        # leaf type from children
        type_el = el.find("nt:type", NS)
        entry_type = type_el.text.strip().lower() if type_el is not None and type_el.text else "dataset"

    entry = TocEntry(
        code=code,
        title=title,
        entry_type=entry_type,
        parent_path=parent_path,
        parent_folder_path=parent_folder_path,
        last_update=_text(el, "lastUpdate"),
        last_structure_change=_text(el, "lastModified"),
        data_start=_text(el, "dataStart"),
        data_end=_text(el, "dataEnd"),
        values=_text(el, "values"),
        metadata_url=_text(el, "metadata"),
        subtitle=_text(el, "shortDescription"),
    )

    # Parse download links
    download_link = el.find("nt:downloadLink", NS)
    if download_link is not None:
        sdmx = download_link.find("nt:sdmx", NS)
        tsv = download_link.find("nt:tsv", NS)
        if sdmx is not None and sdmx.text:
            entry.sdmx_download = sdmx.text.strip()
        if tsv is not None and tsv.text:
            entry.tsv_download = tsv.text.strip()

    # Recurse into children, propagating both code-based and display paths
    current_path = entry.full_path
    current_folder_path = str(entry.folder_path)
    for child_el in el.findall("nt:children/nt:branch", NS):
        entry.children.append(_parse_branch(child_el, current_path, current_folder_path))
    for child_el in el.findall("nt:children/nt:leaf", NS):
        entry.children.append(_parse_branch(child_el, current_path, current_folder_path))

    return entry


def collect_datasets(entry: TocEntry) -> list[TocEntry]:
    """Flatten the tree to get all dataset/table leaf entries."""
    results: list[TocEntry] = []
    if entry.is_dataset:
        results.append(entry)
    for child in entry.children:
        results.extend(collect_datasets(child))
    return results


async def fetch_toc(client: httpx.AsyncClient) -> TocEntry:
    """Download and parse the Eurostat TOC XML into a tree."""
    resp = await client.get(api.TOC_XML, timeout=120)
    resp.raise_for_status()

    root_el = ET.fromstring(resp.content)

    # The root element contains <branch> children
    root = TocEntry(code="eurostat", title="Eurostat", entry_type="folder")
    root_display = root.display_name  # "eurostat"

    for branch_el in root_el.findall("nt:branch", NS):
        root.children.append(_parse_branch(branch_el, root.code, root_display))
    for leaf_el in root_el.findall("nt:leaf", NS):
        root.children.append(_parse_branch(leaf_el, root.code, root_display))

    return root


def print_tree(entry: TocEntry, indent: int = 0) -> None:
    """Debug: print the category tree."""
    prefix = "  " * indent
    icon = "📁" if not entry.is_dataset else "📊"
    print(f"{prefix}{icon} {entry.code} - {entry.title}")
    for child in entry.children:
        print_tree(child, indent + 1)
