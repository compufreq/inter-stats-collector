"""Generic CKAN data source — base class for all CKAN-based portals.

Subclasses only need to provide a CkanPortalConfig instance.
All catalog, download, and tree index logic is handled generically.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from ...base import DataSource, SourceConfig
from ...download_utils import BytesCallback, DownloadResult
from .catalog import (
    FOLDER_STYLE_DISPLAY,
    CkanEntry,
    collect_datasets as _collect_datasets,
    fetch_catalog as _fetch_catalog,
)
from .config import CkanPortalConfig
from .downloader import download_dataset as _download_dataset


class CkanSource(DataSource):
    """Generic CKAN data source.

    Parameterized by a CkanPortalConfig instance that controls
    API endpoints, default filters, field extraction, and
    filename strategies.  All CKAN-based portals (Swiss, HDX,
    Netherlands, Germany, etc.) use this class with different configs.
    """

    def __init__(
        self,
        portal_config: CkanPortalConfig,
        *,
        org_filter: str | None = None,
        download_formats: set[str] | None = None,
    ):
        """Initialise a CKAN data source.

        Args:
            portal_config:    Portal-specific configuration.
            org_filter:       CKAN organization slug to filter by.
                              None = use portal default. "" = all orgs.
            download_formats: Set of format strings to download.
                              None = use portal default.
        """
        self._portal = portal_config
        self._org_filter = org_filter if org_filter is not None else portal_config.default_org
        self._download_formats = download_formats or portal_config.default_formats

    def config(self) -> SourceConfig:
        """Return the SourceConfig for this portal."""
        return SourceConfig(
            name=self._portal.name,
            display_name=self._portal.display_name,
            default_output_subdir=self._portal.output_subdir,
            state_filename=self._portal.state_filename,
            tree_index_filename=self._portal.tree_index_filename,
            file_type_groups=dict(self._portal.file_type_groups),
            data_file_types=set(self._portal.data_file_types),
            recommended_concurrency=self._portal.recommended_concurrency,
        )

    async def fetch_catalog(
        self,
        client: httpx.AsyncClient,
        *,
        output_dir: Path | None = None,
    ) -> CkanEntry:
        """Fetch the CKAN catalog for this portal."""
        return await _fetch_catalog(
            client,
            self._portal,
            output_dir=output_dir,
            org_filter=self._org_filter,
            download_formats=self._download_formats,
        )

    def collect_datasets(self, catalog: CkanEntry) -> list[CkanEntry]:
        """Flatten the catalog tree to a list of dataset entries."""
        return _collect_datasets(catalog)

    async def download_dataset(
        self,
        client: httpx.AsyncClient,
        entry: CkanEntry,
        output_dir: Path,
        *,
        skip_existing: bool = True,
        on_bytes: BytesCallback | None = None,
        folder_style: str = FOLDER_STYLE_DISPLAY,
    ) -> DownloadResult:
        """Download all resources for a single dataset."""
        return await _download_dataset(
            client, entry, output_dir, self._portal,
            skip_existing=skip_existing,
            on_bytes=on_bytes,
            folder_style=folder_style,
            download_formats=self._download_formats,
        )

    def save_tree_index(self, catalog: CkanEntry, output_dir: Path) -> Path:
        """Save the catalog tree as a JSON index file."""

        def _to_dict(entry: CkanEntry) -> dict:
            d = {
                "code": entry.code,
                "title": entry.title,
                "type": entry.entry_type,
                "path": entry.full_path,
                "folder_path": str(entry.folder_path),
            }
            if entry.is_dataset:
                d["updated"] = entry.updated
                d["organization"] = entry.organization
                d["license_url"] = entry.license_url
                d["description"] = entry.description[:200] if entry.description else ""
                d["groups"] = entry.groups
                d["resources"] = [
                    {"format": r["format"], "download_url": r["download_url"]}
                    for r in entry.resources
                ]
            if entry.children:
                d["children"] = [_to_dict(c) for c in entry.children]
            return d

        index = _to_dict(catalog)
        index_file = output_dir / self.config().tree_index_filename
        index_file.write_text(json.dumps(index, indent=2, ensure_ascii=False))
        return index_file
