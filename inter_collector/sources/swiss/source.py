"""Swiss open data source via opendata.swiss (CKAN API).

Portal: https://opendata.swiss
API:    https://ckan.opendata.swiss/api/3/action/
Docs:   https://handbook.opendata.swiss/de/content/nutzen/api-nutzen.html

opendata.swiss is Switzerland's official open government data portal,
managed by the Federal Statistical Office (BFS). It aggregates datasets
from ~288 organizations including federal agencies, cantons, and cities.

Default scope: BFS only (~3,277 datasets).
Use --scope all to include all organizations (~14,300 datasets).

For each dataset, the collector downloads:
  - Catalog info snapshot (_info.json)
  - Data resources in available formats (CSV, XLS, ODS, JSON)

Authentication: None required for read operations.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from . import api
from ...base import DataSource, SourceConfig
from ...download_utils import BytesCallback, DownloadResult
from .catalog import (
    FOLDER_STYLE_DISPLAY,
    SwissEntry,
    collect_datasets as _collect_datasets,
    fetch_catalog,
)
from .downloader import download_dataset as _download_dataset


class SwissSource(DataSource):
    """Swiss open data (opendata.swiss — CKAN API).

    Downloads datasets from the official Swiss open government data portal.
    Default scope is BFS (Federal Statistical Office) only.
    """

    def __init__(
        self,
        *,
        org_filter: str = api.BFS_ORG,
        download_formats: set[str] | None = None,
    ):
        """Initialise the Swiss data source.

        Args:
            org_filter:       CKAN organization slug to filter by.
                              Default "bundesamt-fur-statistik-bfs" (BFS only).
                              Set to "" for all organizations on opendata.swiss.
            download_formats: Set of format strings to download (e.g., {"CSV", "XLS"}).
                              Default: all data formats (CSV, XLS, ODS, JSON).
                              Passed through to catalog and downloader for filtering.
        """
        self._org_filter = org_filter
        self._download_formats = download_formats or api.DEFAULT_DOWNLOAD_FORMATS

    def config(self) -> SourceConfig:
        return SourceConfig(
            name="swiss",
            display_name="Swiss FSO (opendata.swiss)",
            default_output_subdir="swiss",
            state_filename=".swiss_state.json",
            tree_index_filename="swiss_tree_index.json",
            file_type_groups={
                ".csv": "CSV data",
                ".xls": "Excel data",
                ".xlsx": "Excel data",
                ".ods": "ODS data",
                ".json": "JSON data",
                "_info.json": "Dataset info",
            },
            data_file_types={"csv", "xls", "xlsx", "ods", "json"},
        )

    async def fetch_catalog(
        self,
        client: httpx.AsyncClient,
        *,
        output_dir: Path | None = None,
    ) -> SwissEntry:
        return await fetch_catalog(
            client,
            output_dir=output_dir,
            org_filter=self._org_filter,
            download_formats=self._download_formats,
        )

    def collect_datasets(self, catalog: SwissEntry) -> list[SwissEntry]:
        return _collect_datasets(catalog)

    async def download_dataset(
        self,
        client: httpx.AsyncClient,
        entry: SwissEntry,
        output_dir: Path,
        *,
        skip_existing: bool = True,
        on_bytes: BytesCallback | None = None,
        folder_style: str = FOLDER_STYLE_DISPLAY,
    ) -> DownloadResult:
        return await _download_dataset(
            client, entry, output_dir,
            skip_existing=skip_existing,
            on_bytes=on_bytes,
            folder_style=folder_style,
            download_formats=self._download_formats,
        )

    def save_tree_index(self, catalog: SwissEntry, output_dir: Path) -> Path:
        """Save the Swiss catalog tree as a JSON index file."""

        def _to_dict(entry: SwissEntry) -> dict:
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
