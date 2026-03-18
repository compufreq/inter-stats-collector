"""HDX (Humanitarian Data Exchange) data source implementation.

Portal:  https://data.humdata.org
API:     https://data.humdata.org/api/3/action/
Docs:    https://data.humdata.org/faqs/devs

HDX is OCHA's open platform for sharing humanitarian data. It hosts
datasets from hundreds of organizations including UNHCR, WHO, UNICEF,
WFP, and IOM.

Default scope: UNHCR datasets only (~1,114 datasets).
Use --scope all to include all organizations.

License: Varies per dataset. Most UNHCR data is CC BY-IGO.
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
    HDXEntry,
    collect_datasets as _collect_datasets,
    fetch_catalog,
)
from .downloader import download_dataset as _download_dataset


class HDXSource(DataSource):
    """HDX — Humanitarian Data Exchange (CKAN).

    Downloads humanitarian datasets from OCHA's open data platform.
    Default scope is UNHCR datasets only.
    """

    def __init__(
        self,
        *,
        org_filter: str = api.DEFAULT_ORG,
        download_formats: set[str] | None = None,
    ):
        self._org_filter = org_filter
        self._download_formats = download_formats or api.DEFAULT_DOWNLOAD_FORMATS

    def config(self) -> SourceConfig:
        return SourceConfig(
            name="hdx",
            display_name="HDX (Humanitarian Data Exchange)",
            default_output_subdir="hdx",
            state_filename=".hdx_state.json",
            tree_index_filename="hdx_tree_index.json",
            file_type_groups={
                ".csv": "CSV data",
                ".xls": "Excel data",
                ".xlsx": "Excel data",
                "_info.json": "Dataset info",
            },
            data_file_types={"csv", "xls", "xlsx"},
        )

    async def fetch_catalog(
        self,
        client: httpx.AsyncClient,
        *,
        output_dir: Path | None = None,
    ) -> HDXEntry:
        return await fetch_catalog(
            client,
            output_dir=output_dir,
            org_filter=self._org_filter,
            download_formats=self._download_formats,
        )

    def collect_datasets(self, catalog: HDXEntry) -> list[HDXEntry]:
        return _collect_datasets(catalog)

    async def download_dataset(
        self,
        client: httpx.AsyncClient,
        entry: HDXEntry,
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

    def save_tree_index(self, catalog: HDXEntry, output_dir: Path) -> Path:
        """Save the HDX catalog tree as a JSON index file."""

        def _to_dict(entry: HDXEntry) -> dict:
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
                d["license_title"] = entry.license_title
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
