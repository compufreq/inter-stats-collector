"""UNHCR Refugee Statistics data source implementation.

API:     https://api.unhcr.org/population/v1/
Docs:    https://api.unhcr.org/docs/refugee-statistics.html
Portal:  https://www.unhcr.org/refugee-statistics

License: Creative Commons Attribution 4.0 International (CC BY 4.0)
Attribution: "UNHCR Refugee Population Statistics Database"

The UNHCR Refugee Statistics API provides access to global displacement
data spanning 1951–present across 6 data endpoints:
  - Population statistics (refugees, asylum-seekers, IDPs, stateless)
  - Asylum applications
  - Asylum decisions
  - Durable solutions (return, resettlement, naturalisation)
  - Demographics (age/gender breakdown)
  - UNRWA-registered Palestine refugees

For each endpoint+year, the collector downloads all paginated rows
as a single JSON file with country-level breakdown.
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
    UNHCREntry,
    collect_datasets as _collect_datasets,
    fetch_catalog,
)
from .downloader import download_dataset as _download_dataset


class UNHCRSource(DataSource):
    """UNHCR Refugee Statistics (Refugee Data Finder API).

    Downloads global displacement data from the UNHCR Population
    Statistics API. Data is organized by endpoint type and year.
    """

    def __init__(
        self,
        *,
        year_from: int | None = None,
        year_to: int | None = None,
    ):
        self._year_from = year_from
        self._year_to = year_to

    def config(self) -> SourceConfig:
        return SourceConfig(
            name="unhcr",
            display_name="UNHCR Refugee Statistics",
            default_output_subdir="unhcr",
            state_filename=".unhcr_state.json",
            tree_index_filename="unhcr_tree_index.json",
            file_type_groups={
                "_data.json": "Dataset data (JSON)",
                "_info.json": "Dataset info",
            },
            data_file_types={"data"},
            recommended_concurrency=api.RECOMMENDED_CONCURRENCY,
        )

    async def fetch_catalog(
        self,
        client: httpx.AsyncClient,
        *,
        output_dir: Path | None = None,
    ) -> UNHCREntry:
        return await fetch_catalog(
            client,
            output_dir=output_dir,
            year_from=self._year_from,
            year_to=self._year_to,
        )

    def collect_datasets(self, catalog: UNHCREntry) -> list[UNHCREntry]:
        return _collect_datasets(catalog)

    async def download_dataset(
        self,
        client: httpx.AsyncClient,
        entry: UNHCREntry,
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
        )

    def save_tree_index(self, catalog: UNHCREntry, output_dir: Path) -> Path:
        """Save the UNHCR catalog tree as a JSON index file."""

        def _to_dict(entry: UNHCREntry) -> dict:
            d = {
                "code": entry.code,
                "title": entry.title,
                "type": entry.entry_type,
                "path": entry.full_path,
                "folder_path": str(entry.folder_path),
            }
            if entry.is_dataset:
                d["endpoint"] = entry.endpoint_key
                d["year"] = entry.year
                d["max_pages"] = entry.max_pages
                d["description"] = entry.description
            if entry.children:
                d["children"] = [_to_dict(c) for c in entry.children]
            return d

        index = _to_dict(catalog)
        index_file = output_dir / self.config().tree_index_filename
        index_file.write_text(json.dumps(index, indent=2, ensure_ascii=False))
        return index_file
