"""UK Office for National Statistics (ONS) data source implementation.

API Details:
- Developer Hub: https://developer.ons.gov.uk/
- API Base URL: https://api.beta.ons.gov.uk/v1
- List all datasets: GET /datasets (paginated, ~337 datasets)
- Dataset structure: /datasets/{id}/editions/{edition}/versions/{version}
- Downloads served from: https://download.ons.gov.uk
- Formats: CSV, XLSX, CSV-W (metadata JSON)
- Authentication: None required (open API)
- Status: Beta (may have breaking changes)

The ONS CMD (Customise My Data) API provides access to UK statistical
datasets organised by taxonomy.  For each dataset, the collector downloads:
  - Dataset catalog info snapshot
  - Version metadata (dimensions, release info)
  - Data in CSV format
  - Data in Excel (XLSX) format
  - CSV-W metadata (data dictionary)
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from ...base import DataSource, SourceConfig
from ...download_utils import BytesCallback, DownloadResult
from .catalog import (
    FOLDER_STYLE_DISPLAY,
    ONSEntry,
    collect_datasets as _collect_datasets,
    fetch_catalog,
)
from .downloader import download_dataset as _download_dataset


class ONSSource(DataSource):
    """UK Office for National Statistics (ONS).

    Downloads statistical datasets from the CMD API at
    https://api.beta.ons.gov.uk/v1

    For each dataset, downloads:
    - Dataset catalog info snapshot
    - Version metadata JSON (dimensions, release info)
    - Data in CSV and XLSX formats
    - CSV-W metadata (data dictionary)
    """

    def config(self) -> SourceConfig:
        return SourceConfig(
            name="ons",
            display_name="UK ONS",
            default_output_subdir="ons",
            state_filename=".ons_state.json",
            tree_index_filename="ons_tree_index.json",
            file_type_groups={
                ".csv": "CSV data",
                ".xlsx": "Excel data",
                ".csv-metadata.json": "CSV-W metadata",
                "_meta.json": "Version metadata",
                "_info.json": "Dataset info",
            },
            recommended_concurrency=2,
            data_file_types={"csv", "xlsx", "csvw"},
        )

    async def fetch_catalog(
        self,
        client: httpx.AsyncClient,
        *,
        output_dir: Path | None = None,
    ) -> ONSEntry:
        return await fetch_catalog(client)

    def collect_datasets(self, catalog: ONSEntry) -> list[ONSEntry]:
        return _collect_datasets(catalog)

    async def download_dataset(
        self,
        client: httpx.AsyncClient,
        entry: ONSEntry,
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

    def save_tree_index(self, catalog: ONSEntry, output_dir: Path) -> Path:
        """Save the full ONS catalog tree as a JSON index file."""

        def _to_dict(entry: ONSEntry) -> dict:
            d = {
                "code": entry.code,
                "title": entry.title,
                "type": entry.entry_type,
                "path": entry.full_path,
                "folder_path": str(entry.folder_path),
            }
            if entry.is_dataset:
                d["description"] = entry.description
                d["taxonomy_path"] = entry.taxonomy_path
                d["edition"] = entry.edition
                d["version"] = entry.version
                d["last_updated"] = entry.last_updated
                d["release_frequency"] = entry.release_frequency
                d["national_statistic"] = entry.national_statistic
                d["csv_url"] = entry.csv_url
                d["xlsx_url"] = entry.xlsx_url
                d["csvw_url"] = entry.csvw_url
            if entry.children:
                d["children"] = [_to_dict(c) for c in entry.children]
            return d

        index = _to_dict(catalog)
        index_file = output_dir / self.config().tree_index_filename
        index_file.write_text(json.dumps(index, indent=2, ensure_ascii=False))
        return index_file
