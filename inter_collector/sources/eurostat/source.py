"""Eurostat data source implementation."""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from ...base import DataSource, SourceConfig
from ...download_utils import BytesCallback, DownloadResult
from .toc import (
    FOLDER_STYLE_DISPLAY,
    TocEntry,
    collect_datasets as _collect_datasets,
    fetch_toc,
)
from .downloader import download_dataset as _download_dataset


class EurostatSource(DataSource):
    """Eurostat — the official statistical office of the European Union.

    Downloads ~10,400 datasets from the Eurostat SDMX 2.1 Dissemination API
    at https://ec.europa.eu/eurostat/api/dissemination.

    For each dataset, downloads:
    - TSV data (.tsv.gz + extracted .tsv)
    - SDMX 2.1 Structured data (.sdmx.xml.gz + extracted .sdmx.xml)
    - Data Structure Definition (_dsd.xml)
    - Dataflow with resolved references (_dataflow.xml)
    - ESMS reference metadata (_metadata.html)
    - Content constraint (_constraint.xml)
    - TOC info snapshot (_info.json)
    """

    def config(self) -> SourceConfig:
        return SourceConfig(
            name="eurostat",
            display_name="Eurostat",
            default_output_subdir="eurostat",
            state_filename=".eurostat_state.json",
            tree_index_filename="eurostat_tree_index.json",
            file_type_groups={
                ".tsv.gz": "TSV data (compressed)",
                ".tsv": "TSV data (extracted)",
                ".sdmx.xml.gz": "SDMX data (compressed)",
                ".sdmx.xml": "SDMX data (extracted)",
                "_dsd.xml": "Data Structure Definition",
                "_dataflow.xml": "Dataflow definition",
                "_constraint.xml": "Content constraint",
                "_metadata.html": "ESMS metadata",
                "_info.json": "TOC info snapshot",
            },
            data_file_types={"tsv", "sdmx"},
        )

    async def fetch_catalog(
        self,
        client: httpx.AsyncClient,
        *,
        output_dir: Path | None = None,
    ) -> TocEntry:
        return await fetch_toc(client)

    def collect_datasets(self, catalog: TocEntry) -> list[TocEntry]:
        return _collect_datasets(catalog)

    async def download_dataset(
        self,
        client: httpx.AsyncClient,
        entry: TocEntry,
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

    def save_tree_index(self, catalog: TocEntry, output_dir: Path) -> Path:
        """Save the full Eurostat category tree as a JSON index file."""

        def _to_dict(entry: TocEntry) -> dict:
            d = {
                "code": entry.code,
                "title": entry.title,
                "type": entry.entry_type,
                "path": entry.full_path,
                "folder_path": str(entry.folder_path),
            }
            if entry.is_dataset:
                d["last_update"] = entry.last_update
                d["data_range"] = f"{entry.data_start} - {entry.data_end}"
                d["values"] = entry.values
            if entry.children:
                d["children"] = [_to_dict(c) for c in entry.children]
            return d

        index = _to_dict(catalog)
        index_file = output_dir / self.config().tree_index_filename
        index_file.write_text(json.dumps(index, indent=2, ensure_ascii=False))
        return index_file
