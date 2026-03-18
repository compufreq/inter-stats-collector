"""Download dataset files (CSV/TSV, SDMX 2.1, DSD, metadata) from Eurostat."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx

from . import api
from .toc import FOLDER_STYLE_DISPLAY, TocEntry
from ...download_utils import BytesCallback, DownloadResult, _download_file, _extract_gz

log = logging.getLogger(__name__)


async def download_dataset(
    client: httpx.AsyncClient,
    entry: TocEntry,
    output_dir: Path,
    *,
    skip_existing: bool = True,
    on_bytes: BytesCallback | None = None,
    folder_style: str = FOLDER_STYLE_DISPLAY,
) -> DownloadResult:
    """Download all files for a single dataset.

    Creates this structure under output_dir / entry.get_folder_path(style):
        {code}.tsv.gz          - TSV data (compressed)
        {code}.tsv             - TSV data (extracted)
        {code}.sdmx.xml.gz     - SDMX 2.1 data (compressed)
        {code}.sdmx.xml        - SDMX 2.1 data (extracted)
        {code}_dsd.xml         - Data Structure Definition
        {code}_dataflow.xml    - Dataflow definition with references
        {code}_metadata.html   - ESMS reference metadata
        {code}_constraint.xml  - Content constraint
        {code}_info.json       - Entry info from TOC
    """
    code = entry.code
    dest_dir = output_dir / entry.get_folder_path(folder_style)
    dest_dir.mkdir(parents=True, exist_ok=True)

    result = DownloadResult(code)

    # 1. Save TOC entry info
    info_file = dest_dir / f"{code}_info.json"
    if not (skip_existing and info_file.exists()):
        info = {
            "code": code,
            "title": entry.title,
            "type": entry.entry_type,
            "path": entry.full_path,
            "folder_path": str(entry.folder_path),
            "last_update": entry.last_update,
            "last_structure_change": entry.last_structure_change,
            "data_start": entry.data_start,
            "data_end": entry.data_end,
            "values": entry.values,
            "subtitle": entry.subtitle,
            "metadata_url": entry.metadata_url,
        }
        data = json.dumps(info, indent=2, ensure_ascii=False).encode()
        info_file.write_bytes(data)
        result.bytes_downloaded += len(data)
        result.successes.append("info")

    # 2. Download TSV data (compressed) + extract
    tsv_gz = dest_dir / f"{code}.tsv.gz"
    tsv_extracted = dest_dir / f"{code}.tsv"
    if not (skip_existing and tsv_gz.exists()):
        try:
            url = f"{api.SDMX_DATA}/{code}"
            n = await _download_file(
                client, url, tsv_gz,
                params={"format": "TSV", "compressed": "true"},
                on_bytes=on_bytes,
            )
            result.bytes_downloaded += n
            _extract_gz(tsv_gz)
            result.successes.append("tsv")
            log.info("  [TSV] %s OK (extracted → %s)", code, tsv_extracted.name)
        except Exception as e:
            result.failures.append(("tsv", str(e)))
            log.warning("  [TSV] %s FAILED: %s", code, e)
    elif tsv_gz.exists() and not tsv_extracted.exists():
        # Compressed exists from a prior run but was never extracted
        try:
            _extract_gz(tsv_gz)
            log.info("  [TSV] %s re-extracted → %s", code, tsv_extracted.name)
        except Exception as e:
            log.warning("  [TSV] %s extract FAILED: %s", code, e)

    # 3. Download SDMX 2.1 Structured data (compressed) + extract
    sdmx_gz = dest_dir / f"{code}.sdmx.xml.gz"
    sdmx_extracted = dest_dir / f"{code}.sdmx.xml"
    if not (skip_existing and sdmx_gz.exists()):
        try:
            url = f"{api.SDMX_DATA}/{code}"
            n = await _download_file(
                client, url, sdmx_gz,
                params={"format": "SDMX_2.1_STRUCTURED", "compressed": "true"},
                on_bytes=on_bytes,
            )
            result.bytes_downloaded += n
            _extract_gz(sdmx_gz)
            result.successes.append("sdmx")
            log.info("  [SDMX] %s OK (extracted → %s)", code, sdmx_extracted.name)
        except Exception as e:
            result.failures.append(("sdmx", str(e)))
            log.warning("  [SDMX] %s FAILED: %s", code, e)
    elif sdmx_gz.exists() and not sdmx_extracted.exists():
        try:
            _extract_gz(sdmx_gz)
            log.info("  [SDMX] %s re-extracted → %s", code, sdmx_extracted.name)
        except Exception as e:
            log.warning("  [SDMX] %s extract FAILED: %s", code, e)

    # 4. Download Data Structure Definition (DSD)
    dsd_file = dest_dir / f"{code}_dsd.xml"
    if not (skip_existing and dsd_file.exists()):
        try:
            url = f"{api.SDMX_DATASTRUCTURE}/ESTAT/{code}/latest"
            n = await _download_file(client, url, dsd_file, on_bytes=on_bytes)
            result.bytes_downloaded += n
            result.successes.append("dsd")
            log.info("  [DSD] %s OK", code)
        except Exception as e:
            result.failures.append(("dsd", str(e)))
            log.warning("  [DSD] %s FAILED: %s", code, e)

    # 5. Download Dataflow with full references (includes DSD, codelists, concepts)
    dataflow_file = dest_dir / f"{code}_dataflow.xml"
    if not (skip_existing and dataflow_file.exists()):
        try:
            url = f"{api.SDMX_DATAFLOW}/ESTAT/{code}/1.0"
            n = await _download_file(
                client, url, dataflow_file,
                params={"references": "descendants", "detail": "referencepartial"},
                on_bytes=on_bytes,
            )
            result.bytes_downloaded += n
            result.successes.append("dataflow")
            log.info("  [DATAFLOW] %s OK", code)
        except Exception as e:
            result.failures.append(("dataflow", str(e)))
            log.warning("  [DATAFLOW] %s FAILED: %s", code, e)

    # 6. Download metadata via ESMS if URL is available
    if entry.metadata_url:
        meta_file = dest_dir / f"{code}_metadata.html"
        if not (skip_existing and meta_file.exists()):
            try:
                n = await _download_file(
                    client, entry.metadata_url, meta_file, on_bytes=on_bytes,
                )
                result.bytes_downloaded += n
                result.successes.append("metadata")
                log.info("  [META] %s OK", code)
            except Exception as e:
                result.failures.append(("metadata", str(e)))
                log.warning("  [META] %s FAILED: %s", code, e)

    # 7. Download content constraint (available dimension values)
    constraint_file = dest_dir / f"{code}_constraint.xml"
    if not (skip_existing and constraint_file.exists()):
        try:
            url = f"{api.SDMX_CONTENTCONSTRAINT}/ESTAT/{code}/latest"
            n = await _download_file(client, url, constraint_file, on_bytes=on_bytes)
            result.bytes_downloaded += n
            result.successes.append("constraint")
            log.info("  [CONSTRAINT] %s OK", code)
        except Exception as e:
            result.failures.append(("constraint", str(e)))
            log.warning("  [CONSTRAINT] %s FAILED: %s", code, e)

    return result
