"""Configuration dataclass for CKAN-based open data portals.

Each portal (Swiss, HDX, Netherlands, Germany, etc.) provides a
CkanPortalConfig instance that parameterizes the generic catalog
fetcher and downloader.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CkanPortalConfig:
    """Configuration for a single CKAN portal.

    Controls API endpoints, default filters, output layout,
    field extraction, and filename strategies.
    """

    # Portal identity
    ckan_base_url: str           # e.g., "https://ckan.opendata.swiss/api/3/action"
    name: str                    # source name: "swiss", "hdx", "netherlands", "germany"
    display_name: str            # human-readable: "Swiss FSO (opendata.swiss)"
    root_code: str               # root tree node code: "swiss", "hdx", "nl", "de"
    root_title: str              # root tree node title: "Swiss Open Data"

    # Default filters
    default_org: str = ""        # default --scope org filter (empty = all orgs)
    default_formats: set[str] = field(default_factory=lambda: {"CSV", "XLS", "XLSX", "JSON"})

    # Output paths
    output_subdir: str = ""                  # subdirectory under output root
    state_filename: str = ".state.json"      # state file name
    tree_index_filename: str = "tree_index.json"

    # File type groups for status display
    file_type_groups: dict[str, str] = field(default_factory=lambda: {
        ".csv": "CSV data",
        ".xls": "Excel data",
        ".xlsx": "Excel data",
        "_info.json": "Dataset info",
    })

    # Success labels that count as "real data" (not just metadata)
    data_file_types: set[str] = field(default_factory=lambda: {"csv", "xls", "xlsx", "json"})

    # Field extraction — handle differences between CKAN portals
    multilingual: bool = False                # Swiss has multilingual title/desc dicts
    title_langs: tuple[str, ...] = ("en", "de", "fr", "it")  # lang priority for multilingual
    description_fields: tuple[str, ...] = ("notes", "description")  # fields to check for desc
    media_type_fields: tuple[str, ...] = ("media_type",)  # HDX also has "mimetype"

    # Filename strategy when multiple resources share the same format
    #   "language_tags": {code}_{lang}.{ext}  (Swiss — uses resource language field)
    #   "resource_name": sanitized name       (HDX — uses resource name field)
    #   "index":         {code}_{0}.{ext}     (simple fallback)
    filename_strategy: str = "index"

    # Rate limiting
    recommended_concurrency: int | None = None

    # Pagination
    page_size: int = 1000  # CKAN rows per request (max typically 1000)
