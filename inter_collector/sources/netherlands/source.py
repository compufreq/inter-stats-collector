"""Netherlands open data source via data.overheid.nl (CKAN API).

Portal: https://data.overheid.nl
API:    https://data.overheid.nl/data/api/3/action/
License: CC0 / CC BY (varies per dataset)

~18,900 datasets from Dutch government organizations.
No default organization filter — all orgs included by default.
"""

from __future__ import annotations

from ..ckan.config import CkanPortalConfig
from ..ckan.source import CkanSource

NL_CONFIG = CkanPortalConfig(
    ckan_base_url="https://data.overheid.nl/data/api/3/action",
    name="netherlands",
    display_name="Netherlands Open Data",
    root_code="nl",
    root_title="Netherlands Open Data",
    default_org="",  # no default filter — all orgs
    default_formats={"CSV", "XLS", "XLSX", "JSON", "XML", "ZIP", "GEOJSON"},
    output_subdir="netherlands",
    state_filename=".netherlands_state.json",
    tree_index_filename="netherlands_tree_index.json",
    file_type_groups={
        ".csv": "CSV data",
        ".xls": "Excel data",
        ".xlsx": "Excel data",
        ".json": "JSON data",
        ".xml": "XML data",
        ".zip": "Archive (ZIP)",
        ".geojson": "GeoJSON geospatial",
        "_info.json": "Dataset info",
    },
    data_file_types={"csv", "xls", "xlsx", "json", "xml", "zip", "geojson"},
    multilingual=True,
    title_langs=("en", "nl"),
    description_fields=("notes", "description"),
    media_type_fields=("media_type",),
    filename_strategy="index",
)


class NetherlandsSource(CkanSource):
    """Netherlands open data (data.overheid.nl).

    Downloads datasets from the Dutch national open data portal.
    """

    def __init__(
        self,
        *,
        org_filter: str | None = None,
        download_formats: set[str] | None = None,
    ):
        super().__init__(
            NL_CONFIG,
            org_filter=org_filter,
            download_formats=download_formats,
        )
