"""Swiss open data source via opendata.swiss (CKAN API).

Portal: https://opendata.swiss
API:    https://ckan.opendata.swiss/api/3/action/
Docs:   https://handbook.opendata.swiss/de/content/nutzen/api-nutzen.html

Default scope: BFS only (~3,277 datasets).
Use --scope all to include all 288 organizations (~14,300 datasets).
"""

from __future__ import annotations

from ..ckan.config import CkanPortalConfig
from ..ckan.source import CkanSource

# Swiss portal configuration
SWISS_CONFIG = CkanPortalConfig(
    ckan_base_url="https://ckan.opendata.swiss/api/3/action",
    name="swiss",
    display_name="Swiss FSO (opendata.swiss)",
    root_code="swiss",
    root_title="Swiss Open Data",
    default_org="bundesamt-fur-statistik-bfs",
    default_formats={"CSV", "XLS", "ODS", "JSON"},
    output_subdir="swiss",
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
    multilingual=True,
    title_langs=("en", "de", "fr", "it"),
    description_fields=("description", "notes"),
    media_type_fields=("media_type",),
    filename_strategy="language_tags",
)


class SwissSource(CkanSource):
    """Swiss Federal Statistical Office (opendata.swiss).

    Downloads datasets from the official Swiss open government data portal.
    Default scope is BFS (Federal Statistical Office) only.
    """

    def __init__(
        self,
        *,
        org_filter: str | None = None,
        download_formats: set[str] | None = None,
    ):
        super().__init__(
            SWISS_CONFIG,
            org_filter=org_filter,
            download_formats=download_formats,
        )
