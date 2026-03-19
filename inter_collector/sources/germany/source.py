"""Germany open data source via GovData.de (CKAN API).

Portal: https://www.govdata.de
API:    https://www.govdata.de/ckan/api/3/action/
License: Datenlizenz Deutschland 2.0 / CC BY 4.0 (varies per dataset)

~149,000 datasets from German federal, state, and municipal organizations.
No default organization filter — all orgs included by default.
"""

from __future__ import annotations

from ..ckan.config import CkanPortalConfig
from ..ckan.source import CkanSource

DE_CONFIG = CkanPortalConfig(
    ckan_base_url="https://www.govdata.de/ckan/api/3/action",
    name="germany",
    display_name="Germany GovData",
    root_code="de",
    root_title="Germany Open Data",
    default_org="",  # no default filter — all orgs
    default_formats={"CSV", "XLS", "XLSX", "JSON"},
    output_subdir="germany",
    state_filename=".germany_state.json",
    tree_index_filename="germany_tree_index.json",
    file_type_groups={
        ".csv": "CSV data",
        ".xls": "Excel data",
        ".xlsx": "Excel data",
        ".json": "JSON data",
        "_info.json": "Dataset info",
    },
    data_file_types={"csv", "xls", "xlsx", "json"},
    multilingual=True,
    title_langs=("en", "de"),
    description_fields=("notes", "description"),
    media_type_fields=("media_type",),
    filename_strategy="index",
)


class GermanySource(CkanSource):
    """Germany open data (GovData.de).

    Downloads datasets from the German national open data portal.
    """

    def __init__(
        self,
        *,
        org_filter: str | None = None,
        download_formats: set[str] | None = None,
    ):
        super().__init__(
            DE_CONFIG,
            org_filter=org_filter,
            download_formats=download_formats,
        )
