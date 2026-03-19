"""HDX (Humanitarian Data Exchange) data source via CKAN API.

Portal:  https://data.humdata.org
API:     https://data.humdata.org/api/3/action/
Docs:    https://data.humdata.org/faqs/devs

Default scope: UNHCR only.
Use --scope all to include all 400+ organizations.
"""

from __future__ import annotations

from ..ckan.config import CkanPortalConfig
from ..ckan.source import CkanSource

# HDX portal configuration
HDX_CONFIG = CkanPortalConfig(
    ckan_base_url="https://data.humdata.org/api/3/action",
    name="hdx",
    display_name="HDX (Humanitarian Data Exchange)",
    root_code="hdx",
    root_title="Humanitarian Data Exchange",
    default_org="unhcr",
    default_formats={"CSV", "XLSX", "XLS"},
    output_subdir="hdx",
    state_filename=".hdx_state.json",
    tree_index_filename="hdx_tree_index.json",
    file_type_groups={
        ".csv": "CSV data",
        ".xls": "Excel data",
        ".xlsx": "Excel data",
        "_info.json": "Dataset info",
    },
    data_file_types={"csv", "xls", "xlsx"},
    multilingual=False,
    description_fields=("notes",),
    media_type_fields=("media_type", "mimetype"),
    filename_strategy="resource_name",
)


class HDXSource(CkanSource):
    """Humanitarian Data Exchange (data.humdata.org).

    Downloads humanitarian datasets from OCHA's open data platform.
    Default scope is UNHCR only.
    """

    def __init__(
        self,
        *,
        org_filter: str | None = None,
        download_formats: set[str] | None = None,
    ):
        super().__init__(
            HDX_CONFIG,
            org_filter=org_filter,
            download_formats=download_formats,
        )
