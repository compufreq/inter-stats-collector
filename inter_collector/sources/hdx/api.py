"""HDX (Humanitarian Data Exchange) CKAN API constants.

Portal:  https://data.humdata.org
API:     https://data.humdata.org/api/3/action/
Docs:    https://data.humdata.org/faqs/devs

HDX is an open platform for sharing humanitarian data across crises and
organisations, managed by OCHA's Centre for Humanitarian Data. It uses
CKAN as its backend.

License: Varies per dataset (CC BY-IGO for most UNHCR data)
No authentication required for read operations.
"""

# CKAN API base
CKAN_BASE = "https://data.humdata.org/api/3/action"

# Endpoints
PACKAGE_SEARCH = f"{CKAN_BASE}/package_search"
PACKAGE_SHOW = f"{CKAN_BASE}/package_show"

# Default organization filter (UNHCR)
DEFAULT_ORG = "unhcr"

# Pagination
PAGE_SIZE = 1000

# Downloadable data formats (skip PDF, Web App, KMZ, HTML, etc.)
ALL_DATA_FORMATS = {"CSV", "XLS", "XLSX", "ODS", "JSON"}

# Default formats to download
DEFAULT_DOWNLOAD_FORMATS = {"CSV", "XLSX", "XLS"}

# Formats that count as "real data" for state completion tracking
DATA_FILE_TYPES = {"csv", "xls", "xlsx"}
