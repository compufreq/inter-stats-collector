"""Swiss open data API constants (opendata.swiss — CKAN-based portal).

opendata.swiss is the official Swiss open government data portal, managed
by the Federal Statistical Office (BFS). It uses CKAN as its backend and
exposes a standard CKAN REST API.

API Documentation:
    https://handbook.opendata.swiss/de/content/nutzen/api-nutzen.html

CKAN API v3:
  - GET /api/3/action/package_search  → search/list datasets (paginated)
  - GET /api/3/action/package_show    → single dataset detail
  - GET /api/3/action/organization_list → list all organizations
  - GET /api/3/action/group_list       → list thematic categories

No authentication required for read operations.
No documented rate limits (we use polite delays).
"""

# CKAN API base
CKAN_BASE = "https://ckan.opendata.swiss/api/3/action"

# Endpoints
PACKAGE_SEARCH = f"{CKAN_BASE}/package_search"
PACKAGE_SHOW = f"{CKAN_BASE}/package_show"

# Default organization filter (Federal Statistical Office)
BFS_ORG = "bundesamt-fur-statistik-bfs"

# Pagination — CKAN allows up to 1000 rows per request
PAGE_SIZE = 1000

# All downloadable data formats (skip HTML, SERVICE, PDF, WMS, WMTS, etc.)
ALL_DATA_FORMATS = {"CSV", "XLS", "XLSX", "ODS", "JSON"}

# Default formats to download
DEFAULT_DOWNLOAD_FORMATS = {"CSV", "XLS", "ODS", "JSON"}

# Formats that count as "real data" for state completion tracking
DATA_FILE_TYPES = {"csv", "xls", "xlsx", "ods", "json"}
