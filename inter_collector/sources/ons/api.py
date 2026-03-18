"""UK Office for National Statistics (ONS) API endpoint constants.

Developer Hub: https://developer.ons.gov.uk/
API Status: Beta (may have breaking changes)
Authentication: None required (open API)

The ONS CMD (Customise My Data) API is a RESTful JSON API:
  - GET  /datasets                                    → paginated list of all datasets
  - GET  /datasets/{id}                               → dataset metadata
  - GET  /datasets/{id}/editions                      → list of editions (e.g., "time-series", "2021")
  - GET  /datasets/{id}/editions/{ed}/versions/{ver}  → version detail + download links
  - Downloads served from download.ons.gov.uk

Formats: CSV, XLSX, CSV-W (metadata JSON)
Rate limiting: No documented hard limit, but be polite (use concurrency ≤ 5)
"""

# Base URL
API_BASE = "https://api.beta.ons.gov.uk/v1"

# Endpoints
DATASETS = f"{API_BASE}/datasets"

# Pagination
PAGE_SIZE = 20  # max per request

# Download host (files are served from here, not the API)
DOWNLOAD_HOST = "https://download.ons.gov.uk"
