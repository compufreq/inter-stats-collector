"""Eurostat API endpoint definitions and constants."""

BASE = "https://ec.europa.eu/eurostat/api/dissemination"

# Catalogue API
TOC_XML = f"{BASE}/catalogue/toc/xml"
TOC_TXT = f"{BASE}/catalogue/toc/txt"  # ?lang=en
METABASE = f"{BASE}/catalogue/metabase.txt.gz"

# Files / Inventory API
FILES_INVENTORY = f"{BASE}/files/inventory"  # ?type=data&lang=en
FILES_DOWNLOAD = f"{BASE}/files"  # ?file=<path>

# SDMX 2.1 API
SDMX_BASE = f"{BASE}/sdmx/2.1"
SDMX_DATA = f"{SDMX_BASE}/data"  # /{datasetCode}
SDMX_DATAFLOW = f"{SDMX_BASE}/dataflow"  # /ESTAT/{code}/1.0
SDMX_DATASTRUCTURE = f"{SDMX_BASE}/datastructure"  # /ESTAT/{code}/latest
SDMX_CATEGORYSCHEME = f"{SDMX_BASE}/categoryscheme"  # /ESTAT/all/latest
SDMX_CODELIST = f"{SDMX_BASE}/codelist"  # /ESTAT/{code}/latest
SDMX_CONTENTCONSTRAINT = f"{SDMX_BASE}/contentconstraint"  # /ESTAT/{code}/latest

# Statistics API (JSON-stat)
STATISTICS_DATA = f"{BASE}/statistics/1.0/data"  # /{datasetCode}

# Download formats
DATA_FORMATS = {
    "csv": {"format": "TSV", "compressed": "true"},
    "sdmx": {"format": "SDMX_2.1_STRUCTURED", "compressed": "true"},
}
