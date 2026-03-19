# International Statistics Data Collector

A Python CLI tool that bulk-downloads **entire statistical and humanitarian databases** from official public sources — government statistical offices and international organisations — every dataset, in multiple formats, with full metadata — and organises them on disk mirroring each source's category tree.

Currently supports:

| Source | Flag | Status | Datasets |
|--------|------|--------|----------|
| **All sources** | `--stats all` (default) | ✅ Ready | All below |
| **Eurostat** (EU) | `--stats europe` | ✅ Ready | ~10,400 datasets |
| **Swiss FSO** (opendata.swiss) | `--stats switzerland` | ✅ Ready | ~1,977 datasets |
| **UK ONS** | `--stats uk` | ✅ Ready | ~337 datasets |
| **UNHCR** | `--stats unhcr` | ✅ Ready | ~450 datasets (6 endpoints × 75 years) |
| **HDX** (Humanitarian Data Exchange) | `--stats hdx` | ✅ Ready | ~244 UNHCR datasets |
| **Netherlands** (data.overheid.nl) | `--stats netherlands` | ✅ Ready | ~12,245 datasets |
| **Germany** (GovData.de) | `--stats germany` | ✅ Ready | ~92,722 datasets |

---

## Table of Contents

- [What It Does](#what-it-does)
- [Quick Start](#quick-start)
- [Data Sources & APIs](#data-sources--apis)
- [What Gets Downloaded Per Dataset](#what-gets-downloaded-per-dataset)
- [Output Folder Structure](#output-folder-structure)
- [Architecture](#architecture)
- [Installation](#installation)
- [Commands & Usage](#commands--usage)
  - [collect](#collect---download-everything)
  - [extract](#extract---decompress-gz-files)
  - [rename](#rename---switch-folder-naming-style)
  - [status](#status---inspect-progress)
  - [tree](#tree---browse-the-category-hierarchy)
- [Progress Indicators](#progress-indicators)
- [Resumability & State](#resumability--state)
- [Filtering](#filtering)
- [Platform Support](#platform-support)
- [Configuration & Tuning](#configuration--tuning)
- [Eurostat API Reference](#eurostat-api-reference)

---

## What It Does

1. **Fetches the dataset catalog** from the selected data source — parses it into a recursive category tree.

2. **Downloads all artefacts per dataset** — data files (multiple formats), structural metadata, reference metadata, and a TOC info snapshot.

3. **Extracts compressed files** — `.gz` files are automatically decompressed alongside the originals after download.

4. **Tracks state** — every completed and failed dataset is recorded in a JSON state file, so you can stop and resume at any time without re-downloading.

5. **Shows real-time progress** — multi-row Rich progress displays with dataset count, bytes transferred, download speed, per-worker status, and ETAs.

---

## Quick Start

No pre-setup required — all output directories, state files, and tree indexes are created automatically on first run.

```bash
# Clone and install
git clone https://github.com/compufreq/inter-stats-collector.git
cd inter-stats-collector
pip install -e .

# Download from ALL sources (default)
inter-collect collect

# Preview what would be downloaded across all sources
inter-collect collect --dry-run

# Or target a single source
inter-collect --stats europe collect          # Eurostat only
inter-collect --stats switzerland collect     # Swiss FSO only
inter-collect --stats uk collect              # UK ONS only
inter-collect --stats unhcr collect           # UNHCR refugee statistics
inter-collect --stats hdx collect             # HDX humanitarian data
inter-collect --stats netherlands collect     # Netherlands open data
inter-collect --stats germany collect         # Germany GovData

# UNHCR: download specific year range
inter-collect --stats unhcr --year-from 2020 --year-to 2023 collect

# CKAN sources: filter by organization
inter-collect --stats germany --scope statistisches-bundesamt-destatis collect

# Swiss: download only CSV files from all organizations
inter-collect --stats switzerland --scope all --formats csv collect

# Extract all compressed files (parallel, all sources)
inter-collect extract

# Check collection progress (all sources)
inter-collect status

# Check a single source
inter-collect --stats europe status
```

---

## Data Sources & APIs

### Eurostat (`--stats europe`)

All data comes from the **official Eurostat Dissemination API** (`ec.europa.eu/eurostat/api/dissemination`). No scraping — every request hits a documented, public REST endpoint.

| Source | Endpoint | What It Provides |
|--------|----------|-----------------|
| **Catalogue API** | `/catalogue/toc/xml` | Full Table of Contents as XML — master list of all datasets with metadata |
| **SDMX 2.1 Data API** | `/sdmx/2.1/data/{code}` | Statistical data in TSV and SDMX 2.1 XML formats |
| **SDMX 2.1 Datastructure API** | `/sdmx/2.1/datastructure/ESTAT/{code}/latest` | Data Structure Definition (dimensions, attributes, measures) |
| **SDMX 2.1 Dataflow API** | `/sdmx/2.1/dataflow/ESTAT/{code}/1.0` | Dataflow with resolved codelists and concept schemes |
| **SDMX 2.1 Content Constraint API** | `/sdmx/2.1/contentconstraint/ESTAT/{code}/latest` | Valid dimension values for a dataset |
| **ESMS Metadata** | `ec.europa.eu/eurostat/cache/metadata/en/{ref}_esms.htm` | Methodology, quality reports, data collection (HTML) |

### UK ONS (`--stats uk`)

All data comes from the **ONS CMD (Customise My Data) API** at `api.beta.ons.gov.uk/v1`. No authentication required. Downloads served from `download.ons.gov.uk`.

| Source | Endpoint | What It Provides |
|--------|----------|-----------------|
| **Dataset listing** | `GET /datasets` | Paginated list of all ~337 datasets with metadata |
| **Dataset detail** | `GET /datasets/{id}` | Full dataset metadata (description, contacts, links) |
| **Edition listing** | `GET /datasets/{id}/editions` | Editions (e.g., "time-series", "2021") |
| **Version detail** | `GET /datasets/{id}/editions/{ed}/versions/{ver}` | Download URLs, dimensions, release info |
| **Downloads** | `download.ons.gov.uk` | CSV, XLSX, and CSV-W data files |

**Notes**: The API is in Beta status and may have breaking changes. Datasets are organised by taxonomy (e.g., `economy/inflationandpriceindices`). No documented hard rate limit, but the API returns 429 under moderate load — the collector uses batched requests with delays.

### Swiss FSO (`--stats switzerland`)

All data comes from **opendata.swiss** — Switzerland's official open government data portal, powered by CKAN.

| Source | Endpoint | What It Provides |
|--------|----------|-----------------|
| **Dataset search** | `GET /api/3/action/package_search` | Paginated BFS datasets with metadata and resource URLs |
| **Dataset detail** | `GET /api/3/action/package_show` | Full dataset metadata including all download resources |
| **Organizations** | `GET /api/3/action/organization_list` | 288 government organizations publishing data |
| **Categories** | `GET /api/3/action/group_list` | 14 thematic categories (themes) |

**API**: Standard CKAN REST API at `ckan.opendata.swiss/api/3/action/`. No authentication required.

**Scope options**:
- `--scope bfs` (default): ~3,277 datasets from the Federal Statistical Office only
- `--scope all`: ~14,300 datasets from all 288 Swiss government organizations

**Format options** (`--formats`):
- Default: `csv,xls,ods,json` — all downloadable data formats
- Example: `--formats csv` to download only CSV files (~308 datasets)
- Skips HTML, SERVICE, PDF, and other non-data resources automatically

### UNHCR Refugee Statistics (`--stats unhcr`)

Data from the **UNHCR Refugee Statistics API** at `api.unhcr.org/population/v1/`. No authentication required.

| Source | Endpoint | What It Provides |
|--------|----------|-----------------|
| **Population** | `GET /population/` | Stock figures: refugees, asylum-seekers, IDPs, stateless |
| **Asylum applications** | `GET /asylum-applications/` | Individual asylum applications by country |
| **Asylum decisions** | `GET /asylum-decisions/` | Decisions: recognized, rejected, closed |
| **Solutions** | `GET /solutions/` | Durable solutions: return, resettlement, naturalisation |
| **Demographics** | `GET /demographics/` | Age/gender breakdown of displaced populations |
| **UNRWA** | `GET /unrwa/` | UNRWA-registered Palestine refugees |
| **Reference** | `GET /countries/`, `/regions/`, `/years/` | 249 countries, 6 regions, years 1951–2026 |

**Data coverage**: 75+ years (1951–present), 249 countries, full country-of-origin × country-of-asylum breakdown.

**Year range options**:
- `--year-from 2020` — start from 2020
- `--year-to 2023` — end at 2023
- Default: all available years (1951–present, ~450 datasets)

**License**: Creative Commons Attribution 4.0 International (CC BY 4.0). Attribution: "UNHCR Refugee Population Statistics Database".

### HDX — Humanitarian Data Exchange (`--stats hdx`)

Data from **HDX** at `data.humdata.org`, OCHA's open humanitarian data platform (CKAN-based).

| Source | Endpoint | What It Provides |
|--------|----------|-----------------|
| **Dataset search** | `GET /api/3/action/package_search` | Paginated datasets with resource URLs |
| **Dataset detail** | `GET /api/3/action/package_show` | Full metadata and download links |

**Default scope**: UNHCR datasets only (~244 with downloadable CSV/XLS data). Use `--scope all` for all 481 organizations.

**What it has**: Per-country refugee population CSVs, situation reports, border crossing data, camp locations, voluntary repatriation statistics — the country-level breakdown data that the UNHCR Statistics API doesn't expose publicly.

**Organizations on HDX** (`--scope all`): HDX hosts data from **481 organizations** spanning UN agencies, NGOs, research institutions, and governments. Major contributors include:

| Organization | Datasets | Focus Area |
|---|---|---|
| **World Bank Group** | 4,792 | Development indicators, economics, poverty |
| **Humanitarian OpenStreetMap Team (HOT)** | 2,612 | Geospatial / mapping data |
| **UNHCR** (default) | 1,137 | Refugees, asylum-seekers, IDPs, stateless |
| **FEWS NET** | 847 | Food security & early warning |
| **HeiGIT** | 785 | Geoinformatics for humanitarian action |
| **WHO** (World Health Organization) | 683 | Health, disease outbreaks, epidemiology |
| **WFP** (World Food Programme) | 492 | Food assistance, nutrition, logistics |
| **Copernicus** | 478 | Satellite / earth observation |
| **FAO** (Food & Agriculture Organization) | 441 | Agriculture, food systems, land use |
| **UNICEF** | 354 | Children, education, health, nutrition |
| **IOM** (International Organization for Migration) | 273 | Migration, displacement tracking |
| **UNESCO** | 251 | Education, culture, science |
| **ACLED** | 246 | Armed conflict events & political violence |
| **UNEP-WCMC** | 239 | Environment, biodiversity, conservation |
| **Meta (AI for Good)** | 223 | Population density maps, connectivity |
| **UNDP HDRO** | 228 | Human development, inequality |
| **UNDRR** | 208 | Disaster risk reduction |
| **OCHA** (regional offices) | 300+ | Humanitarian coordination by country |
| **REACH Initiative** | 134 | Multi-sector needs assessments |
| **DHS Program** | 175 | Demographic & health surveys |
| ... and 460+ more | | |

**License**: Varies per dataset. Most UNHCR data uses CC BY-IGO. Other common licenses: CC BY, CC BY-SA, ODC-ODbL, CC0.

---

## What Gets Downloaded Per Dataset

### Eurostat

For a dataset with code `prc_hicp_midx`, up to **9 files** are created:

| File | Format | Description |
|------|--------|-------------|
| `prc_hicp_midx.tsv.gz` | Gzip | Raw statistical data in TSV format (compressed) |
| `prc_hicp_midx.tsv` | TSV | Extracted TSV data — tab-separated values |
| `prc_hicp_midx.sdmx.xml.gz` | Gzip | SDMX 2.1 Structured XML data (compressed) |
| `prc_hicp_midx.sdmx.xml` | XML | Extracted SDMX data with observations |
| `prc_hicp_midx_dsd.xml` | XML | Data Structure Definition |
| `prc_hicp_midx_dataflow.xml` | XML | Dataflow with resolved references (DSD + codelists + concepts) |
| `prc_hicp_midx_metadata.html` | HTML | ESMS reference metadata |
| `prc_hicp_midx_constraint.xml` | XML | Content constraint |
| `prc_hicp_midx_info.json` | JSON | TOC entry snapshot |

### Swiss FSO (opendata.swiss)

For a dataset named `erwerbstatige-inlandkonzept`, files depend on available formats:

| File | Format | Description |
|------|--------|-------------|
| `erwerbstatige-inlandkonzept_info.json` | JSON | Catalog entry snapshot (code, title, organization, license, resource URLs) |
| `erwerbstatige-inlandkonzept.csv` | CSV | Data in CSV format |
| `erwerbstatige-inlandkonzept.xls` | Excel | Data in Excel format |
| `erwerbstatige-inlandkonzept.ods` | ODS | Data in ODS format (if available) |
| `erwerbstatige-inlandkonzept.json` | JSON | Data in JSON format (if available) |

When a dataset has multiple resources of the same format in different languages, filenames include a language suffix (e.g., `dataset_de.csv`, `dataset_fr.csv`).

### UNHCR Refugee Statistics

For each endpoint+year combination (e.g., `population_2023`), **2 files** are created:

| File | Format | Description |
|------|--------|-------------|
| `population_2023_info.json` | JSON | Catalog snapshot (endpoint, year, license, attribution) |
| `population_2023_data.json` | JSON | Complete paginated data with all country-level rows |

The `_data.json` file contains all rows across all pages. Six endpoint types are available: population, asylum-applications, asylum-decisions, solutions, demographics, and UNRWA.

### HDX (Humanitarian Data Exchange)

For a dataset named `unhcr-population-data-for-afg`, files depend on available formats:

| File | Format | Description |
|------|--------|-------------|
| `unhcr-population-data-for-afg_info.json` | JSON | Catalog snapshot (organization, license, resource URLs) |
| `unhcr-population-data-for-afg.csv` | CSV | Data file (refugees, asylum-seekers by country) |
| `unhcr-population-data-for-afg.xlsx` | Excel | Data file (if available) |

### UK ONS

For a dataset with code `cpih01`, up to **5 files** are created:

| File | Format | Description |
|------|--------|-------------|
| `cpih01_info.json` | JSON | Dataset catalog snapshot (code, title, description, URLs, taxonomy) |
| `cpih01_meta.json` | JSON | Full version metadata — dimensions, release info, temporal coverage |
| `cpih01.csv` | CSV | Data in CSV format |
| `cpih01.xlsx` | XLSX | Data in Excel format |
| `cpih01.csv-metadata.json` | JSON | CSV-W metadata — data dictionary with column definitions |

Datasets are organised into taxonomy-based category folders (e.g., `economy_inflation_priceindices/cpih01/`).

---

## Output Folder Structure

The default output root is `./stats_data/`, with each source in its own subdirectory:

```
stats_data/
  eurostat/                              # --stats europe
    eurostat_tree_index.json             # Full category tree as JSON
    .eurostat_state.json                 # Download progress state
    eurostat/
      database_by_themes/
        general_and_regional_statistics/
          european_and_national_indicators/
            balance_of_payments/
              ei_bpm6ca_q/
                ei_bpm6ca_q.tsv.gz
                ei_bpm6ca_q.tsv
                ei_bpm6ca_q.sdmx.xml.gz
                ei_bpm6ca_q.sdmx.xml
                ei_bpm6ca_q_dsd.xml
                ei_bpm6ca_q_dataflow.xml
                ei_bpm6ca_q_metadata.html
                ei_bpm6ca_q_constraint.xml
                ei_bpm6ca_q_info.json
        economy_and_finance/
          prices/
            hicp_harmonised_index/
              prc_hicp_midx/
                ...
  swiss/                                 # --stats switzerland
    swiss_tree_index.json
    .swiss_state.json
    swiss_open_data/
      agriculture_fisheries_forestry_food/
        rohholzbilanz3/
          rohholzbilanz3_info.json
          rohholzbilanz3.csv
          rohholzbilanz3.xls
      population_society/
        erwerbstatige-inlandkonzept/
          erwerbstatige-inlandkonzept_info.json
          erwerbstatige-inlandkonzept.csv
          erwerbstatige-inlandkonzept.xls
          erwerbstatige-inlandkonzept.ods
        ...
  ons/                                   # --stats uk
    ons_tree_index.json
    .ons_state.json
    uk_ons/
      economy_inflation_priceindices/
        cpih01/
          cpih01.csv
          cpih01.xlsx
          cpih01.csv-metadata.json
          cpih01_meta.json
          cpih01_info.json
      peoplepopulation_community_wellbeing/
        ...
  unhcr/                                 # --stats unhcr
    unhcr_tree_index.json
    .unhcr_state.json
    unhcr/
      population/
        population_2023/
          population_2023_info.json
          population_2023_data.json
      asylum_applications/
        asylum-applications_2023/
          ...
  hdx/                                   # --stats hdx
    hdx_tree_index.json
    .hdx_state.json
    hdx/
      refugees/
        unhcr-population-data-for-afg/
          unhcr-population-data-for-afg_info.json
          unhcr-population-data-for-afg.csv
      asylum_seekers/
        ...
```

Category folder names are human-readable slugs derived from source titles. Dataset leaf folders keep their canonical code.

### Folder naming styles

You can choose between two naming styles using `--folder-style` on `collect`, or switch with `rename`:

| Style | Flag | Example path | When to use |
|-------|------|-------------|-------------|
| **display** (default) | `--folder-style display` | `economy_and_finance/prices/` | Human-readable browsing |
| **code** | `--folder-style code` | `economy/prc/` | Compact paths, programmatic access |

```bash
# Download with code-style folders
inter-collect --stats europe collect --folder-style code

# Switch existing display folders to code
inter-collect --stats europe rename code

# Switch code folders back to display
inter-collect --stats europe rename display
```

---

## Architecture

```
inter_collector/
  __init__.py
  cli.py                # Click CLI — --stats group option, all commands
  collector.py          # Generic orchestrator — accepts DataSource, manages concurrency
  base.py               # DataSource ABC + SourceConfig dataclass
  download_utils.py     # Shared download/extract helpers (_download_file, _extract_gz, DownloadResult)
  renamer.py            # Directory renamer — switches between code/display naming
  state.py              # Persistent JSON state — tracks completed/failed datasets
  progress.py           # Shared progress utilities — DownloadStats, fmt_bytes()
  sources/
    __init__.py          # SOURCE_REGISTRY + resolve_source() (lazy loading via importlib)
    eurostat/
      __init__.py
      source.py          # EurostatSource(DataSource) — config, fetch, download, tree index
      api.py             # Eurostat API endpoint constants
      toc.py             # TOC XML parser — builds TocEntry category tree
      downloader.py      # Per-dataset download logic (7 file types)
    ons/
      __init__.py
      source.py          # ONSSource(DataSource) — config, fetch, download, tree index
      api.py             # ONS CMD API endpoint constants
      catalog.py         # CMD API catalog fetcher — builds ONSEntry tree by taxonomy
      downloader.py      # Per-dataset download logic (CSV, XLSX, CSV-W, metadata)
    swiss/
      __init__.py
      source.py          # SwissSource(DataSource) — config, fetch, download, tree index
      api.py             # opendata.swiss CKAN API constants
      catalog.py         # CKAN catalog fetcher — builds SwissEntry tree by group
      downloader.py      # Per-dataset download logic (CSV, XLS, ODS, JSON)
    unhcr/
      __init__.py
      source.py          # UNHCRSource(DataSource) — config, fetch, download, tree index
      api.py             # UNHCR Population Statistics API constants
      catalog.py         # Catalog builder — probes endpoints × years
      downloader.py      # Paginated JSON data downloader
    hdx/
      __init__.py
      source.py          # HDXSource(DataSource) — config, fetch, download, tree index
      api.py             # HDX CKAN API constants
      catalog.py         # CKAN catalog fetcher — builds HDXEntry tree
      downloader.py      # Per-dataset resource downloader (CSV, XLSX, XLS)
```

### Adding a new data source

1. Create `sources/newsource/source.py` implementing `DataSource`:
   - `config()` → `SourceConfig` with name, display name, state/index filenames, file types
   - `fetch_catalog(client)` → fetch and parse the remote catalog
   - `collect_datasets(catalog)` → flatten to downloadable entries
   - `download_dataset(client, entry, output_dir, ...)` → download all files for one dataset
   - `save_tree_index(catalog, output_dir)` → save catalog as JSON
2. Register in `sources/__init__.py` `SOURCE_REGISTRY`
3. All CLI commands (collect, extract, status, rename, tree) work automatically

### Data flow

```
cli.py
  │
  ├─ --stats option → resolve_source() → DataSource instance
  │
  ├─ collect command
  │    └─► collector.py run_collection(source, ...)
  │         ├─ source.fetch_catalog(client)
  │         ├─ source.save_tree_index(catalog, output_dir)
  │         ├─ source.collect_datasets(catalog)
  │         ├─ filter by path/codes, skip completed (via state.py)
  │         └─ download loop (async, batched, with semaphore)
  │               └─► source.download_dataset(client, entry, ...)
  │
  ├─ extract command
  │    └─ scans for .gz files, extracts with ThreadPoolExecutor
  │
  ├─ rename command
  │    └─► renamer.py (reads tree index JSON for code↔display mapping)
  │
  ├─ status command
  │    └─ reads source-specific state file, scans disk by file type groups
  │
  └─ tree command
       └─ source.fetch_catalog() → prints category hierarchy
```

### Concurrency model

**Downloads (`collect`):**
- Uses Python `asyncio` with `httpx.AsyncClient` for non-blocking HTTP
- An `asyncio.Semaphore` limits concurrent requests (default: 5)
- Datasets are processed in batches of `concurrency * 2`
- A configurable delay between batches prevents overwhelming the server
- Each request has a 300-second timeout with 3 automatic retries
- HTTP 429 (rate limit) triggers exponential backoff with a 5x multiplier
- HTTP 5xx errors retry with standard exponential backoff

**Extraction (`extract`):**
- Uses `concurrent.futures.ThreadPoolExecutor` — extraction is I/O-bound, so threads avoid the macOS `spawn` overhead of process pools while still achieving real parallelism (GIL is released during file I/O and gzip's C-level decompression)
- Default workers = 2x CPU cores (capped at 32)
- Override with `-c` (e.g., `-c 1` for sequential, `-c 20` for aggressive)
- Configurable I/O buffer per worker via `-b` (default: 64 MB)
- **Sliding window** keeps all workers busy — no batch-then-wait stalls
- Thread-safe byte counters via `threading.Lock` for accurate totals
- **Multi-row live display** (10 fps) with per-worker progress

### State management

- Source-specific state files (e.g., `.eurostat_state.json`, `.ons_state.json`)
- On restart, completed datasets are skipped entirely (no HEAD requests needed)
- Failed datasets can be retried with `--retry-failed`
- State is saved after every dataset — a crash loses at most one in-progress dataset

---

## Installation

### Prerequisites

- **Python 3.10+** (uses `X | Y` union types, `dataclass` features)

### Install from source

```bash
cd inter_stats_data_collector
pip install -e .
```

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `httpx` | >= 0.27 | Async HTTP client with streaming, redirects, timeouts |
| `rich` | >= 13.0 | Terminal progress bars, tables, coloured output |
| `click` | >= 8.1 | CLI framework with commands, options, help text |

---

## Commands & Usage

The CLI is invoked as `inter-collect`. On Windows, if it's not on PATH, use `python -m inter_collector.cli`.

All commands accept `--stats` before the subcommand to select the data source:

```bash
inter-collect <command>                       # All sources (default)
inter-collect --stats all <command>            # All sources (explicit)
inter-collect --stats europe <command>         # Eurostat only
inter-collect --stats uk <command>             # UK ONS only
inter-collect --stats switzerland <command>    # Swiss FSO only
inter-collect --stats unhcr <command>          # UNHCR refugee statistics
inter-collect --stats hdx <command>            # HDX humanitarian data
```

When `--stats all` is used (the default), each command runs sequentially for every registered source.

### `collect` — Download everything

```bash
# Download from all sources (Eurostat + Swiss + UK ONS + UNHCR + HDX)
inter-collect collect

# Download from a single source
inter-collect --stats europe collect

# Custom output directory and concurrency
inter-collect collect -o /data/stats -c 10

# Filter by category path or dataset codes
inter-collect collect --filter-path "eurostat/data/economy"
inter-collect collect --filter-codes "nama_10_gdp,prc_hicp_midx,lfsi_emp_a"

# Preview without downloading
inter-collect collect --dry-run

# Retry previously failed datasets
inter-collect collect --retry-failed

# Verify completed datasets have actual data files on disk
inter-collect collect --verify

# Force re-download everything
inter-collect collect --no-skip-existing

# Use raw source codes for folder names
inter-collect collect --folder-style code
```

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `-o`, `--output` | `./stats_data` | Root output directory (source subdir appended automatically) |
| `-c`, `--concurrency` | `5` | Maximum parallel downloads |
| `-d`, `--delay` | `0.5` | Seconds between download batches |
| `--skip-existing` / `--no-skip-existing` | `true` | Skip files already on disk |
| `--retry-failed` | `false` | Re-attempt previously failed datasets |
| `--filter-path` | — | Only download datasets under this TOC path |
| `--filter-codes` | — | Comma-separated dataset codes |
| `--dry-run` | `false` | Show plan without downloading |
| `--verify` | `false` | Re-check completed datasets for missing data files on disk |
| `--folder-style` | `display` | `display` for readable names, `code` for raw codes |
| `-v`, `--verbose` | `false` | Debug-level logging |

### `extract` — Decompress `.gz` files (parallel)

```bash
# Extract all .gz files (auto-selects worker count)
inter-collect extract

# Use 8 parallel workers
inter-collect extract -c 8

# Force re-extraction even if extracted files exist
inter-collect extract --force

# Increase I/O buffer to 128 MB per worker
inter-collect extract -b 128

# Full example: external drive, 10 workers, 64 MB buffer
inter-collect extract -o /mnt/data/stats_data --force -c 10 -b 64
```

This shows a **multi-row live dashboard** with:

- **Total** — overall progress bar, file count, elapsed time, and ETA
- **Read** — compressed bytes consumed vs total, files/second, read MB/s
- **Write** — decompressed bytes written, compression ratio, write MB/s
- **Per-worker rows** — spinning indicator, current filename, live elapsed timer, completion counter

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `-o`, `--output` | `./stats_data` | Directory containing `.gz` files |
| `-c`, `--concurrency` | auto (2x cores, max 32) | Parallel extraction workers |
| `-b`, `--buffer` | `64` | I/O buffer size in MB per worker |
| `--force` | `false` | Re-extract even if decompressed file exists |
| `-v`, `--verbose` | `false` | Verbose logging |

**Buffer size guide:**

| `-b` value | RAM (20 workers) | Best for |
|------------|-------------------|----------|
| `16` | 320 MB | Low-memory machines |
| **`64`** (default) | **1.3 GB** | **Covers 95%+ of files in a single read** |
| `128` | 2.6 GB | Fast network, large files |
| `256` | 5.1 GB | Only if you have 32+ GB RAM |

### `rename` — Switch folder naming style

```bash
# Convert to display names
inter-collect --stats europe rename display

# Convert to source codes
inter-collect --stats europe rename code

# Preview without making changes
inter-collect --stats europe rename display --dry-run
```

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `-o`, `--output` | `./stats_data` | Directory containing the data |
| `--dry-run` | `false` | Preview renames without applying |
| `-v`, `--verbose` | `false` | Verbose logging |

### `status` — Inspect progress

```bash
inter-collect status
inter-collect --stats europe status -o /mnt/data/stats_data
```

Shows collector state (completed/failed counts), disk statistics by file type, extraction status, and dataset folder count.

### `tree` — Browse the category hierarchy

```bash
inter-collect tree
inter-collect --stats europe tree --depth 5
```

Fetches the live catalog and prints the category hierarchy. Useful for finding `--filter-path` values.

---

## Progress Indicators

During `collect`:

```
⠋ Datasets ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 4,218/10,434  4,195 ok  23 err  3:42:11  4:08:53
⠋ Transfer ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 87.3/? GB  2.1 MB/s  3:42:11
⠋   ei_bpm6ca_q
⠋   prc_hicp_midx
```

During `extract`:

```
Total   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━      8,412/20,858    1h15m30s   ETA 1h02m14s
Read    12.4 GB / 31.2 GB compressed        6.2 files/s     7.9 MB/s
Write   48.3 GB decompressed                3.9x ratio      14.2 MB/s
        ──────────────────────────────
W-01    ⠹ economy/prc/prc_hicp_midx.tsv.gz  #842            12s
W-02    ⠸ transport/rail/rail_pa_total.tsv   #839            4s
W-03    ✓ agric/ef_kvaareg.sdmx.xml.gz       #841
W-04    ⠼ pop/demo_r_mwk_ts.tsv.gz           #840            7s
W-05    ⠋ energy/nrg_cb_sff.sdmx.xml.gz      #838            1s
```

---

## Resumability & State

Each source has its own state file (e.g., `.eurostat_state.json`):

- **Stop and restart any time** — completed datasets are never re-downloaded
- **Partial failures are recorded** — if TSV succeeded but SDMX failed, both are tracked
- **`--retry-failed`** — selectively re-attempts only failed datasets
- **State saved after every dataset** — a crash loses at most one in-progress dataset

**State files per source:**

| Source | State File | Additional Cache Files |
|--------|-----------|----------------------|
| Eurostat | `.eurostat_state.json` | — |
| Swiss FSO | `.swiss_state.json` | — |
| UK ONS | `.ons_state.json` | — |
| UNHCR | `.unhcr_state.json` | — |
| HDX | `.hdx_state.json` | — |

---

## Filtering

### By category path (`--filter-path`)

```bash
inter-collect collect --filter-path "eurostat/data/economy"
inter-collect collect --filter-path "eurostat/data/economy/prc/prc_hicp"
```

Use `inter-collect tree` to discover path codes.

### By dataset code (`--filter-codes`)

```bash
inter-collect collect --filter-codes "nama_10_gdp,prc_hicp_midx,lfsi_emp_a"
```

### Preview first

```bash
inter-collect collect --filter-path "eurostat/data/transp" --dry-run
```

---

## Platform Support

The collector runs on **macOS, Linux, and Windows** without modification.

| Concern | Status |
|---------|--------|
| Dependencies | Pure Python, fully cross-platform |
| File paths | Uses `pathlib.Path` everywhere |
| Async I/O | `asyncio` works natively on all platforms |
| Terminal output | Rich auto-detects terminal capabilities |
| No shell dependencies | No subprocess calls |

```powershell
# Windows — if 'inter-collect' is not found, use:
python -m inter_collector.cli collect -o C:\data\stats -c 5

# macOS / Linux
inter-collect collect -o /data/stats -c 10
```

---

## Configuration & Tuning

### Concurrency

```bash
inter-collect collect -c 10     # Aggressive (10 parallel downloads)
inter-collect collect -c 1      # Conservative (1 at a time)
```

### Extraction tuning

```bash
inter-collect extract -c 4 -b 16     # Conservative (low RAM)
inter-collect extract                  # Balanced (default)
inter-collect extract -c 20 -b 128    # Aggressive (fast network, plenty of RAM)
```

### Disk space

A full Eurostat collection requires approximately **150–200 GB**:

- Compressed data (`.tsv.gz` + `.sdmx.xml.gz`): ~30–40 GB
- Extracted data (`.tsv` + `.sdmx.xml`): ~100–120 GB
- Structural metadata (DSD + Dataflow + Constraint): ~15–20 GB
- Reference metadata + Info JSON: ~1–2 GB

---

## Eurostat API Reference

All endpoints used by this tool are part of the official [Eurostat SDMX 2.1 Dissemination API](https://wikis.ec.europa.eu/display/EUROSTATHELP/API+SDMX+2.1+-+data+query):

| Constant in `api.py` | Full URL | Purpose |
|-----------------------|----------|---------|
| `TOC_XML` | `https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml` | Table of Contents |
| `TOC_TXT` | `https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt` | Table of Contents (text) |
| `METABASE` | `https://ec.europa.eu/eurostat/api/dissemination/catalogue/metabase.txt.gz` | Metabase |
| `FILES_INVENTORY` | `https://ec.europa.eu/eurostat/api/dissemination/files/inventory` | File inventory |
| `FILES_DOWNLOAD` | `https://ec.europa.eu/eurostat/api/dissemination/files` | File download |
| `SDMX_DATA` | `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data` | Data query |
| `SDMX_DATAFLOW` | `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow` | Dataflow definitions |
| `SDMX_DATASTRUCTURE` | `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/datastructure` | Data Structure Definitions |
| `SDMX_CATEGORYSCHEME` | `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/categoryscheme` | Category scheme |
| `SDMX_CODELIST` | `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/codelist` | Code lists |
| `SDMX_CONTENTCONSTRAINT` | `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/contentconstraint` | Content constraints |
| `STATISTICS_DATA` | `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data` | JSON-stat data query |

---

## Data Licenses & Attribution

This tool downloads data from official government statistical offices. All downloaded data remains the intellectual property of the respective source and is subject to its license terms. **You are responsible for complying with these terms when using the data.**

### Eurostat (EU)

- **License**: [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
- **Terms**: [Eurostat Copyright Notice](https://ec.europa.eu/eurostat/help/copyright-notice)
- **Commercial use**: Allowed
- **Attribution**: Required — acknowledge Eurostat as the source
- **Note**: Some datasets originate from third-party sources and may have additional restrictions on commercial reuse. Check each dataset's metadata for details.
- **Legal basis**: Commission Decision 2011/833/EU, updated February 2019 adopting CC BY 4.0 and CC0 as default licenses for Commission content.

### UK ONS (Office for National Statistics)

- **License**: [Open Government Licence v3.0 (OGL v3.0)](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
- **Terms**: [ONS Terms and Conditions](https://www.ons.gov.uk/help/termsandconditions)
- **Commercial use**: Allowed
- **Attribution**: Required — use: *"Source: Office for National Statistics licensed under the Open Government Licence v3.0"*
- **Adapted data**: State: *"Adapted from data from the Office for National Statistics licensed under the Open Government Licence v3.0"*
- **Compatibility**: OGL v3.0 is compatible with CC BY 4.0 and the Open Data Commons Attribution License.

### Swiss FSO (Federal Statistical Office / BFS)

- **License**: [Opendata.swiss Terms of Use](https://opendata.swiss/en/terms-of-use)
- **Terms**: [BFS Terms of Use](https://www.bfs.admin.ch/bfs/en/home/fso/swiss-federal-statistical-office/terms-of-use.html)
- **Commercial use**: **Varies by dataset** — some datasets allow free commercial reuse, others require prior authorisation from the data owner (per Article 13 of the Fee Ordinance) and may involve a fee
- **Attribution**: Always required — provide the source (author, title, and link)
- **Important**: Each dataset on opendata.swiss is marked with a terms-of-use category. Check the specific dataset's metadata to determine whether commercial use is freely permitted or requires authorisation.

### UNHCR (United Nations High Commissioner for Refugees)

- **License**: [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
- **Terms**: [UNHCR Terms of Use for Datasets](https://www.unhcr.org/what-we-do/data-and-publications/data-and-statistics/terms-use-datasets)
- **Commercial use**: Allowed under CC BY 4.0
- **Attribution**: Required — use: *"UNHCR Refugee Population Statistics Database"*
- **API access**: Open to all, no credentials required
- **Disclaimer**: UNHCR disclaims all warranties related to the datasets and APIs. Users are responsible for ensuring appropriate use in context.

### HDX (Humanitarian Data Exchange)

- **License**: Varies per dataset — most UNHCR data uses [CC BY-IGO](https://creativecommons.org/licenses/by/3.0/igo/)
- **Terms**: [HDX Terms of Service](https://data.humdata.org/faqs/licenses)
- **Commercial use**: Allowed under CC BY-IGO with attribution
- **Attribution**: Required — credit the data source organization (e.g., UNHCR)
- **Other licenses**: Individual datasets may use CC BY, CC BY-SA, ODC-ODbL, ODC-BY, ODC-PDDL, or CC0. Check each dataset's metadata.

### This Tool

This tool is licensed under the **Business Source License 1.1 (BSL 1.1)**.

- **Non-production use** (research, academic, personal, evaluation): **Free** — no permission needed
- **Production / commercial use** (any capacity — core product, component, internal tooling, SaaS): **Requires a commercial license** from the author
- **Forking & modification**: Allowed, but forks inherit the same BSL restriction — commercial use of derivative works also requires a license
- **Change date**: 2060-01-01 — after which the license converts to Apache 2.0
- **Licensor**: Alaa Alhorani

See the [LICENSE](LICENSE) file for the full text.

This tool is a download client only — it does not bundle or redistribute any data. The data licenses listed above govern your use of the downloaded data, not the tool itself.
