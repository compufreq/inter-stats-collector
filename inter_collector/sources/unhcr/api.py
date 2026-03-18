"""UNHCR Refugee Statistics API constants.

API Documentation:
    https://api.unhcr.org/docs/refugee-statistics.html
    https://www.unhcr.org/refugee-statistics/insights/explainers/forcibly-displaced-api.html

The UNHCR Refugee Statistics API provides access to global displacement data
spanning 1951–present, covering refugees, asylum-seekers, IDPs, stateless
persons, and durable solutions.

License: Creative Commons Attribution 4.0 International (CC BY 4.0)
Attribution: "UNHCR Refugee Population Statistics Database"
API access: Open to all, no authentication required.
"""

# API base URL
API_BASE = "https://api.unhcr.org/population/v1"

# ─── Data endpoints ───────────────────────────────────────────────────
# Each returns paginated JSON with {page, maxPages, total, items}

POPULATION = f"{API_BASE}/population/"
# Population stock figures (refugees, asylum-seekers, IDPs, stateless, etc.)
# Fields: refugees, asylum_seekers, returned_refugees, idps, returned_idps,
#         stateless, ooc (others of concern), oip (other people in need),
#         hst (host community)

ASYLUM_APPLICATIONS = f"{API_BASE}/asylum-applications/"
# Individual asylum applications by year, country of asylum/origin
# Fields: procedure_type, app_type, dec_level, app_pc, applied

ASYLUM_DECISIONS = f"{API_BASE}/asylum-decisions/"
# Decisions on asylum applications (recognized, rejected, closed)
# Fields: procedure_type, dec_level, dec_pc, dec_recognized, dec_other,
#         dec_rejected, dec_closed, dec_total

SOLUTIONS = f"{API_BASE}/solutions/"
# Durable solutions: return, resettlement, naturalisation
# Fields: returned_refugees, resettlement, naturalisation, returned_idps

DEMOGRAPHICS = f"{API_BASE}/demographics/"
# Age/gender breakdown of displaced populations
# Fields: f_0_4, f_5_11, f_12_17, f_18_59, f_60, f_other, f_total,
#         m_0_4, m_5_11, m_12_17, m_18_59, m_60, m_other, m_total, total

UNRWA = f"{API_BASE}/unrwa/"
# UNRWA-registered Palestine refugees
# Fields: total

# ─── Reference endpoints ──────────────────────────────────────────────

COUNTRIES = f"{API_BASE}/countries/"
# 249 countries with codes, names, regions (3 pages)

REGIONS = f"{API_BASE}/regions/"
# 6 UNHCR operational regions

YEARS = f"{API_BASE}/years/"
# Available years: 1951–2026

# ─── Endpoint registry (for catalog building) ─────────────────────────

DATA_ENDPOINTS = {
    "population": {
        "url": POPULATION,
        "title": "Population Statistics",
        "description": "Stock figures of forcibly displaced and stateless persons at year-end",
    },
    "asylum-applications": {
        "url": ASYLUM_APPLICATIONS,
        "title": "Asylum Applications",
        "description": "Individual asylum applications by country of asylum and origin",
    },
    "asylum-decisions": {
        "url": ASYLUM_DECISIONS,
        "title": "Asylum Decisions",
        "description": "Decisions on asylum applications: recognized, rejected, closed",
    },
    "solutions": {
        "url": SOLUTIONS,
        "title": "Solutions",
        "description": "Durable solutions: return, resettlement, naturalisation",
    },
    "demographics": {
        "url": DEMOGRAPHICS,
        "title": "Demographics",
        "description": "Age and gender breakdown of displaced populations",
    },
    "unrwa": {
        "url": UNRWA,
        "title": "UNRWA",
        "description": "UNRWA-registered Palestine refugees",
    },
}

# ─── Query parameters ─────────────────────────────────────────────────
# Common across all data endpoints:
#   year       - single year filter
#   yearFrom   - start year (range)
#   yearTo     - end year (range)
#   coo        - country of origin (ISO3 code)
#   coa        - country of asylum (ISO3 code)
#   cooAll     - include all countries of origin (boolean)
#   coaAll     - include all countries of asylum (boolean)
#   cfType     - code format type
#   page       - page number (1-indexed)
#   limit      - items per page (default varies, max ~100)

# Pagination
DEFAULT_LIMIT = 100  # items per page
DEFAULT_PAGE = 1

# Rate limiting — UNHCR has undocumented limits; be conservative
INTER_REQUEST_DELAY = 1.0  # seconds between paginated/year requests
INTER_ENDPOINT_DELAY = 2.0  # seconds between different endpoints
RECOMMENDED_CONCURRENCY = 1  # sequential downloads to avoid rate limits
