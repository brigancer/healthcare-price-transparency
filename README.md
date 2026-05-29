# Healthcare Price Transparency Analytics Pipeline

A data engineering capstone project analyzing hospital price transparency compliance and the relationship between price and quality outcomes in the Nashville, TN metropolitan area.

**Nashville Software School — Data Engineering Cohort 02**
Rob Brigance | May 2026

---

## Analytical Questions

1. Do Nashville-area hospitals that charge more deliver better quality outcomes?
2. Which hospitals are complying with federal CMS price transparency requirements — and how?

---

## Engineering Goals

Beyond answering the analytical questions, this project was built to demonstrate production-grade data engineering principles:

- **Clean code:** All scripts follow Clean Code principles — single-responsibility functions, meaningful naming, no magic values, shared utilities in `conns.py`, and consistent linting via Ruff.
- **No local data:** The pipeline never buffers data to disk. MRF files stream directly from hospital URLs to S3 via `boto3.upload_fileobj`. Transformation runs on Databricks. Nothing touches the local filesystem.
- **Scalability:** Hospital scope is geography-driven from the CMS registry — adding a new market requires only a county filter change, not code changes. The Silver transform handles all MRF format variants dynamically through a single routing script.
- **Automation-ready:** Dagster orchestrates the full pipeline as a DAG of typed assets. Each stage (ingestion → transform → dbt → dashboard) is a discrete, observable unit. GitHub Actions CI/CD is scaffolded for dbt test runs on push.

---

## Architecture

```
CMS MRF Files (Hospital Price Data)         CMS Care Compare API (Quality Scores)
         │                                              │
         ▼                                              ▼
  Python + boto3                               Python + Requests
  (Stream direct to S3,                       (REST API pull,
   zero local buffering)                       Nashville MSA hospitals)
         │                                              │
         └──────────────────┬───────────────────────────┘
                            ▼
                     AWS S3 (Bronze)
                  Raw / unmodified files
                            │
                            ▼
              mrf_transform.py (Databricks Connect)
              Python handles all format complexity:
              JSON · CSV · GZIP · ZIP · Wide · Long
              Normalizes to 31-column canonical schema
              Filters to shoppable services via left_semi join
                            │
                            ▼
              workspace.hpt_dev.mrf_prices_silver
                  (Delta table, 142,437 rows)
                            │
                            ▼
                    dbt Fusion 2.0
               ┌────────────────────────┐
               │  Silver Layer (Views)  │
               │  - stg_mrf_prices      │
               │  - stg_quality_scores  │
               └────────────┬───────────┘
                            │
               ┌────────────▼───────────┐
               │   Gold Layer (Tables)  │
               │  - mart_hospital_prices│
               │  - mart_quality_scores │
               │  - mart_prices_vs_     │
               │    quality             │
               └────────────┬───────────┘
                            │
                            ▼
                    Power BI Desktop
                  (3-tab dashboard)
                            │
                   Orchestrated by
                      Dagster
```

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| Ingestion | Python + boto3 | Stream MRF files direct to S3 via `upload_fileobj`; zero local buffering |
| Ingestion | Python + Requests | CMS Care Compare API pull |
| Storage | AWS S3 | Bronze layer — raw, unmodified files; personal AWS account, `us-east-2` |
| Compute | Databricks Free Edition | Spark-based SQL warehouse; Unity Catalog External Volumes |
| Transform (Silver) | Python + Databricks Connect | `mrf_transform.py` handles all MRF format complexity; writes canonical Silver table |
| Transformation | dbt Fusion 2.0 | Silver SQL passthrough + Gold mart models |
| Orchestration | Dagster | Asset-based pipeline orchestration |
| CI/CD | GitHub Actions | Scaffolded; dbt test runs on push |
| Visualization | Power BI Desktop | Interactive 3-tab dashboard via native Databricks OAuth connector |

---

## Repository Structure

```
healthcare-price-transparency/
├── ingestion/
│   ├── conns.py                    # Shared connection utilities (S3, Databricks)
│   ├── cms/
│   │   └── fetch_quality_scores.py # CMS Care Compare API ingestion
│   └── mrf/
│       ├── mrf_discovery.py        # Discover MRF file URLs from hospital seeds
│       ├── mrf_matcher.py          # Match MRF files to hospitals via fuzzy matching
│       └── mrf_ingest.py           # Stream MRF files direct to S3 (no filtering)
├── transform/
│   └── mrf/
│       └── mrf_transform.py        # Databricks Connect script: normalize all MRF formats
│                                   # to 31-column canonical schema, filter to shoppable
│                                   # services, write to mrf_prices_silver Delta table
├── dbt_hpt_pipeline/
│   ├── seeds/
│   │   ├── nashville_msa_hospitals.csv   # 17 acute care hospitals in scope
│   │   └── shoppable_services.csv        # 70 CMS-mandated shoppable service codes
│   ├── models/
│   │   ├── silver/
│   │   │   ├── stg_mrf_prices.sql        # SQL passthrough over mrf_prices_silver
│   │   │   ├── stg_quality_scores.sql
│   │   │   ├── schema.yml                # 19 dbt tests
│   │   │   └── sources.yml
│   │   └── gold/
│   │       ├── mart_hospital_prices.sql
│   │       ├── mart_quality_scores.sql
│   │       ├── mart_prices_vs_quality.sql
│   │       └── schema.yml                # 25 dbt tests
│   └── dbt_project.yml
├── dagster_hpt/
│   └── src/dagster_hpt/
│       ├── defs/
│       │   ├── assets.py
│       │   └── resources.py
│       └── definitions.py
├── .github/
│   └── workflows/                  # CI/CD scaffold
├── .env.example
├── pyproject.toml
└── README.md
```

---

## Hospital Scope

The pipeline covers **17 acute care hospitals** in the Nashville MSA across 7 hospital systems. Hospital selection is geography-driven using CMS Care Compare county filters — not a hardcoded list — to remain maintainable as the registry updates.

**Nashville MSA counties in scope:** Davidson, Williamson, Rutherford, Wilson, Sumner, Robertson, Cheatham, Dickson, Trousdale

**Exclusions and rationale:**

| Category | Rationale |
|---|---|
| VA / federally operated hospitals | Exempt from CMS price transparency rule; no MRF files published |
| Psychiatric hospitals | No shoppable services in scope; incomplete quality measures |
| Rural / Critical Access hospitals | Limited shoppable service overlap; incomplete quality data |
| Dormant CCNs | Some hospital CCNs report under a parent facility; duplicate CCNs excluded |

---

## Seed Files

### `nashville_msa_hospitals.csv`
The authoritative hospital list for the pipeline. All ingestion, matching, and transformation scripts derive their hospital scope from this file.

| Column | Description |
|---|---|
| `ccn` | CMS Certification Number — primary key used throughout the pipeline |
| `hospital_name` | Official hospital name as published by CMS |
| `system` | Hospital system (e.g. HCA Healthcare / TriStar, Vanderbilt Health) |
| `hospital_url` | Root domain used for `cms-hpt.txt` MRF pointer file discovery |
| `location_name_override` | Optional. When populated, `mrf_matcher.py` uses this value instead of `hospital_name` for fuzzy matching against the MRF index. Required for hospitals whose MRF file lists a location name that doesn't match the CMS hospital name. |
| `mrf_url_override` | Optional. When populated, `mrf_matcher.py` uses this URL directly, bypassing the fuzzy match entirely. Required for hospitals whose `cms-hpt.txt` points to a stale or incorrect MRF URL. |

### `shoppable_services.csv`
The 70 CMS-mandated shoppable services, plus 2 alternate urinalysis codes (72 rows total). Used in `mrf_transform.py` as the filter reference for the `left_semi` join that limits Silver data to shoppable service codes only.

| Column | Description |
|---|---|
| `code` | CPT or DRG procedure code (cast to string at read time) |
| `description` | Human-readable service description |
| `code_type` | `CPT` or `DRG` |

---

## Data Sources

### CMS Machine-Readable Files (MRF)
Hospital price transparency files mandated by the CMS Hospital Price Transparency Rule (45 CFR 180). Each hospital publishes a machine-readable file containing negotiated rates with payers for all items and services.

- **Discovery:** CMS `cms-hpt.txt` pointer files per hospital domain, filtered to Nashville MSA
- **Matching:** Fuzzy matching via `rapidfuzz` + `scipy` linear assignment to map CCNs to MRF URLs
- **Format:** Files vary by hospital — true JSON, true CSV, ZIP-compressed CSV, and GZIP files with incorrect `.csv` extensions. All land in Bronze unmodified.
- **Transform:** `mrf_transform.py` handles all format detection and normalization. Shoppable services filter applied here via `left_semi` join against the `shoppable_services` seed.

### CMS Care Compare
Hospital quality scores from the CMS Care Compare dataset (`xubh-q36u`), accessed via the CMS Socrata API.

- **Metrics included:** Overall star rating, mortality measures, safety measures, readmission measures, patient experience measures
- **Filter:** Nashville MSA facility IDs matched to hospital seed

---

## Compliance Findings

During MRF discovery and ingestion, several categories of compliance issues were identified in published hospital price transparency files:

- **Field misuse:** At least one hospital's NPI field was populated with taxonomy codes rather than National Provider Identifiers, which violates the CMS schema specification.
- **Stale URL publication:** At least one hospital published a `cms-hpt.txt` pointer file referencing an outdated MRF URL that no longer resolves. The correct file required manual discovery.
- **Disguised compression:** At least one hospital system published files with `.csv` extensions that were actually GZIP-compressed (detectable via magic bytes `1f8b`). This violates CMS format requirements — the rule permits only JSON or CSV — and breaks standard CSV parsers silently.
- **Non-compliant compression (declared):** At least one hospital published a file with a `.zip` extension. ZIP is also not a permitted format under the CMS price transparency rule regardless of whether the extension matches the actual format.

These findings were reported to the respective hospital price transparency contacts via email.

---

## Databricks Tables

| Table | Type | Rows | Description |
|---|---|---|---|
| `mrf_prices_silver` | Delta Table | 142,437 | Canonical Silver schema, all 17 hospitals, written by `mrf_transform.py` |
| `stg_mrf_prices` | View (dbt) | — | dbt SQL passthrough over `mrf_prices_silver` |
| `stg_quality_scores` | View (dbt) | 16 | CMS quality scores, one row per hospital |
| `mart_hospital_prices` | Delta Table (dbt) | 91,977 | Negotiated and cash prices by hospital and service |
| `mart_quality_scores` | Delta Table (dbt) | 16 | Quality scorecard per hospital — 16 of 17 hospitals; Ascension Saint Thomas Midtown (440133) has no separate CMS quality scores after merger |
| `mart_prices_vs_quality` | Delta Table (dbt) | 2,272 | Joined price and quality for scatter analysis |

---

## Dashboard

Built in **Power BI Desktop** connected to Databricks via the native OAuth connector.

**Tab 1 — Price vs. Quality**
Scatter chart plotting median negotiated price against CMS star rating by hospital system, with a slicer for filtering by service description.

**Tab 2 — Hospital Quality Scorecard**
Table showing star rating with conditional color formatting alongside mortality, readmission, and safety measure counts (above/below national average).

**Tab 3 — Price Comparison**
Clustered bar chart comparing negotiated vs. cash price by hospital for a selected service.

---

## Local Setup

### Prerequisites

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) package manager
- AWS CLI with SSO configured (`aws sso login --profile <your-profile>`)
- Databricks Free Edition account with a Personal Access Token (requires: `clusters`, `environments`, `jobs`, `sql`, `unity-catalog`, `workspace` scopes)
- dbt Fusion 2.0 ([install instructions](https://docs.getdbt.com/docs/core/pip-install))
- Power BI Desktop (Windows only; available via Microsoft Store — no account required for authoring)

### Environment Variables

Copy `.env.example` to `.env` and populate all values:

```
DATABRICKS_HOST=
DATABRICKS_CONNECT_TOKEN=
DATABRICKS_HTTP_PATH=
AWS_PROFILE=
AWS_REGION=
S3_BUCKET_NAME=
S3_PREFIX=
BRONZE_MRF_VOLUME=
SILVER_MRF_TABLE=
CMS_API_BASE_URL=
```

> **Note:** `DATABRICKS_CONNECT_TOKEN` (used by `mrf_transform.py`) and `DATABRICKS_DBT_PYTHON_TOKEN` (used by dbt Fusion) are the same token value — a single Databricks PAT with `all-apis` scope. `DATABRICKS_DBT_PYTHON_TOKEN` must be set as a PowerShell environment variable before running dbt commands; it is not read from `.env`.

> **AWS SSO:** The pipeline targets a personal AWS S3 bucket (`us-east-2`). Run `aws sso login --profile <your-profile>` if using SSO credentials, or configure access key credentials via `~/.aws/credentials`. Sessions expire after ~8 hours for SSO.

### Install Dependencies

```powershell
# Root venv (ingestion + transform)
uv sync

# Dagster venv
cd dagster_hpt
uv sync
```

### dbt Setup

dbt Fusion uses a separate `profiles.yml` at `~/.dbt/profiles.yml`. Configure your Databricks connection there. In a fresh PowerShell session, set the token before running dbt commands:

```powershell
$env:DATABRICKS_DBT_PYTHON_TOKEN = "<your-token>"
dbtf debug   # verify all checks pass
```

### Running the Pipeline

**Step 1 — Ingest CMS quality scores:**
```powershell
uv run python ingestion/cms/fetch_quality_scores.py
```

**Step 2 — Discover MRF file URLs:**
```powershell
uv run python ingestion/mrf/mrf_discovery.py
```

**Step 3 — Match MRF files to hospitals:**
```powershell
uv run python ingestion/mrf/mrf_matcher.py
```

**Step 4 — Stream MRF files to S3 Bronze (no filtering):**
```powershell
uv run python ingestion/mrf/mrf_ingest.py
```

**Step 5 — Transform raw MRF data to Silver (Databricks Connect):**
```powershell
uv run python transform/mrf/mrf_transform.py
```

**Step 6 — Run dbt models (Silver views → Gold tables):**
```powershell
dbtf run
dbtf test
```

**Step 7 — Start Dagster UI:**
```powershell
cd dagster_hpt
.venv\Scripts\activate
dg dev
# Open http://127.0.0.1:3000
```

---

## Known Issues

- **CDM code leakage in Silver:** `mrf_transform.py` left-semi join needs a `code_type` filter to exclude Charge Description Master codes not in the shoppable service list. Add: `code_1_type IN ('CPT','HCPCS','DRG','MS-DRG','TRIS-DRG','RC','APC')`.
- **Cash price nulls in `mart_hospital_prices`:** Cash price is a separate charge type not tied to payer rows; requires a dedicated join path in the Gold model.
- **Dagster lineage gap:** Silver models use `read_files()` rather than dbt `source()` refs, so upstream S3 connections do not appear in the Dagster asset lineage graph.
- **Hospital name truncation:** One hospital name is truncated in `mart_quality_scores` due to a character limit in the CMS source data join.
- **`billing_class` case inconsistency:** Mixed `Facility` / `facility` casing not yet normalized in Silver.
- **GitHub Actions:** CI/CD scaffold is in place but not yet configured for live dbt test runs on push.

---

## Future Work

- **CMS enforcement actions:** An ingestion placeholder exists at `ingestion/cms/` for pulling CMS price transparency enforcement and penalty data. Would power a Regulatory Accountability dashboard tab.
- **openFDA drug/device recalls:** A placeholder exists at `ingestion/fda/` for cross-referencing FDA recall data against Nashville hospital billing codes — a Safety Signals mart.
- **Statewide expansion:** The pipeline is geography-driven; expanding from the Nashville MSA to all Tennessee hospitals requires only a county filter change, not code changes.
- **Automated hospital seed generation:** The `nashville_msa_hospitals.csv` seed is currently maintained manually. A script to auto-generate it from the CMS registry filtered by county and hospital type would make the pipeline fully self-contained.
- **FastAPI endpoint:** A queryable API layer over the Gold tables for programmatic price lookups.

---

## Design Decisions

- **Bronze = truly raw:** MRF files are streamed directly to S3 via `boto3.upload_fileobj` with zero local buffering. No transformation occurs before landing.
- **Python owns format complexity, dbt owns SQL transformation:** MRF files arrive in at least 6 distinct formats (true JSON, true CSV wide, true CSV long, ZIP-compressed CSV, GZIP disguised as CSV, and a national multi-hospital CSV). All format detection and normalization to the 31-column canonical schema is handled by `mrf_transform.py` using Databricks Connect. dbt Silver is a clean SQL passthrough; Gold models operate on consistent, well-typed input.
- **Shoppable services filter in transform:** Filtering to 70 CMS-mandated shoppable service codes occurs in `mrf_transform.py` via a `left_semi` join against the `shoppable_services` seed, keeping ingestion concerns separate from transformation concerns.
- **Geography-driven hospital discovery:** Hospital scope is determined by CMS registry county filters, not a hardcoded list, making the pipeline extensible to other markets.
- **dbt test severity tiers:** Silver tests use `severity: warn` for business-logic checks. Hard failures are reserved for Gold layer integrity tests.
- **Personal AWS account for S3 and Databricks integration:** All ingestion targets a personal S3 bucket (`us-east-2`) co-located with the Databricks Free Edition workspace. This avoids cross-region data transfer costs and enables Unity Catalog External Volume registration via CloudFormation. A school-provisioned bucket was used during early development before Databricks permissions were resolved; the personal bucket is the sole production target.
- **Canonical Silver schema:** All MRF format handlers normalize to an identical 31-column contract — 21 string columns and 10 numeric columns — enforced via explicit `CAST AS STRING` and `TRY_CAST AS DOUBLE` on every field. This is what makes it possible to union output from 6 incompatible source formats into a single Silver table that dbt can operate on cleanly.
- **`mrf_staging` Volume:** ZIP and GZIP files cannot be read back reliably from Unity Catalog Delta tables via Spark Connect due to a client-side schema truncation bug on wide tables. The solution is a transient Databricks Volume (`mrf_staging`) where decompressed CSVs are written before the Silver transform, then deleted afterward. The volume persists; the files do not.
- **Incremental skip logic in `mrf_transform.py`:** At run start, already-processed CCNs are queried from the Silver table and skipped. This makes the transform fully re-runnable — a partial failure can be resumed without reprocessing hospitals that already succeeded.
- **Fuzzy matching with linear assignment:** `mrf_matcher.py` uses `rapidfuzz` token set ratio scoring combined with `scipy` linear_sum_assignment to map CCNs to MRF URLs. Simple fuzzy matching is insufficient when a hospital domain hosts files for multiple locations — linear assignment enforces one-to-one matching across the full domain matrix, preventing two hospitals from claiming the same file.
- **3-file retention in `mrf_discovery.py`:** Only the 3 most recent `cms-hpt.txt` files per hospital domain are retained in S3. This provides a short history for debugging without unbounded storage growth.
- **DRG normalization in `mart_hospital_prices`:** The shoppable services seed uses `DRG` as the code type, but hospitals publish variants including `MS-DRG` and `TRIS-DRG`. The Gold join condition includes an `OR` branch to match all DRG variants, which corrected approximately 4,000 missing rows.
- **Median via `percentile_cont(0.5)` in `mart_prices_vs_quality`:** Price comparisons use the statistical median rather than mean, making the price vs. quality scatter chart resistant to outlier rates that would otherwise skew a hospital's apparent price position.

---

## License

This project was completed as a capstone for the Nashville Software School Data Engineering program. All hospital price data is sourced from publicly available CMS-mandated disclosures.