# Echovita Data Integration

End-to-end data engineering pipeline that scrapes obituary data from [Echovita](https://www.echovita.com), simulates cloud uploads to S3 and GCS, exports to JSONL, and consolidates SCD person/geo data using DuckDB. Orchestrated with Apache Airflow via the Astronomer Astro CLI.

## Project structure

```
echovita_project/
├── dags/
│   └── echovita_pipeline_dag.py   # Airflow DAG (TaskFlow API)
├── echovita_scraper/              # Scrapy project
│   ├── spiders/echovita_spider.py
│   ├── pipelines.py               # MockS3, MockGCS, RealS3 (opt-in), JSONL
│   ├── items.py
│   └── settings.py
├── scripts/
│   └── consolidate_scd.py         # DuckDB SCD consolidation
├── data/
│   └── scd_person_geo_sample.csv
├── include/outputs/               # Mounted volume — all runtime outputs land here
├── tests/
│   └── test_dag_echovita.py
├── Dockerfile                     # Astro Runtime + project dependencies
├── requirements.txt
└── airflow_settings.yaml          # Local connections/variables (add to .gitignore if secret)
```

## Quickstart (Astro CLI)

```bash
astro dev start        # starts Postgres, Scheduler, Webserver, Triggerer
```

Airflow UI: **http://localhost:8080** — credentials: `admin` / `admin`

Enable and trigger the `echovita_pipeline` DAG. Outputs are written to `include/outputs/`
and are immediately visible on the host due to the volume mount.

```bash
astro dev stop         # tear down containers
astro dev restart      # apply code/config changes without full reset
```

## Running components standalone

**Scrapy spider** (writes to `include/outputs/`):

```bash
scrapy crawl echovita
```

**SCD consolidation** (reads `data/scd_person_geo_sample.csv`):

```bash
python -m scripts.consolidate_scd
# custom paths:
python -m scripts.consolidate_scd --input data/scd_person_geo_sample.csv --output data/consolidated_persons.csv
```

**Local setup without Docker:**

```bash
python -m venv .venv && source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install scrapy duckdb
```

## Pipeline overview

### Part 1 — Web scraping

The spider crawls up to 5 pages of the Echovita obituaries listing and extracts four fields per record: `full_name`, `date_of_birth`, `date_of_death`, `obituary_text`. Each scraped item passes through the pipeline chain:

- **MockS3Pipeline** — serialises items to an in-memory dict and writes `upload_manifest_s3.json` on close.
- **MockGCSPipeline** — same pattern, writes `upload_manifest_gcs.json`.
- **RealS3Pipeline** — disabled by default; enable by adding it to `ITEM_PIPELINES` at priority 150 and providing `S3_BUCKET_NAME` and AWS credentials via environment variables.
- **JsonlExportPipeline** — writes all items to `obituaries.jsonl`.

### Part 2 — SCD consolidation

Collapses a Slowly Changing Dimensions table of person geographic history into one row per `person_id`:

| Column | Description |
|--------|-------------|
| `distinct_cities` | Count of distinct non-null cities across all SCD rows |
| `first_city` | City from the earliest `valid_from` row |
| `last_city` | City from the most recent `valid_from` row (may be null) |
| `last_non_null_city` | Most recent non-null city |

### Part 3 — Airflow DAG

See the DAG docstring (visible in the Airflow UI under **Docs**) for the full task graph and per-task descriptions.

## Notes

- S3 and GCS uploads are mocked — no cloud credentials required.
- The spider obeys `robots.txt` and runs with `CONCURRENT_REQUESTS=2` and `DOWNLOAD_DELAY=1.0s`.
- All pipeline outputs are idempotent (overwrite on each run).
