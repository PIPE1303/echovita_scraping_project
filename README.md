# Echovita Data Integration

End-to-end data engineering pipeline that scrapes obituary records from [Echovita](https://www.echovita.com), simulates cloud uploads to S3 and GCS, exports to JSONL, and consolidates historical geographic data (SCD model) with DuckDB. Orchestrated with Apache Airflow via the Astronomer Astro CLI.

## Documentation

| Document | Description |
|----------|-------------|
| [Executive Report](docs/REPORT_EXECUTIVE.md) | High-level summary, results, and design decisions |
| [Technical Report](docs/REPORT_TECHNICAL.md) | Architecture, query design, production-readiness criteria |
| [Task logs](docs/airflow_logs/) | Airflow task logs from a representative pipeline run |
| [Sample outputs](samples/) | 12 scraped records + SCD consolidation result |

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
│   └── scd_person_geo_sample.csv  # Source SCD data (immutable)
├── samples/                       # Representative outputs from an actual run
├── docs/
│   ├── REPORT_EXECUTIVE.md
│   ├── REPORT_TECHNICAL.md
│   ├── images/                    # Airflow UI screenshots
│   └── airflow_logs/              # Task logs per task
├── include/outputs/               # Runtime outputs (volume-mounted, gitignored)
├── tests/
│   └── test_dag_echovita.py
├── Dockerfile
└── requirements.txt
```

## Quickstart (Astro CLI)

```bash
astro dev start        # builds image, starts Postgres, Scheduler, Webserver, Triggerer
```

Airflow UI: **http://localhost:8080** — credentials: `admin` / `admin`

> **Note:** after any code change outside `dags/`, rebuild the image:
> ```bash
> astro dev stop && astro dev start
> ```

Enable and trigger the `echovita_pipeline` DAG. Outputs are written to `include/outputs/` and are immediately visible on the host due to the volume mount.

## Running components standalone

**Scrapy spider** (writes to `include/outputs/`):

```bash
scrapy crawl echovita
```

**SCD consolidation**:

```bash
python -m scripts.consolidate_scd
# custom paths:
python -m scripts.consolidate_scd --input data/scd_person_geo_sample.csv --output data/consolidated_persons.csv
```

**Local setup without Docker:**

```bash
python -m venv .venv && source .venv/bin/activate   # .venv\Scripts\activate on Windows
pip install scrapy duckdb
```

## Pipeline overview

### Part 1 — Web scraping

The spider crawls up to 5 pages of the Echovita obituaries listing and extracts `full_name`, `date_of_birth`, `date_of_death`, `city`, `state`, and `obituary_text` per record. Each item passes through the pipeline chain:

- **MockS3Pipeline** — serialises items in-memory and writes `upload_manifest_s3.json` on close for downstream validation.
- **MockGCSPipeline** — same pattern, writes `upload_manifest_gcs.json`.
- **RealS3Pipeline** — full boto3 implementation, disabled by default. Enable by uncommenting it in `ITEM_PIPELINES` and providing AWS credentials via environment variables.
- **JsonlExportPipeline** — writes all items to `obituaries.jsonl`.

### Part 2 — SCD consolidation

Collapses a Slowly Changing Dimensions table of person geographic history into one row per `person_id`. The `enrich_scd` DAG task appends each run's scraped city/state data to the SCD table before consolidation, connecting the two parts end-to-end.

| Column | Description |
|--------|-------------|
| `distinct_cities` | Count of distinct non-null cities |
| `first_city` | City from the earliest record |
| `last_city` | City from the most recent record (may be null) |
| `last_non_null_city` | Most recent non-null city |

### Part 3 — Airflow DAG

DAG `echovita_pipeline` runs daily at 08:00 UTC. See the **Docs** tab in the Airflow UI for the full task graph and per-task descriptions (rendered from the DAG's module docstring via `doc_md=__doc__`).

Task graph:
```
run_scrapy_spider
    ├── validate_s3_upload    ──┐
    ├── validate_gcs_upload   ──┤
    └── validate_jsonl_export ──┼── log_sample_items ──┐
                                └── enrich_scd         │
                                        └── run_consolidation ──┘
                                                    └── cleanup_temp_files
```

## Notes

- S3 and GCS uploads are mocked — no cloud credentials required to run the pipeline.
- The spider obeys `robots.txt`, runs with `CONCURRENT_REQUESTS=2` and `DOWNLOAD_DELAY=1.0s`.
- All pipeline outputs are idempotent — re-running any number of times produces the same result.
- Retries: 2 attempts per task with a 2-minute delay.
