# Executive Report — Echovita Data Integration

## Overview

This report summarizes the technical assessment completed for the Veritas project's Echovita data integration initiative. The objective was to design and implement an end-to-end data pipeline capable of ingesting obituary records from [Echovita](https://www.echovita.com), simulating cloud storage uploads, and consolidating historical geographic data for deceased individuals.

The deliverable is a fully functional, production-oriented pipeline covering three areas: web scraping, data consolidation, and workflow orchestration.

---

## What Was Built

### Part 1 — Web Scraping

A Scrapy spider crawls up to 5 pages of Echovita's obituary listing and extracts the following fields per record:

| Field | Description |
|-------|-------------|
| `full_name` | Full name of the deceased |
| `date_of_birth` | Date of birth |
| `date_of_death` | Date of death |
| `city` / `state` | Geographic location of residence |
| `obituary_text` | Full obituary text, cleaned of boilerplate |

Each item is passed through a pipeline chain that simulates S3 and GCS uploads (mocked), and exports all records to a JSONL file. Upload manifests are written to disk at the end of each run for downstream validation.

### Part 2 — SCD Data Consolidation

A DuckDB-based script collapses a Slowly Changing Dimensions (SCD) table of person geographic history into one summary row per individual:

| Column | Definition |
|--------|------------|
| `distinct_cities` | Count of distinct cities across all history records |
| `first_city` | City from the earliest record |
| `last_city` | City from the most recent record (may be null) |
| `last_non_null_city` | Most recent city with a non-null value |

The scraping output enriches this table by appending each scraped record as a new open SCD row (`valid_to = NULL`), connecting real-time data ingestion to the historical model.

### Part 3 — Airflow Orchestration

An Apache Airflow DAG (`echovita_pipeline`) coordinates the full pipeline on a daily schedule (08:00 UTC). The task graph reflects clear data dependencies:

```
run_scrapy_spider
    ├── validate_s3_upload    ──┐
    ├── validate_gcs_upload   ──┤
    └── validate_jsonl_export ──┼── log_sample_items ──┐
                                └── enrich_scd         │
                                        └── run_consolidation ──┘
                                                    └── cleanup_temp_files
```

---

## Key Design Decisions

- **No cloud credentials required.** S3 and GCS integrations are fully mocked via in-memory pipelines with file-based manifests for validation. Real S3 upload is available as an opt-in pipeline (`RealS3Pipeline`).
- **Idempotent by design.** Every output file is overwritten on each run. Re-running the pipeline any number of times produces the same result given the same input.
- **Extensibility.** Adding a new data source requires only a new Scrapy spider and item definition. The existing pipeline chain (mock uploads, JSONL export, SCD enrichment) handles any `scrapy.Item` subclass without modification.
- **Respectful crawling.** The spider obeys `robots.txt`, runs with 2 concurrent requests, and enforces a 1-second download delay between requests.

---

## Results

A sample run scraped **12 obituary records** across 2 pages, producing clean structured data:

```json
{
  "full_name": "Debrah Hernandez Garcia",
  "date_of_birth": "November 5, 1956",
  "date_of_death": "February 23, 2026",
  "city": "Oak Harbor",
  "state": "Washington",
  "obituary_text": "With profound sadness, we say goodbye to Debrah Hernandez Garcia..."
}
```

SCD consolidation on the provided sample dataset produced the expected output:

| person_id | distinct_cities | first_city | last_city | last_non_null_city |
|-----------|----------------|------------|-----------|--------------------|
| 1 | 2 | Houston | *(null)* | Dallas |
| 2 | 1 | San Francisco | San Francisco | San Francisco |
| 3 | 0 | *(null)* | *(null)* | *(null)* |

---

## Technologies

| Component | Technology |
|-----------|------------|
| Web scraping | Python 3.12, Scrapy 2.14 |
| Data consolidation | DuckDB |
| Orchestration | Apache Airflow 2.x (Astronomer Astro CLI) |
| Containerization | Docker (Astronomer Runtime 13.5) |
| Cloud simulation | In-memory mock pipelines + JSON manifests |

---

## Sample Outputs and Execution Evidence

| Artifact | Location |
|----------|----------|
| Scraped records (12) | [`../samples/obituaries_sample.jsonl`](../samples/obituaries_sample.jsonl) |
| SCD consolidation result | [`../samples/consolidated_persons_sample.csv`](../samples/consolidated_persons_sample.csv) |
| DAG graph screenshot | [`images/dag_graph.png`](images/dag_graph.png) |
| Task logs (full run) | [`airflow_logs/`](airflow_logs/) |
