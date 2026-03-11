"""
# Echovita Pipeline

Orchestrates the full Echovita obituary data integration: web scraping,
mock cloud uploads, JSONL export, SCD enrichment, consolidation, and cleanup.

## Task graph

```
run_scrapy_spider
    ├── validate_s3_upload    ──┐
    ├── validate_gcs_upload   ──┤
    └── validate_jsonl_export ──┼── log_sample_items ──┐
                                └── enrich_scd                  │
                                        └── run_consolidation ──┘
                                                    └── cleanup_temp_files
```

## Tasks

| Task | Description |
|------|-------------|
| `run_scrapy_spider` | Runs the Scrapy spider as a bash subprocess. Pipelines handle mock S3/GCS uploads and write the JSONL export + manifest files. |
| `validate_s3_upload` | Reads `upload_manifest_s3.json` and asserts at least one item was uploaded. |
| `validate_gcs_upload` | Same check for `upload_manifest_gcs.json`. |
| `validate_jsonl_export` | Asserts `obituaries.jsonl` exists and contains at least one record. |
| `log_sample_items` | Logs the first 5 items from the JSONL for observability. |
| `enrich_scd` | Merges the static SCD sample with scraped city/state data → `scd_enriched.csv`. Each scraped record is appended as an open SCD row (`valid_to = NULL`) using today as `valid_from`. |
| `run_consolidation` | Runs DuckDB SCD consolidation on `scd_enriched.csv` → `consolidated_persons.csv`. |
| `cleanup_temp_files` | Removes the S3 and GCS manifest files produced during the run. |

## Configuration

- **Schedule:** daily at 08:00 UTC
- **Retries:** 2, with a 2-minute delay
- **Output directory:** `include/outputs/` (mounted volume — visible on the host)
- **Catchup:** disabled
"""
import csv
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "include" / "outputs"

SCRAPY_CMD = (
    f"cd {PROJECT_ROOT.as_posix()} && "
    f"scrapy crawl echovita "
    f"-s PROJECT_ROOT={OUTPUT_DIR.as_posix()} "
    f"-s JSONL_OUTPUT_PATH=obituaries.jsonl"
)
JSONL_PATH = OUTPUT_DIR / "obituaries.jsonl"
S3_MANIFEST = OUTPUT_DIR / "upload_manifest_s3.json"
GCS_MANIFEST = OUTPUT_DIR / "upload_manifest_gcs.json"
SCD_INPUT = PROJECT_ROOT / "data" / "scd_person_geo_sample.csv"
SCD_ENRICHED = OUTPUT_DIR / "scd_enriched.csv"
CONSOLIDATION_OUTPUT = OUTPUT_DIR / "consolidated_persons.csv"

DEFAULT_ARGS = {
    "owner": "Andres Marciales",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="echovita_pipeline",
    default_args=DEFAULT_ARGS,
    schedule="0 8 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["echovita", "scraping", "obituaries"],
    doc_md=__doc__,
)
def echovita_pipeline_dag():

    @task.bash
    def run_scrapy_spider() -> str:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        return SCRAPY_CMD

    @task
    def validate_s3_upload() -> int:
        if not S3_MANIFEST.exists():
            raise FileNotFoundError(f"S3 manifest not found: {S3_MANIFEST}")
        data = json.loads(S3_MANIFEST.read_text(encoding="utf-8"))
        count = data.get("count", 0)
        if count == 0:
            raise ValueError("Mock S3 upload produced 0 items")
        print(f"S3 validation OK: {count} items")
        return count

    @task
    def validate_gcs_upload() -> int:
        if not GCS_MANIFEST.exists():
            raise FileNotFoundError(f"GCS manifest not found: {GCS_MANIFEST}")
        data = json.loads(GCS_MANIFEST.read_text(encoding="utf-8"))
        count = data.get("count", 0)
        if count == 0:
            raise ValueError("Mock GCS upload produced 0 items")
        print(f"GCS validation OK: {count} items")
        return count

    @task
    def validate_jsonl_export() -> int:
        if not JSONL_PATH.exists():
            raise FileNotFoundError(f"JSONL not found: {JSONL_PATH}")
        with open(JSONL_PATH, encoding="utf-8") as f:
            count = sum(1 for line in f if line.strip())
        if count == 0:
            raise ValueError("JSONL export is empty")
        print(f"JSONL validation OK: {count} records")
        return count

    @task
    def log_sample_items() -> None:
        if not JSONL_PATH.exists():
            print("No JSONL file to sample")
            return
        with open(JSONL_PATH, encoding="utf-8") as f:
            sample = []
            for i, line in enumerate(f):
                if i >= 5:
                    break
                try:
                    sample.append(json.loads(line))
                except Exception as exc:
                    sample.append(f"<parse error: {exc}>")
        print("Sample scraped records:")
        for rec in sample:
            print(rec)

    @task
    def enrich_scd() -> str:
        with open(SCD_INPUT, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if JSONL_PATH.exists():
            today = date.today().isoformat()
            with open(JSONL_PATH, encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    item = json.loads(line)
                    city = item.get("city")
                    state = item.get("state")
                    if not city and not state:
                        continue
                    slug = item.get("url", "").strip("/").split("/")[-1] or "unknown"
                    rows.append({
                        "person_id": slug,
                        "name": item.get("full_name") or "",
                        "state": state or "",
                        "city": city or "",
                        "valid_from": today,
                        "valid_to": "",
                    })

        SCD_ENRICHED.parent.mkdir(parents=True, exist_ok=True)
        with open(SCD_ENRICHED, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["person_id", "name", "state", "city", "valid_from", "valid_to"]
            )
            writer.writeheader()
            writer.writerows(rows)

        scraped = sum(1 for r in rows if r.get("valid_from") == date.today().isoformat())
        print(f"Enriched SCD: {len(rows)} total rows ({scraped} from today's scrape) → {SCD_ENRICHED}")
        return str(SCD_ENRICHED)

    @task
    def run_consolidation(scd_path: str) -> str:
        if str(PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(PROJECT_ROOT))
        from scripts.consolidate_scd import run_consolidation as _consolidate

        out_path = _consolidate(input_path=scd_path, output_path=CONSOLIDATION_OUTPUT)
        print(f"Consolidation complete: {out_path}")
        return out_path

    @task
    def cleanup_temp_files() -> None:
        for path in [S3_MANIFEST, GCS_MANIFEST]:
            if path.exists():
                path.unlink()
                print(f"Removed: {path}")

    scrape = run_scrapy_spider()
    val_s3 = validate_s3_upload()
    val_gcs = validate_gcs_upload()
    val_jsonl = validate_jsonl_export()
    sample = log_sample_items()
    enriched = enrich_scd()
    consolidate = run_consolidation(enriched)
    cleanup = cleanup_temp_files()

    scrape >> [val_s3, val_gcs, val_jsonl]
    val_jsonl >> [sample, enriched]
    [val_s3, val_gcs, sample, consolidate] >> cleanup


echovita_pipeline = echovita_pipeline_dag()
