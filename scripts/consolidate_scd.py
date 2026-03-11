"""
SCD consolidation: collapses person geographic history into one row per person_id.

Input:  CSV with columns (person_id, name, state, city, valid_from, valid_to).
Output: CSV with (person_id, distinct_cities, first_city, last_city, last_non_null_city).
"""
from pathlib import Path

import duckdb


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def run_consolidation(
    input_path: str | Path,
    output_path: str | Path | None = None,
    input_format: str = "csv",
) -> str:
    """
    Consolidate SCD data and return the output file path.

    Args:
        input_path:   Path to the SCD CSV.
        output_path:  Destination CSV. Defaults to data/consolidated_persons.csv.
        input_format: Only 'csv' is supported.
    """
    root = get_project_root()
    input_path = Path(input_path)
    if not input_path.is_absolute():
        input_path = root / input_path
    out = Path(output_path) if output_path else root / "data" / "consolidated_persons.csv"
    if not out.is_absolute():
        out = root / out
    out.parent.mkdir(parents=True, exist_ok=True)

    if input_format != "csv":
        raise ValueError("Only input_format='csv' is supported.")

    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE scd_geo AS SELECT * FROM read_csv_auto(?, ignore_errors=true)",
        [str(input_path)],
    )
    conn.execute(
        """
        CREATE OR REPLACE TABLE scd_geo AS
        SELECT
            person_id,
            name,
            state,
            NULLIF(TRIM(CAST(city AS VARCHAR)), '') AS city,
            valid_from,
            valid_to
        FROM scd_geo
        """
    )
    conn.execute(
        """
        CREATE TABLE consolidated AS
        WITH ordered AS (
            SELECT
                person_id,
                city,
                valid_from,
                ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY valid_from ASC)  AS rn_asc,
                ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY valid_from DESC) AS rn_desc,
                ROW_NUMBER() OVER (
                    PARTITION BY person_id
                    ORDER BY CASE WHEN city IS NOT NULL THEN 0 ELSE 1 END, valid_from DESC
                ) AS rn_last_non_null
            FROM scd_geo
        ),
        agg AS (
            SELECT
                person_id,
                COUNT(DISTINCT city) FILTER (WHERE city IS NOT NULL) AS distinct_cities,
                MAX(CASE WHEN rn_asc = 1 THEN city END)                              AS first_city,
                MAX(CASE WHEN rn_desc = 1 THEN city END)                             AS last_city,
                MAX(CASE WHEN rn_last_non_null = 1 AND city IS NOT NULL THEN city END) AS last_non_null_city
            FROM ordered
            GROUP BY person_id
        )
        SELECT
            a.person_id,
            COALESCE(a.distinct_cities, 0)::INT AS distinct_cities,
            a.first_city,
            a.last_city,
            a.last_non_null_city
        FROM agg a
        ORDER BY a.person_id
        """
    )

    if out.exists():
        out.unlink()
    out_safe = str(out).replace("\\", "/").replace("'", "''")
    conn.execute(f"COPY consolidated TO '{out_safe}' (HEADER, DELIMITER ',');")
    conn.close()
    return str(out)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Consolidate SCD person/geo data.")
    parser.add_argument(
        "--input",
        default=get_project_root() / "data" / "scd_person_geo_sample.csv",
        help="Path to SCD CSV",
    )
    parser.add_argument("--output", default=None, help="Output CSV path")
    args = parser.parse_args()
    print("Consolidation done. Output:", run_consolidation(args.input, args.output))
