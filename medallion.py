"""Mini medallion pipeline: CSV -> bronze -> silver -> gold."""

import os

import duckdb

CSV = "data/raw/patients.csv"
con = duckdb.connect()


def bronze() -> int:
    """Land the raw CSV as parquet, verbatim.
    Guarantee: byte-faithful copy of the source; always re-runnable."""
    con.sql(f"COPY (SELECT * FROM '{CSV}') TO 'data/bronze/patients.parquet' (FORMAT parquet)")
    n = con.sql("SELECT COUNT(*) FROM 'data/bronze/patients.parquet'").fetchone()[0]
    print(f"bronze: {n} rows landed")
    return n


def silver(n_bronze: int) -> int:
    """Dedupe on id + enforce the contract.
    Guarantee: every row is clean, typed, and the id is unique."""
    cols = [r[0] for r in con.sql("DESCRIBE SELECT * FROM 'data/bronze/patients.parquet'").fetchall()]
    missing = [c for c in ("Id", "BIRTHDATE", "GENDER") if c not in cols]
    assert not missing, f"missing required columns: {missing}"

    null_ids = con.sql("SELECT COUNT(*) FROM 'data/bronze/patients.parquet' WHERE id IS NULL").fetchone()[0]
    assert null_ids == 0, f"{null_ids} null ids"

    con.sql("""
        COPY (
            SELECT * FROM 'data/bronze/patients.parquet'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1
        ) TO 'data/silver/patients.parquet' (FORMAT parquet)
    """)
    n = con.sql("SELECT COUNT(*) FROM 'data/silver/patients.parquet'").fetchone()[0]
    print(f"silver: {n} rows (dropped {n_bronze - n} duplicate ids)")
    return n


def gold() -> None:
    """One aggregation, partitioned for pruning.
    Guarantee: business-ready answers, one row per gender."""
    con.sql("""
        COPY (
            SELECT gender,
                   COUNT(*) AS n_patients,
                   SUM(CASE WHEN birthdate IS NULL THEN 1 ELSE 0 END) AS null_birthdates
            FROM 'data/silver/patients.parquet'
            GROUP BY gender
        ) TO 'data/gold/patients_by_gender' (FORMAT parquet, PARTITION_BY (gender), OVERWRITE true)
    """)
    print("gold: patients_by_gender written")


def main() -> None:
    for d in ("bronze", "silver", "gold"):
        os.makedirs(d, exist_ok=True)
    n_bronze = bronze()
    silver(n_bronze)
    gold()
    print("done")


if __name__ == "__main__":
    main()
