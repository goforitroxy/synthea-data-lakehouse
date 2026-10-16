import duckdb

import medallion

con = duckdb.connect()


def test_bronze_matches_source():
    assert medallion.bronze() == con.sql(
        "SELECT COUNT(*) FROM 'data/raw/patients.csv'"
    ).fetchone()[0]


def test_silver_has_unique_ids():
    medallion.silver(medallion.bronze())
    total, distinct = con.sql(
        "SELECT COUNT(*), COUNT(DISTINCT id) FROM 'data/silver/patients.parquet'"
    ).fetchone()
    assert total == distinct


def test_gold_reconciles_to_silver():
    medallion.silver(medallion.bronze())
    medallion.gold()
    gold_total = con.sql(
        "SELECT SUM(n_patients) FROM 'data/gold/patients_by_gender/*/*.parquet'"
    ).fetchone()[0]
    silver_total = con.sql(
        "SELECT COUNT(*) FROM 'data/silver/patients.parquet'"
    ).fetchone()[0]
    assert gold_total == silver_total
