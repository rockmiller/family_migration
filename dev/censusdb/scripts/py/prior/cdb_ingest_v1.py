#!/usr/bin/env python3
"""
ingest_year.py

Ingests a single census year from gzipped CSV into bucketed Parquet files,
builds hhid, locid, and pid keys, and updates the DuckDB search index and SQLite manifest.

Assumes input CSV is already sorted by (stateicp, countyicp, serial).
"""

from pathlib import Path
import duckdb
import sqlite3
from config import config_for_year

def ensure_manifest(manifest_path: Path):
    conn = sqlite3.connect(manifest_path)
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS manifests (
        year INTEGER,
        bucket INTEGER,
        filename TEXT,
        row_count INTEGER,
        min_hhid TEXT,
        max_hhid TEXT,
        min_histid TEXT,
        max_histid TEXT,
        min_locid TEXT,
        max_locid TEXT,
        PRIMARY KEY(year,bucket)
      );
    """)
    conn.commit()
    return conn

def run_ingest(cfg: dict):
    year = cfg["year"]
    csv_path = cfg["csv"]
    out_dir = cfg["out_root"] / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    bcount = cfg["bucket_count"]

    state_col = cfg["state_col"]
    county_col = cfg["county_col"]
    serial_col = cfg["serial_col"]
    histid_col = cfg["histid_col"]
    hik_col = cfg["hik_col"]

    # Open DuckDB index DB
    idx_con = duckdb.connect(cfg["index_db"])
    idx_con.execute("""
      CREATE TABLE IF NOT EXISTS search_index (
        year INTEGER,
        hhid VARCHAR,
        pid VARCHAR,
        serial VARCHAR,
        histid VARCHAR,
        hik VARCHAR,
        locid VARCHAR,
        stateicp VARCHAR,
        countyicp VARCHAR,
        bucket INTEGER,
        filename VARCHAR
      );
    """)

    # Read CSV and compute hhid, locid, pid, bucket
    base_view = f"src_{year}"
    csv_expr = f"read_csv_auto('{csv_path.as_posix()}', header={str(cfg['read_csv_opts']['header']).lower()}, sample_size={cfg['read_csv_opts']['sample_size']})"
    idx_con.execute(f"""
      CREATE OR REPLACE VIEW {base_view} AS
      SELECT *,
             CAST({year} AS VARCHAR) || '-' || CAST({serial_col} AS VARCHAR) AS hhid,
             {state_col} || '-' || {county_col} AS locid,
             CAST({year} AS VARCHAR) || '-' || CAST({serial_col} AS VARCHAR) || '-' || CAST({histid_col} AS VARCHAR) AS pid
      FROM {csv_expr};
    """)

    idx_con.execute(f"""
      CREATE OR REPLACE VIEW {base_view}_b AS
      SELECT *,
             (abs(hash64(hhid)) % {bcount})::INT AS bucket
      FROM {base_view};
    """)

    staging_table = f"staging_{year}"
    idx_con.execute(f"DROP TABLE IF EXISTS {staging_table};")
    idx_con.execute(f"""
      CREATE TABLE {staging_table} AS
      SELECT * FROM {base_view}_b;
    """)

    # Open manifest
    manifest_conn = ensure_manifest(cfg["manifest"])
    manifest_cur = manifest_conn.cursor()

    # Write per-bucket files and update manifest + index
    for b in range(bcount):
        out_file = out_dir / f"bucket_{b}.parquet"
        idx_con.execute(f"""
          COPY (SELECT * FROM {staging_table} WHERE bucket = {b})
          TO '{out_file.as_posix()}' (FORMAT PARQUET);
        """)

        stats = idx_con.execute(f"""
          SELECT COUNT(*) AS row_count,
                 MIN(hhid), MAX(hhid),
                 MIN({histid_col}), MAX({histid_col}),
                 MIN(locid), MAX(locid)
          FROM {staging_table}
          WHERE bucket = {b};
        """).fetchone()

        manifest_cur.execute("""
          INSERT OR REPLACE INTO manifests
          (year, bucket, filename, row_count, min_hhid, max_hhid, min_histid, max_histid, min_locid, max_locid)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (year, b, str(out_file), *stats))
        manifest_conn.commit()

        idx_con.execute(f"""
          INSERT INTO search_index (year, hhid, pid, serial, histid, hik, locid, stateicp, countyicp, bucket, filename)
          SELECT {year}, hhid, pid, {serial_col}, {histid_col}, {hik_col}, locid, {state_col}, {county_col}, {b}, '{out_file.as_posix()}'
          FROM {staging_table}
          WHERE bucket = {b};
        """)

        print(f"Wrote bucket {b} → {out_file.name} ({stats[0]} rows)")

    # Finalize
    idx_con.execute("CREATE INDEX IF NOT EXISTS idx_search_pid ON search_index(pid);")
    idx_con.execute("CREATE INDEX IF NOT EXISTS idx_search_hhid ON search_index(hhid);")
    idx_con.execute("CREATE INDEX IF NOT EXISTS idx_search_locid ON search_index(locid);")
    idx_con.execute("CREATE INDEX IF NOT EXISTS idx_search_year_hhid ON search_index(year, hhid);")
    idx_con.execute("ANALYZE;")

    idx_con.execute(f"DROP VIEW IF EXISTS {base_view};")
    idx_con.execute(f"DROP VIEW IF EXISTS {base_view}_b;")
    idx_con.execute(f"DROP TABLE IF EXISTS {staging_table};")

    idx_con.close()
    manifest_conn.close()
    print(f"Ingestion complete for year {year}.")

if __name__ == "__main__":
    cfg = config_for_year(1850)
    run_ingest(cfg)