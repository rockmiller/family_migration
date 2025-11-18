#!/usr/bin/env python3
"""
verify_sorted_csv.py

Edit CONFIG below and run. Uses duckdb to scan the gz CSV with minimal memory.
Exits 0 when inversions <= threshold, otherwise exits 1.

Requirements:
  pip install duckdb

CONFIG:
  csv  - path to gzipped CSV
  year - integer year (used only for logging)
  cols - column names: stateicp, countyicp, serial
  threshold - allowed number of ordering inversions before failing
  sample_limit - how many inversion rows to return as examples (0 to disable)
"""

from pathlib import Path
import sys
import duckdb
import json

# ---------- Configuration (edit) ----------
from config import get_config
cfg = get_config(1850)
run_ingest(cfg)  # your ingestion function

# ---------- End configuration ----------

def sql_read_csv_expr(path, opts):
    # Build a read_csv_auto SQL expression with options encoded inline.
    # Keep this minimal; if you need custom delim/encoding adjust CONFIG.read_csv_opts.
    opt_parts = []
    for k, v in opts.items():
        if isinstance(v, bool):
            vv = 'true' if v else 'false'
            opt_parts.append(f"{k}={vv}")
        else:
            opt_parts.append(f"{k}={json.dumps(v)}")
    opts_str = ", ".join(opt_parts)
    return f"read_csv_auto('{path}', {opts_str})"

def run_verify(cfg):
    csv_path = Path(cfg["csv"]).resolve()
    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}")
        return 2

    state_col = cfg["state_col"]
    county_col = cfg["county_col"]
    serial_col = cfg["serial_col"]
    threshold = int(cfg["threshold"])
    sample_limit = int(cfg.get("sample_limit", 0))

    con = duckdb.connect(database=":memory:")
    read_expr = sql_read_csv_expr(str(csv_path), cfg.get("read_csv_opts", {}))
    view_name = "src_view"

    # create view that selects only the needed columns (and keeps them as strings)
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT CAST({state_col} AS VARCHAR) AS {state_col}, CAST({county_col} AS VARCHAR) AS {county_col}, CAST({serial_col} AS VARCHAR) AS {serial_col} FROM {read_expr};")

    # count inversions using lag window. This reads only the three columns, streaming.
    inv_sql = f"""
    SELECT COUNT(*)::BIGINT AS inversions FROM (
      SELECT
        lag({state_col}) OVER (ORDER BY NULL) AS prev_state,
        lag({county_col}) OVER (ORDER BY NULL) AS prev_county,
        lag({serial_col}) OVER (ORDER BY NULL) AS prev_serial,
        {state_col}, {county_col}, {serial_col}
      FROM {view_name}
    ) WHERE
      ( {state_col} < prev_state )
      OR ( {state_col} = prev_state AND {county_col} < prev_county )
      OR ( {state_col} = prev_state AND {county_col} = prev_county AND {serial_col} < prev_serial );
    """
    # Execute inversion count
    try:
        inversions = con.execute(inv_sql).fetchone()[0]
    except Exception as e:
        print("ERROR during inversion count:", str(e))
        con.close()
        return 3

    print(f"Year {cfg['year']}: found {inversions} ordering inversions on ({state_col},{county_col},{serial_col}). Threshold={threshold}.")

    # If sample_limit requested and inversions > 0, sample the first N inversion rows for diagnosis
    if sample_limit and inversions > 0:
        sample_sql = f"""
        SELECT prev_state, prev_county, prev_serial, {state_col}, {county_col}, {serial_col}
        FROM (
          SELECT
            lag({state_col}) OVER (ORDER BY NULL) AS prev_state,
            lag({county_col}) OVER (ORDER BY NULL) AS prev_county,
            lag({serial_col}) OVER (ORDER BY NULL) AS prev_serial,
            {state_col}, {county_col}, {serial_col}
          FROM {view_name}
        ) WHERE
          ( {state_col} < prev_state )
          OR ( {state_col} = prev_state AND {county_col} < prev_county )
          OR ( {state_col} = prev_state AND {county_col} = prev_county AND {serial_col} < prev_serial )
        LIMIT {sample_limit};
        """
        try:
            samples = con.execute(sample_sql).fetchall()
            print("Sample inversions (prev_state, prev_county, prev_serial => state, county, serial):")
            for r in samples:
                print("  ", r)
        except Exception as e:
            print("ERROR sampling inversions:", str(e))

    con.close()

    if inversions > threshold:
        print("Result: NOT OK (file not sufficiently sorted).")
        return 1
    else:
        print("Result: OK (file sufficiently sorted).")
        return 0

if __name__ == "__main__":
    rc = run_verify(CONFIG)
    sys.exit(rc)