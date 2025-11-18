## Libraries
import duckdb
import polars as pl
from pathlib import Path
import os
import sys
from tqdm import tqdm
import typing
from datetime import datetime, timezone
import time
from tabulate import tabulate

# path variables
BASEPATH = Path("D:/censusdb")
SOURCEPATH = Path("D:/source")
BUCKETS_CSV = BASEPATH / "parameters" / "nbuckets.csv"

##configuration functions

def load_folder_map(basepath: Path) -> dict[str, Path]:
    return {
        "1850": basepath / "cp1850",
        "1860": basepath / "cp1860",
        "1870": basepath / "cp1870",
        "1880": basepath / "cp1880",
        "1890": basepath / "cp1890",
        "1900": basepath / "cp1900",
        "1910": basepath / "cp1910",
        "1920": basepath / "cp1920",
    }

def load_bucket_map(path: Path) -> dict[int, int]:
    df = pl.read_csv(path)
    print("Bucket map schema:", df.columns)

    # Use uppercase column name
    if "N_BUCKETS" in df.columns:
        return dict(zip(df["cenyear"].to_list(), df["N_BUCKETS"].to_list()))
    else:
        raise ValueError("Expected columns 'CENYEAR' and 'NBUCKETS' not found in bucket map CSV.")

bucket_map = load_bucket_map(BUCKETS_CSV)

def build_config(filename: str) -> dict:
    cenyear = int(filename[2:6])
    folder_map = load_folder_map(BASEPATH)
    bucket_map = load_bucket_map(BUCKETS_CSV)

    if str(cenyear) not in folder_map:
        raise ValueError(f"No output folder defined for cenyear {cenyear}")

    out_root = folder_map[str(cenyear)]
    csv_path = SOURCEPATH / filename

    return {
        "cenyear": cenyear,
        "csv": csv_path,
        "source_dir": SOURCEPATH,
        "bucket_dir": BASEPATH / "buckets",
        "manifest_db": BASEPATH / "manifest" / "manifest.db",
        "index_db": BASEPATH / "index" / "search_index.db",
        "hik_col": "hik",
        "folder_map": folder_map,
        "out_root": out_root,
        "bucket_map": bucket_map,
        "bucket_count": bucket_map[cenyear],
        "serial_col": "serial",
        "state_col": "stateicp",
        "county_col": "countyicp",
        "histid_col": "histid",
        "read_csv_opts": {
            "has_header": True,
            "separator": ",",
            "infer_schema_length": 10000
        }
    }
    
##source functions

def prepare_output_dirs(config: dict):
    os.makedirs(config["out_root"], exist_ok=True)
    os.makedirs(config["index_db"].parent, exist_ok=True)
    os.makedirs(config["manifest_db"].parent, exist_ok=True)

def read_input_csv(config: dict) -> pl.DataFrame:
    return pl.read_csv(source=config["csv"], **config["read_csv_opts"])

##create census dataframe with composite keys

def add_composite_keys(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    df = df.with_columns([
        pl.lit(config["cenyear"]).cast(str).str.zfill(4).alias("cenyear_str"),
        pl.col(config["serial_col"]).cast(str).str.zfill(6).alias("serial_str"),
        pl.col(config["state_col"]).cast(str).str.zfill(2).alias("state_str"),
        pl.col(config["county_col"]).cast(str).str.zfill(4).alias("county_str"),
        pl.col(config["histid_col"]).cast(str).str.zfill(36).alias("histid_str")
    ])

    df = df.with_columns([
        (pl.col("cenyear_str") + pl.col("serial_str")).alias("hhid"),
        (pl.col("state_str") + pl.col("county_str")).alias("cloc"),
        (pl.col("cenyear_str") + pl.col("serial_str") + pl.col("histid_str")).alias("cper")
    ])

    df = df.sort(["cenyear", "hhid"])
    df = df.with_row_index(name="rownum")
    df = df.with_columns([
        (pl.col("rownum") % config["bucket_count"]).alias("bucket")
    ])

    return df.drop(["cenyear_str", "serial_str", "state_str", "county_str", "histid_str", "rownum"])

## manifest functions
def write_buckets_and_collect_manifest(df: pl.DataFrame, config: dict) -> list[dict]:
    manifest_rows = []

    for b in range(config["bucket_count"]):
        bucket_df = df.filter(pl.col("bucket") == b).drop("bucket")
        if bucket_df.is_empty():
            continue

        bucket_path = config["out_root"] / f"bucket_{b:02}.parquet"
        bucket_df.write_parquet(bucket_path, compression="zstd")

        manifest_rows.append({
            "cenyear": config["cenyear"],
            "bucket": b,
            "file": str(bucket_path),
            "record_count": bucket_df.shape[0],
            "min_hhid": bucket_df["hhid"].min(),
            "max_hhid": bucket_df["hhid"].max(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    return manifest_rows

def write_manifest(manifest_rows: list[dict], config: dict):
    """
    Writes manifest metadata to DuckDB with enforced schema and timestamp.
    """
    # Ensure all expected columns exist
    expected_columns = [
        "cenyear", "bucket", "file", "record_count",
        "min_hhid", "max_hhid", "timestamp"
    ]

    # Fill missing fields and enforce timestamp
    for row in manifest_rows:
        for col in expected_columns:
            row.setdefault(col, None)
        if not row["timestamp"]:
            row["timestamp"] = datetime.utcnow().isoformat()

    # Create Polars DataFrame with explicit types
    df = pl.DataFrame(manifest_rows).select([
        pl.col("cenyear").cast(pl.Int32),
        pl.col("bucket").cast(pl.Int32),
        pl.col("file").cast(pl.Utf8),
        pl.col("record_count").cast(pl.Int32),
        pl.col("min_hhid").cast(pl.Utf8),
        pl.col("max_hhid").cast(pl.Utf8),
        pl.col("timestamp").cast(pl.Utf8)
    ])

    # Optional: print schema for debugging
    print("Manifest DataFrame schema:", df.schema)

    with duckdb.connect(str(config["manifest_db"])) as con:
        con.execute("DROP TABLE IF EXISTS manifest;")
        con.execute("""
        CREATE TABLE manifest (
            cenyear INTEGER,
            bucket INTEGER,
            file TEXT,
            record_count INTEGER,
            min_hhid TEXT,
            max_hhid TEXT,
            timestamp TEXT,
            PRIMARY KEY (cenyear, bucket)
        );
    """)
        con.register("manifest_rows", df)

        # Optional: inspect registered table schema
        print(con.execute("DESCRIBE manifest").fetchall())
        con.execute("DELETE FROM manifest WHERE cenyear = ?;", [config["cenyear"]])
        con.execute("""
        INSERT INTO manifest (
        cenyear, bucket, file, record_count,
        min_hhid, max_hhid, timestamp
        )
        SELECT
            mr.cenyear, mr.bucket, mr.file, mr.record_count,
            mr.min_hhid, mr.max_hhid, mr.timestamp
        FROM manifest_rows AS mr;
    """)
        
    ## search-index functions

def build_search_index(index_df: pl.DataFrame, config: dict, cenyear: int):
    with duckdb.connect(str(config["index_db"])) as con:
        # Drop existing table if schema is incorrect
        con.execute("DROP TABLE IF EXISTS index;")

        # Create fresh table with correct schema
        con.execute("""
            CREATE TABLE index (
                hik     TEXT PRIMARY KEY,
                hhid    TEXT,
                cloc    TEXT,
                cenyear INTEGER
            );
        """)

        # Create indexes
        con.execute("CREATE INDEX idx_hhid ON index(hhid);")
        con.execute("CREATE INDEX idx_cloc ON index(cloc);")
        con.execute("CREATE INDEX idx_cenyear ON index(cenyear);")

        # Insert new data
        con.register("index_df", index_df)
        con.execute("INSERT INTO index SELECT * FROM index_df;")

        print(f"Cenyear {cenyear}: Appended {index_df.shape[0]} rows to search index.")

#execution functions

def report_record_counts(df_source, df_census, manifest_rows, config):
    bucket_record_total = sum(row["record_count"] for row in manifest_rows)
    manifest_count = len(manifest_rows)

    print("\nRecord Summary:")
    print(f"  Source CSV records:     {df_source.shape[0]:,}")
    print(f"  Enriched census records:{df_census.shape[0]:,}")
    print(f"  Bucketed records:       {bucket_record_total:,}")
    print(f"  Manifest entries:       {manifest_count:,}")

    # Optional: if index_df was built inline
    if "index_df" in locals():
        print(f"  Indexed records:        {index_df.shape[0]:,}")

    return {
        "source_records": df_source.shape[0],
        "census_records": df_census.shape[0],
        "bucketed_records": bucket_record_total,
        "manifest_entries": manifest_count
    }

def execute_one_year(filename: str, config: dict) -> dict:
    prepare_output_dirs(config)

    # Step 2: Load source CSV
    print("\nStep 2: Loading source CSV")
    t0 = time.time()
    with tqdm(total=10, desc="Reading CSV", bar_format="{l_bar}{bar} | {elapsed}") as pbar:
        df_source = read_input_csv(config)
        for _ in range(10):
            time.sleep(0.01)
            pbar.update(1)
    print(f"Completed in {time.time() - t0:.2f}s")

    # Step 3: Create df_census with composite keys
    print("\nStep 3: Creating enriched census DataFrame")
    t1 = time.time()
    df_census = add_composite_keys(df_source, config)
    print(f"Completed in {time.time() - t1:.2f}s")

    # Step 4: Write buckets and collect manifest + index rows
    print("\nStep 4: Writing bucketed Parquet files")
    t2 = time.time()
    manifest_rows = []
    index_rows = []
    bucket_count = config["bucket_count"]

    with tqdm(total=bucket_count, desc="Writing Buckets", bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
        for b in range(bucket_count):
            bucket_df = df_census.filter(pl.col("bucket") == b).drop("bucket")
            if bucket_df.is_empty():
                pbar.update(1)
                continue

            bucket_path = config["out_root"] / f"bucket_{b:02}.parquet"
            bucket_df.write_parquet(bucket_path, compression="zstd")

            manifest_rows.append({
                "cenyear": config["cenyear"],
                "bucket": b,
                "file": str(bucket_path),
                "record_count": bucket_df.shape[0],
                "min_hhid": bucket_df["hhid"].min(),
                "max_hhid": bucket_df["hhid"].max(),
                "timestamp": datetime.now(timezone.utc).isoformat()
            })

            index_rows.extend([
                {
                    "hik": row[0],
                    "hhid": row[1],
                    "cloc": row[2],
                    "cenyear": str(config["cenyear"])
                }
                for row in (
                    bucket_df.filter(pl.col(config["hik_col"]).is_not_null())
                    .select([config["hik_col"], "hhid", "cloc"])
                    .rows()
                )
            ])

            pbar.update(1)
    print(f"Completed in {time.time() - t2:.2f}s")

    # Step 5: Write manifest table
    print("\nStep 5: Writing manifest table")
    t3 = time.time()
    write_manifest(manifest_rows, config)
    print(f"Completed in {time.time() - t3:.2f}s")

    # Step 6: Build search index
    print("\nStep 6: Building search index")
    t4 = time.time()
    with tqdm(total=10, desc="Building Index", bar_format="{l_bar}{bar} | {elapsed}") as pbar:
        index_df = pl.DataFrame(index_rows)
        build_search_index(index_df, config, config["cenyear"])
        for _ in range(10):
            time.sleep(0.01)
            pbar.update(1)
    print(f"Completed in {time.time() - t4:.2f}s")

    # Final: Report record counts
    print("\nFinal Step: Reporting record counts")
    counts = report_record_counts(df_source, df_census, manifest_rows, config)
    counts["total_time"] = round(time.time() - t0, 2)

    return counts



