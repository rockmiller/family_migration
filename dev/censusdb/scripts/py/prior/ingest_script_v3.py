# %%
import duckdb
import os
import typing
from pathlib import Path
import polars as pl
from tabulate import tabulate
from datetime import datetime, timezone
import time

# %%
# Define Folders
BASEPATH = Path("D:/censusdb")
SOURCEPATH = Path("D:/source")
BUCKETS_CSV = Path("D:/censusdb/parameters") / 'nbuckets.csv'

folder_map = {
    "1850": BASEPATH / "cp1850",
    "1860": BASEPATH / "cp1860",
    "1870": BASEPATH / "cp1870",
    "1880": BASEPATH / "cp1880",
    "1890": BASEPATH / "cp1890",
    "1900": BASEPATH / "cp1900",
    "1910": BASEPATH / "cp1910",
    "1920": BASEPATH / "cp1920",
}

# %%
# Load bucket counts from CSV
def load_bucket_map(bucket_csv: Path) -> dict[int, int]:
    df = pl.read_csv(bucket_csv)
    return {row["cenyear"]: row["N_BUCKETS"] for row in df.to_dicts()}

bucket_map = load_bucket_map(BUCKETS_CSV)


# %%
def config_for_year(cenyear: int, out_root: Path) -> dict:
    csv_root = SOURCEPATH
    csv_template = "cs{cenyear}.csv.gz"
    index_db = BASEPATH / "index" / "search_index.duckdb"
    manifest_db = BASEPATH / "manifests" / "manifest.duckdb"

    if cenyear not in bucket_map:
        raise ValueError(f"No bucket count defined for year {cenyear}")

    csv_path = csv_root / csv_template.format(cenyear=cenyear)
    bucket_count = bucket_map[cenyear]

    return {
        "cenyear": cenyear,
        "csv": csv_path,
        "bucket_count": bucket_count,
        "out_root": out_root,
        "index_db": index_db,
        "manifest_db": manifest_db,
        "state_col": "stateicp",
        "county_col": "countyicp",
        "serial_col": "serial",
        "histid_col": "histid",
        "hik_col": "hik",
        "read_csv_opts": {
            "has_header": True,
            "separator": ",",
            "infer_schema_length": 10000
        }
    }

# %%
def config_for_filename(filename: str, folder_map: dict[str, Path]) -> dict:
    stem = Path(filename).stem
    cenyear_str = stem[2:6]
    cenyear = int(cenyear_str)

    if cenyear_str not in folder_map:
        raise ValueError(f"No output folder defined for cenyear {cenyear_str}")

    out_root = folder_map[cenyear_str]
    return config_for_year(cenyear, out_root)

# %%
def prepare_output_dirs(config: dict):
    os.makedirs(config["out_root"], exist_ok=True)
    os.makedirs(config["index_db"].parent, exist_ok=True)
    os.makedirs(config["manifest_db"].parent, exist_ok=True)

def read_input_csv(config: dict) -> pl.DataFrame:
    return pl.read_csv(source=config["csv"], **config["read_csv_opts"])

# %%
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

# %%
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


# %%


# %%
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
        con.execute("""
            CREATE TABLE IF NOT EXISTS manifest (
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
        con.execute("DESCRIBE manifest_rows").fetchall()

        con.execute("DELETE FROM manifest WHERE cenyear = ?;", [config["cenyear"]])
        con.execute("""
            INSERT INTO manifest (
                cenyear, bucket, file, record_count,
                min_hhid, max_hhid, timestamp
            )
            SELECT
                cenyear, bucket, file, record_count,
                min_hhid, max_hhid, timestamp
            FROM manifest_rows;
        """)

# %%
def build_search_index(df: pl.DataFrame, config: dict, cenyear: int):
    index_df = df.filter(pl.col(config["hik_col"]).is_not_null()).select([
        config["hik_col"], "hhid", "cloc"
    ]).rename({config["hik_col"]: "hik", "cloc": "locid"})

    with duckdb.connect(str(config["index_db"])) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS index (
                hik   TEXT PRIMARY KEY,
                hhid  TEXT,
                locid TEXT
            );
        """)
        con.register("index_df", index_df)
        con.execute("DELETE FROM index WHERE hik IN (SELECT hik FROM index_df);")
        con.execute("INSERT INTO index SELECT * FROM index_df;")
        print(f"Cenyear {cenyear}: Appended {index_df.shape[0]} rows to search index.")

# %%
def ingest_file(filename: str, folder_map: dict[str, Path]):
    config = config_for_filename(filename, folder_map)
    prepare_output_dirs(config)

    start = time.perf_counter()

    df = read_input_csv(config)
    df = add_composite_keys(df, config)
    manifest_rows = write_buckets_and_collect_manifest(df, config)
    write_manifest(manifest_rows, config)
    build_search_index(df, config, config["cenyear"])

    end = time.perf_counter()
    print(f"✅ Finished processing {filename} in {round(end - start, 2)} seconds.")

# %%
# Execution
filelist = ["cs1850.csv.gz"]
for filename in filelist:
    ingest_file(filename, folder_map)


