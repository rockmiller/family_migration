from pathlib import Path
def get_config(year: int) -> dict:
    """
    Returns a full configuration dictionary for ingestion for the given census year.
    All paths, options, and bucket counts are hard-coded except the year.
    """
    year_str = str(year)

    # Fixed paths and templates
    csv_root = "/mnt/input"                     # where gzipped CSVs live
    csv_template = "{year}.csv.gz"              # filename pattern
    out_root = "/mnt/census"                    # where parquet files go
    index_db = "/data/search_index.duckdb"      # persistent DuckDB index
    manifest = "/data/manifests.sqlite"         # SQLite manifest

    # Bucket counts per year — tune these based on file size
    buckets_map = {
        "1850": 8, "1860": 12, "1870": 16, "1880": 16,
        "1890": 12, "1900": 16, "1910": 20, "1920": 24
    }

    if year_str not in buckets_map:
        raise ValueError(f"No bucket count defined for year {year}")

    # Derived values
    csv_path = str(Path(csv_root) / csv_template.format(year=year_str))
    bucket_count = buckets_map[year_str]

    return {
        # Core ingestion parameters
        "year": year,
        "csv": csv_path,
        "bucket_count": bucket_count,
        "out_root": out_root,
        "index_db": index_db,
        "manifest": manifest,

        # Column names used to compute keys
        "state_col": "stateicp",
        "county_col": "countyicp",
        "serial_col": "serial",
        "histid_col": "histid",
        "hik_col": "hik",

        # DuckDB CSV read options
        "read_csv_opts": {
            "header": True,
            "sample_size": -1
        },

        # Ingestion behavior flags
        "trust_source_sorted": True,        # skip ORDER BY if verified
        "verify_sort_threshold": 0,         # must be perfectly sorted
        "verify_sample_limit": 5            # show up to 5 inversion samples
    }