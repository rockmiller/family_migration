# config.py
from pathlib import Path
import json

# Base settings you can edit once
_BASE = {
    "csv_root": "/data/census_csvs",        # directory where per-year CSVs live
    "csv_template": "{year}.csv.gz",       # filename template relative to csv_root
    "out_root": "/mnt/census",             # parquet output root
    "index_db": "/data/search_index.duckdb",
    "manifest": "/data/manifests.sqlite",
    # default bucket counts per year (edit once)
    "buckets_map": {
        "1850": 8, "1860": 12, "1870": 16, "1880": 16,
        "1890": 12, "1900": 16, "1910": 20, "1920": 24
    },
    # verification defaults
    "trust_source_sorted": True,
    "verify_sort_threshold": 0,
    "verify_sample_limit": 5,
    # read_csv options for DuckDB read_csv_auto expression
    "read_csv_opts": {
        "header": True,
        "sample_size": -1
    }
}

def get_config(year,
               csv_root=None,
               csv_template=None,
               out_root=None,
               index_db=None,
               manifest=None,
               buckets_map=None,
               trust_source_sorted=None,
               verify_sort_threshold=None,
               verify_sample_limit=None,
               read_csv_opts=None,
               extra=None):
    """
    Return a configuration dict for ingestion/verification for the given year.

    Parameters:
    - year (int or str): census year used to derive CSV filename and hhid.
    - overrides: any of the base keys can be overridden via function args.
    - extra (dict): any extra keys to merge into returned config.

    Example:
      cfg = get_config(1850, csv_root="/mnt/input", trust_source_sorted=False)
    """
    y = str(year)
    cfg = dict(_BASE)  # shallow copy

    # apply explicit overrides
    if csv_root is not None:
        cfg["csv_root"] = csv_root
    if csv_template is not None:
        cfg["csv_template"] = csv_template
    if out_root is not None:
        cfg["out_root"] = out_root
    if index_db is not None:
        cfg["index_db"] = index_db
    if manifest is not None:
        cfg["manifest"] = manifest
    if buckets_map is not None:
        cfg["buckets_map"] = buckets_map
    if trust_source_sorted is not None:
        cfg["trust_source_sorted"] = trust_source_sorted
    if verify_sort_threshold is not None:
        cfg["verify_sort_threshold"] = verify_sort_threshold
    if verify_sample_limit is not None:
        cfg["verify_sample_limit"] = verify_sample_limit
    if read_csv_opts is not None:
        cfg["read_csv_opts"] = read_csv_opts

    # derived fields
    csv_name = cfg["csv_template"].format(year=y)
    cfg["csv"] = str(Path(cfg["csv_root"]) / csv_name)
    cfg["year"] = int(y)

    # resolve bucket count for this year; error if missing
    buckets_map_final = cfg.get("buckets_map", {})
    if y in buckets_map_final:
        cfg["bucket_count"] = int(buckets_map_final[y])
    elif str(year) in buckets_map_final:
        cfg["bucket_count"] = int(buckets_map_final[str(year)])
    else:
        raise KeyError(f"Bucket count for year {year} not found in buckets_map")

    # merge extras
    if extra:
        cfg.update(extra)

    return cfg