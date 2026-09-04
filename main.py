import datetime
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from sys import prefix
from typing import Literal
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo
import hashlib
import polars as pl
from loguru import logger

from src.data_generation.generate import generate_data


# ========== Config ==========
staging_v1_path = "./data/staging/v1/data.parquet"
bronze_file_path = "./data/bronze/data.parquet"
staging_v2_path = "./data/staging/v2/data.parquet"
HASH_COLUMNS = [
    "name",
    "age",
    "gender",
    "address",
    "updated_at",
]

def batch_sha256(s: pl.Series) -> pl.Series:
    """Hashes a Polars Series in bulk using Python's hashlib."""
    return pl.Series(
        [hashlib.sha256(val.encode("utf-8")).hexdigest() for val in s]
    )


# ========== Generate data for check ==========

if not Path(bronze_file_path).exists():
    # ==========
    # Generate data for check
    # ==========
    generate_data()


    # ==========
    # Full load data for bronze in first time (old data) with hash column
    # ==========
    df_staging_v1 = pl.scan_parquet(staging_v1_path)

    df_staging_v1_hashed = df_staging_v1.with_columns(
        pl.all()
        .cast(pl.String)
        .fill_null("")
    ).with_columns(
        pl.concat_str(
            pl.col(HASH_COLUMNS)
            .cast(pl.Utf8), separator="|"
        )
        .map_batches(batch_sha256)
        .alias("hashed_row")
    )

    df_staging_v1_hashed.sink_parquet(bronze_file_path)


# ==========
# Load current data in bronze
# ==========
df_current = pl.scan_parquet(bronze_file_path)


# ==========
# Load new data from staging v2
# ==========
df_staging_v2 = pl.scan_parquet(staging_v2_path)

# check schema diff

# validate, deduplicate, get last value

# Hash row new data
df_staging_v2_hashed = df_staging_v2.with_columns(
        pl.all()
        .cast(pl.String)
        .fill_null("")
    ).with_columns(
        pl.concat_str(
            pl.col(HASH_COLUMNS)
            .cast(pl.Utf8), separator="|"
        )
        .map_batches(batch_sha256)
        .alias("hashed_row")
)


# ========== Find data not change, new, update, delete ==========

# Full outer join to detect change
"""
old.id       new.id       old_hash    new_hash
------------------------------------------------
1            1            aaa         aaa
2            2            bbb         xxx
3            null         ccc         null
null         4            null        ddd
"""

old = (
    df_current
    .select(["id", "hashed_row"])
    .rename({"hashed_row": "old_hash"})
)

new = (
    df_staging_v2_hashed
    .select(["id", "hashed_row"])
    .rename({"hashed_row": "new_hash"})
)

comparison = old.join(
    new.select(["id", "new_hash"]),
    on="id",
    how="full",
    suffix="_new"
)
comparison = comparison.select([
    "id",
    "id_new",
    "old_hash",
    "new_hash"
])
logger.info(comparison.collect())

# Find record new
df_data_new = comparison.filter(
    pl.col("old_hash").is_null()
)
logger.info(f"Data new: {df_data_new.select(pl.len()).collect().item()}")

# Find record deleted
df_data_deleted = comparison.filter(
    pl.col("new_hash").is_null()
)
logger.info(f"Data deleted: {df_data_deleted.select(pl.len()).collect().item()}")

# Find record not change
df_data_not_change = comparison.filter(
    (pl.col("old_hash").is_not_null())
    & (pl.col("new_hash").is_not_null())
    & (pl.col("old_hash") == pl.col("new_hash"))
)
logger.info(
    f"Data not change: {df_data_not_change.select(pl.len()).collect().item()}"
)

# Find record need updated
df_data_updated = comparison.filter(
    (pl.col("old_hash").is_not_null())
    & (pl.col("new_hash").is_not_null())
    & (pl.col("old_hash") != pl.col("new_hash"))
)
logger.info(f"Data updated: {df_data_updated.select(pl.len()).collect().item()}")


# ========== Process data ==========
# write data to parquet file
# df_staging_v2_hashed.sink_parquet(bronze_file_path)


# ========== Write change log ==========
# Log table include: batch_id, id, column name, old_value, new_value, action,
# updated_at
# action: NEW, DELETE, UPDATE
# NEW and DELETE do not need old_value and new_value
BATCH_ID = "B_0001_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
df_current_hashed_change = df_current.join(
    df_data_updated.select("id"),
    how="inner",
    on="id"
)
df_new_hashed_change = df_staging_v2_hashed.join(
    df_data_updated.select("id"),
    how="inner",
    on="id"
)

# 2. Join old and new values side-by-side
joined_changes = df_current_hashed_change.select(["id", *HASH_COLUMNS]).join(
    df_new_hashed_change.select(["id", *HASH_COLUMNS]),
    on="id",
    suffix="_new"
)

# 3. Vectorized comparison across all columns simultaneously
log_exprs = []
for col in HASH_COLUMNS:
    old_col = pl.col(col).cast(pl.Utf8)
    new_col = pl.col(f"{col}_new").cast(pl.Utf8)

    # Identify differences (handling null-aware equality)
    diff_condition = old_col.ne_missing(new_col)

    expr = pl.when(diff_condition).then(
        pl.struct(
            pl.lit(BATCH_ID).alias("batch_id"),
            pl.col("id"),
            pl.lit(col).alias("column"),
            old_col.alias("old_value"),
            new_col.alias("new_value"),
            pl.lit("UPDATE").alias("action"),
        )
    )
    log_exprs.append(expr)

# 4. Unnest changed columns into rows natively without Python loops
change_log = (
    joined_changes
    .select(
        pl.concat_arr(log_exprs)
        .alias("changes")
    )
    .explode("changes")
    .drop_nulls("changes")
    .unnest("changes")
    .collect()
)

logger.info(change_log)


