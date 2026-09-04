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
import sys

from src.data_generation.generate import generate_data

pl.Config(set_tbl_rows=-1, set_tbl_cols=-1)

# ========== Config ==========
staging_v1_path = "./data/staging/v1/data.parquet"
bronze_file_path = "./data/bronze/data.parquet"
staging_v2_path = "./data/staging/v2/data.parquet"
BUSINESS_KEY = "id"
FINGERPRINT_COLUMNS = [
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

def detect_schema_diff(df_current: pl.LazyFrame, df_incoming: pl.LazyFrame):
    current_schema = df_current.collect_schema().to_python()
    incoming_schema = df_incoming.collect_schema().to_python()

    current_cols = set(current_schema.keys())
    incoming_cols = set(incoming_schema.keys())

    new_cols = incoming_cols - current_cols
    del_cols = current_cols - incoming_cols
    common_cols = incoming_cols & current_cols

    diff_data_types = []
    for col in common_cols:
        if current_schema[col] != incoming_schema[col]:
            diff_data_types.append({
                "old": (col, current_schema[col]),
                "new": (col, incoming_schema[col])
            })

    return (
        new_cols,
        del_cols,
        diff_data_types
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
        pl.concat_str(
            pl.col(FINGERPRINT_COLUMNS)
            .cast(pl.Utf8)
            .replace("", "<EMPTY>")
            .fill_null("<NULL>"),
            separator="|"
        )
        .map_batches(
            batch_sha256,
            return_dtype=pl.String,
            is_elementwise=True
        )
        .alias("hashed_row")
    )

    df_staging_v1_hashed.sink_parquet(bronze_file_path)


# ==========
# Load current data in bronze
# ==========
df_current = pl.scan_parquet(bronze_file_path)

# validate current data
logger.info("Validate current data")
null_id_count = df_current.select("id").null_count().collect().item()
if null_id_count > 0:
    logger.error(f"Null id count: {null_id_count}")
    sys.exit(1)
else:
    logger.info("No null id")

dup_id_count = df_current.select(
    pl.col("id")
    .is_duplicated()
    .sum()
).collect().item()
if dup_id_count > 0:
    logger.error(f"Dup id count: {dup_id_count}")
    sys.exit(1)
else:
    logger.info("No dup id")

# ==========
# Load new data from staging v2
# ==========
df_staging_v2 = pl.scan_parquet(staging_v2_path)

# Hash row new data
df_staging_v2_hashed = df_staging_v2.with_columns(
        pl.concat_str(
            pl.col(FINGERPRINT_COLUMNS)
            .cast(pl.Utf8)
            .replace("", "<EMPTY>")
            .fill_null("<NULL>"),
            separator="|"
        )
        .map_batches(
            batch_sha256,
            return_dtype=pl.String,
            is_elementwise=True
        )
        .alias("hashed_row")
)

# check schema diff
df_current_schema = df_current.collect_schema()

(
    new_cols,
    del_cols,
    diff_data_types
) = detect_schema_diff(df_current, df_staging_v2_hashed)

if new_cols:
    logger.warning(f"New col added: {new_cols}")

if del_cols:
    logger.error(f"New col added: {del_cols}")
    sys.exit(1)

if diff_data_types:
    logger.error(f"Diff data type: {diff_data_types}")
    sys.exit(1)

# validate new data
logger.info("Validate new data")
null_id_count = df_staging_v2.select("id").null_count().collect().item()
if null_id_count > 0:
    logger.error(f"Null id count: {null_id_count}")
    sys.exit(1)
else:
    logger.info("No null id")

dup_id_count = df_staging_v2.select(
    pl.col("id")
    .is_duplicated()
    .sum()
).collect().item()
if dup_id_count > 0:
    logger.error(f"Dup id count: {dup_id_count}")
    sys.exit(1)
else:
    logger.info("No dup id")


# ========== Find data not change, new, update, delete ==========
"""
INSERT: key only in NEW
DELETE: key only in OLD
UPDATE: key in both, fingerprints differ
NO_CHANGE: key in both, fingerprints equal
"""
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
    coalesce=True,
)

comparison = comparison.with_columns(
    pl.when(
        pl.col("old_hash").is_null()
    ).then(
        pl.lit("INSERT").alias("action")
    ).when(
        pl.col("new_hash").is_null()
    ).then(
        pl.lit("DELETE").alias("action")
    ).when(
        pl.col("old_hash") == pl.col("new_hash")
    ).then(
        pl.lit("NO_CHANGE").alias("action")
    ).otherwise(
        pl.lit("UPDATE").alias("action")
    )
)


logger.info(comparison.collect())

comparison_report = comparison.group_by("action").agg(pl.col("id").unique().len()).sort("action", descending=False)
logger.info(comparison_report.collect())

# Find record new
df_data_new = comparison.filter(
    pl.col("action") == "INSERT"
)

# Find record deleted
df_data_deleted = comparison.filter(
    pl.col("action") == "DELETE"
)

# Find record not change
df_data_not_change = comparison.filter(
    pl.col("action") == "NO_CHANGE"
)

# Find record need updated
df_data_updated = comparison.filter(
    pl.col("action") == "UPDATE"
)


# ========== Process data ==========
# write data to parquet file
# df_staging_v2_hashed.sink_parquet(bronze_file_path)


# ========== Write change log ==========
# Log table include: batch_id, id, column name, old_value, new_value, action,
# updated_at
# action: NEW, DELETE, UPDATE
# NEW and DELETE do not need old_value and new_value
BATCH_ID = "B_0001_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")

DATA_COLUMNS = [col for col in df_staging_v2_hashed.collect_schema().keys()
                if col not in ("id", "hashed_row")]

# Get all rows need update bot old and new data
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
joined_changes = df_current_hashed_change.select(["id", *FINGERPRINT_COLUMNS]).join(
    df_new_hashed_change.select(["id", *FINGERPRINT_COLUMNS]),
    on="id",
    suffix="_new"
)

# 3. Vectorized comparison across all columns simultaneously
log_exprs = []
for col in FINGERPRINT_COLUMNS:
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
)

logger.info(change_log.limit(10).collect())


joined_insert = df_staging_v2_hashed.join(
    df_data_new.select("id"),
    how="inner",
    on="id"
)

# Change log for new insert
insert_log = joined_insert.unpivot(
    index="id",
    on=DATA_COLUMNS,
    variable_name="column",
    value_name="new_value"
).with_columns([
    pl.lit(BATCH_ID).alias("batch_id"),
    pl.lit(None).alias("old_value"),
    pl.lit("INSERT").alias("action")
]).select([
    "batch_id",
    "id",
    "column",
    "old_value",
    "new_value",
    "action"
])

logger.info(insert_log.limit(10).collect())

# change log for delete
joined_delete = df_current.join(
    df_data_deleted.select("id"),
    how="inner",
    on="id"
)

# Change log for new insert
delete_log = joined_delete.unpivot(
    index="id",
    on=DATA_COLUMNS,
    variable_name="column",
    value_name="old_value"
).with_columns([
    pl.lit(BATCH_ID).alias("batch_id"),
    pl.lit(None).alias("new_value"),
    pl.lit("DELETE").alias("action")
]).select([
    "batch_id",
    "id",
    "column",
    "old_value",
    "new_value",
    "action"
])

logger.info(delete_log.limit(10).collect())

final_log = pl.concat([change_log, insert_log, delete_log])

logger.info(final_log.limit(10).collect())

logger.info(
    final_log
    .group_by("action")
    .agg(
        pl.col("id").unique()
         .len()
    )
    .sort("action", descending=False)
    .collect()
)

final_log.collect().write_database(
    table_name="changelog",
    connection="sqlite:///changelog/changelog.db",  # Path to SQLite file
    if_table_exists="replace",  # 'fail', 'replace', or 'append'
)

