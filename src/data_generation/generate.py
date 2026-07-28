"""
Fake data generation for loading.
Data format: parquet
Output folder:
    data/staging/v1: origin data, generate one time. If output file exist, ignore regenerate
    data/staging/v2: changed data, generated based on origin data, contain: UPDATE/DELETE/CREATE cell/record
    checkpoint: save generation status, amount, time, update/delete/created amount and detail.
"""

import datetime
import json
from zoneinfo import ZoneInfo

from dataclasses import dataclass, field, asdict
from loguru import logger
from pathlib import Path
from uuid import uuid4
import polars as pl
import numpy as np


# ==============================
# Constant
# ==============================
today: str = datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d")
datetime_today: str = datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).strftime(
    "%Y-%m-%d_%H-%M-%S"
)

origin_data_folder: Path = Path("./data/staging/v1")
new_data_folder: Path = Path("./data/staging/v2")
checkpoint_folder: Path = Path("./checkpoint")

file_name: str = "data.parquet"
checkpoint_file_name: str = "checkpoint.json"
GENDER: list[str] = ["male", "female", "other"]


@dataclass
class DataGenerationResult:
    rows: int
    column: int
    schema: any
    start_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    end_at: datetime.datetime | None = None
    duration: float | None = None
    error: str | None = None
    des_folder: Path | None = None


@dataclass
class Checkpoint:
    generate_origin_data: DataGenerationResult | None = None
    generate_new_data: DataGenerationResult | None = None
    start_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    end_at: datetime.datetime | None = None
    duration: float | None = None
    error: str | None = None
    origin_data_folder: Path | None = None
    new_data_folder: Path | None = None


# ==============================
# Setup
# ==============================
if not origin_data_folder.exists():
    origin_data_folder.mkdir(parents=True, exist_ok=True)
if not new_data_folder.exists():
    new_data_folder.mkdir(parents=True, exist_ok=True)
if not checkpoint_folder.exists():
    checkpoint_folder.mkdir(parents=True, exist_ok=True)

Path.touch(checkpoint_folder / checkpoint_file_name)


# ==============================
# Function
# ==============================


def generate_origin_data() -> tuple[pl.DataFrame, DataGenerationResult]:
    """
    Generate origin data.
    """

    start_at: datetime.datetime = datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))

    df: pl.DataFrame = (
        pl.DataFrame(
            {
                "id": [str(uuid4()) for _ in range(100)],
                "name": [f"user_{i + 1}" for i in range(100)],
                "age": np.random.randint(18, 65, size=100),
                "gender": np.random.choice(GENDER, size=100),
                "address": [f"address_{i}" for i in range(100)],
                "created_at": [
                    datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))
                    - datetime.timedelta(days=1)
                    for i in range(100)
                ],
                "updated_at": [
                    datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))
                    - datetime.timedelta(days=1)
                    for i in range(100)
                ],
            }
        )
        .with_row_index()
        .with_columns((pl.col("index") + 1).alias("index"))
    )

    df.write_parquet(origin_data_folder / file_name)

    end_at: datetime.datetime = datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))
    duration: float = round((end_at - start_at).total_seconds(), 4)

    return df, DataGenerationResult(
        rows=df.shape[0],
        column=df.shape[1],
        schema={col: str(dtype) for col, dtype in df.schema.items()},
        des_folder=str(origin_data_folder),
        start_at=start_at.strftime("%Y-%m-%d %H:%M:%S"),
        end_at=end_at.strftime("%Y-%m-%d %H:%M:%S"),
        duration=duration,
    )


def generate_new_data(df_origin: pl.DataFrame) -> DataGenerationResult:
    """
    Generate new data.
    """

    start_at: datetime.datetime = datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))

    if df_origin.shape[0] == 0:
        return DataGenerationResult(
            rows=0,
            column=0,
            schema={col: str(dtype) for col, dtype in df_origin.schema.items()},
            des_folder=str(new_data_folder),
            start_at=datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            duration=0,
            error="No data to generate",
        )

    df = df_origin.clone()

    # update data
    df_update_all = df.filter(pl.col("index") <= 10).with_columns(
        [
            (pl.col("name") + "_change_all").alias("name"),
            (pl.col("age") + 1).alias("age"),
            (pl.col("gender") + "_change_all").alias("gender"),
            (pl.col("address") + "_change_all").alias("address"),
            (pl.col("created_at") - datetime.timedelta(days=1)).alias("created_at"),
            (pl.col("updated_at") + datetime.timedelta(days=1)).alias("updated_at"),
        ]
    )

    df_update_name = df.filter(
        (pl.col("index") > 10) & (pl.col("index") <= 20)
    ).with_columns((pl.col("name") + "_change_name").alias("name"))

    df_update_age = df.filter(
        (pl.col("index") > 20) & (pl.col("index") <= 30)
    ).with_columns((pl.col("age") + 2).alias("age"))

    df_update_gender = df.filter(
        (pl.col("index") > 30) & (pl.col("index") <= 40)
    ).with_columns((pl.col("gender") + "_change_gender").alias("gender"))

    df_update_address = df.filter(
        (pl.col("index") > 40) & (pl.col("index") <= 50)
    ).with_columns((pl.col("address") + "_change_address").alias("address"))

    df_update_created_at = df.filter(
        (pl.col("index") > 50) & (pl.col("index") <= 60)
    ).with_columns(
        (pl.col("created_at") - datetime.timedelta(days=2)).alias("created_at")
    )

    df_update_updated_at = df.filter(
        (pl.col("index") > 60) & (pl.col("index") <= 70)
    ).with_columns(
        (pl.col("updated_at") + datetime.timedelta(days=2)).alias("updated_at")
    )

    # Create new data
    df_new = (
        pl.DataFrame(
            {
                "id": [str(uuid4()) for _ in range(10)],
                "name": [f"user_{i + 100}" for i in range(10)],
                "age": np.random.randint(18, 65, size=10),
                "gender": np.random.choice(GENDER, size=10),
                "address": [f"address_{i}" for i in range(10)],
                "created_at": [
                    datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))
                    for i in range(10)
                ],
                "updated_at": [
                    datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))
                    for i in range(10)
                ],
            }
        )
        .with_row_index()
        .with_columns((pl.col("index") + 100).alias("index"))
    )

    # Delete data
    df = df[71:91]

    df = pl.concat(
        [
            df_update_all,
            df_update_name,
            df_update_age,
            df_update_gender,
            df_update_address,
            df_update_created_at,
            df_update_updated_at,
            df,
            df_new,
        ]
    )

    df.write_parquet(new_data_folder / file_name)

    end_at: datetime.datetime = datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))
    duration: float = round((end_at - start_at).total_seconds(), 4)

    return DataGenerationResult(
        rows=df.shape[0],
        column=df.shape[1],
        schema={col: str(dtype) for col, dtype in df.schema.items()},
        des_folder=str(new_data_folder),
        start_at=start_at.strftime("%Y-%m-%d %H:%M:%S"),
        end_at=end_at.strftime("%Y-%m-%d %H:%M:%S"),
        duration=duration,
    )


@logger.catch
def generate_data():
    """
    Generate data.
    """

    with open(checkpoint_folder / checkpoint_file_name, "r") as f:
        try:
            checkpoint = json.load(f)
        except json.decoder.JSONDecodeError:
            checkpoint = {}

    # Generate origin data
    df, result = generate_origin_data()
    if result.error:
        logger.error(f"Generate origin data failed: {result.error}")
        raise RuntimeError(result.error)
    logger.info(f"Generate origin data: {result}")
    checkpoint["generate_origin_data"] = asdict(result)

    # Generate new data
    result: DataGenerationResult = generate_new_data(df)
    if result.error:
        logger.error(f"Generate new data failed: {result.error}")
        raise RuntimeError(result.error)
    logger.info(f"Generate new data: {result}")
    checkpoint["generate_new_data"] = asdict(result)

    # Save checkpoint
    with open(checkpoint_folder / checkpoint_file_name, "w") as f:
        json.dump(checkpoint, f, indent=4)


# ==============================
# Run
# ==============================
if __name__ == "__main__":
    generate_data()
