import datetime
from dataclasses import dataclass, field, asdict
import json
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from loguru import logger
import polars as pl
from uuid import uuid4, UUID

from src.data_generation.generate import generate_data
from utils.read_checkpoint import read_checkpoint
from utils.write_checkpoint import write_checkpoint


@dataclass
class LoadDataCheckpoint:
    is_first_load: bool
    load_folder_path: Path
    load_files: list[str]
    des_folder_path: Path
    des_files: list[str]
    start_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    end_at: datetime.datetime | None = None
    duration: float | None = None
    error: str | None = None


@dataclass
class DataTracking:
    id: UUID
    column: str
    old_value: any
    new_value: any
    change_type: Literal["create", "update", "delete"]
    origin_source: Path
    new_source: Path
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime | None = None


def main():
    generate_data()

    # Checkpoint
    checkpoint = read_checkpoint("checkpoint/checkpoint.json")

    load_data_checkpoint = LoadDataCheckpoint(
        is_first_load=False,
        load_folder_path="./data/staging/v1",
        load_files=["data.parquet"],
        des_folder_path="./data/raw",
        des_files=["data.parquet"],
        start_at=datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")),
        end_at= None
    )
    
    if checkpoint:
        # Path.unlink(brozend_file_path) # Alway run from start
        load_data_checkpoint.is_first_load = False
        # pass
    else:
        load_data_checkpoint.is_first_load = True
    df_origin = pl.scan_parquet("./data/staging/v1/data.parquet")
    df_origin.sink_parquet("./data/brozen/data.parquet")

    start_at = datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))

    # # Load origin data to raw and capture history
    # brozend_file_path: Path = Path("./data/brozen/data.parquet")
    
    # # Get last load data time
    # last_load_data_time = datetime.datetime.strptime(checkpoint["load_data"]["end_at"], format="%Y-%m-%d %H:%M:%S")

    # # Load new data from staging
    # new_source_path = "./data/staging/v2/data.parquet"
    # df_new = pl.scan_parquet(new_source_path)

    # df_changed = df_new.filter(
    #     pl.col("updated_at").to_datetime(format="%Y-%m-%d %H:%M:%S", strict=True) > last_load_data_time
    # )

    # # Load current data in brozen
    # current_data_path = "./data/raw/data.parquet"
    # df_current = pl.scan_parquet(current_data_path)

    # # Find record need updated
    # df_data_updated = df_current.join(df_changed, how="inner", on="id", suffix="_new")
    # logger.info(f"Data updated: {df_data_updated.select(pl.len()).collect().item()}")

    # # Find record new
    # df_data_new = df_changed.filter(~pl.col("id").is_in(df_current["id"]))
    # logger.info(f"Data new: {df_data_new.select(pl.len()).collect().item()}")

    # # Find record deleted
    # df_data_deleted = df_current.filter(~pl.col("id").is_in(df_changed["id"]))
    # logger.info(f"Data deleted: {df_data_deleted.select(pl.len()).collect().item()}")

    # # Find record not change
    # df_data_not_change = df_current.filter(pl.col("id").is_in(df_changed["id"]))
    # logger.info(f"Data not change: {df_data_not_change.select(pl.len()).collect().item()}")

    # # 
    # # Get the IDs of the records that have changed
    # changed_ids_df = df_changed.select("id")

    # # Remove the old versions of these records from the raw data
    # raw_df_without_updates = df_current.join(
    #     changed_ids_df, on="id", how="anti"
    # )

    # # Add the new/updated versions to the dataset
    # updated_raw_df = pl.concat([raw_df_without_updates, df_changed])


    # Tracking data change

    end_at = datetime.datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))
    load_data_checkpoint.duration = round((end_at - start_at).total_seconds(), 4)
    load_data_checkpoint.end_at = end_at.strftime("%Y-%m-%d %H:%M:%S")
    load_data_checkpoint.start_at = start_at.strftime("%Y-%m-%d %H:%M:%S")
    checkpoint["load_data"] = asdict(load_data_checkpoint)
        
    # Save checkpoint
    write_checkpoint(checkpoint, "checkpoint/checkpoint.json")

if __name__ == "__main__":
    main()
