# %%
from __future__ import annotations

import datetime
import fnmatch
import glob
import json
import re
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree as ET

import duckdb
import openpyxl
import polars as pl
from loguru import logger
from python_calamine import CalamineWorkbook

pl.Config(set_tbl_rows=-1, set_tbl_cols=-1)

# %%
# Template as input sheet, that for real file collect by sheet
template_path: Path = Path(
    r"C:\Users\rian.pham\Downloads\OneDrive_2026-08-18\Data template based on 13-Aug version"
)
today = datetime.datetime.now().strftime("%Y-%m-%d")


# %%
# Real folder to collect file ans sheets
# Data folder
folder_path: Path = Path(r"C:\Users\rian.pham\Downloads\cus data august-18")
# Path(r"C:\Users\rian.pham\Downloads\cus data 19 august")
# Path(r"C:\Users\rian.pham\Downloads\cus data august-18")
# Path(r"C:\Users\rian.pham\Downloads\cus_data_c3r2_24_july")
# # Report folder
# folder_path: Path = Path(r"data\staging\report\im\import 20260824")
# Path(r"C:\Users\rian.pham\Downloads\OneDrive_1_8-21-2026")

# %%
# Data folder
output_folder = Path(rf"./data/staging/{folder_path.name}/{today}")
# Report folder
# report_type = ["pre", "im", "com"]
# output_folder = Path(rf"./data/bronze/{report_type[1]}/{folder_path.name}/{today}")

output_folder.mkdir(parents=True, exist_ok=True)

# %%
info_collection_folder = Path("./info_collection")
info_collection_folder.mkdir(parents=True, exist_ok=True)
# Data info
data_info_folder = info_collection_folder / "data"
data_info_folder.mkdir(parents=True, exist_ok=True)
collected_info_file = data_info_folder / f"{folder_path.name}_{today}.json"
collected_info_file.touch(exist_ok=True)
# Report info
# report_info_folder = info_collection_folder / "report" / report_type
# report_info_folder.mkdir(parents=True, exist_ok=True)
# collected_info_file = report_info_folder / f"{folder_path.name}_{today}.json"
# collected_info_file.touch(exist_ok=True)


# %%
@dataclass
class FileCollector:
    folder: Path
    config: dict

    def _validate(self) -> None:
        if not self.folder.exists():
            raise ValueError(f"Folder does not exist: {self.folder}")

        if not self.folder.is_dir():
            raise ValueError(f"Path is not a directory: {self.folder}")

    def collect(self) -> list[Path]:
        """Collect files matching include patterns and excluding exclude patterns."""

        self._validate()

        include_patterns = self.config["include_patterns"]
        exclude_patterns = self.config["exclude_patterns"] or []
        matched_files: set[Path] = set()

        for include_pattern in include_patterns:
            for file in self.folder.rglob(include_pattern):
                if not file.is_file():
                    continue

                if file.name.startswith("~$"):
                    continue

                if any(
                    fnmatch.fnmatch(file.name, pattern) for pattern in exclude_patterns
                ):
                    continue

                matched_files.add(file)

        return sorted(matched_files)


# %%
@dataclass
class SheetCollector:
    file_list: list[Path]
    config: dict
    MAIN_NS = {
        "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }

    REL_NS = {
        "r": "http://schemas.openxmlformats.org/package/2006/relationships",
    }
    ROW_RE = re.compile(rb"<(?:[A-Za-z_][\w.-]*:)?row(?:\s|>)")

    def _get_all_sheets(self, files: list, exclude_patterns: list) -> list[str]:
        """Collect unique sheet names from Excel files."""

        exclude_set = set(exclude_patterns or [])
        result: set[str] = set()

        for file in files:
            if file.name.startswith("~$"):
                continue

            workbook = CalamineWorkbook.from_path(str(file))

            result.update(
                sheet for sheet in workbook.sheet_names if sheet not in exclude_set
            )

        return sorted(result)

    def _read_xlsx_sheet_map(self, path: Path) -> dict[str, str]:
        """
        Return:

            {
                "Sheet1": "xl/worksheets/sheet1.xml",
                "Sheet2": "xl/worksheets/sheet2.xml",
            }

        Only workbook metadata is parsed.
        """

        with zipfile.ZipFile(path) as zf:
            workbook_xml = ET.fromstring(zf.read("xl/workbook.xml"))

            relationships_xml = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

            relationships = {
                rel.attrib["Id"]: rel.attrib["Target"] for rel in relationships_xml
            }

            result: dict[str, str] = {}

            for sheet in workbook_xml.find("main:sheets", self.MAIN_NS):
                name = sheet.attrib["name"]

                rel_id = sheet.attrib[
                    "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
                ]

                target = relationships[rel_id]

                if not target.startswith("/"):
                    target = "xl/" + target.removeprefix("xl/")

                else:
                    target = target.removeprefix("/")

                result[name] = target

            return result

    def _is_more_than_one_row(
        self,
        zf: zipfile.ZipFile,
        sheet_xml_path: str,
    ) -> bool:
        """
        Return True if worksheet contains at least 2 <row> elements.

        Reads incrementally and stops immediately after finding row #2.
        """

        row_count = 0

        with zf.open(sheet_xml_path) as stream:
            carry = b""

            while True:
                chunk = stream.read(64 * 1024)

                if not chunk:
                    break

                data = carry + chunk

                matches = self.ROW_RE.findall(data)

                row_count += len(matches)

                if row_count >= 2:
                    return True

                # Keep enough bytes in case "<row" is split
                # across chunk boundaries.
                carry = data[-16:]

        return False

    def _collect_xlsx_sheet(
        self,
        files: list[Path],
        include_patterns: dict[str, str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> dict[str, list[dict[str, object]]]:

        exclude_set = set(exclude_patterns or [])

        compiled_patterns = {
            name: re.compile(pattern)
            for name, pattern in (include_patterns or {}).items()
        }

        grouped_result: defaultdict[
            str,
            list[dict[str, object]],
        ] = defaultdict(list)

        for file in files:
            if file.suffix.lower() not in {".xlsx", ".xlsm"}:
                continue

            try:
                sheet_map = self._read_xlsx_sheet_map(file)

                with zipfile.ZipFile(file) as zf:
                    for sheet_name, sheet_xml_path in sheet_map.items():
                        if sheet_name in exclude_set:
                            continue

                        if not self._is_more_than_one_row(zf, sheet_xml_path):
                            continue

                        if not compiled_patterns:
                            grouped_result["AllSheets"].append(
                                {
                                    "file": str(file),
                                    "sheets": [sheet_name],
                                }
                            )
                            continue

                        for group_name, pattern in compiled_patterns.items():
                            if pattern.fullmatch(sheet_name):
                                grouped_result[group_name].append(
                                    {
                                        "file": str(file),
                                        "sheets": [sheet_name],
                                    }
                                )

                                break
            except Exception as e:
                logger.error(e)
        return dict(sorted(grouped_result.items()))

    def _to_json(self, result) -> None:
        write_res = {}
        for key, value in result.items():
            write_res[key] = []
            for item in value:
                write_res[key].append(
                    {
                        "file": str(item["file"]).replace("\\", "/"),
                        "sheets": item["sheets"],
                    }
                )

        with open(self.config["collected_info_file"], "w") as f:
            f.write(json.dumps(write_res, indent=4))

    def collect(self) -> dict:

        template_files = self.config["template_files"]
        exclude_patterns = self.config["exclude_patterns"]

        all_sheets = self._get_all_sheets(template_files, exclude_patterns)
        include_sheets = {sheet: rf"{re.escape(sheet)}[\d_]*" for sheet in all_sheets}

        result = self._collect_xlsx_sheet(
            self.file_list, include_sheets, exclude_patterns
        )

        self._to_json(result)

        return result


@dataclass
class ExcelToParquetConverter:
    collected_groups: dict[str, list[dict]]
    output_dir: Path

    def run(
        self,
    ) -> list[dict]:
        """
        Loops through all grouped pattern keys, combines matching Excel files/sheets
        via UNION ALL in DuckDB, and streams them directly into individual Parquet files.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        execution_summary = []

        con = duckdb.connect()
        try:
            con.execute("INSTALL excel; LOAD excel;")
        except Exception:
            pass  # Extension already installed

        for group_name, file_entries in self.collected_groups.items():
            select_queries = []

            start = time.perf_counter()

            # 1. Build SELECT queries for every file & sheet in this group
            for entry in file_entries:
                file_path: Path = entry["file"]
                file_posix = Path(file_path).as_posix()

                for sheet in entry["sheets"]:
                    query = f"""
                            SELECT
                                *,
                                '{file_posix}' AS meta_source_file,
                                '{sheet}' AS meta_source_sheet,
                                '{group_name}' AS meta_pattern_group
                            FROM read_xlsx('{file_posix}', sheet='{sheet}', all_varchar=True)
                        """  # scan all as str: all_varchar=True, detect data type: sample_size=20000
                    select_queries.append(query)

            if not select_queries:
                continue

            # 2. Combine subqueries into a single UNION ALL query
            unified_query = "\nUNION ALL\n".join(
                select_queries
            )  # "\nUNION ALL BY NAME\n".join(select_queries) if using sample_size=20000, if union mix type: as str and integer -> return VARCHAR

            output_file = self.output_dir / f"{group_name}.parquet"
            output_posix = output_file.as_posix()

            # 3. Stream directly to Parquet using DuckDB COPY
            copy_sql = f"""
                COPY (
                    {unified_query}
                ) TO '{output_posix}' (
                    FORMAT PARQUET,
                    COMPRESSION 'ZSTD'
                );
            """

            try:
                con.execute(copy_sql)

                # Gather execution metrics for telemetry
                out_bytes = output_file.stat().st_size
                # Replace the count query with:
                row_count = con.execute(
                    f"SELECT COUNT(*) FROM '{output_posix}'"
                ).fetchone()[0]
                col_count = len(
                    con.execute(f"DESCRIBE SELECT * FROM '{output_posix}'").fetchall()
                )

                execution_summary.append(
                    {
                        "group_name": group_name,
                        "output_file": str(output_file),
                        "file_size_mb": round(out_bytes / (1024 * 1024), 2),
                        "total_rows": row_count,
                        "total_cols": col_count,
                        "status": "SUCCESS",
                        "total_time": round(time.perf_counter() - start, 4),
                    }
                )
                logger.info(
                    f"✓ Exported {group_name} -> {output_file.name} ({row_count:,} rows, {col_count:,} columns)"
                )

            except Exception as e:
                logger.error(f"✗ Failed to export group '{group_name}': {e}")
                execution_summary.append(
                    {
                        "group_name": group_name,
                        "output_file": str(output_file),
                        "status": f"FAILED: {str(e)}",
                    }
                )

        return execution_summary


# %%
template_files = FileCollector(
    template_path,
    {
        "include_patterns": ["*.xlsx", "*.xls"],
        "exclude_patterns": ["^~$", "DataScopFilter", "Split Report"],
    },
).collect()
template_files

# %%
data_files = FileCollector(
    folder_path,
    {
        "include_patterns": ["*.xlsx", "*.xls"],
        "exclude_patterns": ["^~$", "DataScopFilter", "Split Report"],
    },
).collect()
data_files

# %%
result = SheetCollector(
    data_files,
    {
        "template_files": template_files,
        "include_patterns": ["*"],
        "exclude_patterns": ["Config Data", "ErrorCodeInstruction", "Summary"],
        "collected_info_file": collected_info_file,
    },
).collect()
result


# %%
start = time.perf_counter()
summary = ExcelToParquetConverter(
    collected_groups=result,
    output_dir=output_folder,
).run()
logger.info(time.perf_counter() - start)
logger.info(summary)

# %%
transter_file_result_path = Path("./transter_file_result_path")
transter_file_result_path.mkdir(parents=True, exist_ok=True)
result_file = transter_file_result_path / f"{folder_path.name}_{today}.json"
result_file.touch(exist_ok=True)
pl.DataFrame(summary).write_json(result_file)

# %%
df_summary = pl.DataFrame(summary)
df_summary.group_by("status").agg(
    [
        pl.col("total_rows").sum(),
        pl.col("total_time").sum(),
        (
            pl.col("total_time").sum().cast(pl.Int64)
            / pl.col("group_name").len().cast(pl.Int64)
        ).alias("avg_time"),
        pl.col("group_name").len().alias("total_modules"),
    ]
)
