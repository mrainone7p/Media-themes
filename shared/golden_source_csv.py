from __future__ import annotations

import csv
from typing import Iterable

GOLDEN_SOURCE_REQUIRED_COLUMNS = ("tmdb_id", "source_url")
GOLDEN_SOURCE_OPTIONAL_COLUMNS = (
    "title",
    "year",
    "start_offset",
    "end_offset",
    "updated_at",
    "notes",
)
GOLDEN_SOURCE_TOLERATED_LEGACY_FIELDS = ("verified",)
GOLDEN_SOURCE_ALL_COLUMNS = GOLDEN_SOURCE_REQUIRED_COLUMNS + GOLDEN_SOURCE_OPTIONAL_COLUMNS


def normalize_golden_source_offset(value: object) -> str:
    return str(value or "0").strip() or "0"


def _normalized_fieldnames(fieldnames: Iterable[object]) -> list[str]:
    return [str(name or "").strip().lower() for name in fieldnames]


def _normalize_row(row: dict[object, object]) -> dict[str, str]:
    clean = {
        str(key or "").strip().lower(): str(value or "").strip()
        for key, value in row.items()
    }
    clean["start_offset"] = normalize_golden_source_offset(clean.get("start_offset", "0"))
    clean["end_offset"] = normalize_golden_source_offset(clean.get("end_offset", "0"))
    for legacy_field in GOLDEN_SOURCE_TOLERATED_LEGACY_FIELDS:
        clean.pop(legacy_field, None)
    return {column: clean.get(column, "") for column in GOLDEN_SOURCE_ALL_COLUMNS}


def parse_golden_source_csv_rows(text: str, *, require_source_url: bool = False) -> list[dict[str, str]]:
    reader = csv.DictReader(text.splitlines())
    if not reader.fieldnames:
        raise ValueError("Golden Source CSV has no header row")
    fieldnames = _normalized_fieldnames(reader.fieldnames)
    missing = [column for column in GOLDEN_SOURCE_REQUIRED_COLUMNS if column not in fieldnames]
    if missing:
        raise ValueError(
            "Golden Source CSV is missing required column(s): " + ", ".join(missing)
        )
    rows: list[dict[str, str]] = []
    for row in reader:
        clean = _normalize_row(row)
        if not clean["tmdb_id"]:
            continue
        if require_source_url and not clean["source_url"]:
            continue
        rows.append(clean)
    return rows


def parse_golden_source_csv_map(text: str, *, require_source_url: bool = True) -> dict[str, dict[str, str]]:
    rows = parse_golden_source_csv_rows(text, require_source_url=require_source_url)
    return {row["tmdb_id"]: row for row in rows}
