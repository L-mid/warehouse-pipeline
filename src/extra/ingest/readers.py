from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterator, Mapping



def stream_csv_dict_rows(path: Path) -> Iterator[tuple[int, Mapping[str, Any]]]:
    """
    Yields `(source_row, dict)` for CSV data rows.

    `source_row` is 1-based for the first real data row encountered, header is not counted.
    `enumerate(..., start=1)` will always yield an `int` for `source_row`.
    """
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            # csv.DictReader returns dict[str, str | None]
            yield i, row    # pairs


def stream_jsonl_dict_rows(path: Path) -> Iterator[tuple[int, Mapping[str, Any]]]:
    """
    Yields `(source_row, dict)` for JSONL lines.

    `source_row` is 1-based by physical line number (blank lines skipped but still counted
    by enumerate, so stable pointer into the file's data rows).
    """
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            obj = json.loads(s)
            if not isinstance(obj, dict):
                raise ValueError(f"JSONL line {i} is not an object")
            yield i, obj


