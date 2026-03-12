from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID


def _jsonable(value: Any) -> Any:
    """Makes values jsonable, best effort only."""
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


class RunLogger:
    """
    Writes `JSON` lines to disk.
    It'll optionally mirror a short line to stdout with the `echo` `bool` param.
    """

    def __init__(self, *, run_id: UUID, log_path: Path, echo: bool = True) -> None:
        """Start logger."""
        self.run_id = run_id
        self.log_path = log_path.resolve()
        self.echo = echo
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def event(self, name: str, **fields: Any) -> None:
        """r"""
        record = {
            "ts": datetime.now(UTC).isoformat(),
            "run_id": str(self.run_id),
            "event": name,
            # everything else
            **{k: _jsonable(v) for k, v in fields.items()},
        }

        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")  # keys are sorted

        # the prints to stdout
        if self.echo:
            short = f"[{record['ts']}] run_id={record['run_id']} event={name}"
            if fields:
                short += " " + " ".join(f"{k}={_jsonable(v)!r}" for k, v in fields.items())
            print(short)

    def phase_started(self, phase: str) -> None:
        """(new) Phase starts."""
        self.event("phase_started", phase=phase)

    def phase_finished(self, phase: str, *, duration_s: float, **fields: Any) -> None:
        """Store that phase finishes."""
        self.event("phase_finished", phase=phase, duration_s=round(duration_s, 6), **fields)

    def error(self, phase: str, *, error_message: str) -> None:
        """Store that phase fails."""
        self.event("phase_failed", phase=phase, error_message=error_message)
