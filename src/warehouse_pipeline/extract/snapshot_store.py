from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


class SnapshotStore:
    """
    Read or write pinned extraction snapshots atomically.
    
    No staging logic here
    """

    def __init__(self, root: Path) -> None:
        """Hand class the root."""
        self.root = root

    def path_for(self, name: str) -> Path:
        """Put the `.json` file in its expected place."""
        filename = name if name.endswith(".json") else f"{name}.json"
        return self.root / filename  
    

    def write_json(self, name: str, payload: Mapping[str, Any]) -> Path:
        """Write a `.json` file automically to an expected path. Returns the path it used as `final_path`."""
        self.root.mkdir(parents=True, exist_ok=True)

        final_path = self.path_for(name)
        # tmp
        temp_path = final_path.with_suffix(final_path.suffix + ".tmp") 

        temp_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temp_path.replace(final_path)
        return final_path
    

    def read_json(self, name: str) -> dict[str, Any]:
        """Read a `.json` file from an expected path. Returns its read `data`."""
        path = self.path_for(name)  # find path
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"Snapshot {path} is not a JSON object")
        return data




