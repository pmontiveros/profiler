from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


SUPPORTED_TARGET_TYPES = {"table", "query"}


@dataclass
class ProfileTarget:
    type: str
    schema: Optional[str] = None
    table: Optional[str] = None
    sql: Optional[str] = None
    name: Optional[str] = None
    where: Optional[str] = None
    sample_rows: Optional[int] = None

    @property
    def target_name(self) -> str:
        if self.name:
            return self.name
        if self.type == "table" and self.schema and self.table:
            return f"{self.schema}.{self.table}"
        return "query"


class TargetLoader:
    @staticmethod
    def from_json_file(path: str | Path) -> List[ProfileTarget]:
        json_path = Path(path)
        if not json_path.exists():
            raise FileNotFoundError(f"Targets file not found: {json_path}")

        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in targets file {json_path}: {exc}") from exc

        targets_data = data.get("targets")
        if not isinstance(targets_data, list):
            raise ValueError("Targets JSON must contain a list under the 'targets' key.")

        targets: List[ProfileTarget] = []
        for entry in targets_data:
            if not isinstance(entry, dict):
                raise ValueError("Each target entry must be an object.")

            target_type = entry.get("type")
            if target_type not in SUPPORTED_TARGET_TYPES:
                raise ValueError(f"Unsupported target type '{target_type}'. Supported: {SUPPORTED_TARGET_TYPES}")

            if target_type == "table":
                schema = entry.get("schema")
                table = entry.get("table")
                if not schema or not table:
                    raise ValueError("Table targets require 'schema' and 'table'.")
                target = ProfileTarget(
                    type=target_type,
                    schema=schema,
                    table=table,
                    where=entry.get("where"),
                    name=entry.get("name"),
                    sample_rows=entry.get("sample_rows"),
                )
            else:  # query
                sql = entry.get("sql")
                if not sql:
                    raise ValueError("Query targets require 'sql'.")
                target = ProfileTarget(
                    type=target_type,
                    sql=sql,
                    name=entry.get("name"),
                    sample_rows=entry.get("sample_rows"),
                )

            targets.append(target)

        return targets
