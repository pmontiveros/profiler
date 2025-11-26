import json
from pathlib import Path

from profiler.profiling.targets import TargetLoader


def test_target_loader_parses_table_and_query(tmp_path: Path):
    targets_content = {
        "targets": [
            {"type": "table", "schema": "dbo", "table": "Customer", "where": "IsActive = 1"},
            {
                "type": "query",
                "name": "CustomQuery",
                "sql": "SELECT * FROM dbo.Customer WHERE IsActive = 1",
                "sample_rows": 50,
            },
        ]
    }
    target_file = tmp_path / "targets.json"
    target_file.write_text(json.dumps(targets_content), encoding="utf-8")

    targets = TargetLoader.from_json_file(target_file)
    assert len(targets) == 2
    assert targets[0].type == "table"
    assert targets[0].target_name == "dbo.Customer"
    assert targets[1].type == "query"
    assert targets[1].sample_rows == 50
