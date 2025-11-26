from __future__ import annotations

from pathlib import Path

import pandas as pd


class Exporters:
    @staticmethod
    def export_to_csv(df: pd.DataFrame, path: str | Path) -> None:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)

    @staticmethod
    def export_to_json(df: pd.DataFrame, path: str | Path) -> None:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_json(output_path, orient="records", force_ascii=False)
