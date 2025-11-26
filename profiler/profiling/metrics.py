from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_string_dtype,
)


class MetricsCalculator:
    def compute_table_metrics(self, df: pd.DataFrame, target_name: str) -> pd.DataFrame:
        metrics = {
            "target_name": target_name,
            "row_count_sample": int(len(df)),
            "column_count": int(df.shape[1]),
        }
        return pd.DataFrame([metrics])

    def compute_column_metrics(self, df: pd.DataFrame, target_name: str) -> pd.DataFrame:
        total_rows = len(df)
        metrics: List[Dict[str, object]] = []

        for col_name in df.columns:
            series = df[col_name]
            non_null_series = series.dropna()
            col_metrics: Dict[str, object] = {
                "target_name": target_name,
                "column_name": col_name,
                "total_rows": int(total_rows),
                "null_count": int(series.isna().sum()),
            }
            col_metrics["null_ratio"] = (
                col_metrics["null_count"] / total_rows if total_rows > 0 else 0.0
            )

            distinct_count = int(non_null_series.nunique(dropna=True))
            col_metrics["distinct_count"] = distinct_count
            col_metrics["distinct_ratio"] = (
                distinct_count / total_rows if total_rows > 0 else 0.0
            )

            if is_bool_dtype(series):
                # Treat booleans as numeric (0/1) but ensure float dtype to avoid boolean quantile issues
                col_metrics.update(self._numeric_metrics(non_null_series.astype(float)))
            elif is_numeric_dtype(series):
                col_metrics.update(self._numeric_metrics(non_null_series))
            elif is_datetime64_any_dtype(series):
                col_metrics.update(self._datetime_metrics(non_null_series))
            elif is_string_dtype(series):
                col_metrics.update(self._string_metrics(non_null_series))

            metrics.append(col_metrics)

        return pd.DataFrame(metrics)

    def _numeric_metrics(self, series: pd.Series) -> Dict[str, object]:
        # Convert to numeric to avoid issues with boolean dtype during quantile computations
        series = pd.to_numeric(series, errors="coerce").astype(float).dropna()
        if series.empty:
            return {
                "min": np.nan,
                "max": np.nan,
                "mean": np.nan,
                "median": np.nan,
                "std_dev": np.nan,
                "p25": np.nan,
                "p50": np.nan,
                "p75": np.nan,
            }

        return {
            "min": series.min(),
            "max": series.max(),
            "mean": series.mean(),
            "median": series.median(),
            "std_dev": series.std(ddof=0),
            "p25": series.quantile(0.25),
            "p50": series.quantile(0.50),
            "p75": series.quantile(0.75),
        }

    def _datetime_metrics(self, series: pd.Series) -> Dict[str, object]:
        if series.empty:
            return {"min_date": pd.NaT, "max_date": pd.NaT, "date_range_days": np.nan}
        min_date = series.min()
        max_date = series.max()
        return {
            "min_date": min_date,
            "max_date": max_date,
            "date_range_days": (max_date - min_date).days if pd.notna(min_date) and pd.notna(max_date) else np.nan,
        }

    def _string_metrics(self, series: pd.Series) -> Dict[str, object]:
        lengths = series.astype(str).str.len()
        if lengths.empty:
            return {"min_length": np.nan, "max_length": np.nan, "avg_length": np.nan}

        return {
            "min_length": lengths.min(),
            "max_length": lengths.max(),
            "avg_length": lengths.mean(),
        }
