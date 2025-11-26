from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd

from profiler.config import OutliersConfig


@dataclass
class OutlierResult:
    target_name: str
    column_name: str
    method: str
    sample_size: int
    outlier_count: int
    outlier_ratio: float
    min_outlier: float | int | None
    max_outlier: float | int | None


class OutlierDetector:
    def __init__(self, config: OutliersConfig) -> None:
        self.config = config

    def detect(self, df: pd.DataFrame, target_name: str) -> pd.DataFrame:
        if not self.config.enabled:
            return pd.DataFrame(columns=OutlierResult.__annotations__.keys())

        numeric_df = df.select_dtypes(include=[np.number])
        if numeric_df.empty:
            return pd.DataFrame(columns=OutlierResult.__annotations__.keys())

        results: List[Dict[str, object]] = []

        for col in numeric_df.columns:
            series = numeric_df[col].dropna()
            sample_size = len(series)
            if sample_size == 0:
                results.append(
                    {
                        "target_name": target_name,
                        "column_name": col,
                        "method": self.config.method,
                        "sample_size": 0,
                        "outlier_count": 0,
                        "outlier_ratio": 0.0,
                        "min_outlier": np.nan,
                        "max_outlier": np.nan,
                    }
                )
                continue

            outlier_mask = self._compute_outlier_mask(series)
            outlier_values = series[outlier_mask]

            outlier_count = int(outlier_mask.sum())
            outlier_ratio = outlier_count / sample_size if sample_size > 0 else 0.0
            min_outlier = outlier_values.min() if outlier_count > 0 else np.nan
            max_outlier = outlier_values.max() if outlier_count > 0 else np.nan

            results.append(
                {
                    "target_name": target_name,
                    "column_name": col,
                    "method": self.config.method,
                    "sample_size": sample_size,
                    "outlier_count": outlier_count,
                    "outlier_ratio": outlier_ratio,
                    "min_outlier": min_outlier,
                    "max_outlier": max_outlier,
                }
            )

        return pd.DataFrame(results)

    def _compute_outlier_mask(self, series: pd.Series) -> pd.Series:
        method = self.config.method.lower()
        masks: List[pd.Series] = []

        if method in ("zscore", "both"):
            masks.append(self._zscore_mask(series))

        if method in ("iqr", "both"):
            masks.append(self._iqr_mask(series))

        if not masks:
            raise ValueError(f"Unsupported outlier detection method: {self.config.method}")

        mask = masks[0]
        for extra in masks[1:]:
            mask = mask | extra
        return mask

    def _zscore_mask(self, series: pd.Series) -> pd.Series:
        mean = series.mean()
        std = series.std(ddof=0)
        if std == 0 or np.isnan(std):
            return pd.Series([False] * len(series), index=series.index)

        zscores = (series - mean) / std
        return zscores.abs() >= float(self.config.zscore_threshold)

    def _iqr_mask(self, series: pd.Series) -> pd.Series:
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0 or np.isnan(iqr):
            return pd.Series([False] * len(series), index=series.index)

        lower = q1 - float(self.config.iqr_factor) * iqr
        upper = q3 + float(self.config.iqr_factor) * iqr
        return (series < lower) | (series > upper)
