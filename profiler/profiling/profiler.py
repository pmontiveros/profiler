from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from profiler.config import Config
from profiler.connectors import OracleConnector, SqlServerConnector
from profiler.profiling.metrics import MetricsCalculator
from profiler.profiling.outliers import OutlierDetector
from profiler.profiling.targets import ProfileTarget, TargetLoader


@dataclass
class ProfilingResults:
    table_profile: pd.DataFrame
    column_profile: pd.DataFrame
    outliers: pd.DataFrame


class Profiler:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.connector = self._create_connector()
        self.metrics = MetricsCalculator()
        self.outlier_detector = OutlierDetector(config.outliers)

    def _create_connector(self) -> object:
        engine = self.config.engine.lower()
        if engine in ("sqlserver", "mssql", "sql_server"):
            return SqlServerConnector(self.config.connection_string)
        if engine in ("oracle", "ora"):
            return OracleConnector(self.config.connection_string)
        raise ValueError(f"Unsupported engine: {self.config.engine}")

    def _load_targets(self) -> List[ProfileTarget]:
        if not self.config.targets_file:
            raise ValueError("targets_file must be provided in Config.")
        return TargetLoader.from_json_file(self.config.targets_file)

    def _load_target_data(self, target: ProfileTarget) -> pd.DataFrame:
        if target.type == "table":
            base_sql = f"SELECT * FROM {target.schema}.{target.table}"
            if target.where:
                base_sql += f" WHERE {target.where}"
        elif target.type == "query":
            base_sql = target.sql or ""
        else:
            raise ValueError(f"Unknown target type: {target.type}")

        sample_rows = target.sample_rows if target.sample_rows is not None else self.config.sample_rows
        rows = list(self.connector.sample_data(base_sql, sample_rows))
        return pd.DataFrame(rows)

    def run(self) -> ProfilingResults:
        table_profiles: List[pd.DataFrame] = []
        column_profiles: List[pd.DataFrame] = []
        outlier_profiles: List[pd.DataFrame] = []

        self.connector.connect()
        try:
            targets = self._load_targets()
            for target in targets:
                df = self._load_target_data(target)
                target_name = target.target_name

                table_profiles.append(self.metrics.compute_table_metrics(df, target_name))
                column_profiles.append(self.metrics.compute_column_metrics(df, target_name))

                outliers_df = self.outlier_detector.detect(df, target_name)
                if not outliers_df.empty:
                    outlier_profiles.append(outliers_df)
        finally:
            self.connector.close()

        table_profile_df = pd.concat(table_profiles, ignore_index=True) if table_profiles else pd.DataFrame()
        column_profile_df = pd.concat(column_profiles, ignore_index=True) if column_profiles else pd.DataFrame()
        outliers_df = pd.concat(outlier_profiles, ignore_index=True) if outlier_profiles else pd.DataFrame()

        return ProfilingResults(
            table_profile=table_profile_df,
            column_profile=column_profile_df,
            outliers=outliers_df,
        )
