from __future__ import annotations

import argparse
from pathlib import Path

from profiler.config import Config, OutliersConfig
from profiler.profiling.profiler import Profiler
from profiler.reporting.exporters import Exporters


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run data profiling against supported databases.")
    parser.add_argument("--engine", required=True, help="Database engine (sqlserver | oracle).")
    parser.add_argument("--connstr", required=True, help="Connection string for the target database.")
    parser.add_argument("--targets-file", required=True, help="Path to targets JSON file.")
    parser.add_argument("--sample-rows", type=int, default=10000, help="Sample rows per target (default: 10000).")
    parser.add_argument("--outdir", default=".", help="Output directory for exported profiles.")
    parser.add_argument(
        "--outliers-method",
        choices=["iqr", "zscore", "both"],
        help="Outlier detection method override (iqr | zscore | both).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    outliers_config = OutliersConfig()
    if args.outliers_method:
        outliers_config.method = args.outliers_method

    config = Config(
        engine=args.engine,
        connection_string=args.connstr,
        targets_file=args.targets_file,
        sample_rows=args.sample_rows,
        outdir=args.outdir,
        outliers=outliers_config,
    )

    profiler = Profiler(config)
    results = profiler.run()

    outdir = Path(config.outdir or ".")
    Exporters.export_to_csv(results.table_profile, outdir / "table_profile.csv")
    Exporters.export_to_csv(results.column_profile, outdir / "column_profile.csv")
    if not results.outliers.empty:
        Exporters.export_to_csv(results.outliers, outdir / "outliers.csv")


if __name__ == "__main__":
    main()
