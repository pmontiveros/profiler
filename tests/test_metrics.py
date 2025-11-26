import numpy as np
import pandas as pd

from profiler.profiling.metrics import MetricsCalculator


def test_compute_column_metrics_numeric_and_string():
    df = pd.DataFrame(
        {
            "num": [1, 2, 2, 2, np.nan, 5],
            "text": ["a", "bb", None, "ccc", "", "d"],
            "dates": pd.to_datetime(
                ["2024-01-01", "2024-01-02", None, "2024-01-05", None, "2024-01-10"]
            ),
        }
    )
    metrics = MetricsCalculator().compute_column_metrics(df, "t1")

    num_row = metrics[metrics["column_name"] == "num"].iloc[0]
    assert num_row["null_count"] == 1
    assert num_row["distinct_count"] == 4
    assert num_row["min"] == 1
    assert num_row["max"] == 5
    assert num_row["p25"] == 2
    assert num_row["p50"] == 2

    text_row = metrics[metrics["column_name"] == "text"].iloc[0]
    assert text_row["null_count"] == 1
    assert text_row["min_length"] == 0
    assert text_row["max_length"] == 3
    assert text_row["avg_length"] == 1.4

    date_row = metrics[metrics["column_name"] == "dates"].iloc[0]
    assert date_row["date_range_days"] == 9
