import pandas as pd

from profiler.config import OutliersConfig
from profiler.profiling.outliers import OutlierDetector


def test_outlier_detection_zscore_and_iqr():
    df = pd.DataFrame({"num": [1, 2, 2, 2, 1000]})
    detector = OutlierDetector(OutliersConfig(method="both", zscore_threshold=3.0, iqr_factor=1.5))

    result = detector.detect(df, "target")
    row = result[result["column_name"] == "num"].iloc[0]

    assert row["outlier_count"] == 1
    assert row["min_outlier"] == 1000
    assert row["max_outlier"] == 1000
