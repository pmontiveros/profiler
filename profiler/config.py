from dataclasses import dataclass, field
from typing import Any, Dict, Optional

@dataclass
class OutliersConfig:
    enabled: bool = True
    method: str = "iqr"
    zscore_threshold: float = 3.0
    iqr_factor: float = 1.5

@dataclass
class Config:
    engine: str
    connection_string: str
    targets_file: Optional[str] = None
    sample_rows: int = 10000
    outdir: Optional[str] = None
    outliers: OutliersConfig = field(default_factory=OutliersConfig)
    extra: Dict[str, Any] = field(default_factory=dict)
