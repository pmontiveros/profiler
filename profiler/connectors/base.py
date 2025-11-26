from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


class DatabaseConnector:
    """
    Base connector defining the contract for engine-specific connectors.
    Implementations should keep an open connection in `self._conn` and
    provide the metadata and data accessors defined below.
    """

    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string
        self._conn: Optional[Any] = None

    def connect(self) -> None:  # pragma: no cover - interface only
        raise NotImplementedError

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def list_schemas(self) -> List[str]:  # pragma: no cover - interface only
        raise NotImplementedError

    def list_tables(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:  # pragma: no cover - interface only
        raise NotImplementedError

    def get_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:  # pragma: no cover - interface only
        raise NotImplementedError

    def run_query(self, sql: str) -> Iterable[Dict[str, Any]]:  # pragma: no cover - interface only
        raise NotImplementedError

    def sample_data(self, base_sql: str, sample_rows: int) -> Iterable[Dict[str, Any]]:  # pragma: no cover - interface only
        raise NotImplementedError
