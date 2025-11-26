from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from .base import DatabaseConnector


class SqlServerConnector(DatabaseConnector):
    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string)
        self._driver: Optional[Any] = None

    def _import_driver(self) -> Any:
        if self._driver is None:
            try:
                import pyodbc
            except ImportError as exc:  # pragma: no cover - import guard
                raise RuntimeError("pyodbc is required for the SQL Server connector") from exc
            self._driver = pyodbc
        return self._driver

    def connect(self) -> None:
        pyodbc = self._import_driver()
        try:
            self._conn = pyodbc.connect(self.connection_string)
        except pyodbc.Error as exc:  # pragma: no cover - connection failure path
            raise RuntimeError(f"SQL Server connection failed: {exc}") from exc

    def list_schemas(self) -> List[str]:
        sql = "SELECT name FROM sys.schemas ORDER BY name"
        return [row["name"] for row in self.run_query(sql)]

    def list_tables(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        filters = ""
        params: List[Any] = []
        if schema:
            filters = "WHERE s.name = ?"
            params.append(schema)

        sql = f"""
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        {filters}
        ORDER BY s.name, t.name
        """
        return list(self._execute_and_dictify(sql, params=params))

    def get_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        sql = """
        SELECT
            COLUMN_NAME AS name,
            DATA_TYPE AS data_type,
            CHARACTER_MAXIMUM_LENGTH AS max_length,
            NUMERIC_PRECISION AS precision,
            NUMERIC_SCALE AS scale,
            IS_NULLABLE AS is_nullable
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        params = [schema, table]
        columns = list(self._execute_and_dictify(sql, params=params))
        # Normalize nullability to booleans for easier downstream use
        for col in columns:
            nullable_val = col.get("is_nullable")
            col["is_nullable"] = (
                str(nullable_val).strip().upper() == "YES" if nullable_val is not None else None
            )
        return columns

    def run_query(self, sql: str) -> Iterable[Dict[str, Any]]:
        return self._execute_and_dictify(sql)

    def sample_data(self, base_sql: str, sample_rows: int) -> Iterable[Dict[str, Any]]:
        sample_sql = f"SELECT TOP ({int(sample_rows)}) * FROM ({base_sql}) AS x"
        return self.run_query(sample_sql)

    def _execute_and_dictify(self, sql: str, params: Optional[List[Any]] = None) -> Iterable[Dict[str, Any]]:
        if self._conn is None:
            raise RuntimeError("Connection has not been established. Call connect() first.")
        cursor = self._conn.cursor()
        cursor.execute(sql, params or [])
        columns = [col[0] for col in cursor.description]
        for row in cursor:
            yield {col: value for col, value in zip(columns, row)}
