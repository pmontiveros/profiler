from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from .base import DatabaseConnector


class OracleConnector(DatabaseConnector):
    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string)
        self._driver: Optional[Any] = None

    def _import_driver(self) -> Any:
        if self._driver is None:
            try:
                import oracledb as oracle
            except ImportError:
                try:
                    import cx_Oracle as oracle  # pragma: no cover - fallback import
                except ImportError as exc:  # pragma: no cover - import guard
                    raise RuntimeError("oracledb or cx_Oracle is required for the Oracle connector") from exc
            self._driver = oracle
        return self._driver

    def connect(self) -> None:
        oracle = self._import_driver()
        try:
            self._conn = oracle.connect(self.connection_string)
        except Exception as exc:  # pragma: no cover - connection failure path
            raise RuntimeError(f"Oracle connection failed: {exc}") from exc

    def list_schemas(self) -> List[str]:
        sql = "SELECT username AS schema_name FROM all_users ORDER BY username"
        return [row["SCHEMA_NAME"] if "SCHEMA_NAME" in row else row["schema_name"] for row in self.run_query(sql)]

    def list_tables(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        filters = ""
        params: Dict[str, Any] = {}
        if schema:
            filters = "WHERE owner = :owner"
            params["owner"] = schema.upper()

        sql = f"""
        SELECT owner AS schema_name, table_name
        FROM all_tables
        {filters}
        ORDER BY owner, table_name
        """
        return list(self._execute_and_dictify(sql, params=params))

    def get_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        sql = """
        SELECT
            column_name AS name,
            data_type AS data_type,
            data_length AS max_length,
            data_precision AS precision,
            data_scale AS scale,
            nullable AS is_nullable
        FROM all_tab_columns
        WHERE owner = :owner AND table_name = :table
        ORDER BY column_id
        """
        params = {"owner": schema.upper(), "table": table.upper()}
        columns = list(self._execute_and_dictify(sql, params=params))
        for col in columns:
            nullable_val = col.get("is_nullable")
            col["is_nullable"] = (
                str(nullable_val).strip().upper() == "Y" if nullable_val is not None else None
            )
        return columns

    def run_query(self, sql: str) -> Iterable[Dict[str, Any]]:
        return self._execute_and_dictify(sql)

    def sample_data(self, base_sql: str, sample_rows: int) -> Iterable[Dict[str, Any]]:
        sample_sql = f"SELECT * FROM ({base_sql}) WHERE ROWNUM <= {int(sample_rows)}"
        return self.run_query(sample_sql)

    def _execute_and_dictify(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
        if self._conn is None:
            raise RuntimeError("Connection has not been established. Call connect() first.")
        cursor = self._conn.cursor()
        cursor.execute(sql, params or {})
        columns = [col[0] for col in cursor.description]
        for row in cursor:
            yield {col: value for col, value in zip(columns, row)}
