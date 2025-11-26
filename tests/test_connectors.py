from profiler.connectors.sqlserver import SqlServerConnector


class DummySqlServerConnector(SqlServerConnector):
    def __init__(self) -> None:
        super().__init__(connection_string="")
        self.last_sql: str | None = None

    def run_query(self, sql: str):
        self.last_sql = sql
        return []


def test_sample_data_wraps_query_with_top():
    connector = DummySqlServerConnector()
    list(connector.sample_data("SELECT * FROM dbo.Customer", 5))
    assert connector.last_sql.startswith("SELECT TOP (5) * FROM (SELECT * FROM dbo.Customer)")
