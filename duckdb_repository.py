"""DuckDB repository for SQL database operations."""
from typing import List, Dict, Any, Optional
import duckdb


class DuckDBRepository:
    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path: str = db_path
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(db_path)

    def fetch_items(
        self, query: str, params: Optional[tuple[Any, ...]] = None
    ) -> List[Dict[str, Any]]:
        if params:
            result = self.conn.execute(query, params)
        else:
            result = self.conn.execute(query)

        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def execute(self, command: str, params: Optional[tuple[Any, ...]] = None) -> int:
        if params:
            result = self.conn.execute(command, params)
        else:
            result = self.conn.execute(command)

        row = result.fetchone()
        return int(row[0]) if row and result.description else 0

    def insert_from_csv(self, csv_path: str, table_name: str) -> int:
        """Creates table and loads CSV data."""
        create_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
        self.conn.execute(create_query)

        count_query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.conn.execute(count_query)
        row = result.fetchone()
        return int(row[0]) if row else 0

    def close(self) -> None:
        if self.conn:
            self.conn.close()

    def __enter__(self) -> "DuckDBRepository":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
