"""Rapid geographic shift checker using DuckDB window functions."""
from typing import List, Any, Optional, Dict
from checker.window_checker import WindowChecker
from checker.fraud_checker import Transaction


class GeographicShiftChecker(WindowChecker):
    """Detects rapid shifts between merchants (proxy for geography)."""

    def __init__(self, name: str = "GeographicShiftChecker",
                 time_window_minutes: int = 30,
                 threshold: int = 2):
        super().__init__(name)
        self.time_window_minutes = time_window_minutes
        self.threshold = threshold

    def initialize(self, historical_transactions: Optional[List[Transaction]] = None,
                   config: Optional[Dict[str, Any]] = None) -> None:
        super().initialize(historical_transactions, config)
        if config:
            self.time_window_minutes = config.get('time_window_minutes', self.time_window_minutes)
            self.threshold = config.get('threshold', self.threshold)

    def get_window_query(self, table_name: str) -> str:
        return f"""
        WITH windowed AS (
            SELECT
                user_id, timestamp, merchant_name, amount,
                COUNT(DISTINCT merchant_name) OVER (
                    PARTITION BY user_id ORDER BY timestamp
                    RANGE BETWEEN INTERVAL '{self.time_window_minutes}' MINUTES PRECEDING AND CURRENT ROW
                ) as distinct_merchants
            FROM {table_name}
        ),
        flagged AS (SELECT DISTINCT user_id FROM windowed WHERE distinct_merchants > {self.threshold})
        SELECT t.* FROM {table_name} t JOIN flagged f ON t.user_id = f.user_id
        ORDER BY t.user_id, t.timestamp
        """

    def get_group_key(self, txn: Transaction) -> Any:
        return txn.user_id

    def get_reason(self, group_key: Any, txns: List[Transaction]) -> str:
        return f"Rapid geographic shift: >{self.threshold} merchants in {self.time_window_minutes} min"
