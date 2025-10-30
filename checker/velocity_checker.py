"""Velocity spike fraud checker using DuckDB window functions."""
from typing import List, Any, Optional, Dict
from checker.window_checker import WindowChecker
from checker.fraud_checker import Transaction


class VelocityChecker(WindowChecker):
    """Detects velocity spikes using DuckDB window functions."""

    def __init__(self, name: str = "VelocityChecker",
                 time_window_minutes: int = 10,
                 threshold: int = 3):
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
                COUNT(*) OVER (
                    PARTITION BY user_id ORDER BY timestamp
                    RANGE BETWEEN INTERVAL '{self.time_window_minutes}' MINUTES PRECEDING AND CURRENT ROW
                ) as count_in_window
            FROM {table_name}
        ),
        flagged AS (SELECT DISTINCT user_id FROM windowed WHERE count_in_window > {self.threshold})
        SELECT t.* FROM {table_name} t JOIN flagged f ON t.user_id = f.user_id
        ORDER BY t.user_id, t.timestamp
        """

    def get_group_key(self, txn: Transaction) -> Any:
        return txn.user_id

    def get_reason(self, group_key: Any, txns: List[Transaction]) -> str:
        return f"Velocity spike: >{self.threshold} txns in {self.time_window_minutes} min window"
