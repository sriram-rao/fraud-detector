"""Merchant repetition checker using DuckDB window functions."""
from typing import List, Any, Optional, Dict
from checker.window_checker import WindowChecker
from checker.fraud_checker import Transaction


class MerchantRepetitionChecker(WindowChecker):
    """Detects excessive transactions at same merchant."""

    def __init__(self, name: str = "MerchantRepetitionChecker",
                 time_window_hours: int = 24,
                 threshold: int = 5):
        super().__init__(name)
        self.time_window_hours = time_window_hours
        self.threshold = threshold

    def initialize(self, historical_transactions: Optional[List[Transaction]] = None,
                   config: Optional[Dict[str, Any]] = None) -> None:
        super().initialize(historical_transactions, config)
        if config:
            self.time_window_hours = config.get('time_window_hours', self.time_window_hours)
            self.threshold = config.get('threshold', self.threshold)

    def get_window_query(self, table_name: str) -> str:
        return f"""
        WITH windowed AS (
            SELECT
                user_id, merchant_name, timestamp, amount,
                COUNT(*) OVER (
                    PARTITION BY user_id, merchant_name ORDER BY timestamp
                    RANGE BETWEEN INTERVAL '{self.time_window_hours}' HOURS PRECEDING AND CURRENT ROW
                ) as count_in_window
            FROM {table_name}
        ),
        flagged AS (SELECT DISTINCT user_id, merchant_name FROM windowed WHERE count_in_window > {self.threshold})
        SELECT t.* FROM {table_name} t
        JOIN flagged f ON t.user_id = f.user_id AND t.merchant_name = f.merchant_name
        ORDER BY t.user_id, t.merchant_name, t.timestamp
        """

    def get_group_key(self, txn: Transaction) -> Any:
        return (txn.user_id, txn.merchant_name)

    def get_reason(self, group_key: Any, txns: List[Transaction]) -> str:
        _, merchant = group_key
        return f"Excessive txns at {merchant}: >{self.threshold} in {self.time_window_hours}h window"
