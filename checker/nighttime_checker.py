"""Nighttime high-value transaction checker."""
from typing import List, Dict, Any, Optional
from checker.rule_based_checker import RuleBasedChecker
from checker.fraud_checker import Transaction, FraudFlag
from duckdb_repository import DuckDBRepository


class NighttimeChecker(RuleBasedChecker):
    """Detects high-value transactions during late-night hours."""

    def __init__(self, name: str = "NighttimeChecker",
                 start_hour: int = 2,
                 end_hour: int = 5,
                 min_amount: float = 1000.0):
        super().__init__(name)
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.min_amount = min_amount

    def initialize(self, historical_transactions: Optional[List[Transaction]] = None,
                   config: Optional[Dict[str, Any]] = None) -> None:
        super().initialize(historical_transactions, config)
        if config:
            self.start_hour = config.get('start_hour', self.start_hour)
            self.end_hour = config.get('end_hour', self.end_hour)
            self.min_amount = config.get('min_amount', self.min_amount)

    def check(self, transactions: List[Transaction]) -> List[FraudFlag]:
        return []

    def check_with_repo(self, repo: DuckDBRepository, table_name: str = "transactions") -> List[FraudFlag]:
        """Find high-value transactions during nighttime hours."""
        query = f"""
        SELECT user_id, timestamp, merchant_name, amount
        FROM {table_name}
        WHERE HOUR(timestamp) >= {self.start_hour}
          AND HOUR(timestamp) < {self.end_hour}
          AND amount >= {self.min_amount}
        ORDER BY user_id, timestamp
        """

        results = repo.fetch_items(query)

        if not results:
            return []

        all_txns = self.rows_to_transactions(results)

        user_groups: Dict[str, List[Transaction]] = {}
        for txn in all_txns:
            if txn.user_id not in user_groups:
                user_groups[txn.user_id] = []
            user_groups[txn.user_id].append(txn)

        flags: List[FraudFlag] = []
        for user_id, txns in user_groups.items():
            reason = f"High-value (${self.min_amount}+) nighttime txn ({self.start_hour}-{self.end_hour} AM)"
            flags.append(self.create_flag(txns, reason, confidence=0.88))

        return flags
