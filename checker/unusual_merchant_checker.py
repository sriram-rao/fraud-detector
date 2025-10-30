"""Unusual merchant checker using DuckDB."""
from typing import List, Dict, Any, Optional
from checker.rule_based_checker import RuleBasedChecker
from checker.fraud_checker import Transaction, FraudFlag
from duckdb_repository import DuckDBRepository


class UnusualMerchantChecker(RuleBasedChecker):
    """Detects new merchants with anomalously high transactions."""

    def __init__(self, name: str = "UnusualMerchantChecker", multiplier: float = 2.0):
        super().__init__(name)
        self.multiplier = multiplier

    def initialize(self, historical_transactions: Optional[List[Transaction]] = None,
                   config: Optional[Dict[str, Any]] = None) -> None:
        super().initialize(historical_transactions, config)
        if config:
            self.multiplier = config.get('multiplier', self.multiplier)

    def check(self, transactions: List[Transaction]) -> List[FraudFlag]:
        return []

    def check_with_repo(self, repo: DuckDBRepository, table_name: str = "transactions") -> List[FraudFlag]:
        """Find first-time merchants with high amounts relative to user average."""
        query = f"""
        WITH user_avg AS (
            SELECT user_id, AVG(amount) as avg_amount
            FROM {table_name}
            GROUP BY user_id
        ),
        merchant_counts AS (
            SELECT user_id, merchant_name, COUNT(*) as txn_count
            FROM {table_name}
            GROUP BY user_id, merchant_name
        ),
        new_merchants AS (
            SELECT user_id, merchant_name
            FROM merchant_counts
            WHERE txn_count = 1
        )
        SELECT t.user_id, t.timestamp, t.merchant_name, t.amount, ua.avg_amount
        FROM {table_name} t
        JOIN new_merchants nm ON t.user_id = nm.user_id AND t.merchant_name = nm.merchant_name
        JOIN user_avg ua ON t.user_id = ua.user_id
        WHERE t.amount > ua.avg_amount * {self.multiplier}
        ORDER BY t.user_id, t.timestamp
        """

        results = repo.fetch_items(query)

        if not results:
            return []

        all_txns = self.rows_to_transactions(results)

        user_groups: Dict[str, tuple[List[Transaction], float]] = {}
        for i, txn in enumerate(all_txns):
            if txn.user_id not in user_groups:
                user_groups[txn.user_id] = ([], float(results[i]['avg_amount']))
            user_groups[txn.user_id][0].append(txn)

        flags: List[FraudFlag] = []
        for user_id, (txns, avg) in user_groups.items():
            reason = f"New merchant with {self.multiplier}x avg amount (${avg:.2f})"
            flags.append(self.create_flag(txns, reason, confidence=0.82))

        return flags
