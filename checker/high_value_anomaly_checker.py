"""High-value anomaly checker using DuckDB median calculation."""
from typing import List, Dict, Any, Optional
from checker.rule_based_checker import RuleBasedChecker
from checker.fraud_checker import Transaction, FraudFlag
from duckdb_repository import DuckDBRepository


class HighValueAnomalyChecker(RuleBasedChecker):
    """Detects transactions significantly higher than user's median."""

    def __init__(self, name: str = "HighValueAnomalyChecker", multiplier: float = 3.0):
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
        """Find transactions exceeding user's median by multiplier."""
        query = f"""
        WITH user_medians AS (
            SELECT
                user_id,
                MEDIAN(amount) as median_amount
            FROM {table_name}
            GROUP BY user_id
            HAVING COUNT(*) >= 2
        )
        SELECT
            t.user_id,
            t.timestamp,
            t.merchant_name,
            t.amount,
            um.median_amount
        FROM {table_name} t
        JOIN user_medians um ON t.user_id = um.user_id
        WHERE t.amount > um.median_amount * {self.multiplier}
        ORDER BY t.user_id, t.timestamp
        """

        results = repo.fetch_items(query)

        if not results:
            return []

        all_txns = self.rows_to_transactions(results)

        user_groups: Dict[str, tuple[List[Transaction], float]] = {}
        for i, txn in enumerate(all_txns):
            if txn.user_id not in user_groups:
                user_groups[txn.user_id] = ([], float(results[i]['median_amount']))
            user_groups[txn.user_id][0].append(txn)

        flags: List[FraudFlag] = []
        for user_id, (txns, median) in user_groups.items():
            reason = f"Transaction exceeds {self.multiplier}x user median (${median:.2f})"
            flags.append(self.create_flag(txns, reason, confidence=0.85))

        return flags
