"""Generic window function checker base class."""
from typing import List, Dict, Any, Optional, Tuple
from abc import abstractmethod
from collections import defaultdict
from checker.rule_based_checker import RuleBasedChecker
from checker.fraud_checker import Transaction, FraudFlag
from duckdb_repository import DuckDBRepository


class WindowChecker(RuleBasedChecker):
    """Generic checker using DuckDB window functions."""

    @abstractmethod
    def get_window_query(self, table_name: str) -> str:
        """Return SQL query with window function."""
        ...

    @abstractmethod
    def get_group_key(self, txn: Transaction) -> Any:
        """Return grouping key for transactions."""
        ...

    @abstractmethod
    def get_reason(self, group_key: Any, txns: List[Transaction]) -> str:
        """Generate reason string for flag."""
        ...

    def check(self, transactions: List[Transaction]) -> List[FraudFlag]:
        return []

    def check_with_repo(self, repo: DuckDBRepository, table_name: str = "transactions") -> List[FraudFlag]:
        """Generic window function execution."""
        query = self.get_window_query(table_name)
        results = repo.fetch_items(query)

        if not results:
            return []

        all_txns = self.rows_to_transactions(results)

        groups: Dict[Any, List[Transaction]] = defaultdict(list)
        for txn in all_txns:
            key = self.get_group_key(txn)
            groups[key].append(txn)

        flags: List[FraudFlag] = []
        for key, txns in groups.items():
            reason = self.get_reason(key, txns)
            flags.append(self.create_flag(txns, reason, confidence=0.85))

        return flags
