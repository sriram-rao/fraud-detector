"""Fraud checker interfaces and data models."""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from duckdb_repository import DuckDBRepository


@dataclass
class Transaction:
    user_id: str
    timestamp: str
    merchant_name: str
    amount: float


@dataclass
class FraudFlag:
    """Supports both single-transaction and multi-transaction fraud patterns."""
    transactions: List[Transaction]
    checker_name: str
    reason: str
    confidence_score: float = 0.0

    @property
    def transaction_count(self) -> int:
        return len(self.transactions)


class FraudChecker(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def initialize(self, historical_transactions: Optional[List[Transaction]] = None,
                   config: Optional[Dict[str, Any]] = None) -> None:
        ...

    def fetch_relevant_data(self, repo: "DuckDBRepository",
                          filters: Optional[Dict[str, Any]] = None) -> List[Transaction]:
        """Fetch contextual data needed for fraud detection. Override as needed."""
        return []

    @abstractmethod
    def check(self, transactions: List[Transaction]) -> List[FraudFlag]:
        """Returns flags for suspicious transactions only."""
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"
