"""Rule-based fraud checkers."""
from typing import List, Dict, Any, Optional
from checker.fraud_checker import FraudChecker, Transaction, FraudFlag


class RuleBasedChecker(FraudChecker):
    """Base class for simple rule-based fraud detection."""

    def __init__(self, name: str):
        super().__init__(name)
        self.config: Dict[str, Any] = {}

    def initialize(self, historical_transactions: Optional[List[Transaction]] = None,
                   config: Optional[Dict[str, Any]] = None) -> None:
        if config:
            self.config = config

    def create_flag(self, transactions: List[Transaction], reason: str,
                   confidence: float = 0.8) -> FraudFlag:
        """Helper to create fraud flags."""
        return FraudFlag(
            transactions=transactions,
            checker_name=self.name,
            reason=reason,
            confidence_score=confidence
        )

    def rows_to_transactions(self, rows: List[Dict[str, Any]]) -> List[Transaction]:
        """Convert database rows to Transaction objects."""
        return [
            Transaction(
                user_id=row['user_id'],
                timestamp=str(row['timestamp']),
                merchant_name=row['merchant_name'],
                amount=float(row['amount'])
            )
            for row in rows
        ]
