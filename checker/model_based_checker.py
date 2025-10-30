"""Model-based fraud checker base class."""
from typing import List, Dict, Any, Optional
from abc import abstractmethod
from checker.fraud_checker import FraudChecker, Transaction, FraudFlag


class ModelBasedChecker(FraudChecker):
    """Base class for ML/AI model-based fraud detection."""

    @abstractmethod
    def predict(self, transactions: List[Transaction]) -> List[tuple[List[Transaction], str, float]]:
        """
        Run model inference on transactions.

        Returns:
            List of (flagged_transactions, reason, confidence) tuples
        """
        ...

    def check(self, transactions: List[Transaction]) -> List[FraudFlag]:
        """Run model prediction and convert to FraudFlags."""
        if not transactions:
            return []

        predictions = self.predict(transactions)

        flags: List[FraudFlag] = []
        for flagged_txns, reason, confidence in predictions:
            if flagged_txns:
                flag = FraudFlag(
                    transactions=flagged_txns,
                    checker_name=self.name,
                    reason=reason,
                    confidence_score=confidence
                )
                flags.append(flag)

        return flags
