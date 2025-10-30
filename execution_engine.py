"""Execution engine for fraud detection with dual logging support."""
import logging
from logging.handlers import RotatingFileHandler
import sys
from typing import List, Dict, Any
from datetime import datetime
from checker import FraudChecker, Transaction, FraudFlag, PredicateBasedChecker, FieldPredicate, OrPredicate, AndPredicate


class ExecutionEngine:
    def __init__(self, log_file: str = "fraud_detection.log",
                 max_bytes: int = 10*1024*1024,
                 backup_count: int = 5) -> None:
        self.logger: logging.Logger = self._setup_logging(log_file, max_bytes, backup_count)
        self.checkers: List[FraudChecker] = []
        self.logger.info("=" * 80)
        self.logger.info("Execution Engine initialized at %s", datetime.now())
        self.logger.info("=" * 80)

    def _setup_logging(self, log_file: str, max_bytes: int, backup_count: int) -> logging.Logger:
        logger = logging.getLogger("FraudDetectionEngine")
        logger.setLevel(logging.INFO)

        if logger.handlers:
            logger.handlers.clear()

        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stdout_handler)

        return logger

    def configure_checkers(self) -> None:
        """Configure the checker pipeline. Edit this method to add checkers."""
        self.checkers = [
            PredicateBasedChecker(
                name="HighValueChecker",
                predicate=FieldPredicate('amount', '>', 500),
                reason="High value transaction (>$500)",
                confidence=0.75
            ),
            PredicateBasedChecker(
                name="RoundAmountTestChecker",
                predicate=OrPredicate(
                    FieldPredicate('amount', '==', 1.0),
                    FieldPredicate('amount', '==', 5.0),
                    FieldPredicate('amount', '==', 10.0)
                ),
                reason="Round test amount detected ($1, $5, or $10)",
                confidence=0.65
            ),
            PredicateBasedChecker(
                name="CryptoMerchantChecker",
                predicate=OrPredicate(
                    FieldPredicate('merchant_name', 'contains', 'Bitcoin'),
                    FieldPredicate('merchant_name', 'contains', 'Crypto'),
                    FieldPredicate('merchant_name', 'contains', 'Casino')
                ),
                reason="High-risk merchant category detected",
                confidence=0.70
            ),
            PredicateBasedChecker(
                name="VeryHighValueChecker",
                predicate=FieldPredicate('amount', '>', 1000),
                reason="Very high value transaction (>$1000)",
                confidence=0.85
            ),
            PredicateBasedChecker(
                name="HighRiskComboChecker",
                predicate=AndPredicate(
                    FieldPredicate('amount', '>', 200),
                    OrPredicate(
                        FieldPredicate('merchant_name', 'contains', 'Electronics'),
                        FieldPredicate('merchant_name', 'contains', 'Best Buy'),
                        FieldPredicate('merchant_name', 'contains', 'Apple')
                    )
                ),
                reason="High-value electronics purchase (common fraud target)",
                confidence=0.72
            ),
        ]

    def execute(self, transactions: List[Transaction]) -> List[FraudFlag]:
        self.logger.info("Starting execution with %d transactions", len(transactions))

        all_flags: List[FraudFlag] = []
        for checker in self.checkers:
            self.logger.info("Running checker: %s", checker.name)
            flags = checker.check(transactions)
            all_flags.extend(flags)
            self.logger.info("  Found %d fraud flags", len(flags))

        self.logger.info("Execution completed. Total flags: %d", len(all_flags))
        return all_flags

    def shutdown(self) -> None:
        self.logger.info("Shutting down execution engine")
        self.logger.info("=" * 80)
        logging.shutdown()


if __name__ == "__main__":
    engine = ExecutionEngine()
    sample_transactions: List[Dict[str, Any]] = []
    results = engine.execute(sample_transactions)
    engine.shutdown()
