"""Predicate-based fraud checker using predicate trees."""
from typing import List, Dict, Any, Optional, Callable
from abc import ABC, abstractmethod
from checker.rule_based_checker import RuleBasedChecker
from checker.fraud_checker import Transaction, FraudFlag


class Predicate(ABC):
    """Base class for predicate tree nodes."""

    @abstractmethod
    def evaluate(self, transaction: Transaction) -> bool:
        ...

    @abstractmethod
    def to_sql(self) -> str:
        ...


class FieldPredicate(Predicate):
    """Leaf predicate: field comparison."""

    def __init__(self, field: str, operator: str, value: Any):
        self.field = field
        self.operator = operator
        self.value = value

    def evaluate(self, transaction: Transaction) -> bool:
        field_value: Any = getattr(transaction, self.field)

        if self.operator == '>':
            return bool(field_value > self.value)
        elif self.operator == '<':
            return bool(field_value < self.value)
        elif self.operator == '>=':
            return bool(field_value >= self.value)
        elif self.operator == '<=':
            return bool(field_value <= self.value)
        elif self.operator == '==':
            return bool(field_value == self.value)
        elif self.operator == '!=':
            return bool(field_value != self.value)
        elif self.operator == 'contains':
            return bool(self.value in str(field_value))
        return False

    def to_sql(self) -> str:
        if self.operator == 'contains':
            return f"{self.field} LIKE '%{self.value}%'"
        elif self.operator == '==':
            if isinstance(self.value, str):
                return f"{self.field} = '{self.value}'"
            return f"{self.field} = {self.value}"
        else:
            if isinstance(self.value, str):
                return f"{self.field} {self.operator} '{self.value}'"
            return f"{self.field} {self.operator} {self.value}"


class AndPredicate(Predicate):
    """Composite: AND of multiple predicates."""

    def __init__(self, *predicates: Predicate):
        self.predicates = predicates

    def evaluate(self, transaction: Transaction) -> bool:
        return all(p.evaluate(transaction) for p in self.predicates)

    def to_sql(self) -> str:
        clauses = [p.to_sql() for p in self.predicates]
        return f"({' AND '.join(clauses)})"


class OrPredicate(Predicate):
    """Composite: OR of multiple predicates."""

    def __init__(self, *predicates: Predicate):
        self.predicates = predicates

    def evaluate(self, transaction: Transaction) -> bool:
        return any(p.evaluate(transaction) for p in self.predicates)

    def to_sql(self) -> str:
        clauses = [p.to_sql() for p in self.predicates]
        return f"({' OR '.join(clauses)})"


class NotPredicate(Predicate):
    """Composite: NOT of a predicate."""

    def __init__(self, predicate: Predicate):
        self.predicate = predicate

    def evaluate(self, transaction: Transaction) -> bool:
        return not self.predicate.evaluate(transaction)

    def to_sql(self) -> str:
        return f"NOT ({self.predicate.to_sql()})"


class AggregatePredicate(Predicate):
    """Aggregate predicate with GROUP BY support."""

    def __init__(self, group_by: List[str], aggregate_expr: str,
                 operator: str, threshold: Any):
        self.group_by = group_by
        self.aggregate_expr = aggregate_expr
        self.operator = operator
        self.threshold = threshold

    def evaluate(self, transaction: Transaction) -> bool:
        raise NotImplementedError("AggregatePredicate requires group evaluation")

    def to_sql(self) -> str:
        """Returns HAVING clause only."""
        if isinstance(self.threshold, str):
            return f"{self.aggregate_expr} {self.operator} '{self.threshold}'"
        return f"{self.aggregate_expr} {self.operator} {self.threshold}"

    def get_group_by_fields(self) -> List[str]:
        return self.group_by


class PredicateBasedChecker(RuleBasedChecker):
    """Checker that uses predicate trees to identify fraud patterns."""

    def __init__(self, name: str, predicate: Predicate, reason: str,
                 confidence: float = 0.8):
        super().__init__(name)
        self.predicate = predicate
        self.reason = reason
        self.confidence = confidence

    def check(self, transactions: List[Transaction]) -> List[FraudFlag]:
        """Evaluate predicate tree against transactions."""
        flagged = [txn for txn in transactions if self.predicate.evaluate(txn)]

        if not flagged:
            return []

        return [self.create_flag(flagged, self.reason, self.confidence)]

    def get_sql_predicate(self) -> str:
        """Get SQL WHERE clause from predicate tree."""
        return self.predicate.to_sql()
