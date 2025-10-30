import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

"""Test predicate-based checker with predicate trees."""
from checker import (
    PredicateBasedChecker,
    FieldPredicate,
    AndPredicate,
    OrPredicate,
    NotPredicate,
    Transaction
)

# Example 1: Simple field predicate - high value transactions
high_value_pred = FieldPredicate('amount', '>', 500)
high_value_checker = PredicateBasedChecker(
    name="HighValueChecker",
    predicate=high_value_pred,
    reason="Transaction exceeds $500 threshold",
    confidence=0.75
)

print("Example 1: Simple predicate")
print(f"SQL: {high_value_checker.get_sql_predicate()}")
print()

# Example 2: AND predicate - high value at night
night_and_expensive = AndPredicate(
    FieldPredicate('amount', '>', 500),
    FieldPredicate('merchant_name', 'contains', 'Best Buy')
)
night_checker = PredicateBasedChecker(
    name="NightHighValueChecker",
    predicate=night_and_expensive,
    reason="High value transaction at electronics store",
    confidence=0.85
)

print("Example 2: AND predicate")
print(f"SQL: {night_checker.get_sql_predicate()}")
print()

# Example 3: OR predicate - suspicious merchants or high amounts
suspicious_pattern = OrPredicate(
    FieldPredicate('amount', '>', 1000),
    FieldPredicate('merchant_name', 'contains', 'Bitcoin')
)
suspicious_checker = PredicateBasedChecker(
    name="SuspiciousPatternChecker",
    predicate=suspicious_pattern,
    reason="Suspicious merchant or high value transaction",
    confidence=0.9
)

print("Example 3: OR predicate")
print(f"SQL: {suspicious_checker.get_sql_predicate()}")
print()

# Example 4: Complex nested predicate tree
complex_pred = AndPredicate(
    OrPredicate(
        FieldPredicate('amount', '>', 1000),
        FieldPredicate('merchant_name', 'contains', 'Casino')
    ),
    NotPredicate(
        FieldPredicate('user_id', '==', 'user_001')
    )
)
complex_checker = PredicateBasedChecker(
    name="ComplexPatternChecker",
    predicate=complex_pred,
    reason="High-risk pattern detected",
    confidence=0.92
)

print("Example 4: Complex nested predicate")
print(f"SQL: {complex_checker.get_sql_predicate()}")
print()

# Test evaluation with sample transactions
test_txns = [
    Transaction("user_001", "2025-10-29 08:00:00", "Starbucks", 5.0),
    Transaction("user_002", "2025-10-29 14:00:00", "Best Buy", 1299.99),
    Transaction("user_003", "2025-10-29 20:00:00", "Bitcoin Exchange", 500.0),
    Transaction("user_004", "2025-10-29 23:00:00", "Casino Royal", 2000.0),
]

print("Testing evaluation:")
for checker in [high_value_checker, night_checker, suspicious_checker, complex_checker]:
    flags = checker.check(test_txns)
    print(f"{checker.name}: {len(flags[0].transactions) if flags else 0} flagged")
    if flags:
        for txn in flags[0].transactions:
            print(f"  - {txn.merchant_name}: ${txn.amount}")
