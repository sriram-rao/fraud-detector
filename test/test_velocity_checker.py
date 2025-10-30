import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from checker.velocity_checker import VelocityChecker
from duckdb_repository import DuckDBRepository

print("Creating test data with velocity spike...\n")

with DuckDBRepository() as repo:
    repo.execute("""
        CREATE TABLE transactions AS
        SELECT * FROM (VALUES
            ('user_001', TIMESTAMP '2025-10-29 10:00:00', 'Store A', 50.0),
            ('user_001', TIMESTAMP '2025-10-29 10:02:00', 'Store B', 75.0),
            ('user_001', TIMESTAMP '2025-10-29 10:05:00', 'Store C', 100.0),
            ('user_001', TIMESTAMP '2025-10-29 10:08:00', 'Store D', 125.0),
            ('user_002', TIMESTAMP '2025-10-29 09:00:00', 'Store X', 200.0),
            ('user_002', TIMESTAMP '2025-10-29 15:00:00', 'Store Y', 300.0),
            ('user_003', TIMESTAMP '2025-10-29 11:00:00', 'Store Z', 400.0)
        ) AS t(user_id, timestamp, merchant_name, amount)
    """)

    checker = VelocityChecker(
        name="VelocityChecker",
        time_window_minutes=10,
        threshold=3
    )
    checker.initialize()

    flags = checker.check_with_repo(repo)

    print(f"Found {len(flags)} velocity spike patterns:\n")
    for flag in flags:
        print(f"Checker: {flag.checker_name}")
        print(f"Reason: {flag.reason}")
        print(f"Confidence: {flag.confidence_score}")
        print(f"Transactions ({flag.transaction_count}):")
        for txn in flag.transactions:
            print(f"  - {txn.timestamp}: {txn.merchant_name} ${txn.amount}")
        print()

print("Expected: user_001 should be flagged (4 txns in 10 min)")
print("Expected: user_002 should NOT be flagged (2 txns, 6 hours apart)")
print("Expected: user_003 should NOT be flagged (only 1 txn)")
