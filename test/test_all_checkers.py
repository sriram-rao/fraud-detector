import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from checker import (
    VelocityChecker,
    HighValueAnomalyChecker,
    MerchantRepetitionChecker,
    GeographicShiftChecker,
    NighttimeChecker,
    UnusualMerchantChecker
)
from duckdb_repository import DuckDBRepository

print("Testing all DuckDB-based checkers\n")

with DuckDBRepository() as repo:
    repo.execute("""
        CREATE TABLE transactions AS
        SELECT * FROM (VALUES
            ('user_001', TIMESTAMP '2025-10-29 10:00:00', 'Starbucks', 5.0),
            ('user_001', TIMESTAMP '2025-10-29 10:02:00', 'Starbucks', 6.0),
            ('user_001', TIMESTAMP '2025-10-29 10:05:00', 'Starbucks', 7.0),
            ('user_001', TIMESTAMP '2025-10-29 10:08:00', 'Starbucks', 500.0),
            ('user_002', TIMESTAMP '2025-10-29 03:00:00', 'Casino', 2000.0),
            ('user_002', TIMESTAMP '2025-10-29 09:00:00', 'Target', 50.0),
            ('user_003', TIMESTAMP '2025-10-29 11:00:00', 'Store A', 100.0),
            ('user_003', TIMESTAMP '2025-10-29 11:05:00', 'Store B', 150.0),
            ('user_003', TIMESTAMP '2025-10-29 11:10:00', 'Store C', 200.0),
            ('user_004', TIMESTAMP '2025-10-29 12:00:00', 'NewPlace', 1000.0)
        ) AS t(user_id, timestamp, merchant_name, amount)
    """)

    checkers = [
        VelocityChecker(threshold=3),
        HighValueAnomalyChecker(multiplier=3.0),
        MerchantRepetitionChecker(threshold=2),
        GeographicShiftChecker(threshold=2),
        NighttimeChecker(min_amount=1000),
        UnusualMerchantChecker(multiplier=2.0)
    ]

    for checker in checkers:
        print(f"Running {checker.name}...")
        checker.initialize()
        flags = checker.check_with_repo(repo)
        print(f"  Found {len(flags)} flags")
        for flag in flags:
            print(f"    - {flag.reason}")
        print()

print("✓ All checkers executed successfully")
