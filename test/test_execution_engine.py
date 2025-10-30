import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from execution_engine import ExecutionEngine
from checker import Transaction

engine = ExecutionEngine()

engine.configure_checkers()

test_transactions = [
    Transaction("user_001", "2025-10-29 08:15:23", "Starbucks", 5.75),
    Transaction("user_002", "2025-10-29 09:22:11", "Amazon", 129.99),
    Transaction("user_003", "2025-10-29 10:05:47", "Shell Gas Station", 45.00),
]

print(f"\nTesting with {len(test_transactions)} transactions\n")

flags = engine.execute(test_transactions)

print(f"\n{len(flags)} fraud flags found:")
for flag in flags:
    print(f"  - {flag.checker_name}: {len(flag.transactions)} transactions")
    print(f"    Reason: {flag.reason}")

engine.shutdown()
