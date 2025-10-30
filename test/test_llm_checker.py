import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from checker import LLMChecker, Transaction

try:
    from api_config import ANTHROPIC_API_KEY
    api_key = ANTHROPIC_API_KEY
except ImportError:
    print("⚠️  Create api_config.py with ANTHROPIC_API_KEY to test LLM checker")
    print("Example: echo 'ANTHROPIC_API_KEY = \"your-key-here\"' > api_config.py")
    sys.exit(0)

print("Testing LLM-based fraud detection\n")

test_transactions = [
    Transaction("user_001", "2025-10-29 10:00:00", "Starbucks", 5.0),
    Transaction("user_001", "2025-10-29 10:02:00", "Shell", 50.0),
    Transaction("user_001", "2025-10-29 10:05:00", "Target", 75.0),
    Transaction("user_001", "2025-10-29 10:08:00", "BestBuy", 2000.0),
    Transaction("user_002", "2025-10-29 03:00:00", "Casino", 5000.0),
    Transaction("user_002", "2025-10-29 15:00:00", "Grocery", 50.0),
    Transaction("user_003", "2025-10-29 11:00:00", "Restaurant", 100.0),
]

checker = LLMChecker(api_key=api_key)
checker.initialize()

print(f"Sending {len(test_transactions)} transactions to Claude...\n")

flags = checker.check(test_transactions)

print(f"LLM found {len(flags)} fraud patterns:\n")

for flag in flags:
    print(f"Checker: {flag.checker_name}")
    print(f"Reason: {flag.reason}")
    print(f"Confidence: {flag.confidence_score}")
    print(f"Flagged transactions ({flag.transaction_count}):")
    for txn in flag.transactions:
        print(f"  - {txn.user_id} | {txn.timestamp} | {txn.merchant_name} | ${txn.amount}")
    print()

print("✓ LLM checker test complete")
