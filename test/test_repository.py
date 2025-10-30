import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from duckdb_repository import DuckDBRepository

with DuckDBRepository() as repo:
    rows_inserted = repo.insert_from_csv('sample_transactions.csv', 'transactions')
    print(f"Inserted {rows_inserted} rows")

    all_transactions = repo.fetch_items("SELECT * FROM transactions")
    print(f"\nAll transactions:")
    for txn in all_transactions:
        print(f"  {txn}")

    high_value = repo.fetch_items("SELECT * FROM transactions WHERE amount > 100")
    print(f"\nHigh value transactions (>{100}):")
    for txn in high_value:
        print(f"  User: {txn['user_id']}, Merchant: {txn['merchant_name']}, Amount: ${txn['amount']}")

    total = repo.fetch_items("SELECT COUNT(*) as count, SUM(amount) as total FROM transactions")
    print(f"\nSummary: {total[0]['count']} transactions, Total: ${total[0]['total']:.2f}")
