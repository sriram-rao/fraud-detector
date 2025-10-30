"""Main entry point for fraud detection system."""
import sys
from pathlib import Path
from typing import List
from execution_engine import ExecutionEngine
from duckdb_repository import DuckDBRepository
from checker import Transaction, FraudFlag


def load_transactions_from_csv(csv_path: str, repo: DuckDBRepository) -> List[Transaction]:
    """Load transactions from CSV into DuckDB and return as Transaction objects."""
    row_count = repo.insert_from_csv(csv_path, 'transactions')
    print(f"Loaded {row_count} transactions from {csv_path}")

    results = repo.fetch_items("SELECT * FROM transactions ORDER BY user_id, timestamp")

    transactions = [
        Transaction(
            user_id=row['user_id'],
            timestamp=str(row['timestamp']),
            merchant_name=row['merchant_name'],
            amount=float(row['amount'])
        )
        for row in results
    ]

    return transactions


def write_results(flags: List[FraudFlag], output_path: str) -> None:
    """Write fraud flags to output file."""
    with open(output_path, 'w') as f:
        f.write("Fraud Detection Results\n")
        f.write("=" * 80 + "\n\n")

        if not flags:
            f.write("No suspicious transactions found.\n")
            return

        f.write(f"Total fraud patterns detected: {len(flags)}\n\n")

        for i, flag in enumerate(flags, 1):
            f.write(f"Pattern #{i}\n")
            f.write(f"  Checker: {flag.checker_name}\n")
            f.write(f"  Reason: {flag.reason}\n")
            f.write(f"  Confidence: {flag.confidence_score:.2f}\n")
            f.write(f"  Transactions ({flag.transaction_count}):\n")

            for txn in flag.transactions:
                f.write(f"    - {txn.user_id} | {txn.timestamp} | {txn.merchant_name} | ${txn.amount:.2f}\n")

            f.write("\n")

    print(f"Results written to {output_path}")


def main(csv_path: str, output_path: str = "fraud_results.txt") -> None:
    """Main fraud detection pipeline."""
    engine = ExecutionEngine()

    engine.configure_checkers()

    with DuckDBRepository() as repo:
        transactions = load_transactions_from_csv(csv_path, repo)

        print(f"\nRunning fraud detection on {len(transactions)} transactions...\n")

        flags = engine.execute(transactions)

        print(f"\nFound {len(flags)} fraud patterns\n")

    write_results(flags, output_path)

    engine.shutdown()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <csv_file> [output_file]")
        print("Example: python main.py sample_transactions.csv fraud_results.txt")
        sys.exit(1)

    csv_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "fraud_results.txt"

    if not Path(csv_file).exists():
        print(f"Error: File not found: {csv_file}")
        sys.exit(1)

    main(csv_file, output_file)
