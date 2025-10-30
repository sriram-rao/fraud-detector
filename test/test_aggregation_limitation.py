import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

"""Test current limitations with aggregated conditions."""
from duckdb_repository import DuckDBRepository

with DuckDBRepository() as repo:
    repo.insert_from_csv('sample_transactions.csv', 'transactions')

    # Example: Find users with more than 1 transaction (aggregation)
    # This requires GROUP BY and HAVING - can't express with current predicate tree

    query = """
    SELECT user_id, COUNT(*) as txn_count, SUM(amount) as total_amount
    FROM transactions
    GROUP BY user_id
    HAVING COUNT(*) > 1
    """

    results = repo.fetch_items(query)
    print("Users with multiple transactions:")
    for row in results:
        print(f"  {row['user_id']}: {row['txn_count']} transactions, total ${row['total_amount']:.2f}")

    print("\n" + "="*60)

    # Example: Find transactions where amount > user's average (window function/subquery)
    query2 = """
    WITH user_avg AS (
        SELECT user_id, AVG(amount) as avg_amount
        FROM transactions
        GROUP BY user_id
    )
    SELECT t.*, ua.avg_amount
    FROM transactions t
    JOIN user_avg ua ON t.user_id = ua.user_id
    WHERE t.amount > ua.avg_amount * 2
    """

    results2 = repo.fetch_items(query2)
    print("\nTransactions > 2x user's average:")
    for row in results2:
        print(f"  {row['user_id']}: ${row['amount']:.2f} (avg: ${row['avg_amount']:.2f})")
