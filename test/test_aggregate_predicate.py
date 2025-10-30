import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

"""Test aggregate predicates with GROUP BY."""
from checker import AggregatePredicate, FieldPredicate, AndPredicate
from duckdb_repository import DuckDBRepository

# Example 1: Users with more than 1 transaction
velocity_pred = AggregatePredicate(
    group_by=['user_id'],
    aggregate_expr='COUNT(*)',
    operator='>',
    threshold=1
)

print("Example 1: Users with multiple transactions")
print(f"GROUP BY: {velocity_pred.get_group_by_fields()}")
print(f"HAVING: {velocity_pred.to_sql()}")

# Build full query manually for now
with DuckDBRepository() as repo:
    repo.insert_from_csv('sample_transactions.csv', 'transactions')

    query = f"""
    SELECT {', '.join(velocity_pred.group_by)}, COUNT(*) as txn_count
    FROM transactions
    GROUP BY {', '.join(velocity_pred.group_by)}
    HAVING {velocity_pred.to_sql()}
    """
    print(f"Full query: {query}")

    results = repo.fetch_items(query)
    print(f"Results: {len(results)} users")
    for row in results:
        print(f"  {row}")
    print()

# Example 2: Users spending > $200 total
high_spender_pred = AggregatePredicate(
    group_by=['user_id'],
    aggregate_expr='SUM(amount)',
    operator='>',
    threshold=200
)

print("Example 2: High spenders")
print(f"GROUP BY: {high_spender_pred.get_group_by_fields()}")
print(f"HAVING: {high_spender_pred.to_sql()}")

with DuckDBRepository() as repo:
    repo.insert_from_csv('sample_transactions.csv', 'transactions')

    query = f"""
    SELECT {', '.join(high_spender_pred.group_by)}, SUM(amount) as total_spent
    FROM transactions
    GROUP BY {', '.join(high_spender_pred.group_by)}
    HAVING {high_spender_pred.to_sql()}
    """
    print(f"Full query: {query}")

    results = repo.fetch_items(query)
    print(f"Results: {len(results)} users")
    for row in results:
        print(f"  {row}")
    print()

# Example 3: Multiple merchants per user (fraud indicator)
multi_merchant_pred = AggregatePredicate(
    group_by=['user_id'],
    aggregate_expr='COUNT(DISTINCT merchant_name)',
    operator='>=',
    threshold=2
)

print("Example 3: Users with multiple merchants")
print(f"GROUP BY: {multi_merchant_pred.get_group_by_fields()}")
print(f"HAVING: {multi_merchant_pred.to_sql()}")
