import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

"""Test nested queries combining WHERE and HAVING clauses."""
from checker import AggregatePredicate, FieldPredicate, AndPredicate
from duckdb_repository import DuckDBRepository

# Complex example: Users with multiple high-value transactions
# WHERE amount > 100 GROUP BY user_id HAVING COUNT(*) > 1

where_pred = FieldPredicate('amount', '>', 100)
having_pred = AggregatePredicate(
    group_by=['user_id'],
    aggregate_expr='COUNT(*)',
    operator='>',
    threshold=1
)

print("Complex Query: Users with multiple transactions over $100")
print(f"WHERE: {where_pred.to_sql()}")
print(f"GROUP BY: {', '.join(having_pred.get_group_by_fields())}")
print(f"HAVING: {having_pred.to_sql()}")

with DuckDBRepository() as repo:
    repo.insert_from_csv('sample_transactions.csv', 'transactions')

    query = f"""
    SELECT {', '.join(having_pred.group_by)},
           COUNT(*) as high_value_count,
           SUM(amount) as total_high_value
    FROM transactions
    WHERE {where_pred.to_sql()}
    GROUP BY {', '.join(having_pred.group_by)}
    HAVING {having_pred.to_sql()}
    """
    print(f"\nFull query:\n{query}\n")

    results = repo.fetch_items(query)
    print(f"Results: {len(results)} users with multiple high-value transactions")
    for row in results:
        print(f"  {row['user_id']}: {row['high_value_count']} transactions, ${row['total_high_value']:.2f} total")
