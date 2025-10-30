#!/usr/bin/env python3
# Debug script to understand JOIN parsing

import sqlglot
from sqlglot import exp

def debug_join_parsing():
    """Debug JOIN parsing to understand structure"""

    # Test SQL with JOIN but no ON condition
    test_sql_bad_join = """
    SELECT
        a.user_id,
        b.order_id
    FROM dwd_users a
    JOIN dwd_orders b
    WHERE a.dt = '2023-01-01'
    """

    print("Parsing SQL with JOIN but no ON condition:")
    parsed = sqlglot.parse_one(test_sql_bad_join, read='hive')

    # Find all JOIN expressions
    joins = list(parsed.find_all(exp.Join))
    print(f"Found {len(joins)} JOIN expressions")

    for i, join in enumerate(joins):
        print(f"JOIN {i+1}: {join}")
        print(f"  Type of join.on: {type(join.on)}")
        print(f"  join.on value: {join.on}")
        if hasattr(join.on, 'this'):
            print(f"  join.on.this: {join.on.this}")
            print(f"  Type of join.on.this: {type(join.on.this)}")

        # Check if it's a Boolean with True value
        if isinstance(join.on, exp.Boolean) and join.on.this is True:
            print("  This is a trivial ON TRUE condition")
        else:
            print("  This is NOT a trivial ON TRUE condition")

    print("\n" + "="*50 + "\n")

    # Test SQL with CROSS JOIN (which should be flagged)
    test_sql_cross_join = """
    SELECT
        a.user_id,
        b.order_id
    FROM dwd_users a
    CROSS JOIN dwd_orders b
    WHERE a.dt = '2023-01-01'
    """

    print("Parsing SQL with CROSS JOIN:")
    try:
        parsed = sqlglot.parse_one(test_sql_cross_join, read='hive')

        # Find all JOIN expressions
        joins = list(parsed.find_all(exp.Join))
        print(f"Found {len(joins)} JOIN expressions")

        for i, join in enumerate(joins):
            print(f"JOIN {i+1}: {join}")
            print(f"  Type of join.on: {type(join.on)}")
            print(f"  join.on value: {join.on}")
            if hasattr(join.on, 'this'):
                print(f"  join.on.this: {join.on.this}")
                print(f"  Type of join.on.this: {type(join.on.this)}")
    except Exception as e:
        print(f"Error parsing CROSS JOIN: {e}")

if __name__ == "__main__":
    debug_join_parsing()