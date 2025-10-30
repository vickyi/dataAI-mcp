#!/usr/bin/env python3
# Debug script to understand SQL parsing

import sqlglot
from sqlglot import exp

def debug_sql_parsing():
    """Debug SQL parsing to understand JOIN structure"""

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
    print(f"Parsed SQL: {parsed}")

    # Find all JOIN expressions
    joins = list(parsed.find_all(exp.Join))
    print(f"Found {len(joins)} JOIN expressions")

    for i, join in enumerate(joins):
        print(f"JOIN {i+1}: {join}")
        print(f"  Has ON condition: {join.on is not None}")
        if join.on:
            print(f"  ON condition: {join.on}")
        else:
            print("  No ON condition found")

    print("\n" + "="*50 + "\n")

    # Test SQL with proper JOIN condition
    test_sql_good_join = """
    SELECT
        a.user_id,
        b.order_id
    FROM dwd_users a
    JOIN dwd_orders b ON a.user_id = b.user_id
    WHERE a.dt = '2023-01-01'
    """

    print("Parsing SQL with proper JOIN condition:")
    parsed = sqlglot.parse_one(test_sql_good_join, read='hive')
    print(f"Parsed SQL: {parsed}")

    # Find all JOIN expressions
    joins = list(parsed.find_all(exp.Join))
    print(f"Found {len(joins)} JOIN expressions")

    for i, join in enumerate(joins):
        print(f"JOIN {i+1}: {join}")
        print(f"  Has ON condition: {join.on is not None}")
        if join.on:
            print(f"  ON condition: {join.on}")
        else:
            print("  No ON condition found")

if __name__ == "__main__":
    debug_sql_parsing()