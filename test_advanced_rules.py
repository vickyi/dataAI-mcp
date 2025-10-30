#!/usr/bin/env python3
# Test script to verify advanced rule checking

import asyncio
import sys
import os
import shutil

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Backup original config and use test config
original_config = os.path.join(os.path.dirname(__file__), 'sql_rules.toml')
test_config = os.path.join(os.path.dirname(__file__), 'test_rules.toml')
backup_config = os.path.join(os.path.dirname(__file__), 'sql_rules.toml.backup')

# Backup original config if it exists
if os.path.exists(original_config):
    shutil.copy2(original_config, backup_config)

# Copy test config to main config location
shutil.copy2(test_config, original_config)

# Now import and reload the server module to use new config
import importlib
if 'server' in sys.modules:
    importlib.reload(sys.modules['server'])
from server import lint_sql

async def test_advanced_rules():
    """Test advanced rule checking"""
    print("Testing advanced rule checking with enabled rules...")

    # Test SQL with JOIN but no ON condition (should trigger join_conditions rule)
    test_sql_join = """
    SELECT
        a.user_id,
        b.order_id
    FROM dwd_users a
    JOIN dwd_orders b
    WHERE a.dt = '2023-01-01'
    """

    print("Testing JOIN condition rule...")
    result = await lint_sql(test_sql_join)
    print(f"Test SQL: {test_sql_join}")
    print(f"Lint result: {result}")

    # Test SQL with multiple JOINs (should trigger query_complexity rule when enabled)
    test_sql_complex = """
    SELECT
        a.user_id,
        b.order_id,
        c.product_name,
        d.category_name,
        e.payment_method,
        f.shipping_address
    FROM dwd_users a
    JOIN dwd_orders b ON a.user_id = b.user_id
    JOIN dwd_products c ON b.product_id = c.product_id
    JOIN dwd_categories d ON c.category_id = d.category_id
    JOIN dwd_payments e ON b.payment_id = e.payment_id
    JOIN dwd_addresses f ON b.address_id = f.address_id
    JOIN dwd_profiles g ON a.user_id = g.user_id
    WHERE a.dt = '2023-01-01'
    """

    print("\nTesting query complexity rule...")
    result = await lint_sql(test_sql_complex)
    print(f"Test SQL: {test_sql_complex}")
    print(f"Lint result: {result}")

    # Restore original config
    if os.path.exists(backup_config):
        shutil.move(backup_config, original_config)
    elif os.path.exists(original_config):
        os.remove(original_config)

if __name__ == "__main__":
    try:
        asyncio.run(test_advanced_rules())
    except Exception as e:
        # Restore original config even if test fails
        if os.path.exists(backup_config):
            shutil.move(backup_config, original_config)
        elif os.path.exists(original_config):
            os.remove(original_config)
        print(f"Error during test: {e}")
        sys.exit(1)