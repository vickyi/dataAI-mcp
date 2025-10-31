#!/usr/bin/env python3
# Test script to verify JOIN condition rule

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
from server import lint_sql, load_rules_config

def test_config_loading():
    """Test that the test configuration is loaded correctly"""
    config = load_rules_config()
    print("Test configuration loaded:")
    print(f"  - join_conditions enabled: {config['rules'].get('join_conditions', {}).get('enabled', False)}")
    print(f"  - query_complexity enabled: {config['rules'].get('query_complexity', {}).get('enabled', False)}")

async def test_join_rule():
    """Test JOIN condition rule"""
    print("Testing JOIN condition rule...")

    # Test SQL with CROSS JOIN (should trigger join_conditions rule)
    test_sql_cross_join = """
    SELECT
        a.user_id,
        b.order_id
    FROM dwd_users a
    CROSS JOIN dwd_orders b
    WHERE a.dt = '2023-01-01'
    """

    print("Testing SQL with CROSS JOIN:")
    result = await lint_sql(test_sql_cross_join)
    print(f"Lint result: {result}")

    # Restore original config
    if os.path.exists(backup_config):
        shutil.move(backup_config, original_config)
    elif os.path.exists(original_config):
        os.remove(original_config)

if __name__ == "__main__":
    try:
        test_config_loading()
        asyncio.run(test_join_rule())
    except Exception as e:
        # Restore original config even if test fails
        if os.path.exists(backup_config):
            shutil.move(backup_config, original_config)
        elif os.path.exists(original_config):
            os.remove(original_config)
        print(f"Error during test: {e}")
        sys.exit(1)