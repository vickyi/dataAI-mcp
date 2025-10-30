#!/usr/bin/env python3
# Test script to verify Hive DDL rules

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
    print(f"  - hive_ddl_keywords enabled: {config['rules'].get('hive_ddl_keywords', {}).get('enabled', False)}")
    print(f"  - hive_ddl_alignment enabled: {config['rules'].get('hive_ddl_alignment', {}).get('enabled', False)}")
    print(f"  - hive_external_table enabled: {config['rules'].get('hive_external_table', {}).get('enabled', False)}")

async def test_hive_ddl_rules():
    """Test Hive DDL rules"""
    print("Testing Hive DDL rules...")

    # Test SQL with improper Hive DDL (uppercase keywords, no EXTERNAL)
    test_sql_bad_ddl = """
    CREATE TABLE user_info (
        user_id BIGINT COMMENT '用户ID',
        username STRING COMMENT '用户名',
        email STRING COMMENT '邮箱'
    )
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION '/data/user_info'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """

    print("Testing SQL with improper Hive DDL:")
    result = await lint_sql(test_sql_bad_ddl)
    print(f"Lint result: {result}")

    # Test SQL with proper Hive DDL (lowercase keywords, EXTERNAL)
    test_sql_good_ddl = """
    CREATE EXTERNAL TABLE user_info (
        user_id BIGINT COMMENT '用户ID',
        username STRING COMMENT '用户名',
        email STRING COMMENT '邮箱'
    )
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION '/data/user_info'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """

    print("\nTesting SQL with proper Hive DDL:")
    result = await lint_sql(test_sql_good_ddl)
    print(f"Lint result: {result}")

    # Restore original config
    if os.path.exists(backup_config):
        shutil.move(backup_config, original_config)
    elif os.path.exists(original_config):
        os.remove(original_config)

if __name__ == "__main__":
    try:
        test_config_loading()
        asyncio.run(test_hive_ddl_rules())
    except Exception as e:
        # Restore original config even if test fails
        if os.path.exists(backup_config):
            shutil.move(backup_config, original_config)
        elif os.path.exists(original_config):
            os.remove(original_config)
        print(f"Error during test: {e}")
        sys.exit(1)