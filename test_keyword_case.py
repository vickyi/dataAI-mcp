#!/usr/bin/env python3
# Test script to verify Hive DDL keyword case rule

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

async def test_keyword_case():
    """Test Hive DDL keyword case rule"""
    print("Testing Hive DDL keyword case rule...")

    # Test SQL with uppercase keywords
    test_sql_uppercase = """
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

    print("Testing SQL with uppercase Hive DDL keywords:")
    result = await lint_sql(test_sql_uppercase)
    print(f"Lint result: {result}")

    # Test SQL with lowercase keywords
    test_sql_lowercase = """
    create external table user_info (
        user_id bigint comment '用户ID',
        username string comment '用户名',
        email string comment '邮箱'
    )
    partitioned by (dt string)
    stored as parquet
    location '/data/user_info'
    tblproperties ('parquet.compression'='SNAPPY')
    """

    print("\nTesting SQL with lowercase Hive DDL keywords:")
    result = await lint_sql(test_sql_lowercase)
    print(f"Lint result: {result}")

    # Restore original config
    if os.path.exists(backup_config):
        shutil.move(backup_config, original_config)
    elif os.path.exists(original_config):
        os.remove(original_config)

if __name__ == "__main__":
    try:
        asyncio.run(test_keyword_case())
    except Exception as e:
        # Restore original config even if test fails
        if os.path.exists(backup_config):
            shutil.move(backup_config, original_config)
        elif os.path.exists(original_config):
            os.remove(original_config)
        print(f"Error during test: {e}")
        sys.exit(1)