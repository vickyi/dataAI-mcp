#!/usr/bin/env python3
# Test script to verify DDL statement handling

import asyncio
import sys
import os

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import lint_sql

async def test_ddl_handling():
    """Test DDL statement handling"""
    print("Testing DDL statement handling...")

    # Test DDL statement - should only apply DDL rules
    test_ddl_sql = """
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

    print("Testing DDL statement (should skip SELECT/JOIN rules):")
    result = await lint_sql(test_ddl_sql)
    print(f"Lint result: {result}")

    # Test query statement - should apply all query rules
    test_query_sql = """
    SELECT
        user_id,
        username,
        email
    FROM dwd_users
    WHERE dt = '2023-01-01'
    """

    print("\nTesting query statement (should apply all query rules):")
    result = await lint_sql(test_query_sql)
    print(f"Lint result: {result}")

    # Test proper DDL statement with EXTERNAL
    test_proper_ddl_sql = """
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

    print("\nTesting proper DDL statement with EXTERNAL (should pass DDL rules):")
    result = await lint_sql(test_proper_ddl_sql)
    print(f"Lint result: {result}")

if __name__ == "__main__":
    try:
        asyncio.run(test_ddl_handling())
    except Exception as e:
        print(f"Error during test: {e}")
        sys.exit(1)