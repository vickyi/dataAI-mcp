#!/usr/bin/env python3
# Test script to verify different DDL statements

import asyncio
import sys
import os

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import lint_sql

async def test_ddl_statements():
    """Test different DDL statements"""
    print("Testing different DDL statements...")

    # Test DROP TABLE statement
    test_drop_sql = """
    DROP TABLE user_info
    """

    print("Testing DROP TABLE statement:")
    result = await lint_sql(test_drop_sql)
    print(f"Lint result: {result}")

    # Test ALTER TABLE statement
    test_alter_sql = """
    ALTER TABLE user_info ADD COLUMNS (age INT)
    """

    print("\nTesting ALTER TABLE statement:")
    result = await lint_sql(test_alter_sql)
    print(f"Lint result: {result}")

if __name__ == "__main__":
    try:
        asyncio.run(test_ddl_statements())
    except Exception as e:
        print(f"Error during test: {e}")
        sys.exit(1)