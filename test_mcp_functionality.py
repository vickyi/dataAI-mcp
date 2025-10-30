#!/usr/bin/env python3
# Test script to verify MCP functionality works correctly

import asyncio
import sys
import os

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import lint_sql

async def test_mcp_functionality():
    """Test that the MCP lint_sql function works correctly"""
    print("Testing MCP functionality...")

    # Test SQL that should pass all checks (avoiding sensitive fields)
    test_sql = """
    SELECT
        user_id,
        username,
        created_date
    FROM dwd_users a
    WHERE dt = '2023-01-01'
    """

    result = await lint_sql(test_sql)
    print(f"Test SQL: {test_sql}")
    print(f"Lint result: {result}")

    # Check if the result is as expected
    if "符合所有规范" in result:
        print("✅ MCP functionality test PASSED")
        return True
    else:
        print("❌ MCP functionality test FAILED")
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(test_mcp_functionality())
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"Error during test: {e}")
        sys.exit(1)