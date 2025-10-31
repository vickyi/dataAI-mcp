#!/usr/bin/env python3
# Test script to verify rule configuration via TOML

import asyncio
import sys
import os
import shutil

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Backup original config and create a test config with disabled rules
original_config = os.path.join(os.path.dirname(__file__), 'sql_rules.toml')
backup_config = os.path.join(os.path.dirname(__file__), 'sql_rules.toml.backup')

# Backup original config if it exists
if os.path.exists(original_config):
    shutil.copy2(original_config, backup_config)

# Create a test config with some rules disabled
test_config_content = '''
# SQL规则配置文件
# 使用TOML格式定义SQL检查规则

[general]
# 通用配置
sql_dialect = "hive"
enabled = true

[rules.select_star]
# 禁止使用 SELECT *
id = "R001"
enabled = false  # Disable this rule
level = "error"
description = "禁止使用 SELECT *，请明确列出所需字段"
message = "禁止使用 SELECT *，请明确列出所需字段"
exclude_functions = ["COUNT"]

[rules.partition_filter]
# 检查分区字段过滤
id = "R101"
enabled = false  # Disable this rule
level = "error"
description = "查询必须包含分区字段过滤条件，以避免全表扫描"
partition_fields = ["dt", "date"]
require_where_clause = true

[rules.table_alias]
# 检查表别名
id = "R002"
enabled = true
level = "warning"
description = "建议为表使用别名"

[rules.sensitive_columns]
# 检查敏感字段
id = "R301"
enabled = true
level = "error"
description = "查询中包含敏感字段，请确认是否有权限访问并已进行脱敏处理"
sensitive_keywords = [
    "phone",
    "email",
    "id_card",
    "password",
    "credit_card"
]

[rules.field_alias_naming]
# 检查字段别名命名规范
id = "R201"
enabled = true
level = "warning"
description = "字段别名建议使用下划线命名法"
naming_pattern = "^[a-z]+(_[a-z]+)*$"
invalid_patterns = ["^[a-z]+[A-Z][a-z]*"]
'''

# Write the test config
with open(original_config, 'w', encoding='utf-8') as f:
    f.write(test_config_content)

# Now import and reload the server module to use new config
import importlib
if 'server' in sys.modules:
    importlib.reload(sys.modules['server'])
from server import lint_sql

async def test_config_rules():
    """Test rule configuration via TOML"""
    print("Testing rule configuration via TOML...")

    # Test query statement - should only apply enabled rules
    test_query_sql = """
    SELECT * FROM dwd_users WHERE dt = '2023-01-01'
    """

    print("Testing query statement with disabled SELECT * and partition rules:")
    result = await lint_sql(test_query_sql)
    print(f"Lint result: {result}")

    # Restore original config
    if os.path.exists(backup_config):
        shutil.move(backup_config, original_config)
    elif os.path.exists(original_config):
        os.remove(original_config)

if __name__ == "__main__":
    try:
        asyncio.run(test_config_rules())
    except Exception as e:
        # Restore original config even if test fails
        if os.path.exists(backup_config):
            shutil.move(backup_config, original_config)
        elif os.path.exists(original_config):
            os.remove(original_config)
        print(f"Error during test: {e}")
        sys.exit(1)