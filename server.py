from mcp.server import FastMCP
import sqlglot
from sqlglot import exp
import re
import toml
import os
from typing import List, Dict, Any

# Create FastMCP instance
app = FastMCP("sql-linter-mcp-server")

# Load rules from TOML configuration
def load_rules_config() -> Dict[str, Any]:
    """Load SQL rules configuration from TOML file"""
    config_path = os.path.join(os.path.dirname(__file__), 'sql_rules.toml')
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            return toml.load(f)
    else:
        # Return default configuration if file doesn't exist
        return {
            "rules": {
                "select_star": {"enabled": True, "level": "error", "exclude_functions": ["COUNT"]},
                "partition_filter": {"enabled": True, "level": "error", "partition_fields": ["dt", "date"]},
                "table_alias": {"enabled": True, "level": "warning"},
                "sensitive_columns": {"enabled": True, "level": "error", "sensitive_keywords": [
                    "phone", "email", "id_card", "password", "credit_card"]},
                "field_alias_naming": {"enabled": True, "level": "warning"}
            }
        }

# Load the rules configuration
RULES_CONFIG = load_rules_config()

@app.tool()
async def lint_sql(sql_string: str) -> str:
    """
    对输入的SQL字符串进行规范检查，返回检查结果。

    Args:
        sql_string: 需要检查的SQL语句

    Returns:
        包含所有检查问题和建议的格式化字符串
    """
    try:
        # 1. 使用sqlglot解析SQL
        # Get SQL dialect from config, default to hive
        dialect = RULES_CONFIG.get("general", {}).get("sql_dialect", "hive")
        parsed_sql = sqlglot.parse_one(sql_string, read=dialect)
    except Exception as e:
        return f"SQL解析失败: {str(e)}"

    # 2. Determine if this is a DDL statement
    is_ddl = isinstance(parsed_sql, (exp.Create, exp.Drop, exp.Alter, exp.TruncateTable))

    # 3. Initialize rules based on statement type and configuration
    rules = []

    if is_ddl:
        # For DDL statements, apply DDL-specific rules
        rules.append(_check_ddl_rules)
    else:
        # For query statements, apply configured query rules
        if RULES_CONFIG["rules"].get("select_star", {}).get("enabled", True):
            rules.append(_check_select_star)
        if RULES_CONFIG["rules"].get("partition_filter", {}).get("enabled", True):
            rules.append(_check_partition_filter)
        if RULES_CONFIG["rules"].get("table_alias", {}).get("enabled", True):
            rules.append(_check_table_alias)
        if RULES_CONFIG["rules"].get("sensitive_columns", {}).get("enabled", True):
            rules.append(_check_sensitive_columns)
        if RULES_CONFIG["rules"].get("field_alias_naming", {}).get("enabled", True):
            rules.append(_check_field_alias_naming)

    issues = []

    # 4. 应用所有规则进行检查
    for rule_func in rules:
        rule_issues = rule_func(parsed_sql, sql_string)
        issues.extend(rule_issues)

    # 5. 格式化输出结果
    if not issues:
        return "✅ SQL符合所有规范！"
    else:
        result = ["SQL规范检查报告:"]
        for i, issue in enumerate(issues, 1):
            result.append(f"{i}. {issue}")
        return "\n".join(result)

def _check_select_star(parsed_sql, original_sql):
    """检查是否使用 SELECT * """
    issues = []
    # Get configuration for this rule
    rule_config = RULES_CONFIG["rules"].get("select_star", {})
    exclude_functions = rule_config.get("exclude_functions", ["COUNT"])

    for select in parsed_sql.find_all(exp.Star):
        # 排除 COUNT(*) 的情况
        if not any(isinstance(select.parent, getattr(exp, func, type(None))) for func in exclude_functions):
            level = rule_config.get("level", "error")
            message = rule_config.get("message", "禁止使用 SELECT *，请明确列出所需字段。")
            issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R001')}] {message}")
            break
    return issues

def _check_partition_filter(parsed_sql, original_sql):
    """检查是否包含分区字段过滤"""
    issues = []
    # Get configuration for this rule
    rule_config = RULES_CONFIG["rules"].get("partition_filter", {})
    partition_fields = rule_config.get("partition_fields", ["dt", "date"])
    require_where_clause = rule_config.get("require_where_clause", True)

    where_clause = parsed_sql.find(exp.Where)
    if where_clause:
        where_str = where_clause.sql().lower()
        # 检查是否存在对分区字段的过滤
        if not any(field in where_str for field in partition_fields):
            level = rule_config.get("level", "error")
            message = rule_config.get("description", "查询必须包含分区字段过滤条件，以避免全表扫描。")
            issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R101')}] {message}")
    elif require_where_clause:
        level = rule_config.get("level", "error")
        message = rule_config.get("description", "查询缺少 WHERE 子句，必须包含分区字段过滤。")
        issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R101')}] {message}")
    return issues

def _check_table_alias(parsed_sql, original_sql):
    """检查表是否使用了别名"""
    issues = []
    # Get configuration for this rule
    rule_config = RULES_CONFIG["rules"].get("table_alias", {})

    for table in parsed_sql.find_all(exp.Table):
        if not table.alias:
            level = rule_config.get("level", "warning")
            message = rule_config.get("description", f"建议为表 '{table.name}' 使用别名。")
            issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R002')}] {message}")
    return issues

def _check_sensitive_columns(parsed_sql, original_sql):
    """检查敏感字段"""
    issues = []
    # Get configuration for this rule
    rule_config = RULES_CONFIG["rules"].get("sensitive_columns", {})
    sensitive_keywords = rule_config.get("sensitive_keywords", [
        'phone', 'email', 'id_card', 'password', 'credit_card'
    ])

    for column in parsed_sql.find_all(exp.Column):
        col_name = column.sql().lower()
        for keyword in sensitive_keywords:
            if keyword in col_name:
                level = rule_config.get("level", "error")
                message = rule_config.get("description", f"查询中包含敏感字段 '{column.sql()}'，请确认是否有权限访问并已进行脱敏处理。")
                issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R301')}] {message}")
                break
    return issues

def _check_field_alias_naming(parsed_sql, original_sql):
    """检查字段别名命名规范"""
    issues = []
    # Get configuration for this rule
    rule_config = RULES_CONFIG["rules"].get("field_alias_naming", {})

    aliases = parsed_sql.find_all(exp.Alias)
    for alias in aliases:
        alias_name = alias.alias
        # 检查是否是驼峰命名，应改为下划线
        if re.match(r'^[a-z]+[A-Z][a-z]*', alias_name):
            level = rule_config.get("level", "warning")
            snake_name = re.sub(r'(?<!^)(?=[A-Z])', '_', alias_name).lower()
            message = rule_config.get("description", f"字段别名 '{alias_name}' 建议改为下划线形式 '{snake_name}'。")
            issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R201')}] {message}")
    return issues

def _check_ddl_rules(parsed_sql, original_sql):
    """检查DDL语句的规则"""
    issues = []

    # Get configuration for DDL rules
    hive_external_rule = RULES_CONFIG["rules"].get("hive_external_table", {})
    hive_keyword_rule = RULES_CONFIG["rules"].get("hive_ddl_keywords", {})
    hive_alignment_rule = RULES_CONFIG["rules"].get("hive_ddl_alignment", {})

    # Check for EXTERNAL keyword in CREATE TABLE statements
    if isinstance(parsed_sql, exp.Create) and parsed_sql.kind == "TABLE":
        if hive_external_rule.get("enabled", True) and "EXTERNAL" not in original_sql.upper():
            level = hive_external_rule.get("level", "error")
            message = hive_external_rule.get("description", "Hive建表语句应使用EXTERNAL关键字创建外表")
            issues.append(f"[{level.capitalize()}-{hive_external_rule.get('id', 'R703')}] {message}")

    # Check for uppercase keywords in DDL statements - using only config values, no hard-coded defaults
    if hive_keyword_rule.get("enabled", False):  # Default to False if not specified
        ddl_keywords = hive_keyword_rule.get("keywords", [])  # Empty list if not specified

        # Only check if keywords are defined in config
        if ddl_keywords:
            for keyword in ddl_keywords:
                # Look for uppercase versions of the keywords
                pattern = r'\b' + keyword + r'\b'
                if re.search(pattern, original_sql):
                    level = hive_keyword_rule.get("level", "warning")
                    message = hive_keyword_rule.get("description", f"Hive DDL关键字 '{keyword}' 应使用小写")
                    issues.append(f"[{level.capitalize()}-{hive_keyword_rule.get('id', 'R701')}] {message}")

    # Check for proper alignment in DDL statements - using only config values, no hard-coded defaults
    if hive_alignment_rule.get("enabled", False):  # Default to False if not specified
        alignment_spaces = hive_alignment_rule.get("alignment_spaces", 0)  # Default to 0 if not specified

        # Only check if alignment_spaces is defined and greater than 0
        if alignment_spaces > 0:
            lines = original_sql.split('\n')
            for line in lines:
                # Check if line starts with a keyword that should be aligned
                if line.strip().upper().startswith(('PARTITIONED', 'STORED', 'LOCATION', 'TBLPROPERTIES')):
                    # Check if it's properly indented
                    leading_spaces = len(line) - len(line.lstrip(' '))
                    if leading_spaces != alignment_spaces:
                        level = hive_alignment_rule.get("level", "warning")
                        message = hive_alignment_rule.get("description", f"Hive DDL关键字应对齐，使用{alignment_spaces}个空格缩进")
                        issues.append(f"[{level.capitalize()}-{hive_alignment_rule.get('id', 'R702')}] {message}")
                        break

    return issues

def _camel_to_snake(name):
    """辅助函数：驼峰转下划线"""
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

if __name__ == "__main__":
    # 使用标准输入输出运行服务器，这是MCP协议的要求
    app.run()