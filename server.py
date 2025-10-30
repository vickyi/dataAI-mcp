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
    # Initialize rules based on configuration
    rules = []

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
    if RULES_CONFIG["rules"].get("join_conditions", {}).get("enabled", False):
        rules.append(_check_join_conditions)
    if RULES_CONFIG["rules"].get("query_complexity", {}).get("enabled", False):
        rules.append(_check_query_complexity)
    if RULES_CONFIG["rules"].get("hive_ddl_keywords", {}).get("enabled", True):
        rules.append(_check_hive_ddl_keywords)
    if RULES_CONFIG["rules"].get("hive_ddl_alignment", {}).get("enabled", True):
        rules.append(_check_hive_ddl_alignment)
    if RULES_CONFIG["rules"].get("hive_external_table", {}).get("enabled", True):
        rules.append(_check_hive_external_table)

    try:
        # 1. 使用sqlglot解析SQL
        # Get SQL dialect from config, default to hive
        dialect = RULES_CONFIG.get("general", {}).get("sql_dialect", "hive")
        parsed_sql = sqlglot.parse_one(sql_string, read=dialect)
    except Exception as e:
        return f"SQL解析失败: {str(e)}"

    issues = []

    # 2. 应用所有规则进行检查
    for rule_func in rules:
        rule_issues = rule_func(parsed_sql, sql_string)
        issues.extend(rule_issues)

    # 3. 格式化输出结果
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

def _check_join_conditions(parsed_sql, original_sql):
    """检查JOIN条件"""
    issues = []
    # Get configuration for this rule
    rule_config = RULES_CONFIG["rules"].get("join_conditions", {})

    # Find all JOIN expressions
    joins = list(parsed_sql.find_all(exp.Join))
    for join in joins:
        # Check if join is a CROSS JOIN or has no ON condition
        # For CROSS JOIN, the kind attribute will be 'CROSS'
        if hasattr(join, 'kind') and join.kind == 'CROSS':
            level = rule_config.get("level", "warning")
            message = rule_config.get("description", "CROSS JOIN可能导致笛卡尔积，请谨慎使用。")
            issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R401')}] {message}")
        else:
            # For regular JOINs, check if there's an ON condition
            # The on attribute is a method, so we need to call it
            on_condition = join.on()
            # If there's no ON condition or it's a trivial condition like TRUE
            if not on_condition or (isinstance(on_condition, exp.Boolean) and on_condition.this is True):
                level = rule_config.get("level", "warning")
                message = rule_config.get("description", "JOIN语句应该包含明确的连接条件，避免笛卡尔积。")
                issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R401')}] {message}")
    return issues

def _check_query_complexity(parsed_sql, original_sql):
    """检查查询复杂度"""
    issues = []
    # Get configuration for this rule
    rule_config = RULES_CONFIG["rules"].get("query_complexity", {})

    max_joins = rule_config.get("max_joins", 5)
    max_subqueries = rule_config.get("max_subqueries", 3)
    max_tables = rule_config.get("max_tables", 10)

    # Count JOINs
    join_count = len(list(parsed_sql.find_all(exp.Join)))
    if join_count > max_joins:
        level = rule_config.get("level", "warning")
        message = rule_config.get("description", f"查询包含 {join_count} 个JOIN，超过最大限制 {max_joins}。")
        issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R501')}] {message}")

    # Count subqueries
    subquery_count = len(list(parsed_sql.find_all(exp.Subquery)))
    if subquery_count > max_subqueries:
        level = rule_config.get("level", "warning")
        message = rule_config.get("description", f"查询包含 {subquery_count} 个子查询，超过最大限制 {max_subqueries}。")
        issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R501')}] {message}")

    # Count tables
    table_count = len(list(parsed_sql.find_all(exp.Table)))
    if table_count > max_tables:
        level = rule_config.get("level", "warning")
        message = rule_config.get("description", f"查询涉及 {table_count} 个表，超过最大限制 {max_tables}。")
        issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R501')}] {message}")

    return issues

def _check_hive_ddl_keywords(parsed_sql, original_sql):
    """检查Hive DDL关键字规范（小写）"""
    issues = []
    # Get configuration for this rule
    rule_config = RULES_CONFIG["rules"].get("hive_ddl_keywords", {})

    # Check if this is a CREATE statement
    if isinstance(parsed_sql, exp.Create):
        # Get the keywords that should be lowercase
        keywords = rule_config.get("keywords", [
            "CREATE", "TABLE", "EXTERNAL", "PARTITIONED", "STORED", "LOCATION",
            "ROW", "FORMAT", "FIELDS", "TERMINATED", "COLLECTION", "ITEMS",
            "KEYS", "LINES", "TBLPROPERTIES"
        ])

        # Check the original SQL for uppercase keywords
        for keyword in keywords:
            # Look for uppercase versions of the keywords that are not properly quoted
            # We use word boundaries to avoid false positives
            pattern = r'\b' + keyword + r'\b'
            if re.search(pattern, original_sql):
                level = rule_config.get("level", "warning")
                message = rule_config.get("description", f"Hive DDL关键字 '{keyword}' 应使用小写")
                issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R701')}] {message}")

    return issues

def _check_hive_ddl_alignment(parsed_sql, original_sql):
    """检查Hive DDL关键字对齐"""
    issues = []
    # Get configuration for this rule
    rule_config = RULES_CONFIG["rules"].get("hive_ddl_alignment", {})

    # Check if this is a CREATE statement
    if isinstance(parsed_sql, exp.Create):
        # Get the required alignment spaces
        alignment_spaces = rule_config.get("alignment_spaces", 4)

        # Simple check for alignment by looking at common patterns
        lines = original_sql.split('\n')
        for line in lines:
            # Check if line starts with a keyword that should be aligned
            if line.strip().upper().startswith(('PARTITIONED', 'STORED', 'LOCATION', 'TBLPROPERTIES')):
                # Check if it's properly indented
                leading_spaces = len(line) - len(line.lstrip(' '))
                if leading_spaces != alignment_spaces:
                    level = rule_config.get("level", "warning")
                    message = rule_config.get("description", f"Hive DDL关键字应对齐，使用{alignment_spaces}个空格缩进")
                    issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R702')}] {message}")
                    break

    return issues

def _check_hive_external_table(parsed_sql, original_sql):
    """检查Hive建表语句是否使用EXTERNAL关键字"""
    issues = []
    # Get configuration for this rule
    rule_config = RULES_CONFIG["rules"].get("hive_external_table", {})

    # Check if this is a CREATE TABLE statement
    if isinstance(parsed_sql, exp.Create) and parsed_sql.kind == "TABLE":
        # Check if EXTERNAL keyword is present
        if "EXTERNAL" not in original_sql.upper():
            level = rule_config.get("level", "error")
            message = rule_config.get("description", "Hive建表语句应使用EXTERNAL关键字创建外表")
            issues.append(f"[{level.capitalize()}-{rule_config.get('id', 'R703')}] {message}")

    return issues

def _camel_to_snake(name):
    """辅助函数：驼峰转下划线"""
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

if __name__ == "__main__":
    # 使用标准输入输出运行服务器，这是MCP协议的要求
    app.run()