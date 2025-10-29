import mcp
import sqlglot
from sqlglot import exp
import re

class SQLintterMCPServer:
    def __init__(self):
        self.rules = [
            self._check_select_star,
            self._check_partition_filter,
            self._check_table_alias,
            self._check_sensitive_columns,
            self._check_field_alias_naming
        ]

    @mcp.tool()
    async def lint_sql(self, sql_string: str) -> str:
        """
        对输入的SQL字符串进行规范检查，返回检查结果。

        Args:
            sql_string: 需要检查的SQL语句

        Returns:
            包含所有检查问题和建议的格式化字符串
        """
        try:
            # 1. 使用sqlglot解析SQL
            parsed_sql = sqlglot.parse_one(sql_string, read='hive') # 假设是Hive SQL
        except Exception as e:
            return f"SQL解析失败: {str(e)}"

        issues = []

        # 2. 应用所有规则进行检查
        for rule_func in self.rules:
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

    def _check_select_star(self, parsed_sql, original_sql):
        """检查是否使用 SELECT * """
        issues = []
        for select in parsed_sql.find_all(exp.Star):
            # 排除 COUNT(*) 的情况
            if not isinstance(select.parent, exp.Count):
                issues.append("[Error-R001] 禁止使用 SELECT *，请明确列出所需字段。")
                break
        return issues

    def _check_partition_filter(self, parsed_sql, original_sql):
        """检查是否包含分区字段过滤"""
        issues = []
        # 这是一个简化版的检查，实际中可能需要更复杂的逻辑
        where_clause = parsed_sql.find(exp.Where)
        if where_clause:
            where_str = where_clause.sql().lower()
            # 检查是否存在对 dt 分区的过滤
            if 'dt' not in where_str and 'date' not in where_str:
                issues.append("[Error-R101] 查询必须包含分区字段 'dt' 的过滤条件，以避免全表扫描。")
        else:
            issues.append("[Error-R101] 查询缺少 WHERE 子句，必须包含分区字段过滤。")
        return issues

    def _check_table_alias(self, parsed_sql, original_sql):
        """检查表是否使用了别名"""
        issues = []
        for table in parsed_sql.find_all(exp.Table):
            if not table.alias:
                issues.append(f"[Warning-R002] 建议为表 '{table.name}' 使用别名。")
        return issues

    def _check_sensitive_columns(self, parsed_sql, original_sql):
        """检查敏感字段"""
        sensitive_keywords = ['phone', 'email', 'id_card', 'password', 'credit_card']
        issues = []
        for column in parsed_sql.find_all(exp.Column):
            col_name = column.sql().lower()
            for keyword in sensitive_keywords:
                if keyword in col_name:
                    issues.append(f"[Error-R301] 查询中包含敏感字段 '{column.sql()}'，请确认是否有权限访问并已进行脱敏处理。")
                    break
        return issues

    def _check_field_alias_naming(self, parsed_sql, original_sql):
        """检查字段别名命名规范"""
        issues = []
        aliases = parsed_sql.find_all(exp.Alias)
        for alias in aliases:
            alias_name = alias.alias
            # 检查是否是驼峰命名，应改为下划线
            if re.match(r'^[a-z]+[A-Z][a-z]*', alias_name):
                issues.append(f"[Warning-R201] 字段别名 '{alias_name}' 建议改为下划线形式 '{self._camel_to_snake(alias_name)}'。")
        return issues

    def _camel_to_snake(self, name):
        """辅助函数：驼峰转下划线"""
        return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

# 创建MCP服务器实例
app = mcp.Server(SQLintterMCPServer())

if __name__ == "__main__":
    # 使用标准输入输出运行服务器，这是MCP协议的要求
    app.run()