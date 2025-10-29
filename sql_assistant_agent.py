import openai
from mcp import Client
import asyncio

class SQLAssistantAgent:
    def __init__(self, mcp_server_path):
        self.mcp_client = Client(mcp_server_path)
        # 设置系统提示词，定义智能体的角色和能力
        self.system_prompt = """
        你是一个专业的数据分析SQL助手，专门帮助业务分析师编写高效、规范的SQL查询。

        你的工作流程：
        1. 理解用户的业务需求
        2. 生成符合大数据开发规范的SQL代码
        3. 自动对生成的SQL进行规范检查
        4. 根据检查结果优化SQL，并向用户解释修改原因

        重要规范：
        - 禁止使用 SELECT *
        - 必须指定分区字段 dt 的过滤条件
        - 表必须使用别名
        - 字段别名使用下划线命名法
        - 注意敏感字段的访问权限

        始终用中文与用户交流。
        """

    async def generate_and_review_sql(self, user_request):
        """生成并审核SQL的核心方法"""

        # 1. 首先生成初始SQL
        initial_sql = await self._generate_initial_sql(user_request)
        if not initial_sql:
            return "抱歉，我无法理解您的需求并生成SQL。"

        print(f"📝 生成的初始SQL:\n{initial_sql}\n")

        # 2. 调用MCP服务器进行规范检查
        print("🔍 正在执行规范检查...")
        lint_result = await self.mcp_client.lint_sql(initial_sql)

        # 3. 如果有问题，尝试修复
        if "符合所有规范" not in lint_result:
            print("⚠️ 发现规范问题，正在优化...")
            optimized_sql = await self._optimize_sql(initial_sql, lint_result)

            # 再次检查优化后的SQL
            if optimized_sql != initial_sql:
                final_check = await self.mcp_client.lint_sql(optimized_sql)
                if "符合所有规范" in final_check:
                    result = f"✅ 已为您生成符合规范的SQL：\n```sql\n{optimized_sql}\n```"
                else:
                    result = f"🔄 已优化SQL，但仍存在一些建议：\n```sql\n{optimized_sql}\n```\n检查结果：{final_check}"
            else:
                result = f"ℹ️ 生成的SQL有一些建议：\n```sql\n{initial_sql}\n```\n检查结果：{lint_result}"
        else:
            result = f"✅ 生成的SQL符合所有规范：\n```sql\n{initial_sql}\n```"

        return result

    async def _generate_initial_sql(self, user_request):
        """调用LLM生成初始SQL"""
        # 这里简化实现，实际应该调用LLM API
        prompt = f"""
        根据以下业务需求，生成Hive SQL查询：

        需求：{user_request}

        请生成可以直接执行的SQL代码，只返回SQL语句，不要额外解释。
        """

        # 模拟LLM生成 - 实际环境中替换为真实的LLM调用
        if "新增用户" in user_request and "渠道" in user_request:
            return """
            SELECT
                channel_id,
                COUNT(DISTINCT user_id) as new_user_count
            FROM dwd_user_register_d
            WHERE dt = '2024-09-11'
            GROUP BY channel_id
            """
        elif "用户留存" in user_request:
            return """
            SELECT * FROM user_retention
            WHERE register_date = '2024-09-10'
            """
        else:
            # 默认返回一个简单查询用于测试规范检查
            return "SELECT * FROM my_table WHERE status = 1"

    async def _optimize_sql(self, sql, lint_feedback):
        """根据检查结果优化SQL"""
        # 这里可以调用LLM来根据lint_feedback优化SQL
        # 简化版：直接进行一些字符串替换
        optimized = sql

        if "禁止使用 SELECT *" in lint_feedback:
            # 在实际中，这里需要解析SQL并替换为具体字段
            # 这里只是示例
            optimized = optimized.replace("SELECT *", "SELECT user_id, user_name")

        if "必须包含分区字段" in lint_feedback and "dt" not in optimized.lower():
            if "WHERE" in optimized:
                optimized = optimized.replace("WHERE", "WHERE dt = '2024-09-11' AND ")
            else:
                optimized += "\nWHERE dt = '2024-09-11'"

        return optimized

# 使用示例
async def main():
    # 启动MCP服务器（在实际中，这应该是一个独立的进程）
    # 这里我们假设MCP服务器已经在运行

    agent = SQLAssistantAgent("path_to_mcp_server")

    # 测试用例
    test_requests = [
        "帮我统计每个渠道昨天的新增用户数",
        "计算用户的次日留存率",
        "查看所有用户信息"
    ]

    for request in test_requests:
        print(f"\n🎯 用户需求: {request}")
        print("=" * 50)
        result = await agent.generate_and_review_sql(request)
        print(result)
        print("=" * 50)

if __name__ == "__main__":
    asyncio.run(main())