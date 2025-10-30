import requests
import json
import asyncio
import os
from typing import Optional
# Import the lint function directly from the server module
from server import lint_sql
# 导入配置
from config import config, setup_environment

class SQLAssistantAgent:
    def __init__(self, deepseek_api_key: Optional[str] = None):
        """
        初始化SQL助手智能体

        Args:
            deepseek_api_key: DeepSeek API密钥，如果为None则从环境变量读取
        """

        if not setup_environment():
            print("❌ 环境设置失败，程序退出")
            return

        # 使用配置中的值
        # self.mcp_server_path = mcp_server_path or config.mcp_server_path
        # We don't need the server path anymore since we're calling the function directly
        self.api_key = config.deepseek_api_key or os.getenv("DEEPSEEK_API_KEY")
        self.base_url = "https://api.deepseek.com/v1/chat/completions"  # DeepSeek API端点

        if not self.api_key:
            raise ValueError("DeepSeek API密钥未提供，请设置DEEPSEEK_API_KEY环境变量或传入api_key参数")

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

        生成SQL时请遵循以下约定：
        - 使用Hive SQL语法
        - 表名格式：ods_*, dwd_*, dws_*, app_*
        - 分区字段使用 dt，格式为 'yyyy-MM-dd'
        - 字段命名使用蛇形命名法（snake_case）

        请用中文与用户交流，生成的SQL代码要可以直接执行。
        """

    async def _call_deepseek_api(self, messages: list, temperature: float = 0.1) -> str:
        """
        调用DeepSeek API

        Args:
            messages: 消息列表
            temperature: 生成温度

        Returns:
            API返回的文本内容
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        payload = {
            "model": "deepseek-chat",  # 使用deepseek-chat模型
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 2048,
            "stream": False
        }

        try:
            response = requests.post(self.base_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()

            result = response.json()
            return result["choices"][0]["message"]["content"]

        except requests.exceptions.RequestException as e:
            raise Exception(f"DeepSeek API调用失败: {str(e)}")
        except KeyError as e:
            raise Exception(f"解析DeepSeek API响应失败: {str(e)}")

    async def generate_and_review_sql(self, user_request: str) -> str:
        """生成并审核SQL的核心方法"""

        # 1. 首先生成初始SQL
        print("🤖 正在理解您的需求并生成SQL...")
        initial_sql = await self._generate_initial_sql(user_request)
        if not initial_sql:
            return "抱歉，我无法理解您的需求并生成SQL。"

        print(f"📝 生成的初始SQL:\n{initial_sql}\n")

        # 2. 调用MCP服务器进行规范检查
        print("🔍 正在执行规范检查...")
        # Call the lint function directly instead of using MCP client
        lint_result = await lint_sql(initial_sql)

        # 3. 如果有问题，尝试修复
        if "符合所有规范" not in lint_result:
            print("⚠️ 发现规范问题，正在优化...")
            optimized_sql = await self._optimize_sql(initial_sql, lint_result, user_request)

            # 再次检查优化后的SQL
            if optimized_sql != initial_sql:
                # Call the lint function directly instead of using MCP client
                final_check = await lint_sql(optimized_sql)
                if "符合所有规范" in final_check:
                    result = f"✅ 已为您生成符合规范的SQL：\n```sql\n{optimized_sql}\n```\n\n💡 **优化说明**: 根据规范检查结果，我对SQL进行了优化，确保其符合大数据开发标准。"
                else:
                    result = f"🔄 已优化SQL，但仍存在一些建议：\n```sql\n{optimized_sql}\n```\n\n📋 **检查结果**:\n{final_check}"
            else:
                result = f"ℹ️ 生成的SQL有一些建议：\n```sql\n{initial_sql}\n```\n\n📋 **检查结果**:\n{lint_result}"
        else:
            result = f"✅ 生成的SQL符合所有规范：\n```sql\n{initial_sql}\n```"

        return result

    async def _generate_initial_sql(self, user_request: str) -> str:
        """调用DeepSeek API生成初始SQL"""
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": f"""
请根据以下业务需求生成Hive SQL查询：

业务需求：{user_request}

请只返回SQL代码，不要额外的解释或标记。确保SQL符合规范且可以直接执行。
"""}
        ]

        try:
            response = await self._call_deepseek_api(messages, temperature=0.1)

            # 清理响应，提取SQL代码
            sql_code = self._extract_sql_from_response(response)
            return sql_code

        except Exception as e:
            print(f"DeepSeek API调用失败: {e}")
            return ""

    async def _optimize_sql(self, original_sql: str, lint_feedback: str, user_request: str) -> str:
        """根据检查结果调用DeepSeek优化SQL"""
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content":
                f"""原始业务需求：{user_request}
                    原始SQL代码：
                ```sql
                {original_sql}
                规范检查反馈：
                {lint_feedback}

                请根据规范检查反馈优化原始SQL代码，解决所有错误和警告问题。
                请只返回优化后的SQL代码，不要额外的解释或标记。
                """}
                ]
        try:
            response = await self._call_deepseek_api(messages, temperature=0.1)
            optimized_sql = self._extract_sql_from_response(response)

            # 如果优化失败，返回原始SQL
            return optimized_sql if optimized_sql else original_sql

        except Exception as e:
            print(f"SQL优化失败: {e}")
            return original_sql

    def _extract_sql_from_response(self, response: str) -> str:
        """
        从DeepSeek响应中提取SQL代码

        Args:
            response: API返回的文本

        Returns:
            提取的SQL代码
        """
        # 清理响应文本
        cleaned_response = response.strip()

        # 如果响应中包含```sql ... ```，提取其中的内容
        if "```sql" in cleaned_response:
            start_idx = cleaned_response.find("```sql") + 6
            end_idx = cleaned_response.find("```", start_idx)
            if end_idx != -1:
                return cleaned_response[start_idx:end_idx].strip()

        # 如果响应中包含``` ... ```，提取其中的内容
        elif "```" in cleaned_response:
            start_idx = cleaned_response.find("```") + 3
            end_idx = cleaned_response.find("```", start_idx)
            if end_idx != -1:
                return cleaned_response[start_idx:end_idx].strip()

        # 直接返回清理后的响应
        return cleaned_response


    async def chat(self, message: str) -> str:
        """
        与智能体对话的简化接口

        Args:
            message: 用户消息

        Returns:
            智能体回复
        """
        if any(keyword in message.lower() for keyword in ['sql', '查询', '统计', '数据', '报表', '分析']):
            return await self.generate_and_review_sql(message)
        else:
            # 对于非SQL相关的对话，直接调用DeepSeek
            messages = [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": message}
            ]
            return await self._call_deepseek_api(messages)