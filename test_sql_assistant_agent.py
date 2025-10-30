# 使用示例
import asyncio
import sys
import os
from sql_assistant_agent import SQLAssistantAgent
from config import setup_environment

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def main():
    # 启动MCP服务器（在实际中，这应该是一个独立的进程）
    # 这里我们假设MCP服务器已经在运行

    path_to_mcp_server = "python3 /c/Users/admin/Documents/Develop/dataAI-mcp/server.py"

    agent = SQLAssistantAgent(deepseek_api_key="dummy-key-for-testing")

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
    setup_environment()
    asyncio.run(main())