import asyncio
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 测试server.py中的lint_sql函数
from server import lint_sql

# 测试sql_assistant_agent.py中的SQLAssistantAgent类
from sql_assistant_agent import SQLAssistantAgent

async def test_lint_sql():
    """测试lint_sql函数"""
    print("测试lint_sql函数...")

    # 测试一个简单的SQL
    test_sql = "SELECT * FROM users"
    result = await lint_sql(test_sql)
    print(f"输入SQL: {test_sql}")
    print(f"检查结果:\n{result}\n")

    # 测试一个符合规范的SQL
    test_sql2 = "SELECT user_id, username FROM dwd_users a WHERE dt = '2023-01-01'"
    result2 = await lint_sql(test_sql2)
    print(f"输入SQL: {test_sql2}")
    print(f"检查结果:\n{result2}\n")

async def test_sql_assistant_agent():
    """测试SQLAssistantAgent类"""
    print("测试SQLAssistantAgent类...")

    try:
        # 创建agent实例（使用虚拟API密钥）
        agent = SQLAssistantAgent(deepseek_api_key="test-key")
        print("✅ SQLAssistantAgent实例创建成功")

        # 测试_generate_initial_sql方法
        test_request = "帮我统计每个渠道昨天的新增用户数"
        print(f"测试请求: {test_request}")

        # 由于我们没有真正的API密钥，这里会失败，但我们测试一下流程
        try:
            result = await agent._generate_initial_sql(test_request)
            print(f"生成的SQL: {result}")
        except Exception as e:
            print(f"生成SQL时出现预期的错误: {e}")

    except Exception as e:
        print(f"测试SQLAssistantAgent时出现错误: {e}")

async def main():
    """主测试函数"""
    print("开始测试各个组件...\n")

    # 测试lint_sql函数
    await test_lint_sql()

    # 测试SQLAssistantAgent类
    await test_sql_assistant_agent()

    print("测试完成!")

if __name__ == "__main__":
    asyncio.run(main())