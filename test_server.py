import asyncio
from server import SQLintterMCPServer

async def test():
    server = SQLintterMCPServer()

    # 测试有问题的SQL
    bad_sql = """
    SELECT * FROM ods_user
    WHERE status = 1
    """

    result = await server.lint_sql(bad_sql)
    print("=== 问题SQL检查结果 ===")
    print(result)
    print()

    # 测试规范的SQL
    good_sql = """
    SELECT
        u.user_id,
        u.user_name as user_name,
        COUNT(o.order_id) as total_orders
    FROM ods_user u
    JOIN ods_order o ON u.user_id = o.user_id
    WHERE u.dt = '2024-09-11'
        AND o.dt = '2024-09-11'
    GROUP BY u.user_id, u.user_name
    """

    result = await server.lint_sql(good_sql)
    print("=== 规范SQL检查结果 ===")
    print(result)

if __name__ == "__main__":
    asyncio.run(test())