# web_interface.py
import gradio as gr
from ..core.sql_assistant_agent import SQLAssistantAgent
import sys
import os
import asyncio

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Initialize the agent with a dummy API key for testing
agent = SQLAssistantAgent(deepseek_api_key="dummy-key-for-testing")

# 创建一个事件循环用于异步操作
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

async def process_query_async(user_input):
    """异步处理用户查询"""
    print(">>>>>>>>>>>>>>>>>>>>>开始处理用户需求<<<<<<<<<<<<<<<<<<<<<")
    print(f"用户输入: {repr(user_input)} \n")

    # 严格的输入验证
    if user_input is None:
        result = "请输入您的数据需求描述"
        print(f"输入为None，返回结果: {result}")
        return result

    if isinstance(user_input, str) and not user_input.strip():
        result = "请输入您的数据需求描述"
        print(f"输入为空字符串，返回结果: {result}")
        return result

    try:
        print(f"调用AI助手生成SQL，输入内容: {user_input}")

        # 调用异步处理函数
        result = await agent.generate_and_review_sql(user_input)

        print(f"AI助手返回结果: {result}")

        return result
    except asyncio.CancelledError:
        print("任务已被取消")
        # 根据 SonarQube 规则 python:S7497，需要重新抛出 CancelledError
        raise
    except Exception as e:
        error_msg = f"处理过程中出现错误: {str(e)}"
        print(f"错误信息: {error_msg}")
        return error_msg

def process_query(user_input):
    """Process the user query and generate SQL (同步包装版本)"""
    # 使用我们创建的事件循环运行异步代码
    return loop.run_until_complete(process_query_async(user_input))

def stop_processing():
    """停止当前处理任务"""
    global agent

    print("收到停止请求")

    try:
        # 尝试取消agent内部的任务
        # 在同步上下文中直接调用异步方法
        future = asyncio.run_coroutine_threadsafe(agent.cancel_current_task(), loop)
        result = future.result(timeout=5)  # 等待最多5秒
        return result

    except Exception as e:
        error_msg = f"停止处理时出现错误: {str(e)}"
        print(error_msg)
        return error_msg

# 创建界面
with gr.Blocks(title="SQL智能助手") as demo:
    gr.Markdown("# SQL智能助手")
    gr.Markdown("用自然语言描述你的数据分析需求，AI会帮你生成规范的SQL代码")

    with gr.Row():
        with gr.Column():
            user_input = gr.Textbox(
                label="描述你的数据需求",
                lines=6,
                placeholder="例如：帮我统计每个渠道昨天的新增用户数"
            )
            with gr.Row():
                submit_btn = gr.Button("提交", variant="primary")
                stop_btn = gr.Button("停止", variant="stop")
                clear_btn = gr.Button("清除", variant="secondary")

        with gr.Column():
            output = gr.Markdown(label="生成的SQL及检查结果")

    # 事件处理
    submit_btn.click(
        fn=process_query,
        inputs=[user_input],
        outputs=[output]
    )

    stop_btn.click(
        fn=stop_processing,
        inputs=None,
        outputs=[output]
    )

    clear_btn.click(
        fn=lambda: ("", ""),
        inputs=None,
        outputs=[user_input, output]
    )

if __name__ == "__main__":
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False
    )