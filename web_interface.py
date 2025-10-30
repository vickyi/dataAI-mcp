# web_interface.py
import gradio as gr
from sql_assistant_agent import SQLAssistantAgent
import sys
import os
import asyncio

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Initialize the agent with a dummy API key for testing
agent = SQLAssistantAgent(deepseek_api_key="dummy-key-for-testing")

async def process_query(user_input):
    """Process the user query and generate SQL"""
    if not user_input or not user_input.strip():
        return "请输入您的数据需求描述"

    try:
        # Process the query
        result = await agent.generate_and_review_sql(user_input)
        return result
    except Exception as e:
        return f"处理过程中出现错误: {str(e)}"

# Create the interface using Blocks with horizontal layout
with gr.Blocks(title="SQL智能助手") as demo:
    gr.Markdown("# SQL智能助手")
    gr.Markdown("用自然语言描述你的数据分析需求，AI会帮你生成规范的SQL代码")

    # Create horizontal layout with two columns
    with gr.Row():
        # Left column for input
        with gr.Column():
            user_input = gr.Textbox(
                label="描述你的数据需求",
                lines=6,
                placeholder="例如：帮我统计每个渠道昨天的新增用户数"
            )

            with gr.Row():
                submit_btn = gr.Button("提交", variant="primary")
                clear_btn = gr.Button("清除")

            # Examples section
            gr.Examples(
                examples=[
                    "帮我统计每个渠道昨天的新增用户数",
                    "计算用户的次日留存率",
                    "查看所有用户信息"
                ],
                inputs=user_input
            )

        # Right column for output - always visible
        with gr.Column():
            output = gr.Textbox(label="生成的SQL及检查结果",
            value="请输入您的需求并点击提交按钮",
            interactive=False,
            lines=6,
            show_copy_button=True)

    # Event handlers with proper concurrency control
    submit_btn.click(
        fn=process_query,
        inputs=user_input,
        outputs=output
    )

    clear_btn.click(
        fn=lambda: ("", "请输入您的需求并点击提交按钮"),
        inputs=None,
        outputs=[user_input, output]
    )

# Enable queue with single concurrency to prevent duplicate submissions
# This ensures only one request is processed at a time
demo.queue()

if __name__ == "__main__":
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False
    )