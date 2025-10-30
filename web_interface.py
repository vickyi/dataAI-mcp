# web_interface.py
import gradio as gr
from sql_assistant_agent import SQLAssistantAgent
import sys
import os
import asyncio
from config import setup_environment

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


# Provide a dummy API key for testing
agent = SQLAssistantAgent(deepseek_api_key="")

async def process_query(user_input):
    result = await agent.generate_and_review_sql(user_input)
    return result

iface = gr.Interface(
    fn=process_query,
    inputs=gr.Textbox(label="描述你的数据需求", lines=3),
    outputs=gr.Markdown(label="生成的SQL及检查结果"),
    title="SQL智能助手",
    description="用自然语言描述你的数据分析需求，AI会帮你生成规范的SQL代码"
)

iface.launch(server_name="0.0.0.0", server_port=7860)