import gradio as gr
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def process_query(user_input):
    """简化版处理函数"""
    print(f"开始处理用户输入: {repr(user_input)}")
    print(f"输入类型: {type(user_input)}")

    if user_input is None:
        return "输入为None"
    elif isinstance(user_input, str) and not user_input.strip():
        return "输入为空字符串"
    else:
        return f"成功接收到输入: {user_input}"

# 创建简化版界面
with gr.Blocks(title="简化版SQL助手") as demo:
    gr.Markdown("# 简化版SQL助手")

    with gr.Row():
        with gr.Column():
            user_input = gr.Textbox(
                label="描述你的数据需求",
                lines=6,
                placeholder="例如：帮我统计每个渠道昨天的新增用户数"
            )
            submit_btn = gr.Button("提交", elem_id="submit-btn")

        with gr.Column():
            output = gr.Textbox(label="输出结果")

    submit_btn.click(
        fn=process_query,
        inputs=[user_input],
        outputs=[output]
    )

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7872)